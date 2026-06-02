import os
import json
import re
from typing import Any, Dict, List, Optional, Set, Tuple

import nltk
# nltk.download('stopwords')
# nltk.download('punkt_tab')
import requests
from neo4j import GraphDatabase
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

from langchain_core.messages import HumanMessage, SystemMessage
# from langchain_community.chat_models import ChatOllama
from langchain_ollama import OllamaLLM
from dotenv import load_dotenv
load_dotenv()

NEO4J_URI     = os.getenv("NEO4J_URI")
NEO4J_USER    = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

from neo4j import GraphDatabase

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)

_EXTRA_STOPWORDS = {
    # question words + helpers
    "who", "what", "when", "where", "why", "how", "which",
    "tell", "me", "give", "show", "find", "please",
}

def build_stopwords():
    return set(stopwords.words("english")).union(_EXTRA_STOPWORDS)

def keywords_from_question(question: str, min_len: int = 3):
    """
    Step 1:
        - tokenize (NLTK)
        - remove stopwords (NLTK)
        - de-dupe preserving order
    """
    sw = build_stopwords()
    toks = [t.lower() for t in word_tokenize(question or "")]
    toks = [t for t in toks if any(ch.isalnum() for ch in t)]
    toks = [t for t in toks if t not in sw and len(re.sub(r"[^a-z0-9]", "", t)) >= min_len]

    seen = set()
    out = []
    for t in toks:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


# =============================================================================
# EMBEDDING HELPER  (same strategy as process.py embed_texts_for_neo4j)
# =============================================================================

def embed_query(query: str) -> List[float]:
    """
    Embed the user query using the same model pool strategy as ingestion.
    Returns a flat list of floats.
    """
    from utils.models import get_model_from_pool
    import torch
    from transformers import AutoTokenizer

    model_init_method = (os.getenv("MODEL_INIT_METHOD") or "").lower()

    with get_model_from_pool() as model:
        if model_init_method == "sentencetransformer":
            emb = model.encode([query])
            return emb[0].tolist()

        elif model_init_method == "ollama":
            emb = model.embed_documents([query])
            return emb[0]

        elif model_init_method == "vllm":
            response = model.embeddings.create(
                model=os.getenv("VLLM_MODEL"),
                input=[query]
            )
            return response.data[0].embedding

        elif model_init_method == "minillm":
            emb = model.encode([query], convert_to_tensor=True)
            return emb[0].tolist()

        elif model_init_method == "transformers":
            model_dir = os.getenv("TRANSFORMERS_MODEL_DIR", r"D:\LFM model Try\LFM2-1.2B")
            tokenizer = AutoTokenizer.from_pretrained(model_dir, trust_remote_code=True)
            inputs = tokenizer([query], return_tensors="pt", truncation=True, max_length=512)
            with torch.no_grad():
                outputs = model(**inputs)
            last_hidden_state = outputs.last_hidden_state
            attention_mask = inputs["attention_mask"]
            mask_expanded = attention_mask.unsqueeze(-1).expand(last_hidden_state.size()).float()
            sum_hidden = torch.sum(last_hidden_state * mask_expanded, 1)
            sum_mask = torch.clamp(mask_expanded.sum(1), min=1e-9)
            embedding = sum_hidden / sum_mask
            return embedding[0].tolist()

        else:
            emb = model.encode([query])
            return emb[0].tolist()


# =============================================================================
# STEP 1 — DOCUMENT SELECTION VIA COSINE SIMILARITY ON relationship_summary_json
# =============================================================================

def fetch_documents_by_cosine_similarity(
    driver,
    query_embedding: List[float],
    threshold: float = 0.5,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Compute cosine similarity between the query embedding and each Document's
    relationship_summary_json_embedding entirely inside Cypher (no APOC).

    Cosine similarity formula:
        dot(a, b) / (norm(a) * norm(b))

    Only documents whose similarity >= threshold are returned.
    """
    cypher = """
    MATCH (d:Document)
    WHERE d.relationship_summary_json_embedding IS NOT NULL

    WITH d,
         d.relationship_summary_json_embedding AS doc_vec,
         $query_vec                             AS q_vec

    WITH d, doc_vec, q_vec,

         // dot product
         reduce(dot = 0.0, i IN range(0, size(q_vec) - 1) |
             dot + q_vec[i] * doc_vec[i]
         ) AS dot_product,

         // norm of query vector
         sqrt(reduce(sq = 0.0, x IN q_vec    | sq + x * x)) AS q_norm,

         // norm of document vector
         sqrt(reduce(sq = 0.0, x IN doc_vec  | sq + x * x)) AS d_norm

    WITH d,
         CASE
             WHEN q_norm = 0.0 OR d_norm = 0.0 THEN 0.0
             ELSE dot_product / (q_norm * d_norm)
         END AS similarity

    WHERE similarity >= $threshold

    RETURN
        d.fsid                          AS document_id,
        d.relationship_summary          AS relationship_summary,
        d.important_keywords            AS important_keywords,
        d.branches                      AS branches,
        similarity                      AS similarity
    ORDER BY similarity DESC
    LIMIT $limit
    """

    out: List[Dict[str, Any]] = []
    with driver.session() as session:
        for rec in session.run(
            cypher,
            query_vec=query_embedding,
            threshold=float(threshold),
            limit=int(limit),
        ):
            out.append({
                "document_id":          rec["document_id"],
                "relationship_summary": rec["relationship_summary"],
                "important_keywords":   rec["important_keywords"],
                "branches":             rec["branches"],
                "similarity":           rec["similarity"],
            })
    return out


# =============================================================================
# STEP 2 — FETCH ALL RELATIONSHIPS FOR SELECTED DOCUMENTS
#           (includes relationships that span multiple docs if ≥1 doc passes)
# =============================================================================

def fetch_relationships_for_documents(
    driver,
    document_ids: List[str],
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """
    Retrieve every relationship where AT LEAST ONE of its document_ids is in
    the set of qualifying documents.

    Relationships store their originating documents in r.document_ids (list).
    We also JOIN to justify via (Entity)-[:MENTIONED_IN]->(Document) so we can
    pull citations back correctly.
    """
    cypher = """
    MATCH (a:Entity)-[r]->(b:Entity)
    WHERE NOT type(r) = 'MENTIONED_IN'
      AND any(doc_id IN coalesce(r.document_ids, []) WHERE doc_id IN $doc_ids)

    RETURN
        elementId(r)                    AS rel_eid,
        coalesce(r.rel_uid, "")         AS rel_uid,
        type(r)                         AS rel_type,
        r                               AS rel_obj,
        r.document_ids                  AS rel_document_ids,
        a.uid                           AS a_uid,
        a.display_name                  AS a_name,
        a.entity_type                   AS a_type,
        b.uid                           AS b_uid,
        b.display_name                  AS b_name,
        b.entity_type                   AS b_type
    ORDER BY elementId(r)
    LIMIT $limit
    """

    out: List[Dict[str, Any]] = []
    with driver.session() as session:
        for rec in session.run(cypher, doc_ids=document_ids, limit=int(limit)):
            out.append({
                "rel_eid":          rec["rel_eid"],
                "rel_uid":          rec["rel_uid"] or "",
                "rel_type":         rec["rel_type"],
                "rel_props":        dict(rec["rel_obj"]),
                "rel_document_ids": rec["rel_document_ids"] or [],
                "source": {
                    "uid":          rec["a_uid"],
                    "display_name": rec["a_name"],
                    "entity_type":  rec["a_type"],
                },
                "target": {
                    "uid":          rec["b_uid"],
                    "display_name": rec["b_name"],
                    "entity_type":  rec["b_type"],
                },
                "score": 0,   # will be filled by cosine re-rank below
            })
    return out


# =============================================================================
# STEP 3 — RE-RANK RELATIONSHIPS BY COSINE SIMILARITY ON justification_embedding
# =============================================================================

def rerank_relationships_by_cosine(
    relationships: List[Dict[str, Any]],
    query_embedding: List[float],
) -> List[Dict[str, Any]]:
    """
    For each relationship that carries a justification_embedding, compute cosine
    similarity with the query and attach it as the `score`.
    Relationships without an embedding keep score = 0.

    All computation is done in Python so we avoid round-trips to Neo4j for
    each individual relationship.
    """
    import math

    def _cosine(a: List[float], b: List[float]) -> float:
        dot  = sum(x * y for x, y in zip(a, b))
        na   = math.sqrt(sum(x * x for x in a))
        nb   = math.sqrt(sum(x * x for x in b))
        if na == 0.0 or nb == 0.0:
            return 0.0
        return dot / (na * nb)

    q = query_embedding

    for rel in relationships:
        props = rel.get("rel_props") or {}
        j_emb = props.get("justification_embedding")
        if j_emb and isinstance(j_emb, list) and len(j_emb) == len(q):
            rel["score"] = _cosine(q, j_emb)
        else:
            rel["score"] = 0.0

    relationships.sort(key=lambda r: r.get("score", 0.0), reverse=True)
    return relationships


# =============================================================================
# PROVENANCE HELPERS  (unchanged from original)
# =============================================================================

def fetch_nodes_provenance(driver, uids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """
    For each entity uid, return the list of documents and chunk_ids where it was mentioned.
    Uses: (Entity)-[:MENTIONED_IN {chunk_ids}]->(Document)
    """
    cypher = """
    UNWIND $uids AS uid
    MATCH (e:Entity {uid: uid})-[m:MENTIONED_IN]->(d:Document)
    RETURN uid AS uid, d.fsid AS document_id, m.chunk_ids AS chunk_ids
    ORDER BY uid, document_id
    """

    out: Dict[str, List[Dict[str, Any]]] = {u: [] for u in uids}
    if not uids:
        return out

    with driver.session() as session:
        for rec in session.run(cypher, uids=uids):
            out.setdefault(rec["uid"], []).append(
                {"document_id": rec["document_id"], "chunk_ids": rec["chunk_ids"]}
            )
    return out


def fetch_edge_provenance(driver, rel_uid: str, rel_eid: str) -> List[Dict[str, Any]]:
    """
    Relationship-level doc+chunk inference.
    Intersects r.chunk_ids with endpoint MENTIONED_IN chunk_ids per Document.
    """
    cypher = """
    MATCH (a:Entity)-[r]->(b:Entity)
    WHERE
        ($rel_uid <> "" AND r.rel_uid = $rel_uid)
        OR
        ($rel_eid <> "" AND elementId(r) = $rel_eid)
    OPTIONAL MATCH (a)-[ma:MENTIONED_IN]->(d:Document)<-[mb:MENTIONED_IN]-(b)
    WITH d, ma, mb, r,
        [x IN coalesce(r.chunk_ids, [])
            WHERE x IN coalesce(ma.chunk_ids, []) AND x IN coalesce(mb.chunk_ids, [])] AS common_chunks
    WHERE d IS NOT NULL AND size(common_chunks) > 0
    RETURN d.fsid AS document_id, common_chunks AS chunk_ids
    ORDER BY size(common_chunks) DESC, d.fsid
    """
    with driver.session() as session:
        rows = session.run(cypher, rel_uid=rel_uid or "", rel_eid=rel_eid or "")
        return [{"document_id": r["document_id"], "chunk_ids": r["chunk_ids"]} for r in rows]


# =============================================================================
# EVIDENCE BUILDING  (unchanged from original)
# =============================================================================

_WORD_RE = re.compile(r"[a-z0-9]+")

def tokenize(s: str) -> List[str]:
    return _WORD_RE.findall((s or "").lower())

def rel_text(rel: Dict[str, Any]) -> str:
    props = rel.get("rel_props") or {}
    return (
        props.get("description")
        or props.get("justification")
        or f"{(rel.get('source') or {}).get('display_name','')} {rel.get('rel_type','')} {(rel.get('target') or {}).get('display_name','')}"
    )

def overlap_score(question: str, rel: Dict[str, Any]) -> int:
    q = set(tokenize(question))
    r = set(tokenize(rel_text(rel))) | set(tokenize(rel.get("rel_type", "")))
    r |= set(tokenize((rel.get("source") or {}).get("display_name", "")))
    r |= set(tokenize((rel.get("target") or {}).get("display_name", "")))
    return len(q & r)

def best_citation_for_relationship(
    rel: Dict[str, Any],
    edge_prov: Dict[str, Any],
    node_prov: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Return exactly one citation:
      - Edge provenance if available
      - Else source node provenance
      - Else target node provenance
      - Else []
    """
    rid = rel.get("rel_uid") or rel.get("rel_eid")

    # 1) Edge provenance (preferred)
    prov_list = edge_prov.get(rid) or []
    if prov_list:
        p0 = prov_list[0] or {}
        doc = p0.get("document_id")
        chunks = p0.get("chunk_ids") or []
        if doc and chunks:
            return [{"document_id": doc, "chunk_id": chunks[0]}]

    # 2) Node provenance fallback (source then target)
    src_uid = (rel.get("source") or {}).get("uid")
    tgt_uid = (rel.get("target") or {}).get("uid")

    for uid in (src_uid, tgt_uid):
        if not uid:
            continue
        nprov = node_prov.get(uid) or []
        if not nprov:
            continue
        p0 = nprov[0] or {}
        doc = p0.get("document_id")
        chunks = p0.get("chunk_ids") or []
        if doc and chunks:
            return [{"document_id": doc, "chunk_id": chunks[0]}]

    return []

def rank_key(question: str, rel: Dict[str, Any]) -> Tuple[float, int, int]:
    # Primary: cosine score; Secondary: lexical overlap; Tertiary: has date
    score   = float(rel.get("score") or 0.0)
    overlap = overlap_score(question, rel)
    date    = (rel.get("rel_props") or {}).get("date")
    has_date = 1 if isinstance(date, str) and len(date) >= 8 else 0
    return (score, overlap, has_date)

def build_relationship_only_evidence(
    question: str,
    keywords: Optional[List[str]],
    seed_rels: List[Dict[str, Any]],
    edge_prov: Dict[str, Any],
    node_prov: Dict[str, Any],
    top_k: int = 50,
) -> Dict[str, Any]:
    keywords = keywords or []

    rels = list(seed_rels or [])
    rels.sort(key=lambda r: rank_key(question, r), reverse=True)
    rels = rels[:top_k]

    out_rels = []
    for r in rels:
        out_rels.append({
            "text": rel_text(r),
            "citations": best_citation_for_relationship(r, edge_prov=edge_prov, node_prov=node_prov),
        })

    return {
        "question":          question,
        "keywords":          keywords,
        "seed_relationships": out_rels,
    }


def format_evidence(data):
    formatted_output = []
    for item in data['seed_relationships']:
        text = item['text']
        for citation in item['citations']:
            document_id  = citation['document_id']
            chunk_id     = citation['chunk_id']
            justification = text
            formatted_output.append(
                f"Document_id = {document_id}\nChunk_id = {chunk_id}\nJustification = {justification}\n"
            )
    return "\n".join(formatted_output)

def format_evidence_multi_query(data):
    formatted_output = []
    for item in data:
        rel_type          = item['rel_type']
        description       = item['rel_props'].get('justification') or item['rel_props'].get('description', '')
        source_name       = item['source']['display_name']
        source_entity_type = item['source']['entity_type']
        target_name       = item['target']['display_name']
        target_entity_type = item['target']['entity_type']
        formatted_output.append(
            f"rel_type = {rel_type}\ndescription = {description}\n"
            f"source_name = {source_name}\nsource_entity_type = {source_entity_type}\n"
            f"target_name = {target_name}\ntarget_entity_type = {target_entity_type}\n"
        )
    return "\n".join(formatted_output)


# =============================================================================
# JSON / THINK-TAG HELPERS  (unchanged)
# =============================================================================

def extract_last_json(text):
    json_pattern = r'\{.*?\}'
    matches = re.findall(json_pattern, text, re.DOTALL)
    if matches:
        last_json_str = matches[-1]
        try:
            return json.loads(last_json_str)
        except json.JSONDecodeError:
            return None
    return None

def remove_think_tags_output(response):
    response = re.sub(r".*?</think>", "", response, flags=re.DOTALL).strip()
    start    = response.index('{')
    decoder  = json.JSONDecoder()
    obj, _   = decoder.raw_decode(response, start)
    return obj


# =============================================================================
# MAIN CALLING FUNCTION  — replaces the old keyword-only callingFunction
# =============================================================================

def callingFunction(
    question: str,
    doc_similarity_threshold: float = None,
    max_docs: int = 50,
    max_rels: int = 500,
    top_k_evidence: int = 50,
):
    """
    New retrieval pipeline:

    1. Embed the query.
    2. Cosine similarity against Document.relationship_summary_json_embedding
       → select qualifying documents (above threshold).
    3. Fetch ALL relationships whose r.document_ids overlaps the selected docs.
       (Relationships spanning multiple docs are included if even 1 doc qualifies.)
    4. Re-rank relationships by cosine similarity of r.justification_embedding
       against the query.
    5. Build evidence and call LLM for final summarisation.
    """

    # Threshold: env var > caller arg > default 0.5
    threshold = float(
        os.getenv("DOC_SIMILARITY_THRESHOLD")
        or (doc_similarity_threshold if doc_similarity_threshold is not None else 0.5)
    )

    # ── keywords (still useful for multi-query generation) ──────────────────
    kw = keywords_from_question(question)

    # ── Step 1: embed query ──────────────────────────────────────────────────
    print(f"[Retrieval] Embedding query ...")
    query_embedding = embed_query(question)

    # ── Step 2: document selection via cosine on summary embedding ───────────
    print(f"[Retrieval] Selecting documents (threshold={threshold}) ...")
    qualifying_docs = fetch_documents_by_cosine_similarity(
        driver,
        query_embedding=query_embedding,
        threshold=threshold,
        limit=max_docs,
    )

    if not qualifying_docs:
        print("[Retrieval] No documents crossed the similarity threshold.")
        return {
            "summary": "No relevant documents found for the given query.",
            "sources": [],
            "queries": [],
        }

    document_ids = [d["document_id"] for d in qualifying_docs]
    print(f"[Retrieval] {len(document_ids)} qualifying document(s): {document_ids}")

    # ── Step 3: fetch relationships for those documents ──────────────────────
    print("[Retrieval] Fetching relationships for qualifying documents ...")
    seed_rels = fetch_relationships_for_documents(
        driver,
        document_ids=document_ids,
        limit=max_rels,
    )
    print(f"[Retrieval] {len(seed_rels)} relationship(s) fetched.")

    if not seed_rels:
        print("[Retrieval] No relationships found for qualifying documents.")
        return {
            "summary": "Relevant documents were found but contained no extractable relationships.",
            "sources": [],
            "queries": [],
        }

    # ── Step 4: re-rank relationships by cosine on justification_embedding ───
    print("[Retrieval] Re-ranking relationships by cosine similarity ...")
    seed_rels = rerank_relationships_by_cosine(seed_rels, query_embedding)

    # ── Step 5: provenance ───────────────────────────────────────────────────
    uids: Set[str] = set()
    for r in seed_rels:
        uids.add(r["source"]["uid"])
        uids.add(r["target"]["uid"])
    uids_list = sorted(uids)

    node_prov = fetch_nodes_provenance(driver, uids_list)

    edge_prov: Dict[str, List[Dict[str, Any]]] = {}
    for r in seed_rels:
        rid = r["rel_uid"] or r["rel_eid"]
        edge_prov[rid] = fetch_edge_provenance(
            driver, r.get("rel_uid", ""), r.get("rel_eid", "")
        )

    # ── Step 6: build evidence & call LLM ────────────────────────────────────
    evidence      = build_relationship_only_evidence(
        question, kw, seed_rels, edge_prov, node_prov, top_k=top_k_evidence
    )
    evidence_text = format_evidence(evidence)

    llm_host  = os.getenv("LLM_HOST")
    llm_port  = os.getenv("LLM_PORT")
    model     = os.getenv("JSON_MODEL")
    num_ctx   = int(os.getenv("LLM_NUM_CTX"))
    llm       = OllamaLLM(
        base_url=f"http://{llm_host}:{llm_port}",
        model=model,
        num_ctx=num_ctx,
        keep_alive="5m",
        temperature=0,
    )

    multi_query_structure = format_evidence_multi_query(seed_rels)

    if os.getenv("LLM_MODE") == "vllm":
        multiple_queries = extract_multiple_queries_vllm(multi_query_structure, question, llm)
        result           = summarizationSystem_vllm(question, evidence_text, llm)
    else:
        multiple_queries = extract_multiple_queries(multi_query_structure, question, llm)
        result           = summarizationSystem(question, evidence_text, llm)

    result["queries"] = multiple_queries
    return result


# =============================================================================
# LLM SUMMARISATION  (unchanged from original)
# =============================================================================

def summarizationSystem(question, evidence_text, llm):
    system = (
        "You are a summarization system.\n"
        "Use ONLY the provided bullet EVIDENCE to write the summary.\n"
        "Do NOT invent facts or add details not present in the bullets.\n"
        "Preserve meaning; you may merge/reorder bullets for clarity.\n"
    )
    human = (
        f"QUESTION:\n{question}\n\n"
        "EVIDENCE (bullets with citations):\n"
        f"{evidence_text}\n\n"
        "TASK:\n"
        "1) Write a very detailed but comprehensive narrative summary of the events described in the EVIDENCE as per the user question.\n"
        "   - Strictly combine only those bullets in summary that are related with QUESTION.\n"
        "   - Keep the key actions, escalation, and notable claims.\n"
        "   - Do NOT repeat every bullet verbatim.\n"
        "2) Citations handling:\n"
        "   - Do NOT sprinkle citations throughout the summary text.\n"
        "   - Extract each unique (document_id, chunk_id) pair that appears in EVIDENCE.\n"
        "   - Sort them by document_id, then chunk_id.\n"
        "   - Document id and chunk id should be STRICTLY taken from the provided text. DO not put any random text inside them."
        "   - If any bullet has '(no citation in evidence)', ignore it.\n"
        "3) Output format (STRICT):\n"
        "   - Output MUST be valid JSON only. No markdown, no surrounding text.\n"
        "   - Strictly, JSON schema MUST be exactly:\n"
        "     {\n"
        "       \"summary\": \"<string narrative summary>\",\n"
        "       \"sources\": [\n"
        "         {\"document_id\": \"...\", \"chunk_id\": <int>},\n"
        "         {\"document_id\": \"...\", \"chunk_id\": <int>}\n"
        "       ]\n"
        "     }\n"
    )

    messages = [
        SystemMessage(content=system),
        HumanMessage(content=human),
    ]

    result = llm.invoke(messages)
    result = remove_think_tags_output(result)
    return result


def summarizationSystem_vllm(question, evidence_text, llm):
    model = os.getenv("VLLM_LLM_MODEL")

    system = (
        "You are a summarization system.\n"
        "Use ONLY the provided bullet EVIDENCE to write the summary.\n"
        "Do NOT invent facts or add details not present in the bullets.\n"
        "Preserve meaning; you may merge/reorder bullets for clarity.\n"
    )
    human = (
        f"QUESTION:\n{question}\n\n"
        "EVIDENCE (bullets with citations):\n"
        f"{evidence_text}\n\n"
        "TASK:\n"
        "1) Write a very detailed but comprehensive narrative summary of the events described in the EVIDENCE as per the user question.\n"
        "   - Strictly combine only those bullets in summary that are related with QUESTION.\n"
        "   - Keep the key actions, escalation, and notable claims.\n"
        "   - Do NOT repeat every bullet verbatim.\n"
        "2) Citations handling:\n"
        "   - Do NOT sprinkle citations throughout the summary text.\n"
        "   - Extract each unique (document_id, chunk_id) pair that appears in EVIDENCE.\n"
        "   - Sort them by document_id, then chunk_id.\n"
        "   - Document id and chunk id should be STRICTLY taken from the provided text. DO not put any random text inside them."
        "   - If any bullet has '(no citation in evidence)', ignore it.\n"
        "3) Output format (STRICT):\n"
        "   - Output MUST be valid JSON only. No markdown, no surrounding text.\n"
        "   - Strictly, JSON schema MUST be exactly:\n"
        "     {\n"
        "       \"summary\": \"<string narrative summary>\",\n"
        "       \"sources\": [\n"
        "         {\"document_id\": \"...\", \"chunk_id\": <int>},\n"
        "         {\"document_id\": \"...\", \"chunk_id\": <int>}\n"
        "       ]\n"
        "     }\n"
    )

    payload = {
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": human},
        ],
        "enable_thinking": False,
    }

    response = requests.post(
        "http://172.22.10.189:8005/v1/chat/completions",
        json=payload,
    )
    result = response.json()
    result = result["choices"][0]["message"]["content"]
    result = remove_think_tags_output(result)
    return result


# =============================================================================
# MULTI-QUERY GENERATION  (unchanged from original)
# =============================================================================

def extract_multiple_queries(multi_query_structure, question, llm):
    system = (
        f"""You are an expert analytical query generator.
        Your role is to generate focused, self-contained queries based on a user question and provided content chunks.

        STRICT RULES YOU MUST FOLLOW:

        1. RELEVANCE CHECK (do this first):
        - Determine if the user question is related to the provided content.
        - If the question has NO meaningful connection to the content, return exactly:
        {{"queries": []}}
        - Do NOT proceed further if the content is irrelevant.

        2. Only consider parts of the content directly relevant to the user question.

        3. Do NOT introduce any new entities, organizations, locations, events, or dates 
        that are not present in the relevant content.

        4. Every query must be fully self-contained:
        - It must include enough context (entities, subject matter, timeframe, etc if applicable) so that it makes complete sense WITHOUT reading the other queries or the original question.
        - Strictly do not use any third-person references, including pronouns or phrases that refer to entities outside the query.
        - Each query should read like a standalone search or research question with proper context.

        5. The 3 queries must collectively cover the overall topic from different angles:
        - Entity-focused: who or what is involved
        - Relationship/action-focused: how things interact or what happened
        - Impact/outcome-focused: consequences, results, or significance

        6. Do NOT repeat or paraphrase the original question directly.

        7. Do NOT explain your reasoning. Output only the JSON."""
    )

    human = f"""
       You are given content chunks and a user question.

       INPUT FORMAT:

        CONTENT CHUNKS:
        {multi_query_structure}

        USER QUESTION:
        {question}

        ---

        TASK:
        1. First, check if the user question is relevant to the content. If not, return {{"queries": []}}.
        2. If relevant, generate exactly 3 self-contained queries that:
        - Each stands alone with full context embedded in the query itself
        - Cover different angles of the topic (entity, relationship/action, impact/outcome, etc.)
        - Are answerable strictly using the provided content

        ---

        OUTPUT FORMAT:
        Return output in exactly this JSON format:
        {{
        "queries": [
            "<fully self-contained query 1>",
            "<fully self-contained query 2>",
            "<fully self-contained query 3>"
        ]
        }}
    """

    messages = [
        SystemMessage(content=system),
        HumanMessage(content=human),
    ]

    result = llm.invoke(messages)
    result = remove_think_tags_output(result)
    return result["queries"]


def extract_multiple_queries_vllm(multi_query_structure, question, llm):
    model = os.getenv("VLLM_LLM_MODEL")

    system = (
        f"""You are an expert analytical query generator.
        Your role is to generate focused, self-contained queries based on a user question and provided content chunks.

        STRICT RULES YOU MUST FOLLOW:

        1. RELEVANCE CHECK (do this first):
        - Determine if the user question is related to the provided content.
        - If the question has NO meaningful connection to the content, return exactly:
        {{"queries": []}}
        - Do NOT proceed further if the content is irrelevant.

        2. Only consider parts of the content directly relevant to the user question.

        3. Do NOT introduce any new entities, organizations, locations, events, or dates 
        that are not present in the relevant content.

        4. Every query must be fully self-contained:
        - It must include enough context (entities, subject matter, timeframe, etc if applicable) so that it makes complete sense WITHOUT reading the other queries or the original question.
        - Strictly do not use any third-person references, including pronouns or phrases that refer to entities outside the query.
        - Each query should read like a standalone search or research question with proper context.

        5. The 3 queries must collectively cover the overall topic from different angles:
        - Entity-focused: who or what is involved
        - Relationship/action-focused: how things interact or what happened
        - Impact/outcome-focused: consequences, results, or significance

        6. Do NOT repeat or paraphrase the original question directly.

        7. Do NOT explain your reasoning. Output only the JSON."""
    )

    human = """
       You are given content chunks and a user question.

       INPUT FORMAT:

        CONTENT CHUNKS:
        {multi_query_structure}

        USER QUESTION:
        {question}

        ---

        TASK:
        1. First, check if the user question is relevant to the content. If not, return {{"queries": []}}.
        2. If relevant, generate exactly 3 self-contained queries that:
        - Each stands alone with full context embedded in the query itself
        - Cover different angles of the topic (entity, relationship/action, impact/outcome, etc.)
        - Are answerable strictly using the provided content

        ---

        OUTPUT FORMAT:
        Return output in exactly this JSON format:
        {{
        "queries": [
            "<fully self-contained query 1>",
            "<fully self-contained query 2>",
            "<fully self-contained query 3>"
        ]
        }}
    """

    prompt_chunk = human.format(
        multi_query_structure=multi_query_structure,
        question=question,
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": prompt_chunk},
        ],
        "enable_thinking": False,
    }

    response = requests.post(
        "http://172.22.10.189:8005/v1/chat/completions",
        json=payload,
    )
    result = response.json()
    result = result["choices"][0]["message"]["content"]
    result = remove_think_tags_output(result)
    return result["queries"]
