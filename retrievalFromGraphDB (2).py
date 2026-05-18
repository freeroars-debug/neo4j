import os
import json
import re
from typing import Any, Dict, List, Optional, Set

import nltk
# nltk.download('stopwords')
# nltk.download('punkt_tab')
import numpy as np
import requests
from neo4j import GraphDatabase
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

from langchain_core.messages import HumanMessage, SystemMessage
# from langchain_community.chat_models import ChatOllama
from langchain_ollama import OllamaLLM
from dotenv import load_dotenv
load_dotenv()


# ---------------------------------------------------------------------------
# Embedding helpers
# ---------------------------------------------------------------------------

def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Cosine similarity between two equal-length vectors."""
    va = np.array(a, dtype=np.float32)
    vb = np.array(b, dtype=np.float32)
    denom = np.linalg.norm(va) * np.linalg.norm(vb)
    if denom == 0.0:
        return 0.0
    return float(np.dot(va, vb) / denom)


def parse_embedding(raw) -> Optional[List[float]]:
    """
    Parse the justification_embedding stored in Neo4j.
    The field can be:
      - already a list of floats (Neo4j native float array)
      - a comma-separated string  "0.017, -0.031, ..."
    Returns None if the value is missing or unparseable.
    """
    if raw is None:
        return None
    if isinstance(raw, (list, tuple)):
        try:
            return [float(v) for v in raw]
        except (TypeError, ValueError):
            return None
    if isinstance(raw, str):
        try:
            return [float(v.strip()) for v in raw.split(",") if v.strip()]
        except ValueError:
            return None
    return None


def _build_query_embedding(
    question: str,
    keywords: List[str],
    embedding_model=None,
) -> Optional[List[float]]:
    """
    Embed the user question (+ keywords) using the project's already-initialised
    embedding_model — the same object created by vector() / vector_neo() in
    vector.py (OllamaEmbeddings or SentenceTransformer, depending on
    MODEL_INIT_METHOD).

    The model is passed in explicitly so this module never needs to spin up its
    own embedding backend.

    Falls back gracefully to None if embedding_model is not provided or fails,
    which will cause the callers to use their lexical-overlap fallback.
    """
    if embedding_model is None:
        return None

    combined = question.strip()
    if keywords:
        combined += " " + " ".join(keywords)

    try:
        # LangChain-style embeddings (OllamaEmbeddings, HuggingFaceEmbeddings …)
        if hasattr(embedding_model, "embed_query"):
            return list(embedding_model.embed_query(combined))

        # SentenceTransformer / raw encode() interface
        if hasattr(embedding_model, "encode"):
            vec = embedding_model.encode(combined, convert_to_numpy=True)
            return vec.tolist()

        # Fallback: try calling the model directly (some wrappers are callable)
        result = embedding_model(combined)
        return list(result)

    except Exception as exc:
        print(f"[_build_query_embedding] WARNING: embedding failed – {exc}")
        return None

NEO4J_URI= os.getenv("NEO4J_URI")
NEO4J_USER= os.getenv("NEO4J_USER")
NEO4J_PASSWORD= os.getenv("NEO4J_PASSWORD")

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

def fetch_seed_nodes(
    driver,
    keywords: List[str],
    limit: int = 300,
):
    cypher = """
    MATCH (n:Entity)
    WITH n, $kw AS kw
    WITH n,
        reduce(score = 0, k IN kw |
            score +
           // weight display_name higher
            CASE WHEN toLower(coalesce(n.display_name, "")) CONTAINS k THEN 3 ELSE 0 END +
           // uid match is usually useful but less semantic
            CASE WHEN toLower(coalesce(n.uid, ""))          CONTAINS k THEN 2 ELSE 0 END +
           // description match is weaker signal
            CASE WHEN toLower(coalesce(n.description, ""))  CONTAINS k THEN 1 ELSE 0 END +
           // aliases: medium signal
            CASE WHEN n.aliases IS NOT NULL
                AND any(a IN n.aliases WHERE toLower(a) CONTAINS k)
                THEN 2 ELSE 0 END
        ) AS score
    WHERE score > 0
    RETURN
        elementId(n) AS node_eid,
        coalesce(n.uid, "") AS uid,
        n.display_name AS display_name,
        n.entity_type AS entity_type,
        n AS node_obj,
        score AS score
    ORDER BY score DESC
    LIMIT $limit
    """
    kw = [k.lower() for k in (keywords or [])]

    out: List[Dict[str, Any]] = []
    with driver.session() as session:
        for rec in session.run(cypher, kw=kw, limit=int(limit)):
            out.append(
                {
                    "node_eid": rec["node_eid"],
                    "uid": rec["uid"] or "",
                    "display_name": rec["display_name"],
                    "entity_type": rec["entity_type"],
                    "node_props": dict(rec["node_obj"]),
                    "score": rec["score"],
                }
            )
    return out



def fetch_relationships_for_nodes(
    driver,
    node_eids: List[str],
    keywords: List[str],
    question: str = "",
    embedding_model=None,
    limit: int = 300,
    similarity_threshold: float = 0.0,
) -> List[Dict[str, Any]]:
    """
    Fetch all relationships adjacent to the given seed nodes, then rank them
    by cosine similarity between the stored ``justification_embedding`` and an
    embedding of the user question + keywords.

    Falls back to a normalised lexical overlap score when:
      - the embedding service is unavailable, OR
      - a relationship has no ``justification_embedding`` stored.

    Parameters
    ----------
    node_eids            : element-IDs of seed Entity nodes
    keywords             : extracted keywords from the question
    question             : raw user question (used to build the query embedding)
    limit                : maximum relationships to return after ranking
    similarity_threshold : minimum cosine score to keep (0.0 = keep all)
    """
    # -- 1. Pull ALL adjacent relationships with their stored embeddings ------
    cypher = """
    MATCH (a:Entity)-[r]->(b:Entity)
    WHERE elementId(a) IN $node_eids OR elementId(b) IN $node_eids
    RETURN
        elementId(r)                    AS rel_eid,
        coalesce(r.rel_uid, "")         AS rel_uid,
        type(r)                         AS rel_type,
        r                               AS rel_obj,
        r.justification_embedding       AS justification_embedding,
        a.uid AS a_uid, a.display_name AS a_name, a.entity_type AS a_type,
        b.uid AS b_uid, b.display_name AS b_name, b.entity_type AS b_type
    """
    node_eids = node_eids or []

    raw_rows: List[Dict[str, Any]] = []
    with driver.session() as session:
        for rec in session.run(cypher, node_eids=node_eids):
            raw_rows.append({
                "rel_eid":        rec["rel_eid"],
                "rel_uid":        rec["rel_uid"] or "",
                "rel_type":       rec["rel_type"],
                "rel_props":      dict(rec["rel_obj"]),
                "source":         {"uid": rec["a_uid"], "display_name": rec["a_name"], "entity_type": rec["a_type"]},
                "target":         {"uid": rec["b_uid"], "display_name": rec["b_name"], "entity_type": rec["b_type"]},
                "_raw_embedding": rec["justification_embedding"],
            })

    if not raw_rows:
        return []

    # -- 2. Build query embedding using the project's embedding_model ----------
    query_vec = _build_query_embedding(
        question or " ".join(keywords), keywords, embedding_model
    )

    # -- 3. Score each relationship by cosine similarity ---------------------
    out: List[Dict[str, Any]] = []
    for row in raw_rows:
        rel_vec = parse_embedding(row.pop("_raw_embedding", None))

        if query_vec and rel_vec and len(query_vec) == len(rel_vec):
            score = cosine_similarity(query_vec, rel_vec)
        else:
            # Fallback: normalised lexical overlap (mirrors old CONTAINS logic)
            kw = [k.lower() for k in (keywords or [])]
            rt   = (row["rel_type"] or "").lower()
            just = (row["rel_props"].get("justification") or "").lower()
            desc = (row["rel_props"].get("description")   or "").lower()
            hits = sum(1 for k in kw if k in rt or k in just or k in desc)
            score = hits / max(len(kw), 1)

        if score < similarity_threshold:
            continue
        row["score"] = score
        out.append(row)

    # -- 4. Sort descending and return top-k ---------------------------------
    out.sort(key=lambda x: x["score"], reverse=True)
    return out[:limit]
def fetch_seed_relationships(
    driver,
    keywords: List[str],
    question: str = "",
    embedding_model=None,
    limit: int = 300,
    similarity_threshold: float = 0.0,
):
    """
    Fetch the most semantically relevant relationships from the graph by
    computing dot-product similarity (≈ cosine, assuming unit-normalised
    embeddings) entirely inside Neo4j via a Cypher reduce().

    Only relationships that have a stored justification_embedding are
    considered. The dot-product filter + ORDER BY + LIMIT all execute in
    Neo4j — nothing is pulled to Python until it is already ranked and
    trimmed to `limit` rows.

    Falls back to a lexical keyword-overlap query when embedding_model is
    unavailable (e.g. cold-start / test environments).

    Parameters
    ----------
    keywords             : extracted keywords from the question
    question             : raw user question (used to build the query embedding)
    embedding_model      : the project's already-initialised embedding model
                           (OllamaEmbeddings or SentenceTransformer from vector.py)
    limit                : maximum relationships to return
    similarity_threshold : minimum dot-product score to keep (0.0 = keep all
                           above zero; raise to e.g. 0.3 to filter weak matches)
    """
    # -- 1. Build query vector in Python using the project's embedding model --
    query_vec = _build_query_embedding(
        question or " ".join(keywords), keywords, embedding_model
    )

    # -- 2a. Embedding path: dot-product computed fully inside Neo4j ----------
    if query_vec is not None:
        cypher = """
        MATCH (a:Entity)-[r]->(b:Entity)
        WHERE r.justification_embedding IS NOT NULL
        WITH a, r, b,
             vector.similarity.cosine(r.justification_embedding, $query_vector) AS score
        WHERE score >= $threshold
        RETURN
            elementId(r)            AS rel_eid,
            coalesce(r.rel_uid, "") AS rel_uid,
            type(r)                 AS rel_type,
            r                       AS rel_obj,
            score                   AS score,
            a.uid AS a_uid, a.display_name AS a_name, a.entity_type AS a_type,
            b.uid AS b_uid, b.display_name AS b_name, b.entity_type AS b_type
        ORDER BY score DESC
        LIMIT $limit
        """
        out: List[Dict[str, Any]] = []
        with driver.session() as session:
            for rec in session.run(
                cypher,
                query_vector=query_vec,
                threshold=float(similarity_threshold),
                limit=int(limit),
            ):
                out.append({
                    "rel_eid":   rec["rel_eid"],
                    "rel_uid":   rec["rel_uid"] or "",
                    "rel_type":  rec["rel_type"],
                    "rel_props": dict(rec["rel_obj"]),
                    "score":     rec["score"],
                    "source":    {"uid": rec["a_uid"], "display_name": rec["a_name"], "entity_type": rec["a_type"]},
                    "target":    {"uid": rec["b_uid"], "display_name": rec["b_name"], "entity_type": rec["b_type"]},
                })
        return out

    # -- 2b. Fallback: lexical keyword overlap (no embedding available) -------
    print("[fetch_seed_relationships] WARNING: embedding_model unavailable, "
          "falling back to lexical keyword overlap.")
    kw_lower = [k.lower() for k in (keywords or [])]
    if not kw_lower:
        return []

    cypher_fallback = """
    MATCH (a:Entity)-[r]->(b:Entity)
    WITH a, r, b,
         split(toLower(type(r)), "_") AS rt_parts,
         $kw AS kw
    WITH a, r, b,
         reduce(score = 0, k IN kw |
             score +
             CASE WHEN k IN rt_parts                                              THEN 2 ELSE 0 END +
             CASE WHEN r.justification IS NOT NULL AND toLower(r.justification) CONTAINS k THEN 1 ELSE 0 END +
             CASE WHEN r.description   IS NOT NULL AND toLower(r.description)   CONTAINS k THEN 1 ELSE 0 END
         ) AS raw_score
    WHERE raw_score > 0
    RETURN
        elementId(r)            AS rel_eid,
        coalesce(r.rel_uid, "") AS rel_uid,
        type(r)                 AS rel_type,
        r                       AS rel_obj,
        toFloat(raw_score) / toFloat(size($kw) * 4) AS score,
        a.uid AS a_uid, a.display_name AS a_name, a.entity_type AS a_type,
        b.uid AS b_uid, b.display_name AS b_name, b.entity_type AS b_type
    ORDER BY score DESC
    LIMIT $limit
    """
    out_fallback: List[Dict[str, Any]] = []
    with driver.session() as session:
        for rec in session.run(cypher_fallback, kw=kw_lower, limit=int(limit)):
            out_fallback.append({
                "rel_eid":   rec["rel_eid"],
                "rel_uid":   rec["rel_uid"] or "",
                "rel_type":  rec["rel_type"],
                "rel_props": dict(rec["rel_obj"]),
                "score":     rec["score"],
                "source":    {"uid": rec["a_uid"], "display_name": rec["a_name"], "entity_type": rec["a_type"]},
                "target":    {"uid": rec["b_uid"], "display_name": rec["b_name"], "entity_type": rec["b_type"]},
            })
    return out_fallback

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
    Relationship-level doc+chunk inference:

    We intersect:
        - r.chunk_ids
        - endpoint mentions chunk_ids for a & b in the same Document

    This yields (document_id -> chunk_ids) where both endpoints are present and
    the relationship was seen in those chunks.

    Note: If your chunk_ids are only stored on r and not aligned with MENTIONED_IN,
    you may need to revise the logic (e.g., store document_id on r, or use doc_chunk_refs).
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
    

    
def build_llm_payload(
    question: str,
    keywords: List[str],
    seed_rels: List[Dict[str, Any]],
    node_prov: Dict[str, List[Dict[str, Any]]],
    edge_prov: Dict[str, List[Dict[str, Any]]],
):
    return {
        "user question": question,
        "keywords": keywords,
        "seed_relationships": seed_rels,
        "node_provenance": node_prov,
        "edge_provenance": edge_prov,
    }


import re
from typing import Any, Dict, List, Optional, Tuple

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

def rank_key(question: str, rel: Dict[str, Any]) -> Tuple[int, int, int]:
    # Primary: graph score; Secondary: lexical overlap; Tertiary: has date
    score = int(rel.get("score") or 0)
    overlap = overlap_score(question, rel)
    date = (rel.get("rel_props") or {}).get("date")
    has_date = 1 if isinstance(date, str) and len(date) >= 8 else 0
    return (score, overlap, has_date)

# out_rels.append({
#     "rel_type": r.get("rel_type"),
#     "date": props.get("date"),
#     "source_name": src.get("display_name"),
#     "target_name": tgt.get("display_name"),
#     "text": rel_text(r),
#     "score": r.get("score", 0),
#     # Edge-only citation if present; node fallback only if edge missing
#     "citations": best_citation_for_relationship(r, edge_prov=edge_prov, node_prov=node_prov),
# })

def build_relationship_only_evidence(
    question: str,
    keywords: Optional[List[str]],
    seed_rels: List[Dict[str, Any]],   # merged/deduped list
    edge_prov: Dict[str, Any],
    node_prov: Dict[str, Any],
    top_k: int = 50,
) -> Dict[str, Any]:
    keywords = keywords or []

    rels = list(seed_rels or [])
    # re-rank for the question (not only score)
    rels.sort(key=lambda r: rank_key(question, r), reverse=True)
    rels = rels[:top_k]

    out_rels = []
    for r in rels:
        props = r.get("rel_props") or {}
    
        out_rels.append({
            "text": rel_text(r),
            # Edge-only citation if present; node fallback only if edge missing
            "citations": best_citation_for_relationship(r, edge_prov=edge_prov, node_prov=node_prov),
        })

    return {
        "question": question,
        "keywords": keywords,
        "seed_relationships": out_rels,
    }
    

def format_evidence(data):
    formatted_output = []
    for item in data['seed_relationships']:
        text = item['text']
        for citation in item['citations']:
            document_id = citation['document_id']
            chunk_id = citation['chunk_id']
            justification = text
            
            formatted_output.append(f"Document_id = {document_id}\nChunk_id = {chunk_id}\nJustification = {justification}\n")
    
    return "\n".join(formatted_output)

def format_evidence_multi_query(data):
    formatted_output = []
    for item in data:
        rel_type = item['rel_type']
        description = item['rel_props']['justification'] or item['rel_props']['description']
        source_name = item['source']['display_name']
        source_entity_type = item['source']['entity_type']
        target_name = item['target']['display_name']
        target_entity_type = item['target']['entity_type']

        formatted_output.append(f"rel_type = {rel_type}\ndescription = {description}\nsource_name = {source_name}\nsource_entity_type = {source_entity_type}\ntarget_name = {target_name}\ntarget_entity_type = {target_entity_type}\n")

    return "\n".join(formatted_output)

def extract_last_json(text):
    # Use regex to find all JSON blocks (objects starting with '{' and ending with '}')
    json_pattern = r'\{.*?\}'
    
    # Find all JSON-like strings in the text
    matches = re.findall(json_pattern, text, re.DOTALL)
    
    # Get the last match
    if matches:
        last_json_str = matches[-1]
        try:
            # Parse the last match into a Python dictionary
            return json.loads(last_json_str)
        except json.JSONDecodeError:
            return None
    return None

def remove_think_tags_output(response):
    # print("r/////////////e respomse:",response)
    response = re.sub(r"<think>.*?</think>", "", response, flags=re.DOTALL).strip()
    start = response.index('{')
    decoder = json.JSONDecoder()
    obj, _ = decoder.raw_decode(response, start) 
    return obj

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
    
    result=remove_think_tags_output(result)

    return result["queries"]


def extract_multiple_queries_vllm(multi_query_structure, question, llm):
    llm_host = os.getenv('LLM_HOST')
    llm_port = os.getenv('LLM_PORT')
    model = os.getenv('VLLM_LLM_MODEL')
    
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

    prompt_chunk = human.format(multi_query_structure=multi_query_structure, question=question)
    payload={
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": system
            },
            {
                "role": "user",
                "content": prompt_chunk
            }
        ],
        "enable_thinking": False
    }
    
    response = requests.post(
        f"http://172.22.10.188:30009/v1/chat/completions",
        json=payload
    )
    result = response.json()
    result=remove_think_tags_output(result)

    return result["queries"]



def callingFunction(question, embedding_model=None):
    kw = keywords_from_question(question)
    seed_nodes = fetch_seed_nodes(driver, kw, limit=300)
    node_eids = [n["node_eid"] for n in seed_nodes]
    rels_from_nodes = fetch_relationships_for_nodes(
        driver, node_eids=node_eids, keywords=kw, question=question,
        embedding_model=embedding_model, limit=300
    )

    seed_rels_text = fetch_seed_relationships(
        driver, kw, question=question, embedding_model=embedding_model, limit=300
    )
    rel_by_eid = {r["rel_eid"]: r for r in seed_rels_text}
    for r in rels_from_nodes:
        eid = r["rel_eid"]
        if eid not in rel_by_eid:
            rel_by_eid[eid] = r
        else:
            if (r.get("score", 0) > rel_by_eid[eid].get("score", 0)):
                rel_by_eid[eid] = r

    seed_rels = sorted(rel_by_eid.values(), key=lambda x: x.get("score", 0), reverse=True)
    
    
    uids: Set[str] = set()
    for r in seed_rels:
        uids.add(r["source"]["uid"])
        uids.add(r["target"]["uid"])
    uids_list = sorted(uids)

    node_prov = fetch_nodes_provenance(driver, uids_list)
    
    edge_prov: Dict[str, List[Dict[str, Any]]] = {}
    for r in seed_rels:
        rid = r["rel_uid"] or r["rel_eid"]
        edge_prov[rid] = fetch_edge_provenance(driver, r.get("rel_uid", ""), r.get("rel_eid", ""))
        
    payload = build_llm_payload(
            question=question,
            keywords=kw,
            seed_rels=seed_rels,
            node_prov=node_prov,
            edge_prov=edge_prov,
        )
    
    evidence = build_relationship_only_evidence(question, kw, seed_rels, edge_prov, node_prov)
    evidence_text = format_evidence(evidence)
    
    llm_host = os.getenv('LLM_HOST')
    llm_port = os.getenv('LLM_PORT')

    model = os.getenv("JSON_MODEL")
    num_ctx = int(os.getenv("LLM_NUM_CTX"))
    llm = OllamaLLM(base_url="http://"+llm_host+":"+llm_port, model=model, num_ctx=num_ctx, keep_alive='5m', temperature=0)

    multi_query_structure = format_evidence_multi_query(seed_rels)
    
    if os.getenv('LLM_MODE') == "vllm":
        multiple_queries = extract_multiple_queries_vllm(multi_query_structure, question, llm)
        result = summarizationSystem_vllm(question, evidence_text, llm)
    else:
        multiple_queries = extract_multiple_queries(multi_query_structure, question, llm)
        result = summarizationSystem(question, evidence_text, llm)
    # result = json.loads(result.content)
    result["queries"] = multiple_queries

    return result


def summarizationSystem(question, evidence_text, llm):
    system = (
        "You are a summarization system.\n"
        "Use ONLY the provided bullet EVIDENCE to write the summary.\n"
        "Do NOT invent facts or add details not present in the bullets.\n"
        "Preserve meaning; you may merge/reorder bullets for clarity.\n"
    )
    # print("//////////// evidence text")
    # print(evidence_text)
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
    model = os.getenv('VLLM_LLM_MODEL')
    
    system = (
        "You are a summarization system.\n"
        "Use ONLY the provided bullet EVIDENCE to write the summary.\n"
        "Do NOT invent facts or add details not present in the bullets.\n"
        "Preserve meaning; you may merge/reorder bullets for clarity.\n"
    )
    # print("//////////// evidence text")
    # print(evidence_text)
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
    
    system = (
    "You are an expert intelligence analyst. "
    "Your job is to synthesize evidence from a knowledge graph into a clear, "
    "accurate narrative. You reason across multiple evidence bullets to produce "
    "a coherent answer. You never invent facts not present in the evidence."
)

    human = (
        f"QUESTION:\n{question}\n\n"
        
        "EVIDENCE:\n"
        f"{final_evidence_text}\n\n"
        
        "INSTRUCTIONS:\n"
        "Read ALL the evidence carefully. Then:\n\n"
        
        "1) SUMMARY:\n"
        "   - Write a detailed narrative that directly answers the QUESTION.\n"
        "   - Synthesize across bullets — do not just list them back.\n"
        "   - Include: who did what, when, where, why, and with whom.\n"
        "   - Include all entities, events, and relationships relevant to the QUESTION.\n"
        "   - If more than one bullets are about the same event, merge them into one cohesive sentence.\n"
        "   - Preserve specific details: names, dates, locations, claims.\n"
        "   - Do NOT add any fact not present in the evidence.\n"
        "   - Do NOT say 'based on the evidence' or 'according to the bullets'.\n\n"
        
        "2) SOURCES:\n"
        "   - List only the unique (document_id, chunk_id) pairs from the evidence.\n"
        "   - Use ONLY document_id and chunk_id values that appear verbatim in the evidence.\n"
        "   - Do NOT invent or modify any document_id or chunk_id.\n"
        "   - Omit any bullet that has no citation.\n\n"
        
        "3) IF EVIDENCE IS EMPTY OR INSUFFICIENT:\n"
        "   - Set summary to exactly: 'Sufficient data is not available to answer this question.'\n"
        
        "Output format (STRICT):\n"
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
 
    payload={
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": system
            },
            {
                "role": "user",
                "content": human
            }
        ],
        "enable_thinking": False
    }
    
    response = requests.post(
        f"http://172.22.10.188:30009/v1/chat/completions",
        json=payload
    )
    
    result = response.json()
    result=remove_think_tags_output(result)
    return result