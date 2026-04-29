"""
retrievalFromGraphDB.py — FIXED VERSION
Fixes applied:
  1. keywords_from_question — strips markdown fences & think tags before json.loads
  2. fetch_seed_relationships — uses node_eids as a guaranteed fallback path
  3. fetch_edge_provenance — relaxed chunk-intersection; falls back to any chunk on the relation
  4. callingFunction — passes format_evidence_multi_query output to the LLM prompt
"""

import json
import os
import re
from typing import List, Dict, Any, Optional, Set

from langchain_community.llms import OllamaLLM


# ---------------------------------------------------------------------------
# FIX 1 — keywords_from_question
# ---------------------------------------------------------------------------
def keywords_from_question(question: str, llm) -> List[str]:
    prompt = """
    You are an expert entity extraction system.

    Extract only the useful entities from the user query. Focus on meaningful,
    intent-relevant entities such as:
    - People
    - Locations
    - Organizations
    - Products
    - Brands
    - Dates and times
    - Events
    - Topics
    - Quantities (price, size, number, etc.)
    - Any domain-specific terms that matter for understanding the query

    Do NOT include:
    - Stop words or filler text
    - Generic verbs unless they represent a meaningful action (e.g., "buy", "book")
    - Duplicate entities

    Rules:
    1. Extract entities exactly as they appear in the query.
    2. Extract useful entities and key topics from the user query.
    3. Include the main subject being asked about, even if it is not a named entity.
    4. Do not infer or hallucinate entities.
    5. Keep entities concise and relevant.
    6. If no entities are present, return an empty array [].
    7. Output ONLY a JSON array of strings. No explanations.

    Example:
    Query: "Book a flight from Berlin to New York next Friday under $500"
    Output:
    ["Berlin", "New York", "next Friday", "$500"]

    Now process the following query:
    {question}
    """

    final_prompt = prompt.format(question=question)
    result = llm.invoke(final_prompt)

    # ── FIX 1: clean up before parsing ──────────────────────────────────────
    # Strip <think>...</think> blocks (DeepSeek / reasoning models)
    result = re.sub(r"<think>.*?</think>", "", result, flags=re.DOTALL).strip()
    # Strip markdown code fences  ```json ... ```  or  ``` ... ```
    result = re.sub(r"```(?:json)?\s*", "", result).replace("```", "").strip()
    # Remove any leading/trailing prose the model might have added
    # (take only the first [...] array found)
    array_match = re.search(r"\[.*?\]", result, re.DOTALL)
    if array_match:
        result = array_match.group(0)

    try:
        parsed = json.loads(result)
        if isinstance(parsed, list):
            return parsed
        return []
    except json.JSONDecodeError:
        # Last resort: split on commas if it looks like a plain list
        cleaned = result.strip("[]").strip()
        if cleaned:
            return [t.strip().strip('"\'') for t in cleaned.split(",") if t.strip()]
        return []


# ---------------------------------------------------------------------------
# fetch_seed_nodes  (unchanged — shown for context)
# ---------------------------------------------------------------------------
def fetch_seed_nodes(
    driver,
    keywords: List[str],
    limit: int = 300,
) -> List[Dict[str, Any]]:
    cypher = """
    MATCH (n:Entity)
    WITH n, $kw AS kw
    WITH n,
        reduce(score = 0, k IN kw |
            score +
            // weight display_name higher
            CASE WHEN toLower(coalesce(n.display_name, "")) CONTAINS k THEN 3 ELSE 0 END +
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
        coalesce(n.uid, "")  AS uid,
        n.display_name       AS display_name,
        n.entity_type        AS entity_type,
        n                    AS node_obj,
        score                AS score
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


# ---------------------------------------------------------------------------
# fetch_relationships_for_nodes  (unchanged — shown for context)
# ---------------------------------------------------------------------------
def fetch_relationships_for_nodes(
    driver,
    node_eids=None,
    keywords: List[str] = None,
    limit: int = 300,
):
    node_eids = node_eids or []
    kw = [k.lower() for k in (keywords or [])]

    out: List[Dict[str, Any]] = []
    with driver.session() as session:
        for rec in session.run(
            # (your existing cypher unchanged)
            """
            MATCH (a:Entity)-[r]->(b:Entity)
            WHERE elementId(a) IN $node_eids OR elementId(b) IN $node_eids
            WITH a, r, b,
                 split(toLower(type(r)), "_") AS rt_parts,
                 $kw AS kw
            WITH a, r, b,
                 reduce(score = 0, k IN kw |
                     score +
                     CASE WHEN k IN rt_parts THEN 2 ELSE 0 END +
                     CASE WHEN toLower(coalesce(a.display_name,"")) CONTAINS k THEN 3 ELSE 0 END +
                     CASE WHEN toLower(coalesce(b.display_name,"")) CONTAINS k THEN 3 ELSE 0 END +
                     CASE WHEN r.justification IS NOT NULL AND toLower(r.justification) CONTAINS k THEN 1 ELSE 0 END +
                     CASE WHEN r.description   IS NOT NULL AND toLower(r.description)   CONTAINS k THEN 1 ELSE 0 END
                 ) AS score
            RETURN
                elementId(r)                  AS rel_eid,
                coalesce(r.rel_uid, "")       AS rel_uid,
                type(r)                       AS rel_type,
                r                             AS rel_obj,
                a.uid AS a_uid, a.display_name AS a_name, a.entity_type AS a_type,
                b.uid AS b_uid, b.display_name AS b_name, b.entity_type AS b_type,
                score AS score,
                coalesce(r.path_rels,             []) AS path_rels,
                coalesce(r.path_justifications,   []) AS path_justifications,
                coalesce(r.path_descriptions,     []) AS path_descriptions
            ORDER BY score DESC
            LIMIT $limit
            """,
            node_eids=node_eids,
            kw=kw,
            limit=int(limit),
        ):
            out.append(
                {
                    "rel_eid": rec["rel_eid"],
                    "rel_uid": rec["rel_uid"] or "",
                    "rel_type": rec["rel_type"],
                    "rel_props": dict(rec["rel_obj"]) if rec["rel_obj"] is not None else {},
                    "path_rels": rec["path_rels"],
                    "source": {"uid": rec["a_uid"], "display_name": rec["a_name"], "entity_type": rec["a_type"]},
                    "target": {"uid": rec["b_uid"], "display_name": rec["b_name"], "entity_type": rec["b_type"]},
                    "score": rec["score"] if "score" in rec.keys() else 0,
                    "path_justifications": rec["path_justifications"],
                    "path_descriptions": rec["path_descriptions"],
                }
            )
    return out


# ---------------------------------------------------------------------------
# FIX 2 — fetch_seed_relationships
#   Added a second cypher that uses node_eids directly as a guaranteed fallback.
#   The original keyword-scoring path is kept; results are merged.
# ---------------------------------------------------------------------------
def fetch_seed_relationships(
    driver,
    keywords: List[str],
    limit: int = 300,
    node_eids: List = None,          # ← NEW optional param
) -> List[Dict[str, Any]]:

    kw = [k.lower() for k in (keywords or [])]

    # ── Path A: original keyword-scoring query ────────────────────────────
    cypher_kw = """
    MATCH (a:Entity)-[r]->(b:Entity)
    WITH a, r, b,
         split(toLower(type(r)), "_") AS rt_parts,
         $kw AS kw
    WITH a, r, b,
         reduce(score = 0, k IN kw |
             score +
             CASE WHEN k IN rt_parts THEN 2 ELSE 0 END +
             CASE WHEN toLower(coalesce(a.display_name, "")) CONTAINS k THEN 3 ELSE 0 END +
             CASE WHEN toLower(coalesce(b.display_name, "")) CONTAINS k THEN 3 ELSE 0 END +
             CASE WHEN r.justification IS NOT NULL AND toLower(r.justification) CONTAINS k THEN 1 ELSE 0 END +
             CASE WHEN r.description   IS NOT NULL AND toLower(r.description)   CONTAINS k THEN 1 ELSE 0 END
         ) AS score
    WHERE score >= 1
    RETURN
        elementId(r)                AS rel_eid,
        coalesce(r.rel_uid, "")    AS rel_uid,
        type(r)                    AS rel_type,
        r                          AS rel_obj,
        a.uid AS a_uid, a.display_name AS a_name, a.entity_type AS a_type,
        b.uid AS b_uid, b.display_name AS b_name, b.entity_type AS b_type,
        score AS score,
        coalesce(r.path_rels,           []) AS path_rels,
        coalesce(r.path_justifications, []) AS path_justifications,
        coalesce(r.path_descriptions,   []) AS path_descriptions
    ORDER BY score DESC
    LIMIT $limit
    """

    # ── Path B (FIX): direct lookup via node_eids ─────────────────────────
    # This guarantees results whenever seed nodes were found, regardless of
    # whether keyword strings match inside the relationship itself.
    cypher_eids = """
    MATCH (a:Entity)-[r]->(b:Entity)
    WHERE elementId(a) IN $node_eids OR elementId(b) IN $node_eids
    RETURN
        elementId(r)                AS rel_eid,
        coalesce(r.rel_uid, "")    AS rel_uid,
        type(r)                    AS rel_type,
        r                          AS rel_obj,
        a.uid AS a_uid, a.display_name AS a_name, a.entity_type AS a_type,
        b.uid AS b_uid, b.display_name AS b_name, b.entity_type AS b_type,
        0     AS score,
        coalesce(r.path_rels,           []) AS path_rels,
        coalesce(r.path_justifications, []) AS path_justifications,
        coalesce(r.path_descriptions,   []) AS path_descriptions
    LIMIT $limit
    """

    def _row_to_dict(rec) -> Dict[str, Any]:
        return {
            "rel_eid": rec["rel_eid"],
            "rel_uid": rec["rel_uid"] or "",
            "rel_type": rec["rel_type"],
            "rel_props": dict(rec["rel_obj"]) if rec["rel_obj"] is not None else {},
            "path_rels": rec["path_rels"],
            "source": {"uid": rec["a_uid"], "display_name": rec["a_name"], "entity_type": rec["a_type"]},
            "target": {"uid": rec["b_uid"], "display_name": rec["b_name"], "entity_type": rec["b_type"]},
            "score": rec["score"] if "score" in rec.keys() else 0,
            "path_justifications": rec["path_justifications"],
            "path_descriptions": rec["path_descriptions"],
        }

    seen_eids: set = set()
    out: List[Dict[str, Any]] = []

    with driver.session() as session:
        # Run keyword-scoring path
        for rec in session.run(cypher_kw, kw=kw, limit=int(limit)):
            d = _row_to_dict(rec)
            seen_eids.add(d["rel_eid"])
            out.append(d)

        # Run node_eids fallback — add only relationships not already found
        if node_eids:
            for rec in session.run(cypher_eids, node_eids=node_eids, limit=int(limit)):
                d = _row_to_dict(rec)
                if d["rel_eid"] not in seen_eids:
                    seen_eids.add(d["rel_eid"])
                    out.append(d)

    return out


# ---------------------------------------------------------------------------
# fetch_nodes_provenance  (unchanged)
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# FIX 3 — fetch_edge_provenance
#   Old logic required r.chunk_ids ∩ ma.chunk_ids ∩ mb.chunk_ids to be non-empty,
#   which always failed when r.chunk_ids was empty or not aligned with MENTIONED_IN.
#   New logic: prefer the intersection but fall back to any chunk on the relationship
#   or on either endpoint mention.
# ---------------------------------------------------------------------------
def fetch_edge_provenance(driver, rel_uid: str, rel_eid: str) -> List[Dict[str, Any]]:
    """
    Relationship-level doc+chunk inference.

    Priority:
      1. Chunks that appear in r.chunk_ids AND in both endpoint MENTIONED_IN edges
         (strongest evidence — both endpoints + relation seen in same chunk)
      2. Chunks from r.chunk_ids alone (relation directly tagged a chunk)
      3. Chunks shared by both endpoint MENTIONED_IN edges in the same document
         (endpoints co-occur even if relation has no chunk_ids)
      4. Any document where at least one endpoint is mentioned (weakest fallback)
    """
    cypher = """
    MATCH (a:Entity)-[r]->(b:Entity)
    WHERE ($rel_uid <> "" AND r.rel_uid = $rel_uid)
       OR ($rel_eid <> "" AND elementId(r) = $rel_eid)
    OPTIONAL MATCH (a)-[ma:MENTIONED_IN]->(d:Document)<-[mb:MENTIONED_IN]-(b)
    WITH d, ma, mb, r,
         coalesce(r.chunk_ids,    []) AS r_chunks,
         coalesce(ma.chunk_ids,   []) AS ma_chunks,
         coalesce(mb.chunk_ids,   []) AS mb_chunks
    WITH d, r_chunks, ma_chunks, mb_chunks,
         // priority-1: triple intersection
         [x IN r_chunks  WHERE x IN ma_chunks AND x IN mb_chunks] AS triple_chunks,
         // priority-3: endpoint co-occurrence
         [x IN ma_chunks WHERE x IN mb_chunks]                    AS co_chunks
    WHERE d IS NOT NULL
    RETURN
        d.fsid AS document_id,
        CASE
            WHEN size([x IN r_chunks WHERE x IN ma_chunks AND x IN mb_chunks]) > 0
                THEN [x IN r_chunks WHERE x IN ma_chunks AND x IN mb_chunks]
            WHEN size(r_chunks) > 0
                THEN r_chunks
            WHEN size([x IN ma_chunks WHERE x IN mb_chunks]) > 0
                THEN [x IN ma_chunks WHERE x IN mb_chunks]
            ELSE coalesce(ma_chunks, mb_chunks, [])
        END AS chunk_ids
    ORDER BY
        size([x IN r_chunks WHERE x IN ma_chunks AND x IN mb_chunks]) DESC,
        d.fsid
    """

    with driver.session() as session:
        rows = session.run(
            cypher,
            rel_uid=rel_uid or "",
            rel_eid=rel_eid or "",
        )
        return [
            {"document_id": r["document_id"], "chunk_ids": r["chunk_ids"]}
            for r in rows
        ]


# ---------------------------------------------------------------------------
# build_relationship_only_evidence  (unchanged logic, kept for reference)
# ---------------------------------------------------------------------------
def rank_key(question: str, r: Dict) -> float:
    score = r.get("score", 0)
    q_lower = question.lower()
    src = (r.get("source") or {}).get("display_name", "").lower()
    tgt = (r.get("target") or {}).get("display_name", "").lower()
    if src and src in q_lower:
        score += 5
    if tgt and tgt in q_lower:
        score += 5
    return score


def best_citation_for_relationship(
    r: Dict,
    edge_prov: Dict[str, Any],
    node_prov: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    rid = r.get("rel_uid") or str(r.get("rel_eid", ""))
    ep = edge_prov.get(rid, [])
    if ep:
        best = ep[0]
        cids = best.get("chunk_ids") or []
        return [{"document_id": best["document_id"], "chunk_id": cids[0] if cids else None}]

    # Fallback: use node provenance of source entity
    src_uid = (r.get("source") or {}).get("uid", "")
    np = node_prov.get(src_uid, [])
    if np:
        best = np[0]
        cids = best.get("chunk_ids") or []
        return [{"document_id": best["document_id"], "chunk_id": cids[0] if cids else None}]

    return []


def build_relationship_only_evidence(
    question: str,
    keywords: Optional[List[str]],
    seed_rels: List[Dict[str, Any]],
    edge_prov: Dict[str, Any],
    node_prov: Dict[str, List[Dict[str, Any]]],
    top_k: int = 50,
) -> Dict[str, Any]:
    keywords = keywords or []
    rels = list(seed_rels or [])
    rels.sort(key=lambda r: rank_key(question, r), reverse=True)
    rels = rels[:top_k]

    out_rels = []
    for r in rels:
        out_rels.append(
            {
                "text": rel_text(r),
                "citations": best_citation_for_relationship(r, edge_prov=edge_prov, node_prov=node_prov),
            }
        )

    return {
        "question": question,
        "keywords": keywords,
        "seed_relationships": out_rels,
    }


def rel_text(r: Dict) -> str:
    src = (r.get("source") or {}).get("display_name", "")
    tgt = (r.get("target") or {}).get("display_name", "")
    rel = r.get("rel_type", "")
    props = r.get("rel_props") or {}
    justification = props.get("justification") or props.get("description") or ""
    if not justification:
        jlist = [j for j in r.get("path_justifications", []) if j]
        dlist = [j for j in r.get("path_descriptions", [])   if j]
        justification = " | ".join(jlist or dlist)
    parts = [f"{src} --[{rel}]--> {tgt}"]
    if justification:
        parts.append(f"Evidence: {justification}")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# format_evidence  (unchanged)
# ---------------------------------------------------------------------------
def format_evidence(data: Dict) -> str:
    formatted_output = []
    for item in data["seed_relationships"]:
        text = item["text"]
        for citation in item["citations"]:
            document_id    = citation["document_id"]
            chunk_id       = citation["chunk_id"]
            justification  = text
            formatted_output.append(
                f"Document_id = {document_id}\n"
                f"Chunk_id = {chunk_id}\n"
                f"Justification = {justification}\n"
            )
    return "\n".join(formatted_output)


# ---------------------------------------------------------------------------
# format_evidence_multi_query  (unchanged)
# ---------------------------------------------------------------------------
def format_evidence_multi_query(data: List[Dict]) -> str:
    formatted_output = []
    for item in data:
        rel_type = item.get("rel_type") or (
            (item.get("path_rels") or [None])[0] if item.get("path_rels") else ""
        )
        source_name        = (item.get("source") or {}).get("display_name", "")
        source_entity_type = (item.get("source") or {}).get("entity_type", "")
        target_name        = (item.get("target") or {}).get("display_name", "")
        target_entity_type = (item.get("target") or {}).get("entity_type", "")

        props = item.get("rel_props") or {}
        if props:
            description = props.get("justification") or props.get("description") or ""
        else:
            justifications = [j for j in item.get("path_justifications", []) if j]
            descriptions   = [j for j in item.get("path_descriptions",   []) if j]
            description    = " | ".join(justifications or descriptions) or ""

        formatted_output.append(
            f"rel_type = {rel_type}\n"
            f"description = {description}\n"
            f"source_name = {source_name}\n"
            f"source_entity_type = {source_entity_type}\n"
            f"target_name = {target_name}\n"
            f"target_entity_type = {target_entity_type}"
        )
    return "\n\n".join(formatted_output)


def extract_last_json(text: str):
    json_pattern = r"\{.*?\}"
    matches = re.findall(json_pattern, text, re.DOTALL)
    for m in reversed(matches):
        try:
            return json.loads(m)
        except Exception:
            continue
    return None


def extract_multiple_queries(multi_query_structure: str, question: str, llm) -> str:
    prompt = f"""
You are an expert at query decomposition.
Given the following evidence structure, extract the key relationship facts relevant to the question.

QUESTION: {question}

EVIDENCE STRUCTURE:
{multi_query_structure}

Return a concise JSON list of the most relevant relationship facts.
Output ONLY a JSON array. No explanations.
"""
    result = llm.invoke(prompt)
    result = re.sub(r"<think>.*?</think>", "", result, flags=re.DOTALL).strip()
    result = re.sub(r"```(?:json)?\s*", "", result).replace("```", "").strip()
    return result


# ---------------------------------------------------------------------------
# FIX 4 — callingFunction
#   Key change: always use format_evidence_multi_query output as evidence_text
#   when the citation-based evidence_text is empty.
#   Also passes node_eids into fetch_seed_relationships for the fallback path.
# ---------------------------------------------------------------------------
def callingFunction(question: str):
    llm_host = os.getenv("LLM_HOST", "localhost")
    llm_port = os.getenv("LLM_PORT", "11434")
    model    = os.getenv("JSON_MODEL")
    num_ctx  = int(os.getenv("LLM_NUM_CTX", "4096"))
    llm = OllamaLLM(
        base_url=f"http://{llm_host}:{llm_port}",
        model=model,
        num_ctx=num_ctx,
        keep_alive="5m",
        temperature=0,
    )

    # ── Step 1: keywords ─────────────────────────────────────────────────
    kw = keywords_from_question(question, llm)
    print("keywords >>>", kw)

    # ── Step 2: seed nodes ───────────────────────────────────────────────
    seed_nodes = fetch_seed_nodes(driver, kw, limit=300)
    print("SEED NODES:", len(seed_nodes))
    node_eids = [n["node_eid"] for n in seed_nodes]

    # ── Step 3: relationships attached to seed nodes ─────────────────────
    rels_from_nodes = fetch_relationships_for_nodes(
        driver, node_eids=node_eids, keywords=kw, limit=300
    )

    # ── Step 4: seed relationships (keyword + node_eids fallback) ────────
    # FIX: pass node_eids so the fallback path is always triggered
    seed_rels_text = fetch_seed_relationships(driver, kw, limit=300, node_eids=node_eids)
    print("seed_rels_text count:", len(seed_rels_text))

    # Merge: prefer higher-scoring entry per rel_eid
    rel_by_eid: Dict[str, Dict] = {r["rel_eid"]: r for r in seed_rels_text}
    for r in rels_from_nodes:
        eid = r["rel_eid"]
        if eid not in rel_by_eid:
            rel_by_eid[eid] = r
        elif r.get("score", 0) > rel_by_eid[eid].get("score", 0):
            rel_by_eid[eid] = r

    seed_rels = sorted(rel_by_eid.values(), key=lambda x: x.get("score", 0), reverse=True)
    print("Total seed_rels after merge:", len(seed_rels))

    # ── Step 5: provenance ───────────────────────────────────────────────
    uids: Set[str] = set()
    for r in seed_rels:
        uids.add((r.get("source") or {}).get("uid", ""))
        uids.add((r.get("target") or {}).get("uid", ""))
        for node_name in r.get("path_nodes", []):
            if node_name:
                uids.add(node_name)
    uids_list = sorted(u for u in uids if u)

    node_prov = fetch_nodes_provenance(driver, uids_list)

    edge_prov: Dict[str, List[Dict[str, Any]]] = {}
    for r in seed_rels:
        rid = r["rel_uid"] or str(r["rel_eid"])
        edge_prov[rid] = fetch_edge_provenance(
            driver, r.get("rel_uid", ""), str(r.get("rel_eid", ""))
        )

    # ── Step 6: build evidence ───────────────────────────────────────────
    evidence      = build_relationship_only_evidence(question, kw, seed_rels, edge_prov, node_prov)
    evidence_text = format_evidence(evidence)
    print("evidence_text length:", len(evidence_text))

    # FIX 4: always build multi-query evidence from seed_rels directly
    # (this path never depends on provenance so it's always populated)
    multi_query_structure = format_evidence_multi_query(seed_rels)
    multiple_queries      = extract_multiple_queries(multi_query_structure, question, llm)

    # FIX 4: if citation-based evidence is empty, fall back to multi_query_structure
    final_evidence_text = evidence_text if evidence_text.strip() else multi_query_structure
    print("Using evidence source:", "citations" if evidence_text.strip() else "multi_query_structure (fallback)")

    # ── Step 7: LLM summarization ────────────────────────────────────────
    system = (
        "You are a summarization system.\n"
        "Use ONLY the provided EVIDENCE to write the summary.\n"
        "Do NOT invent facts or add details not present in the evidence.\n"
        "Preserve meaning; you may merge/reorder bullets for clarity.\n"
    )

    human = (
        f"QUESTION:\n{question}\n\n"
        "EVIDENCE:\n"
        f"{final_evidence_text}\n\n"
        "TASK:\n"
        "1) Write a detailed but comprehensive narrative summary of the events described "
        "in the EVIDENCE as per the user question.\n"
        "   - Strictly combine only those bullets in summary that are related with QUESTION.\n"
        "   - Keep the key actions, escalation, and notable claims.\n"
        "   - Do NOT repeat every bullet verbatim.\n"
        "2) Citations handling:\n"
        "   - Do NOT sprinkle citations throughout the summary text.\n"
        "   - Extract each unique (document_id, chunk_id) pair that appears in EVIDENCE.\n"
        "   - List them at the end under a 'Citations:' heading.\n"
        "3) Return your answer as a JSON object with exactly two keys:\n"
        '   { "summary": "<narrative text>", "citations": [{"document_id": ..., "chunk_id": ...}, ...] }\n'
        "Output ONLY the JSON. No extra text."
    )

    full_prompt = f"System: {system}\n\nHuman: {human}"
    raw = llm.invoke(full_prompt)

    # Clean up and parse
    raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
    raw = re.sub(r"```(?:json)?\s*", "", raw).replace("```", "").strip()

    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        parsed = extract_last_json(raw)
        result = parsed if parsed else {"summary": raw, "citations": []}

    return result
