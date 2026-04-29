"""
lightrag_ingestion.py
=====================
LightRAG-style entity + relationship extraction with gleaning loops,
cross-chunk entity resolution, and Neo4j ingestion — all wired into
your existing schema (Entity nodes, MENTIONED_IN edges, rel edges with
chunk_ids / provenance).

Drop-in replacement for whatever ingestion script you had before.
The retrieval side (retrievalFromGraphDB_fixed.py) is unchanged.

Pipeline overview
-----------------
1.  Chunk the raw document
2.  For each chunk → extract entities + relationships  (LightRAG gleaning)
3.  Across all chunks  → resolve / deduplicate entities  (LLM-assisted)
4.  Merge everything into Neo4j, preserving provenance

Schema written to Neo4j
------------------------
(:Entity  {uid, display_name, entity_type, description, aliases: []})
(:Document {fsid})
(:Entity)-[:MENTIONED_IN {chunk_ids: []}]->(:Document)
(:Entity)-[:<REL_TYPE> {rel_uid, justification, description, chunk_ids: []}]->(:Entity)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import uuid
from dataclasses import dataclass, field
from typing import Any

from langchain_community.llms import OllamaLLM
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ══════════════════════════════════════════════════════════════════════════════
# 0.  Config
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Config:
    # Ollama
    llm_host: str         = field(default_factory=lambda: os.getenv("LLM_HOST", "localhost"))
    llm_port: str         = field(default_factory=lambda: os.getenv("LLM_PORT", "11434"))
    json_model: str       = field(default_factory=lambda: os.getenv("JSON_MODEL", "llama3"))
    llm_num_ctx: int      = field(default_factory=lambda: int(os.getenv("LLM_NUM_CTX", "8192")))

    # Neo4j
    neo4j_uri: str        = field(default_factory=lambda: os.getenv("NEO4J_URI",      "bolt://localhost:7687"))
    neo4j_user: str       = field(default_factory=lambda: os.getenv("NEO4J_USER",     "neo4j"))
    neo4j_password: str   = field(default_factory=lambda: os.getenv("NEO4J_PASSWORD", "password"))

    # Chunking
    chunk_size: int       = 1200   # characters
    chunk_overlap: int    = 200

    # LightRAG gleaning
    max_gleaning_rounds: int = 2   # extra "did you miss anything?" passes
    gleaning_threshold: int  = 1   # stop if fewer than N new entities found

    # Entity resolution
    resolution_batch: int    = 40  # entities per resolution prompt


CFG = Config()


# ══════════════════════════════════════════════════════════════════════════════
# 1.  LLM helpers
# ══════════════════════════════════════════════════════════════════════════════

def _build_llm() -> OllamaLLM:
    return OllamaLLM(
        base_url=f"http://{CFG.llm_host}:{CFG.llm_port}",
        model=CFG.json_model,
        num_ctx=CFG.llm_num_ctx,
        keep_alive="10m",
        temperature=0,
    )


def _clean_llm_output(raw: str) -> str:
    """Strip <think> blocks and markdown fences — same fix as in retrieval."""
    raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL)
    raw = re.sub(r"```(?:json)?\s*", "", raw).replace("```", "")
    return raw.strip()


def _parse_json_safe(raw: str) -> Any:
    cleaned = _clean_llm_output(raw)
    # Try to find a JSON array or object even if surrounded by prose
    for pattern in (r"\[.*\]", r"\{.*\}"):
        m = re.search(pattern, cleaned, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass
    return None


# ══════════════════════════════════════════════════════════════════════════════
# 2.  Chunking
# ══════════════════════════════════════════════════════════════════════════════

def chunk_text(text: str, chunk_size: int = CFG.chunk_size,
               overlap: int = CFG.chunk_overlap) -> list[dict]:
    """
    Returns list of {"chunk_id": str, "text": str, "start": int, "end": int}.
    chunk_id is a stable sha256 hash of the chunk text.
    """
    chunks = []
    start  = 0
    while start < len(text):
        end  = min(start + chunk_size, len(text))
        # try to break on sentence boundary
        if end < len(text):
            for sep in (". ", ".\n", "\n\n", "\n", " "):
                idx = text.rfind(sep, start, end)
                if idx > start + chunk_size // 2:
                    end = idx + len(sep)
                    break
        chunk_text_str = text[start:end].strip()
        if chunk_text_str:
            cid = hashlib.sha256(chunk_text_str.encode()).hexdigest()[:16]
            chunks.append({"chunk_id": cid, "text": chunk_text_str,
                           "start": start, "end": end})
        start = end - overlap if end < len(text) else end
    return chunks


# ══════════════════════════════════════════════════════════════════════════════
# 3.  LightRAG-style extraction prompt + gleaning
# ══════════════════════════════════════════════════════════════════════════════

_EXTRACTION_SYSTEM = """You are an expert information-extraction system for building knowledge graphs.

Your job is to read a text passage and extract:
  A) All named ENTITIES (people, organisations, locations, events, dates, topics, etc.)
  B) All RELATIONSHIPS between those entities that are explicitly stated or strongly implied.

Output Rules:
- Output ONLY valid JSON — no prose, no markdown fences.
- Entity object keys: "name", "type", "description"
- Relationship object keys: "source", "target", "relation_type", "justification"
  - "relation_type" must be UPPER_SNAKE_CASE (e.g. MEMBER_OF, ATTENDED_MEETING)
  - "source" and "target" must exactly match an entity "name" from the entities list
- Do NOT invent facts not present in the text.
- Prefer full proper names over pronouns or abbreviations.

Output format (strict):
{
  "entities": [
    {"name": "...", "type": "...", "description": "..."},
    ...
  ],
  "relationships": [
    {"source": "...", "target": "...", "relation_type": "...", "justification": "..."},
    ...
  ]
}
"""

_EXTRACTION_HUMAN = """TEXT:
\"\"\"
{text}
\"\"\"

Extract all entities and relationships from the text above.
Output ONLY the JSON object."""

_GLEANING_HUMAN = """You previously extracted these entities and relationships from the same text:

ALREADY EXTRACTED:
{already_extracted}

TEXT:
\"\"\"
{text}
\"\"\"

Are there any entities or relationships you MISSED?
If yes, return ONLY the NEW ones in the same JSON format:
{{"entities": [...], "relationships": [...]}}
If nothing was missed, return:
{{"entities": [], "relationships": []}}

Output ONLY the JSON object."""


def extract_from_chunk(
    chunk_text: str,
    llm: OllamaLLM,
    max_gleaning: int = CFG.max_gleaning_rounds,
    gleaning_threshold: int = CFG.gleaning_threshold,
) -> dict:
    """
    LightRAG-style extraction with gleaning.
    Returns {"entities": [...], "relationships": [...]}
    """
    # ── Initial extraction pass ───────────────────────────────────────────
    prompt = f"System: {_EXTRACTION_SYSTEM}\n\nHuman: {_EXTRACTION_HUMAN.format(text=chunk_text)}"
    raw    = llm.invoke(prompt)
    result = _parse_json_safe(raw) or {"entities": [], "relationships": []}

    all_entities      = {e["name"]: e for e in result.get("entities", [])}
    all_relationships = list(result.get("relationships", []))

    # ── Gleaning loops ────────────────────────────────────────────────────
    for glean_round in range(max_gleaning):
        already = json.dumps(
            {"entities": list(all_entities.values()), "relationships": all_relationships},
            indent=2,
        )
        glean_prompt = (
            f"System: {_EXTRACTION_SYSTEM}\n\n"
            f"Human: {_GLEANING_HUMAN.format(already_extracted=already, text=chunk_text)}"
        )
        glean_raw    = llm.invoke(glean_prompt)
        glean_result = _parse_json_safe(glean_raw) or {"entities": [], "relationships": []}

        new_entities = glean_result.get("entities", [])
        new_rels     = glean_result.get("relationships", [])

        # Count genuinely new entities
        novel = [e for e in new_entities if e.get("name") and e["name"] not in all_entities]
        logger.info(f"  Gleaning round {glean_round+1}: +{len(novel)} new entities, +{len(new_rels)} new rels")

        for e in new_entities:
            if e.get("name") and e["name"] not in all_entities:
                all_entities[e["name"]] = e
        all_relationships.extend(new_rels)

        if len(novel) < gleaning_threshold:
            logger.info(f"  Gleaning stopped early (< {gleaning_threshold} new entities)")
            break

    return {
        "entities":      list(all_entities.values()),
        "relationships": all_relationships,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 4.  Cross-chunk entity resolution (deduplication)
# ══════════════════════════════════════════════════════════════════════════════

_RESOLUTION_SYSTEM = """You are an entity resolution expert building a knowledge graph.

Given a list of entity names extracted from multiple text chunks, identify groups of names
that refer to the SAME real-world entity (aliases, abbreviations, partial names, different
spellings, etc.).

Output ONLY valid JSON — a list of groups. Each group is a list of name strings.
Names that are unique (no alias found) should appear as singleton groups.

Example output:
[
  ["Wahdat-e-Islami", "WeI", "Wahdat e Islami"],
  ["Jamiat Ulama-I-Hind", "JUH", "Jamiat"],
  ["Farhan Raye"]
]

Rules:
- Every input name must appear in exactly one group.
- Do not merge entities unless you are confident they refer to the same entity.
- Output ONLY the JSON array. No explanations."""

_RESOLUTION_HUMAN = """Entity names to resolve (one per line):
{names}

Group them into alias clusters. Output ONLY the JSON array of groups."""


def resolve_entities(
    all_entity_names: list[str],
    llm: OllamaLLM,
    batch_size: int = CFG.resolution_batch,
) -> dict[str, str]:
    """
    Returns a mapping {raw_name -> canonical_name}.
    The canonical name is the longest / most complete name in each cluster.
    """
    if not all_entity_names:
        return {}

    name_to_canonical: dict[str, str] = {}

    # Process in batches to stay within context window
    for i in range(0, len(all_entity_names), batch_size):
        batch = all_entity_names[i : i + batch_size]
        names_str = "\n".join(f"- {n}" for n in batch)
        prompt = (
            f"System: {_RESOLUTION_SYSTEM}\n\n"
            f"Human: {_RESOLUTION_HUMAN.format(names=names_str)}"
        )
        raw    = llm.invoke(prompt)
        groups = _parse_json_safe(raw)

        if not isinstance(groups, list):
            # Fall back: each name is its own canonical
            for n in batch:
                name_to_canonical[n] = n
            continue

        for group in groups:
            if not isinstance(group, list) or not group:
                continue
            # Pick longest name as canonical (heuristic: longer = more complete)
            canonical = max(group, key=lambda x: len(x))
            for name in group:
                if name:
                    name_to_canonical[name] = canonical

        # Any batch member not assigned gets identity mapping
        for n in batch:
            if n not in name_to_canonical:
                name_to_canonical[n] = n

    logger.info(f"Entity resolution: {len(all_entity_names)} names → "
                f"{len(set(name_to_canonical.values()))} canonical entities")
    return name_to_canonical


# ══════════════════════════════════════════════════════════════════════════════
# 5.  Data model
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class EntityRecord:
    uid:          str
    display_name: str
    entity_type:  str
    description:  str
    aliases:      set[str] = field(default_factory=set)
    chunk_ids:    set[str] = field(default_factory=set)   # chunks where mentioned
    document_ids: set[str] = field(default_factory=set)


@dataclass
class RelationRecord:
    rel_uid:       str
    source_uid:    str
    target_uid:    str
    relation_type: str
    justification: str
    chunk_ids:     set[str] = field(default_factory=set)


# ══════════════════════════════════════════════════════════════════════════════
# 6.  Assemble full document graph
# ══════════════════════════════════════════════════════════════════════════════

def _stable_uid(display_name: str) -> str:
    return hashlib.sha256(display_name.lower().strip().encode()).hexdigest()[:24]


def _stable_rel_uid(source_uid: str, target_uid: str, rel_type: str) -> str:
    key = f"{source_uid}|{target_uid}|{rel_type}"
    return hashlib.sha256(key.encode()).hexdigest()[:24]


def build_document_graph(
    chunks:             list[dict],           # from chunk_text()
    chunk_extractions:  list[dict],           # parallel list, from extract_from_chunk()
    name_to_canonical:  dict[str, str],       # from resolve_entities()
    document_id:        str,
) -> tuple[dict[str, EntityRecord], dict[str, RelationRecord]]:
    """
    Combines per-chunk extractions into unified Entity and Relation records.
    """
    entities:  dict[str, EntityRecord]  = {}   # uid -> record
    relations: dict[str, RelationRecord] = {}   # rel_uid -> record

    for chunk, extraction in zip(chunks, chunk_extractions):
        chunk_id = chunk["chunk_id"]

        # ── Entities ─────────────────────────────────────────────────────
        for e in extraction.get("entities", []):
            raw_name = e.get("name", "").strip()
            if not raw_name:
                continue
            canonical = name_to_canonical.get(raw_name, raw_name)
            uid = _stable_uid(canonical)

            if uid not in entities:
                entities[uid] = EntityRecord(
                    uid=uid,
                    display_name=canonical,
                    entity_type=e.get("type", "Unknown"),
                    description=e.get("description", ""),
                )
            rec = entities[uid]
            # Accumulate aliases
            if raw_name != canonical:
                rec.aliases.add(raw_name)
            # Accumulate provenance
            rec.chunk_ids.add(chunk_id)
            rec.document_ids.add(document_id)
            # Keep the longest description seen
            if len(e.get("description", "")) > len(rec.description):
                rec.description = e["description"]

        # ── Relationships ─────────────────────────────────────────────────
        for r in extraction.get("relationships", []):
            raw_src = r.get("source", "").strip()
            raw_tgt = r.get("target", "").strip()
            rel_type = re.sub(r"\s+", "_", r.get("relation_type", "RELATED_TO").upper())
            if not raw_src or not raw_tgt:
                continue

            can_src = name_to_canonical.get(raw_src, raw_src)
            can_tgt = name_to_canonical.get(raw_tgt, raw_tgt)
            src_uid = _stable_uid(can_src)
            tgt_uid = _stable_uid(can_tgt)
            rel_uid = _stable_rel_uid(src_uid, tgt_uid, rel_type)

            if rel_uid not in relations:
                relations[rel_uid] = RelationRecord(
                    rel_uid=rel_uid,
                    source_uid=src_uid,
                    target_uid=tgt_uid,
                    relation_type=rel_type,
                    justification=r.get("justification", ""),
                )
            rel_rec = relations[rel_uid]
            rel_rec.chunk_ids.add(chunk_id)
            # Accumulate justification evidence
            new_just = r.get("justification", "")
            if new_just and new_just not in rel_rec.justification:
                rel_rec.justification = (
                    rel_rec.justification + " | " + new_just
                    if rel_rec.justification else new_just
                )

            # Ensure both endpoints exist as entities (even if not explicitly extracted)
            for uid, name in [(src_uid, can_src), (tgt_uid, can_tgt)]:
                if uid not in entities:
                    entities[uid] = EntityRecord(
                        uid=uid, display_name=name,
                        entity_type="Unknown", description="",
                    )
                entities[uid].chunk_ids.add(chunk_id)
                entities[uid].document_ids.add(document_id)

    return entities, relations


# ══════════════════════════════════════════════════════════════════════════════
# 7.  Neo4j writer — matches your existing schema exactly
# ══════════════════════════════════════════════════════════════════════════════

_MERGE_ENTITY = """
MERGE (e:Entity {uid: $uid})
ON CREATE SET
    e.display_name = $display_name,
    e.entity_type  = $entity_type,
    e.description  = $description,
    e.aliases      = $aliases
ON MATCH SET
    e.entity_type  = CASE WHEN e.entity_type IN ['Unknown', '', null]
                          THEN $entity_type ELSE e.entity_type END,
    e.description  = CASE WHEN size($description) > size(coalesce(e.description,''))
                          THEN $description ELSE e.description END,
    e.aliases      = apoc.coll.toSet(coalesce(e.aliases,[]) + $aliases)
RETURN e.uid AS uid
"""

# fallback if APOC not available
_MERGE_ENTITY_NO_APOC = """
MERGE (e:Entity {uid: $uid})
ON CREATE SET
    e.display_name = $display_name,
    e.entity_type  = $entity_type,
    e.description  = $description,
    e.aliases      = $aliases
ON MATCH SET
    e.entity_type  = CASE WHEN e.entity_type IN ['Unknown', '', null]
                          THEN $entity_type ELSE e.entity_type END,
    e.description  = CASE WHEN size($description) > size(coalesce(e.description,''))
                          THEN $description ELSE e.description END
RETURN e.uid AS uid
"""

_MERGE_DOCUMENT = """
MERGE (d:Document {fsid: $fsid})
RETURN d.fsid AS fsid
"""

_MERGE_MENTIONED_IN = """
MATCH (e:Entity {uid: $uid})
MATCH (d:Document {fsid: $fsid})
MERGE (e)-[m:MENTIONED_IN]->(d)
ON CREATE SET m.chunk_ids = $chunk_ids
ON MATCH  SET m.chunk_ids = apoc.coll.toSet(coalesce(m.chunk_ids,[]) + $chunk_ids)
"""

_MERGE_MENTIONED_IN_NO_APOC = """
MATCH (e:Entity {uid: $uid})
MATCH (d:Document {fsid: $fsid})
MERGE (e)-[m:MENTIONED_IN]->(d)
ON CREATE SET m.chunk_ids = $chunk_ids
ON MATCH  SET m.chunk_ids = $chunk_ids
"""

_MERGE_RELATION_TEMPLATE = """
MATCH (a:Entity {{uid: $src_uid}})
MATCH (b:Entity {{uid: $tgt_uid}})
MERGE (a)-[r:{rel_type} {{rel_uid: $rel_uid}}]->(b)
ON CREATE SET
    r.justification = $justification,
    r.description   = $justification,
    r.chunk_ids     = $chunk_ids
ON MATCH SET
    r.chunk_ids     = apoc.coll.toSet(coalesce(r.chunk_ids,[]) + $chunk_ids),
    r.justification = CASE WHEN size($justification) > size(coalesce(r.justification,''))
                           THEN $justification ELSE r.justification END
"""

_MERGE_RELATION_NO_APOC = """
MATCH (a:Entity {{uid: $src_uid}})
MATCH (b:Entity {{uid: $tgt_uid}})
MERGE (a)-[r:{rel_type} {{rel_uid: $rel_uid}}]->(b)
ON CREATE SET
    r.justification = $justification,
    r.description   = $justification,
    r.chunk_ids     = $chunk_ids
ON MATCH SET
    r.justification = CASE WHEN size($justification) > size(coalesce(r.justification,''))
                           THEN $justification ELSE r.justification END
"""


def _has_apoc(session) -> bool:
    try:
        session.run("RETURN apoc.version()").single()
        return True
    except Exception:
        return False


def write_to_neo4j(
    driver,
    document_id:   str,
    entities:      dict[str, EntityRecord],
    relations:     dict[str, RelationRecord],
) -> None:
    """
    Upserts everything into Neo4j using your existing schema.
    Works with or without APOC.
    """
    with driver.session() as session:
        apoc = _has_apoc(session)
        logger.info(f"APOC available: {apoc}")

        # ── Document node ─────────────────────────────────────────────────
        session.run(_MERGE_DOCUMENT, fsid=document_id)

        # ── Entity nodes ──────────────────────────────────────────────────
        entity_cypher   = _MERGE_ENTITY if apoc else _MERGE_ENTITY_NO_APOC
        mentioned_cypher = _MERGE_MENTIONED_IN if apoc else _MERGE_MENTIONED_IN_NO_APOC

        for uid, rec in entities.items():
            session.run(
                entity_cypher,
                uid=rec.uid,
                display_name=rec.display_name,
                entity_type=rec.entity_type,
                description=rec.description or "",
                aliases=sorted(rec.aliases),
            )
            # MENTIONED_IN per document
            for doc_id in rec.document_ids:
                session.run(
                    mentioned_cypher,
                    uid=rec.uid,
                    fsid=doc_id,
                    chunk_ids=sorted(rec.chunk_ids),
                )

        logger.info(f"  Wrote {len(entities)} entity nodes")

        # ── Relationship edges ────────────────────────────────────────────
        rel_cypher_tpl = _MERGE_RELATION_TEMPLATE if apoc else _MERGE_RELATION_NO_APOC

        for rel_uid, rec in relations.items():
            # Skip if endpoints don't exist (safety guard)
            if rec.source_uid not in entities or rec.target_uid not in entities:
                logger.warning(f"  Skipping rel {rel_uid}: missing endpoint entity")
                continue
            cypher = rel_cypher_tpl.format(rel_type=rec.relation_type)
            session.run(
                cypher,
                src_uid=rec.source_uid,
                tgt_uid=rec.target_uid,
                rel_uid=rec.rel_uid,
                justification=rec.justification or "",
                chunk_ids=sorted(rec.chunk_ids),
            )

        logger.info(f"  Wrote {len(relations)} relationship edges")


# ══════════════════════════════════════════════════════════════════════════════
# 8.  Master ingestion function  (replaces your old ingestion script)
# ══════════════════════════════════════════════════════════════════════════════

def ingest_document(
    text:        str,
    document_id: str,            # your document's fsid / unique ID
    driver=None,                 # pass existing driver or None to create one
    llm:  OllamaLLM | None = None,
    cfg:  Config            = CFG,
) -> dict:
    """
    Full LightRAG-style ingestion pipeline for a single document.

    Parameters
    ----------
    text        : raw document text
    document_id : stable unique identifier for the document (used as fsid)
    driver      : existing Neo4j driver (optional — created from env if None)
    llm         : existing OllamaLLM (optional — created from env if None)

    Returns
    -------
    dict with ingestion statistics
    """
    llm    = llm    or _build_llm()
    driver = driver or GraphDatabase.driver(
        cfg.neo4j_uri, auth=(cfg.neo4j_user, cfg.neo4j_password)
    )

    logger.info(f"=== Ingesting document: {document_id} ===")

    # ── Step 1: Chunk ─────────────────────────────────────────────────────
    chunks = chunk_text(text, cfg.chunk_size, cfg.chunk_overlap)
    logger.info(f"Step 1: {len(chunks)} chunks created")

    # ── Step 2: Extract per chunk (with gleaning) ─────────────────────────
    chunk_extractions: list[dict] = []
    all_raw_names: list[str]      = []

    for i, chunk in enumerate(chunks):
        logger.info(f"Step 2: Extracting chunk {i+1}/{len(chunks)} "
                    f"(chunk_id={chunk['chunk_id']})")
        extraction = extract_from_chunk(
            chunk["text"], llm,
            max_gleaning=cfg.max_gleaning_rounds,
            gleaning_threshold=cfg.gleaning_threshold,
        )
        chunk_extractions.append(extraction)
        for e in extraction.get("entities", []):
            if e.get("name"):
                all_raw_names.append(e["name"])
        logger.info(f"  → {len(extraction.get('entities',[]))} entities, "
                    f"{len(extraction.get('relationships',[]))} rels")

    # ── Step 3: Entity resolution (cross-chunk dedup) ─────────────────────
    unique_names = sorted(set(all_raw_names))
    logger.info(f"Step 3: Resolving {len(unique_names)} unique entity names")
    name_to_canonical = resolve_entities(unique_names, llm, cfg.resolution_batch)

    # ── Step 4: Build unified graph records ───────────────────────────────
    logger.info("Step 4: Building unified document graph")
    entities, relations = build_document_graph(
        chunks, chunk_extractions, name_to_canonical, document_id
    )
    logger.info(f"  → {len(entities)} canonical entities, {len(relations)} relationships")

    # ── Step 5: Write to Neo4j ────────────────────────────────────────────
    logger.info("Step 5: Writing to Neo4j")
    write_to_neo4j(driver, document_id, entities, relations)

    stats = {
        "document_id":       document_id,
        "chunks":            len(chunks),
        "raw_entity_names":  len(all_raw_names),
        "canonical_entities":len(entities),
        "relationships":     len(relations),
    }
    logger.info(f"=== Done: {stats} ===")
    return stats


# ══════════════════════════════════════════════════════════════════════════════
# 9.  Batch ingestion helper (multiple documents)
# ══════════════════════════════════════════════════════════════════════════════

def ingest_documents(
    documents: list[dict],      # each dict: {"text": str, "document_id": str}
    driver=None,
    llm:  OllamaLLM | None = None,
    cfg:  Config            = CFG,
) -> list[dict]:
    """
    Ingest multiple documents, reusing the same LLM and Neo4j driver.
    documents = [{"text": "...", "document_id": "doc_001"}, ...]
    """
    llm    = llm    or _build_llm()
    driver = driver or GraphDatabase.driver(
        cfg.neo4j_uri, auth=(cfg.neo4j_user, cfg.neo4j_password)
    )

    all_stats = []
    for doc in documents:
        try:
            stats = ingest_document(
                text=doc["text"],
                document_id=doc["document_id"],
                driver=driver,
                llm=llm,
                cfg=cfg,
            )
            all_stats.append(stats)
        except Exception as exc:
            logger.error(f"Failed to ingest {doc.get('document_id')}: {exc}", exc_info=True)
            all_stats.append({"document_id": doc.get("document_id"), "error": str(exc)})

    return all_stats


# ══════════════════════════════════════════════════════════════════════════════
# 10.  Neo4j index setup  (run once on a fresh database)
# ══════════════════════════════════════════════════════════════════════════════

_INDEXES = [
    "CREATE INDEX entity_uid IF NOT EXISTS FOR (e:Entity)   ON (e.uid)",
    "CREATE INDEX entity_name IF NOT EXISTS FOR (e:Entity)  ON (e.display_name)",
    "CREATE INDEX entity_type IF NOT EXISTS FOR (e:Entity)  ON (e.entity_type)",
    "CREATE INDEX doc_fsid    IF NOT EXISTS FOR (d:Document) ON (d.fsid)",
    # Full-text index for keyword search (used by your retrieval pipeline)
    """CREATE FULLTEXT INDEX entity_fulltext IF NOT EXISTS
       FOR (e:Entity) ON EACH [e.display_name, e.description, e.aliases]""",
]


def ensure_indexes(driver) -> None:
    """Create Neo4j indexes if they don't exist yet. Safe to call repeatedly."""
    with driver.session() as session:
        for idx in _INDEXES:
            try:
                session.run(idx)
                logger.info(f"Index ensured: {idx[:60]}...")
            except Exception as exc:
                logger.warning(f"Index creation skipped ({exc}): {idx[:60]}...")


# ══════════════════════════════════════════════════════════════════════════════
# 11.  Quick smoke-test / CLI entry point
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    # Usage: python lightrag_ingestion.py <path_to_text_file> [document_id]
    if len(sys.argv) < 2:
        print("Usage: python lightrag_ingestion.py <text_file> [document_id]")
        sys.exit(1)

    text_path   = sys.argv[1]
    document_id = sys.argv[2] if len(sys.argv) > 2 else os.path.basename(text_path)

    with open(text_path, encoding="utf-8") as f:
        raw_text = f.read()

    _driver = GraphDatabase.driver(CFG.neo4j_uri, auth=(CFG.neo4j_user, CFG.neo4j_password))
    ensure_indexes(_driver)

    result = ingest_document(
        text=raw_text,
        document_id=document_id,
        driver=_driver,
    )
    print(json.dumps(result, indent=2))
    _driver.close()
