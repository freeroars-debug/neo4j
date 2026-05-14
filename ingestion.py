"""
ingestion.py
============
Production-ready ingestion pipeline with:

1. DocumentEntityRegistry  — lightweight per-document, discarded after each doc
2. Neo4j as GlobalRegistry — entity UID resolution always queries Neo4j first
3. Crash recovery          — checkpoint table in Neo4j, resume from any failure
4. Gleaning                — LightRAG-style multi-pass extraction per chunk
5. Idempotent writes       — all Neo4j writes use MERGE, safe to re-run

Architecture
------------
Write path:
  Documents → chunk → extract (+ gleaning) → DocumentEntityRegistry
            → resolve UIDs against Neo4j → write to Neo4j → checkpoint

Crash recovery:
  Restart → read checkpoints → skip completed docs → resume from first incomplete

Key design decision:
  Neo4j IS the global entity registry.
  In-memory DocumentEntityRegistry is per-document only and safe to discard.
  Entity UID resolution always queries Neo4j so crash never causes duplicates.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional

from langchain_community.chat_models import ChatOllama
from langchain_core.messages import HumanMessage, SystemMessage
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


# ══════════════════════════════════════════════════════════════════════════════
# 0.  Config
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Config:
    # Neo4j
    neo4j_uri:      str = field(default_factory=lambda: os.getenv("NEO4J_URI",      "bolt://localhost:7687"))
    neo4j_user:     str = field(default_factory=lambda: os.getenv("NEO4J_USER",     "neo4j"))
    neo4j_password: str = field(default_factory=lambda: os.getenv("NEO4J_PASSWORD", "password"))
    neo4j_pool_size:int = 50

    # Ollama LLM
    llm_host:    str = field(default_factory=lambda: os.getenv("LLM_HOST",    "localhost"))
    llm_port:    str = field(default_factory=lambda: os.getenv("LLM_PORT",    "11434"))
    json_model:  str = field(default_factory=lambda: os.getenv("JSON_MODEL",  "qwen3:latest"))
    llm_num_ctx: int = field(default_factory=lambda: int(os.getenv("LLM_NUM_CTX", "8192")))

    # Chunking
    chunk_size:    int = 1200
    chunk_overlap: int = 200

    # Gleaning
    max_gleaning_rounds: int = 2
    gleaning_stop_below: int = 1   # stop if new entities < this

    # Ingestion
    max_entities_per_doc: int = 500


CFG = Config()


# ══════════════════════════════════════════════════════════════════════════════
# 1.  Connection helpers
# ══════════════════════════════════════════════════════════════════════════════

def get_neo4j_driver(cfg: Config = CFG):
    return GraphDatabase.driver(
        cfg.neo4j_uri,
        auth=(cfg.neo4j_user, cfg.neo4j_password),
        max_connection_pool_size=cfg.neo4j_pool_size,
    )


def get_llm(cfg: Config = CFG) -> ChatOllama:
    return ChatOllama(
        base_url=f"http://{cfg.llm_host}:{cfg.llm_port}",
        model=cfg.json_model,
        num_ctx=cfg.llm_num_ctx,
        temperature=0,
    )


# ══════════════════════════════════════════════════════════════════════════════
# 2.  Neo4j index + constraint setup  (run once on fresh DB)
# ══════════════════════════════════════════════════════════════════════════════

_INDEXES = [
    # Constraints — guarantee uniqueness + create implicit index
    "CREATE CONSTRAINT entity_uid_unique IF NOT EXISTS "
    "FOR (e:Entity) REQUIRE e.uid IS UNIQUE",

    "CREATE CONSTRAINT document_fsid_unique IF NOT EXISTS "
    "FOR (d:Document) REQUIRE d.fsid IS UNIQUE",

    "CREATE CONSTRAINT checkpoint_unique IF NOT EXISTS "
    "FOR (c:IngestionCheckpoint) REQUIRE c.document_id IS UNIQUE",

    # Regular indexes for lookup speed
    "CREATE INDEX entity_display_name IF NOT EXISTS "
    "FOR (e:Entity) ON (e.display_name)",

    "CREATE INDEX entity_norm_name IF NOT EXISTS "
    "FOR (e:Entity) ON (e.normalized_name)",

    "CREATE INDEX entity_type_idx IF NOT EXISTS "
    "FOR (e:Entity) ON (e.entity_type)",

    # Full-text index for fuzzy name matching during UID resolution
    """CREATE FULLTEXT INDEX entity_name_fulltext IF NOT EXISTS
       FOR (n:Entity) ON EACH [n.display_name, n.normalized_name, n.aliases]""",
]


def ensure_indexes(driver) -> None:
    """Create all required indexes. Safe to call repeatedly."""
    with driver.session() as session:
        for idx in _INDEXES:
            try:
                session.run(idx)
                logger.info(f"Index ensured: {idx[:80]}...")
            except Exception as e:
                logger.debug(f"Index skipped ({e}): {idx[:60]}...")


# ══════════════════════════════════════════════════════════════════════════════
# 3.  Utility helpers
# ══════════════════════════════════════════════════════════════════════════════

def _stable_uid(normalized_name: str) -> str:
    """Deterministic UID from normalized entity name."""
    return hashlib.sha256(normalized_name.encode()).hexdigest()[:24]


def _stable_rel_uid(src_uid: str, tgt_uid: str, rel_type: str) -> str:
    key = f"{src_uid}|{tgt_uid}|{rel_type}"
    return hashlib.sha256(key.encode()).hexdigest()[:24]


def _stable_chunk_id(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def normalize_entity_name(name: str) -> str:
    """
    Normalize entity name for deduplication.
    Lowercases, strips accents, collapses whitespace.
    """
    if not name:
        return ""
    name = name.strip()
    # Unicode normalize
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    # Lowercase + collapse whitespace
    name = re.sub(r"\s+", " ", name.lower()).strip()
    # Remove common noise
    noise = ["the ", "a ", "an ", "mr. ", "mrs. ", "dr. ", "shri ", "sri "]
    for n in noise:
        if name.startswith(n):
            name = name[len(n):]
    return name.strip()


def _clean_llm_output(raw: str) -> str:
    raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL)
    raw = re.sub(r"```(?:json)?\s*", "", raw).replace("```", "")
    return raw.strip()


def _parse_json_safe(raw: str) -> Any:
    cleaned = _clean_llm_output(raw)
    for pattern in (r"\[.*\]", r"\{.*\}"):
        m = re.search(pattern, cleaned, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass
    return None


# ══════════════════════════════════════════════════════════════════════════════
# 4.  Checkpoint system — persisted in Neo4j
# ══════════════════════════════════════════════════════════════════════════════

"""
Neo4j checkpoint node schema:
(:IngestionCheckpoint {
    document_id:    str,   ← unique
    status:         str,   ← 'pending' | 'in_progress' | 'completed' | 'failed'
    chunks_total:   int,
    chunks_done:    int,
    started_at:     int,   ← epoch ms
    completed_at:   int,   ← epoch ms or null
    error:          str,   ← null or error message
    retry_count:    int,
})
"""


def checkpoint_mark_started(
    driver,
    document_id:  str,
    chunks_total: int,
) -> None:
    cypher = """
    MERGE (c:IngestionCheckpoint {document_id: $doc_id})
    SET
        c.status       = 'in_progress',
        c.chunks_total = $total,
        c.chunks_done  = 0,
        c.started_at   = $now,
        c.error        = null,
        c.retry_count  = coalesce(c.retry_count, 0) + 1
    """
    with driver.session() as session:
        session.run(cypher, doc_id=document_id,
                    total=chunks_total, now=int(time.time() * 1000))


def checkpoint_update_progress(
    driver,
    document_id: str,
    chunks_done: int,
) -> None:
    cypher = """
    MATCH (c:IngestionCheckpoint {document_id: $doc_id})
    SET c.chunks_done = $done
    """
    with driver.session() as session:
        session.run(cypher, doc_id=document_id, done=chunks_done)


def checkpoint_mark_completed(driver, document_id: str) -> None:
    cypher = """
    MERGE (c:IngestionCheckpoint {document_id: $doc_id})
    SET
        c.status       = 'completed',
        c.completed_at = $now
    """
    with driver.session() as session:
        session.run(cypher, doc_id=document_id, now=int(time.time() * 1000))


def checkpoint_mark_failed(
    driver,
    document_id: str,
    error:       str,
) -> None:
    cypher = """
    MERGE (c:IngestionCheckpoint {document_id: $doc_id})
    SET
        c.status = 'failed',
        c.error  = $error
    """
    with driver.session() as session:
        session.run(cypher, doc_id=document_id, error=error[:1000])


def get_documents_to_process(
    driver,
    all_document_ids: list[str],
) -> list[str]:
    """
    Returns document IDs that are NOT yet successfully completed.
    Includes: never started, in_progress (crashed mid-way), failed.
    Excludes: completed.
    """
    cypher = """
    UNWIND $doc_ids AS doc_id
    OPTIONAL MATCH (c:IngestionCheckpoint {
        document_id: doc_id,
        status: 'completed'
    })
    WITH doc_id, c
    WHERE c IS NULL
    RETURN doc_id
    ORDER BY doc_id
    """
    with driver.session() as session:
        result = session.run(cypher, doc_ids=all_document_ids)
        return [rec["doc_id"] for rec in result]


def get_ingestion_status(driver) -> dict:
    """Returns summary of ingestion progress across all documents."""
    cypher = """
    MATCH (c:IngestionCheckpoint)
    RETURN
        c.status    AS status,
        count(c)    AS count
    """
    with driver.session() as session:
        result  = session.run(cypher)
        summary = {rec["status"]: rec["count"] for rec in result}
    return summary


# ══════════════════════════════════════════════════════════════════════════════
# 5.  Neo4j as GlobalEntityRegistry — UID resolution
# ══════════════════════════════════════════════════════════════════════════════

def resolve_entity_uid_from_neo4j(
    driver,
    display_name: str,
    entity_type:  str,
) -> str:
    """
    This function IS the global entity registry.
    Always queries Neo4j to find if entity already exists.
    Returns existing UID if found, stable new UID otherwise.

    This means:
    - After any crash, restarting a document always finds
      entities from previous documents correctly
    - No in-memory state needed for cross-document deduplication
    - Deterministic: same name always produces same UID

    Matching strategy (in order):
    1. Exact normalized name match (fastest)
    2. Alias match
    3. Falls back to generating new stable UID
    """
    norm = normalize_entity_name(display_name)
    if not norm:
        return _stable_uid(display_name.lower().strip())

    # Strategy 1: exact normalized name match
    cypher_exact = """
    MATCH (e:Entity)
    WHERE e.normalized_name = $norm
    RETURN e.uid AS uid
    LIMIT 1
    """
    with driver.session() as session:
        rec = session.run(cypher_exact, norm=norm).single()
        if rec:
            return rec["uid"]

    # Strategy 2: alias match
    cypher_alias = """
    MATCH (e:Entity)
    WHERE $norm IN coalesce(e.aliases_normalized, [])
    RETURN e.uid AS uid
    LIMIT 1
    """
    with driver.session() as session:
        rec = session.run(cypher_alias, norm=norm).single()
        if rec:
            return rec["uid"]

    # Strategy 3: stable new UID (deterministic — same name = same UID)
    return _stable_uid(norm)


# ══════════════════════════════════════════════════════════════════════════════
# 6.  DocumentEntityRegistry — per-document, in-memory only
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class EntityRecord:
    uid:           str
    display_name:  str
    normalized_name: str
    entity_type:   str
    description:   str
    aliases:       set[str]   = field(default_factory=set)
    chunk_ids:     set[str]   = field(default_factory=set)
    # NOTE: document_ids is always just {self.document_id}
    # kept for schema compatibility with Neo4j writer


@dataclass
class RelationRecord:
    rel_uid:        str
    source_uid:     str
    target_uid:     str
    relation_type:  str
    justification:  str
    chunk_ids:      set[str] = field(default_factory=set)


class DocumentEntityRegistry:
    """
    Lightweight per-document entity accumulator.

    Key properties:
    - Only holds entities and relationships for ONE document
    - Discarded completely after document is written to Neo4j
    - Resolves entity UIDs from Neo4j (crash-safe deduplication)
    - Safe to recreate from scratch after any crash
    - Does NOT accumulate cross-document state

    This solves:
    - Crash recovery: fresh registry per doc, UIDs from Neo4j
    - Cross-doc contamination: registry holds only current doc
    - Idempotency: MERGE writes mean safe re-runs
    """

    def __init__(self, document_id: str, driver):
        self.document_id = document_id
        self.driver      = driver
        self.entities:     dict[str, EntityRecord]   = {}  # uid → record
        self.relationships: dict[str, RelationRecord] = {}  # rel_uid → record

        # Local name → uid cache to avoid repeated Neo4j lookups
        # within the SAME document (not persisted, not cross-doc)
        self._local_uid_cache: dict[str, str] = {}

    def register_entity(
        self,
        display_name: str,
        entity_type:  str,
        description:  str,
        chunk_id:     str,
        aliases:      list[str] | None = None,
    ) -> Optional[str]:
        """
        Register entity for this document.
        Resolves UID from Neo4j — handles cross-document deduplication.
        Returns uid or None if name is invalid.
        """
        display_name = display_name.strip() if display_name else ""
        if not display_name:
            return None

        norm = normalize_entity_name(display_name)
        if not norm:
            return None

        # Check local cache first (saves Neo4j round-trips within same doc)
        cache_key = f"{norm}:{entity_type.lower()}"
        if cache_key in self._local_uid_cache:
            uid = self._local_uid_cache[cache_key]
        else:
            # Resolve from Neo4j — this is the crash-safe global lookup
            uid = resolve_entity_uid_from_neo4j(
                self.driver, display_name, entity_type
            )
            self._local_uid_cache[cache_key] = uid

        # Add to this document's registry
        if uid not in self.entities:
            self.entities[uid] = EntityRecord(
                uid=uid,
                display_name=display_name,
                normalized_name=norm,
                entity_type=entity_type or "Unknown",
                description=description or "",
                aliases=set(aliases or []),
            )
        else:
            rec = self.entities[uid]
            # Keep longest description
            if len(description or "") > len(rec.description):
                rec.description = description
            # Merge aliases
            if aliases:
                rec.aliases.update(aliases)

        # Track chunk provenance for THIS document
        self.entities[uid].chunk_ids.add(chunk_id)
        return uid

    def register_relationship(
        self,
        source_name:   str,
        target_name:   str,
        source_type:   str,
        target_type:   str,
        relation_type: str,
        justification: str,
        chunk_id:      str,
    ) -> Optional[str]:
        """
        Register relationship. Both endpoints must be in this registry.
        Returns rel_uid or None if endpoints not found.
        """
        # Resolve source and target UIDs
        src_uid = self._local_uid_cache.get(
            f"{normalize_entity_name(source_name)}:{source_type.lower()}"
        )
        tgt_uid = self._local_uid_cache.get(
            f"{normalize_entity_name(target_name)}:{target_type.lower()}"
        )

        # If not in local cache, resolve from Neo4j
        if not src_uid:
            src_uid = resolve_entity_uid_from_neo4j(
                self.driver, source_name, source_type
            )
        if not tgt_uid:
            tgt_uid = resolve_entity_uid_from_neo4j(
                self.driver, target_name, target_type
            )

        if not src_uid or not tgt_uid:
            return None

        rel_type = re.sub(r"\s+", "_", relation_type.upper())
        rel_uid  = _stable_rel_uid(src_uid, tgt_uid, rel_type)

        if rel_uid not in self.relationships:
            self.relationships[rel_uid] = RelationRecord(
                rel_uid=rel_uid,
                source_uid=src_uid,
                target_uid=tgt_uid,
                relation_type=rel_type,
                justification=justification or "",
            )
        else:
            # Accumulate justification evidence
            rec = self.relationships[rel_uid]
            if justification and justification not in rec.justification:
                rec.justification = (
                    rec.justification + " | " + justification
                    if rec.justification else justification
                )

        self.relationships[rel_uid].chunk_ids.add(chunk_id)
        return rel_uid

    def stats(self) -> dict:
        return {
            "document_id":   self.document_id,
            "entities":      len(self.entities),
            "relationships": len(self.relationships),
        }


# ══════════════════════════════════════════════════════════════════════════════
# 7.  Chunking
# ══════════════════════════════════════════════════════════════════════════════

def chunk_text(
    text:    str,
    size:    int = CFG.chunk_size,
    overlap: int = CFG.chunk_overlap,
) -> list[dict]:
    """
    Returns list of:
    {"chunk_id": str, "text": str, "index": int}
    chunk_id is stable sha256 of chunk text.
    """
    chunks = []
    start  = 0
    idx    = 0

    while start < len(text):
        end = min(start + size, len(text))

        # Try to break on sentence boundary
        if end < len(text):
            for sep in (". ", ".\n", "\n\n", "\n", " "):
                pos = text.rfind(sep, start, end)
                if pos > start + size // 2:
                    end = pos + len(sep)
                    break

        chunk_str = text[start:end].strip()
        if chunk_str:
            chunks.append({
                "chunk_id": _stable_chunk_id(chunk_str),
                "text":     chunk_str,
                "index":    idx,
            })
            idx += 1

        start = end - overlap if end < len(text) else end

    return chunks


# ══════════════════════════════════════════════════════════════════════════════
# 8.  LLM extraction prompts
# ══════════════════════════════════════════════════════════════════════════════

# ── Initial extraction prompt (your existing prompt, kept intact) ─────────

EXTRACTION_PROMPT = """
You are an expert Knowledge Graph construction engine specialized in Neo4j.
Your task is to extract entities and relationships from unstructured text
and output a STRICTLY VALID Neo4j-ingestable JSON graph.

CRITICAL OUTPUT RULES (NON-NEGOTIABLE):
- Output MUST be valid JSON.
- Output MUST contain ONLY ONE JSON object.
- Output MUST contain EXACTLY two top-level keys:
  1. "nodes"
  2. "relationships"
- Do NOT include markdown, comments, explanations, or extra text.
- Do NOT include code fences.
- Do NOT repeat or quote the input text.
- If any rule is violated, the output is INVALID.

GRAPH DATA MODEL (STRICT)

NODES
Each node MUST follow this exact structure:
{{
  "id": "N1",
  "label": "Person | Organization | Product | Technology | Event | Location | Algorithm | Year | Book | Movie",
  "properties": {{
    "name": "string (REQUIRED when applicable)",
    "document_id": "{document_id}",
    "chunk_id": {chunk_id},
    "...": "any additional relevant properties as key-value pairs"
  }}
}}

NODE RULES:
- Use ONE node per real-world entity (NO DUPLICATES).
- Do NOT use generic labels like "Entity"; use a meaningful label.
- The "name" property is REQUIRED for all nodes.
- The "document_id", "chunk_id" is REQUIRED for all nodes.
- Node IDs MUST be: Unique, Deterministic, Sequential (N1, N2, N3, ...)
- Reuse the SAME node ID whenever the same entity appears again.
- Do NOT store relationship information inside node properties.

RELATIONSHIPS
Each relationship MUST follow this exact structure:
{{
  "from": "N1",
  "to": "N2",
  "type": "UPPER_SNAKE_CASE_VERB",
  "properties": {{
    "justification": "CLAIM: <FROM display_name> <type verb phrase> <TO display_name>.\\nWHEN: <date if known>\\nDETAILS: <details>\\nEVIDENCE: <<=100 word snippet from text>",
    "...": "other metadata like date, location, role, quantity, etc"
  }}
}}

JUSTIFICATION RULES:
- "justification" is always REQUIRED for every relationship.
- The FIRST line MUST be the core claim: "CLAIM: <FROM> <verb> <TO>."
- Additional lines are OPTIONAL and MUST be included ONLY if in text:
  - "WHEN: <normalized date or time>"
  - "WHERE: <location phrase>"
  - "DETAILS: <weapon/operation/quantity/role/etc>"
  - "EVIDENCE: <short snippet from text, <=100 words>"
- Do NOT include a WHEN/WHERE line unless the text explicitly states it.
- Do NOT invent facts. If not stated, omit the line.

RELATIONSHIP RULES:
- Every relationship MUST reference valid node IDs in "from" and "to".
- Convert any action, interaction, or connection between entities into a
  short, meaningful verb or phrase in UPPER_SNAKE_CASE.
- Include relationships that are social, legal, scientific, academic,
  business, or technical.
- Always store relevant metadata (roles, dates, titles, quantities,
  locations, scores, URLs, etc).
- EVERY relationship MUST contain exactly four keys:
  "from", "to", "type", "properties".

MODELING GUIDELINES:
- Prefer relationships for actions/roles.
- Create separate nodes for distinct entities.
- Do not guess missing facts; if unsure, omit that node/relationship.
- Avoid unnecessary nodes/relationships.

INPUT TEXT:
{content}

FAILSAFE BEHAVIOR (IMPORTANT):
- If you detect no extractable entities: return
  <JSON>{{"nodes":[],"relationships":[]}}</JSON>
- If you detect entities but no reliable relationships: output nodes and
  relationships as [].
- Under no circumstances output questions or conversational text.

FINAL VALIDATION — STRICTLY NEEDED:
- Is the JSON structure valid?
- Are there EXACTLY two top-level keys?
- Are all node IDs unique and reused correctly?
- Are all relationships semantically correct?
- Does every relationship have exactly four keys?
- Can this JSON be ingested directly into Neo4j?

OUTPUT FORMAT (MUST FOLLOW EXACTLY):
Return ONLY the JSON wrapped like this, with nothing before or after:
<JSON>{{"nodes":[...],"relationships":[...]}}</JSON>
"""

# ── Gleaning prompt ───────────────────────────────────────────────────────

GLEANING_PROMPT = """
You are an expert Knowledge Graph construction engine specialized in Neo4j.
You previously extracted a partial graph from a text chunk.
Your ONLY job now is to find what was MISSED — do NOT repeat anything
already extracted.

════════════════════════════════════════════
ALREADY EXTRACTED (do NOT repeat these)
════════════════════════════════════════════

NODES ALREADY FOUND:
{already_nodes}

RELATIONSHIPS ALREADY FOUND:
{already_relationships}

════════════════════════════════════════════
ORIGINAL TEXT
════════════════════════════════════════════

{content}

════════════════════════════════════════════
YOUR TASK
════════════════════════════════════════════

Find ONLY the entities and relationships that were MISSED in the first pass.

Common things that get missed:
- People mentioned briefly or only by title/role
  (e.g. "the minister", "a spokesman", "senior official")
- Organizations referenced by acronym only
  (e.g. "JAH", "WeI", "JUH/A&M")
- Locations used as context
  (e.g. "in Lucknow", "near Karachi", "at the venue")
- Events or dates mentioned in passing
- Relationships implied by the text but not explicitly stated
- Secondary connections between already-found entities
- Aliases or alternate names for already-found entities

CRITICAL OUTPUT RULES (NON-NEGOTIABLE):
- Output MUST be valid JSON.
- Output MUST contain ONLY ONE JSON object.
- Output MUST contain EXACTLY two top-level keys: "nodes" and "relationships"
- Do NOT repeat nodes or relationships already in ALREADY EXTRACTED.
- Do NOT include markdown, comments, or extra text.
- Node IDs for NEW nodes MUST continue the sequence:
  existing nodes go up to N{last_node_index},
  start new nodes at N{next_node_index}.
- For NEW relationships between ALREADY EXISTING nodes,
  use their existing IDs.
- If truly nothing was missed, return:
  <JSON>{{"nodes":[],"relationships":[]}}</JSON>

GRAPH DATA MODEL (same as before — STRICT):

NODES — each new node MUST follow:
{{
  "id": "N{next_node_index}",
  "label": "Person | Organization | Location | Event | Product | Technology | Algorithm | Year | Book | Movie",
  "properties": {{
    "name": "string (REQUIRED)",
    "document_id": "{document_id}",
    "chunk_id": {chunk_id},
    "...": "any additional relevant properties"
  }}
}}

RELATIONSHIPS — each new relationship MUST follow:
{{
  "from": "<existing or new node ID>",
  "to":   "<existing or new node ID>",
  "type": "UPPER_SNAKE_CASE_VERB",
  "properties": {{
    "justification": "CLAIM: <from_name> <verb phrase> <to_name>.\\nEVIDENCE: <<=100 word snippet from text>"
  }}
}}

"justification" is REQUIRED for every relationship.
Do NOT invent facts. If unsure, omit the node/relationship.

FINAL VALIDATION before outputting:
- Are ALL new node IDs unique and not duplicating existing ones?
- Does every new relationship reference valid node IDs?
- Is every relationship semantically grounded in the text?

OUTPUT FORMAT (MUST FOLLOW EXACTLY):
Return ONLY the JSON wrapped like this, with nothing before or after:
<JSON>{{"nodes":[...],"relationships":[...]}}</JSON>
"""


# ══════════════════════════════════════════════════════════════════════════════
# 9.  LLM extraction helpers
# ══════════════════════════════════════════════════════════════════════════════

def _parse_extraction_response(raw: str) -> dict:
    """
    Robust parser for LLM extraction output.
    Handles: <JSON>...</JSON> wrapper, markdown fences,
             bare JSON, think tags.
    """
    # Strip think tags (qwen3, deepseek reasoning models)
    raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()

    # Try <JSON>...</JSON> wrapper first
    m = re.search(r"<JSON>(.*?)</JSON>", raw, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1).strip())
        except json.JSONDecodeError:
            pass

    # Strip markdown fences
    raw = re.sub(r"```(?:json)?\s*", "", raw).replace("```", "").strip()

    # Find outermost JSON object
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass

    return {"nodes": [], "relationships": []}


def _extract_initial(
    chunk_text:  str,
    chunk_id:    int,
    document_id: str,
    llm:         ChatOllama,
) -> dict:
    """Single extraction pass — returns {nodes, relationships}."""
    prompt = EXTRACTION_PROMPT.format(
        content=chunk_text,
        document_id=document_id,
        chunk_id=chunk_id,
    )
    messages = [
        SystemMessage(content=(
            "You are a Neo4j knowledge-graph extraction engine. "
            "Return ONLY a single JSON object wrapped in <JSON>...</JSON> "
            "with keys: nodes, relationships. "
            "Never ask questions. Never output explanations. "
            "Never output markdown. "
            "If extraction is not possible, return: "
            "<JSON>{\"nodes\":[],\"relationships\":[]}</JSON>."
        )),
        HumanMessage(content=prompt),
    ]
    result = llm.invoke(messages)
    raw    = result.content if hasattr(result, "content") else str(result)
    return _parse_extraction_response(raw)


def _extract_gleaning_round(
    chunk_text:      str,
    chunk_id:        int,
    document_id:     str,
    existing_nodes:  list[dict],
    existing_rels:   list[dict],
    llm:             ChatOllama,
    round_num:       int,
) -> dict:
    """One gleaning pass — returns only NEW {nodes, relationships}."""

    # Compact summary of already-found nodes (id + label + name only)
    node_summary = json.dumps([
        {
            "id":    n.get("id", ""),
            "label": n.get("label", ""),
            "name":  (n.get("properties") or {}).get("name", n.get("id", "")),
        }
        for n in existing_nodes
    ], indent=2)

    # Compact summary of already-found relationships
    rel_summary = json.dumps([
        {
            "from": r.get("from", ""),
            "to":   r.get("to",   ""),
            "type": r.get("type", ""),
            "justification": (
                (r.get("properties") or {}).get("justification", "")[:120]
            ),
        }
        for r in existing_rels
    ], indent=2)

    # Compute next node index
    last_index = 0
    for n in existing_nodes:
        nid    = str(n.get("id", "N0"))
        digits = re.sub(r"\D", "", nid)
        if digits:
            last_index = max(last_index, int(digits))
    next_index = last_index + 1

    prompt = GLEANING_PROMPT.format(
        already_nodes         = node_summary,
        already_relationships = rel_summary,
        content               = chunk_text,
        document_id           = document_id,
        chunk_id              = chunk_id,
        last_node_index       = last_index,
        next_node_index       = next_index,
    )

    messages = [
        SystemMessage(content=(
            "You are a Neo4j knowledge-graph extraction engine. "
            "Return ONLY new entities and relationships that were MISSED. "
            "Output ONLY a single JSON object wrapped in <JSON>...</JSON> "
            "with keys: nodes, relationships. "
            "Never repeat already-extracted items. "
            "If nothing was missed, return: "
            "<JSON>{\"nodes\":[],\"relationships\":[]}</JSON>."
        )),
        HumanMessage(content=prompt),
    ]

    result = llm.invoke(messages)
    raw    = result.content if hasattr(result, "content") else str(result)
    parsed = _parse_extraction_response(raw)

    new_nodes = parsed.get("nodes",         []) or []
    new_rels  = parsed.get("relationships",  []) or []

    # Hard dedup: remove any node whose ID already exists
    existing_ids = {str(n.get("id", "")) for n in existing_nodes}
    new_nodes    = [n for n in new_nodes
                    if str(n.get("id", "")) not in existing_ids]

    logger.info(
        f"    [Gleaning round {round_num}] "
        f"+{len(new_nodes)} nodes, +{len(new_rels)} rels"
    )
    return {"nodes": new_nodes, "relationships": new_rels}


def extract_chunk_with_gleaning(
    chunk_text_str: str,
    chunk_id:       int,
    document_id:    str,
    llm:            ChatOllama,
    max_gleaning:   int = CFG.max_gleaning_rounds,
    stop_below:     int = CFG.gleaning_stop_below,
) -> dict:
    """
    Full extraction for one chunk:
    1. Initial extraction pass
    2. Up to max_gleaning gleaning rounds
    3. Early stop if new entities < stop_below

    Returns merged {nodes, relationships}.
    """
    # ── Initial pass ──────────────────────────────────────────────────────
    result    = _extract_initial(chunk_text_str, chunk_id, document_id, llm)
    all_nodes = list(result.get("nodes",        []) or [])
    all_rels  = list(result.get("relationships", []) or [])

    logger.info(
        f"  [Pass 0 — initial] "
        f"{len(all_nodes)} nodes, {len(all_rels)} rels"
    )

    # ── Gleaning rounds ───────────────────────────────────────────────────
    for round_num in range(1, max_gleaning + 1):
        glean = _extract_gleaning_round(
            chunk_text_str, chunk_id, document_id,
            all_nodes, all_rels, llm, round_num,
        )
        new_nodes = glean.get("nodes",        [])
        new_rels  = glean.get("relationships", [])

        all_nodes.extend(new_nodes)
        all_rels.extend(new_rels)

        # Early stop
        if len(new_nodes) < stop_below and len(new_rels) < stop_below:
            logger.info(
                f"  [Gleaning] Early stop after round {round_num}"
            )
            break

    return {"nodes": all_nodes, "relationships": all_rels}


# ══════════════════════════════════════════════════════════════════════════════
# 10.  Registry population from LLM output
# ══════════════════════════════════════════════════════════════════════════════

def populate_registry_from_extraction(
    extraction:  dict,
    chunk_id:    str,
    document_id: str,
    registry:    DocumentEntityRegistry,
) -> None:
    """
    Takes raw LLM extraction output (nodes + relationships)
    and populates the DocumentEntityRegistry.

    Handles the node ID → uid mapping internally.
    """
    nodes         = extraction.get("nodes",        []) or []
    relationships = extraction.get("relationships", []) or []

    # Map: LLM local node ID (e.g. "N1") → canonical uid in registry
    local_id_to_uid: dict[str, str] = {}

    # ── Register entities ─────────────────────────────────────────────────
    for node in nodes:
        props       = node.get("properties") or {}
        local_id    = str(node.get("id", "")).strip()
        label       = (node.get("label") or "Entity").strip()
        name        = str(props.get("name") or node.get("id") or "").strip()
        description = str(props.get("description") or
                          props.get("summary") or "").strip()

        # Collect aliases from alternate name fields
        aliases = []
        for alias_key in ("aliases", "alternate_names", "also_known_as"):
            av = props.get(alias_key)
            if isinstance(av, list):
                aliases.extend([str(a) for a in av if a])
            elif isinstance(av, str) and av:
                aliases.append(av)

        if not name or not local_id:
            continue

        uid = registry.register_entity(
            display_name=name,
            entity_type=label,
            description=description,
            chunk_id=chunk_id,
            aliases=aliases,
        )
        if uid:
            local_id_to_uid[local_id] = uid

    # ── Register relationships ────────────────────────────────────────────
    for rel in relationships:
        from_local = str(rel.get("from", "")).strip()
        to_local   = str(rel.get("to",   "")).strip()
        rel_type   = str(rel.get("type", "RELATED_TO")).strip()
        props      = rel.get("properties") or {}
        just       = str(props.get("justification") or "").strip()

        # Resolve local IDs to canonical UIDs
        src_uid = local_id_to_uid.get(from_local)
        tgt_uid = local_id_to_uid.get(to_local)

        if not src_uid or not tgt_uid:
            # Try to resolve by querying registry entities
            logger.debug(
                f"  Skipping rel {from_local}→{to_local}: "
                f"endpoint not in local_id_to_uid"
            )
            continue

        # Get display names for relationship registration
        src_entity  = registry.entities.get(src_uid)
        tgt_entity  = registry.entities.get(tgt_uid)
        src_name    = src_entity.display_name if src_entity else from_local
        tgt_name    = tgt_entity.display_name if tgt_entity else to_local
        src_type    = src_entity.entity_type  if src_entity else "Unknown"
        tgt_type    = tgt_entity.entity_type  if tgt_entity else "Unknown"

        registry.register_relationship(
            source_name=src_name,
            target_name=tgt_name,
            source_type=src_type,
            target_type=tgt_type,
            relation_type=rel_type,
            justification=just,
            chunk_id=chunk_id,
        )


# ══════════════════════════════════════════════════════════════════════════════
# 11.  Neo4j writer — batched, idempotent MERGE writes
# ══════════════════════════════════════════════════════════════════════════════

def _has_apoc(session) -> bool:
    try:
        session.run("RETURN apoc.version()").single()
        return True
    except Exception:
        return False


def write_document_to_neo4j(
    driver,
    document_id: str,
    registry:    DocumentEntityRegistry,
) -> None:
    """
    Writes all entities and relationships from DocumentEntityRegistry
    to Neo4j using batched MERGE operations.

    Idempotent: safe to run multiple times (MERGE + SET).
    Cross-document safe: entities shared across documents are merged,
    not duplicated — MERGE on uid guarantees this.
    """
    entities  = registry.entities
    relations = registry.relationships

    if not entities:
        logger.warning(f"No entities to write for {document_id}")
        return

    with driver.session() as session:
        apoc = _has_apoc(session)

        # ── 1. Ensure Document node ───────────────────────────────────────
        session.run(
            "MERGE (d:Document {fsid: $fsid})",
            fsid=document_id,
        )

        # ── 2. Batch upsert Entity nodes ──────────────────────────────────
        # MERGE on uid — if entity already exists (from another doc),
        # we only update description if longer and accumulate aliases.
        # We NEVER overwrite display_name to preserve first-seen casing.
        entity_list = [
            {
                "uid":             e.uid,
                "display_name":    e.display_name,
                "normalized_name": e.normalized_name,
                "entity_type":     e.entity_type,
                "description":     e.description or "",
                "aliases":         sorted(e.aliases),
                "aliases_norm":    sorted(
                    normalize_entity_name(a) for a in e.aliases
                ),
            }
            for e in entities.values()
        ]

        if apoc:
            cypher_entity = """
            UNWIND $items AS item
            MERGE (e:Entity {uid: item.uid})
            ON CREATE SET
                e.display_name     = item.display_name,
                e.normalized_name  = item.normalized_name,
                e.entity_type      = item.entity_type,
                e.description      = item.description,
                e.aliases          = item.aliases,
                e.aliases_normalized = item.aliases_norm
            ON MATCH SET
                e.entity_type = CASE
                    WHEN e.entity_type IN ['Unknown','',null]
                    THEN item.entity_type ELSE e.entity_type END,
                e.description = CASE
                    WHEN size(item.description) > size(coalesce(e.description,''))
                    THEN item.description ELSE e.description END,
                e.aliases = apoc.coll.toSet(
                    coalesce(e.aliases,[]) + item.aliases
                ),
                e.aliases_normalized = apoc.coll.toSet(
                    coalesce(e.aliases_normalized,[]) + item.aliases_norm
                )
            """
        else:
            cypher_entity = """
            UNWIND $items AS item
            MERGE (e:Entity {uid: item.uid})
            ON CREATE SET
                e.display_name     = item.display_name,
                e.normalized_name  = item.normalized_name,
                e.entity_type      = item.entity_type,
                e.description      = item.description,
                e.aliases          = item.aliases,
                e.aliases_normalized = item.aliases_norm
            ON MATCH SET
                e.entity_type = CASE
                    WHEN e.entity_type IN ['Unknown','',null]
                    THEN item.entity_type ELSE e.entity_type END,
                e.description = CASE
                    WHEN size(item.description) > size(coalesce(e.description,''))
                    THEN item.description ELSE e.description END
            """

        session.run(cypher_entity, items=entity_list)
        logger.debug(f"  Wrote {len(entity_list)} entity nodes")

        # ── 3. Batch MENTIONED_IN edges ───────────────────────────────────
        # Scoped to this document — accumulates chunk_ids
        if apoc:
            cypher_mentioned = """
            UNWIND $items AS item
            MATCH (e:Entity {uid: item.uid})
            MATCH (d:Document {fsid: $fsid})
            MERGE (e)-[m:MENTIONED_IN]->(d)
            ON CREATE SET m.chunk_ids = item.chunk_ids
            ON MATCH  SET m.chunk_ids = apoc.coll.toSet(
                coalesce(m.chunk_ids,[]) + item.chunk_ids
            )
            """
        else:
            cypher_mentioned = """
            UNWIND $items AS item
            MATCH (e:Entity {uid: item.uid})
            MATCH (d:Document {fsid: $fsid})
            MERGE (e)-[m:MENTIONED_IN]->(d)
            ON CREATE SET m.chunk_ids = item.chunk_ids
            """

        session.run(
            cypher_mentioned,
            fsid=document_id,
            items=[
                {
                    "uid":       e.uid,
                    "chunk_ids": sorted(e.chunk_ids),
                }
                for e in entities.values()
            ],
        )

        # ── 4. Batch relationship edges (grouped by type) ─────────────────
        # Group by relation_type — each type needs its own MERGE pattern
        by_type: dict[str, list] = defaultdict(list)
        for rel in relations.values():
            if (rel.source_uid in entities and
                    rel.target_uid in entities):
                by_type[rel.relation_type].append(rel)

        for rel_type, rels in by_type.items():
            if apoc:
                cypher_rel = f"""
                UNWIND $rels AS r
                MATCH (a:Entity {{uid: r.src_uid}})
                MATCH (b:Entity {{uid: r.tgt_uid}})
                MERGE (a)-[rel:{rel_type} {{rel_uid: r.rel_uid}}]->(b)
                ON CREATE SET
                    rel.justification = r.justification,
                    rel.description   = r.justification,
                    rel.chunk_ids     = r.chunk_ids
                ON MATCH SET
                    rel.chunk_ids = apoc.coll.toSet(
                        coalesce(rel.chunk_ids,[]) + r.chunk_ids
                    ),
                    rel.justification = CASE
                        WHEN size(r.justification) >
                             size(coalesce(rel.justification,''))
                        THEN r.justification
                        ELSE rel.justification END
                """
            else:
                cypher_rel = f"""
                UNWIND $rels AS r
                MATCH (a:Entity {{uid: r.src_uid}})
                MATCH (b:Entity {{uid: r.tgt_uid}})
                MERGE (a)-[rel:{rel_type} {{rel_uid: r.rel_uid}}]->(b)
                ON CREATE SET
                    rel.justification = r.justification,
                    rel.description   = r.justification,
                    rel.chunk_ids     = r.chunk_ids
                ON MATCH SET
                    rel.justification = CASE
                        WHEN size(r.justification) >
                             size(coalesce(rel.justification,''))
                        THEN r.justification
                        ELSE rel.justification END
                """

            session.run(
                cypher_rel,
                rels=[
                    {
                        "src_uid":      r.source_uid,
                        "tgt_uid":      r.target_uid,
                        "rel_uid":      r.rel_uid,
                        "justification":r.justification or "",
                        "chunk_ids":    sorted(r.chunk_ids),
                    }
                    for r in rels
                ],
            )

        logger.debug(
            f"  Wrote {len(relations)} relationships "
            f"({len(by_type)} types)"
        )


# ══════════════════════════════════════════════════════════════════════════════
# 12.  Single document ingestion
# ══════════════════════════════════════════════════════════════════════════════

def ingest_single_document(
    text:        str,
    document_id: str,
    driver,
    llm:         ChatOllama,
    cfg:         Config = CFG,
) -> dict:
    """
    Ingest one document completely.

    Flow:
    1. Chunk text
    2. For each chunk: extract + gleaning → populate registry
    3. Write registry to Neo4j (batched MERGE)
    4. Mark checkpoint completed

    Registry is per-document and discarded after write.
    UID resolution always goes to Neo4j → crash safe.
    """
    chunks = chunk_text(text, cfg.chunk_size, cfg.chunk_overlap)
    logger.info(
        f"[{document_id}] Starting: {len(chunks)} chunks"
    )

    # Checkpoint: mark started
    checkpoint_mark_started(driver, document_id, len(chunks))

    # Fresh registry — only holds this document's data
    registry = DocumentEntityRegistry(
        document_id=document_id,
        driver=driver,
    )

    for i, chunk in enumerate(chunks):
        chunk_id     = chunk["chunk_id"]
        chunk_text_s = chunk["text"]

        logger.info(
            f"  [{document_id}] Chunk {i+1}/{len(chunks)} "
            f"(id={chunk_id})"
        )

        # Extract with gleaning
        extraction = extract_chunk_with_gleaning(
            chunk_text_str=chunk_text_s,
            chunk_id=i,            # integer index for LLM context
            document_id=document_id,
            llm=llm,
            max_gleaning=cfg.max_gleaning_rounds,
            stop_below=cfg.gleaning_stop_below,
        )

        # Populate registry from extraction
        populate_registry_from_extraction(
            extraction=extraction,
            chunk_id=chunk_id,     # stable hash string for provenance
            document_id=document_id,
            registry=registry,
        )

        # Update progress checkpoint after each chunk
        checkpoint_update_progress(driver, document_id, i + 1)

        logger.info(
            f"  [{document_id}] After chunk {i+1}: "
            f"{registry.stats()}"
        )

    # Write entire document to Neo4j in one batched operation
    logger.info(f"[{document_id}] Writing to Neo4j...")
    write_document_to_neo4j(driver, document_id, registry)

    # Mark completed
    checkpoint_mark_completed(driver, document_id)

    stats = {
        "document_id":   document_id,
        "chunks":        len(chunks),
        "entities":      len(registry.entities),
        "relationships": len(registry.relationships),
    }
    logger.info(f"[{document_id}] Completed: {stats}")

    # Registry goes out of scope here — garbage collected
    # No cross-document state retained
    return stats


# ══════════════════════════════════════════════════════════════════════════════
# 13.  Batch ingestion with crash recovery
# ══════════════════════════════════════════════════════════════════════════════

def ingest_documents(
    documents:   list[dict],    # [{"text": str, "document_id": str}, ...]
    driver=None,
    llm:         ChatOllama | None = None,
    cfg:         Config = CFG,
    resume:      bool = True,   # set False to reprocess all docs
) -> list[dict]:
    """
    Ingest multiple documents with full crash recovery.

    If resume=True (default):
      - Checks Neo4j checkpoints
      - Skips already-completed documents
      - Resumes from first incomplete document

    If resume=False:
      - Processes all documents regardless of checkpoint state

    Each document gets a fresh DocumentEntityRegistry.
    UIDs resolved from Neo4j → no cross-doc contamination.
    Safe to call multiple times — idempotent.

    Usage:
        driver = get_neo4j_driver()
        ensure_indexes(driver)

        docs = [
            {"text": "...", "document_id": "doc_001"},
            {"text": "...", "document_id": "doc_002"},
        ]
        stats = ingest_documents(docs, driver=driver)
    """
    driver = driver or get_neo4j_driver(cfg)
    llm    = llm    or get_llm(cfg)

    all_ids = [d["document_id"] for d in documents]

    # Determine which documents to process
    if resume:
        to_process = set(get_documents_to_process(driver, all_ids))
        skipped    = len(all_ids) - len(to_process)
        if skipped > 0:
            logger.info(
                f"Crash recovery: skipping {skipped} completed documents, "
                f"processing {len(to_process)} remaining"
            )
    else:
        to_process = set(all_ids)
        logger.info(f"Processing all {len(to_process)} documents (resume=False)")

    all_stats = []

    for doc in documents:
        doc_id = doc["document_id"]

        if doc_id not in to_process:
            logger.debug(f"Skipping completed: {doc_id}")
            all_stats.append({"document_id": doc_id, "status": "skipped"})
            continue

        try:
            stats = ingest_single_document(
                text=doc["text"],
                document_id=doc_id,
                driver=driver,
                llm=llm,
                cfg=cfg,
            )
            stats["status"] = "completed"
            all_stats.append(stats)

        except Exception as e:
            logger.error(
                f"Failed to ingest {doc_id}: {e}",
                exc_info=True,
            )
            checkpoint_mark_failed(driver, doc_id, str(e))
            all_stats.append({
                "document_id": doc_id,
                "status":      "failed",
                "error":       str(e),
            })
            # Continue with next document — never abort the batch

    # Print final summary
    completed = sum(1 for s in all_stats if s.get("status") == "completed")
    failed    = sum(1 for s in all_stats if s.get("status") == "failed")
    skipped   = sum(1 for s in all_stats if s.get("status") == "skipped")
    logger.info(
        f"Batch complete: {completed} done, "
        f"{failed} failed, {skipped} skipped"
    )

    return all_stats


# ══════════════════════════════════════════════════════════════════════════════
# 14.  CLI entry point
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python ingestion.py setup")
        print("  python ingestion.py ingest <text_file> [document_id]")
        print("  python ingestion.py status")
        sys.exit(1)

    cmd    = sys.argv[1]
    driver = get_neo4j_driver()

    if cmd == "setup":
        ensure_indexes(driver)
        print("Neo4j indexes created.")

    elif cmd == "ingest":
        if len(sys.argv) < 3:
            print("Usage: python ingestion.py ingest <text_file> [document_id]")
            sys.exit(1)
        path    = sys.argv[2]
        doc_id  = sys.argv[3] if len(sys.argv) > 3 else os.path.basename(path)
        with open(path, encoding="utf-8") as f:
            text = f.read()
        result = ingest_documents(
            documents=[{"text": text, "document_id": doc_id}],
            driver=driver,
        )
        print(json.dumps(result, indent=2))

    elif cmd == "status":
        status = get_ingestion_status(driver)
        print(json.dumps(status, indent=2))

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)

    driver.close()
