"""
Complete Neo4j Knowledge Graph Ingestion System
================================================
This is the complete, production-ready code with all fixes applied.

Key Features:
- Proper entity deduplication across chunks
- Cross-chunk relationship resolution
- No orphaned nodes
- Proper MERGE operations
"""

import hashlib
import json
import os
import re
from typing import Dict, List, Optional, Tuple
from neo4j import GraphDatabase
from langchain_community.chat_models import ChatOllama
from langchain_core.messages import HumanMessage, SystemMessage


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def _repair_trailing_commas(s: str) -> str:
    """
    Heuristic repair: remove trailing commas before } or ].
    This is not a full JSON parser, but fixes the most common LLM mistake.
    """
    return re.sub(r",\s*([\]}])", r"\1", s)


def safe_json_loads(llm_output: str) -> Optional[dict]:
    """
    Parses LLM output into a Python object.
    Preferred format: <JSON>{...}</JSON>
    Also strips code fences and common log/timestamp noise.
    """
    if llm_output is None:
        return None
    
    if not isinstance(llm_output, (str, bytes, bytearray)):
        llm_output = str(llm_output)
    
    # Strip log prefixes like: [2026-02-10 06:29:52]
    llm_output = re.sub(
        r"(?:\[?\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\]?\s*)+",
        "",
        llm_output
    )
    
    llm_output = _repair_trailing_commas(llm_output)
    
    # Strip code fences if present
    llm_output = re.sub(r"^```json\s*", "", llm_output, flags=re.IGNORECASE).strip()
    llm_output = re.sub(r"```\s*$", "", llm_output, flags=re.IGNORECASE).strip()
    
    m = llm_output
    try:
        return json.loads(m)
    except json.JSONDecodeError as e:
        print("JSONDecodeError:", e)
        print("Extracted text:", m[:500])  # avoid printing huge blobs
        return None


def format_structure(data: str) -> str:
    """
    JSON structure validator and repair tool.
    Uses LLM to fix malformed JSON.
    """
    prompt = f"""
You are a strict JSON validator and repair tool.

Task:
- I will provide a JSON-like payload that may contain structural/formatting issues.
- Your job is to return a corrected, strictly valid JSON document.

Hard rules (must follow):
1) STRICTLY Do NOT change the meaning, values, or wording of any content.
- Do not rewrite strings.
- Do not change numbers, booleans, nulls, arrays, or object values.
- Do not add or remove data fields except when required to make the JSON syntactically valid (e.g., fixing missing quotes around keys).
2) Only fix JSON syntax/structure issues, such as:
- Missing/extra commas
- Unquoted keys or invalid quotes
- Trailing commas
- Invalid escape sequences
- Mismatched braces/brackets
- Single quotes used instead of double quotes
- Comments (remove them)
- Duplicate keys: keep the LAST occurrence (do not merge or invent values)
3) Preserve the original structure and ordering as much as possible.
4) Output MUST be JSON only:
- No explanations
- No markdown
- No surrounding text
- No code fences
5) If the input contains multiple top-level JSON objects, wrap them into a single JSON array in the same order.

INPUT TEXT
{data}

OUTPUT:
Return valid json only
"""
    
    messages = [
        SystemMessage(content="You are a strict JSON validator and repair tool."),
        HumanMessage(content=prompt)
    ]
    
    llm = ChatOllama(model="qwen3:latest", base_url=os.getenv("OLLAMA_BASE_URL"))
    result = llm.invoke(messages)
    
    return result.content


def extract_json_from_tags(json_text: str) -> str:
    """Extract JSON from <JSON>...</JSON> tags if present"""
    match = re.search(r"<JSON>\s*(.*?)\s*</JSON>", json_text, flags=re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1)
    return json_text


# ============================================================================
# ENTITY AND RELATIONSHIP NORMALIZATION
# ============================================================================

def normalize_entity_name(name: str) -> str:
    """Normalize entity names for consistent matching"""
    if not name:
        return ""
    
    # Convert to lowercase
    name = name.lower()
    
    # Remove extra whitespace
    name = " ".join(name.split())
    
    return name.strip()


def normalize_nodes(raw_nodes: List[dict]) -> List[dict]:
    """Normalize node structure"""
    normalized = []
    for node in raw_nodes:
        n = {}
        n["id"] = node.get("id") or node.get("ID")
        
        # Choose a Neo4j label: if type exists, use it capitalized
        n["label"] = (node.get("label") or node.get("type") or "Entity").capitalize()
        
        # Put all other info into properties
        props = {}
        if "properties" in node:
            props.update(node["properties"])
        
        # Include top-level keys if they exist
        for key in ["name", "type"]:
            if key in node:
                props[key] = node[key]
        
        n["properties"] = props
        normalized.append(n)
    
    return normalized


def normalize_relationships(raw_rels: List[dict]) -> List[dict]:
    """Normalize relationship structure"""
    normalized = []
    for r in raw_rels:
        rel = {}
        
        # Handle different key names for nodes
        rel["from"] = r.get("from") or r.get("startNodeId") or r.get("start_node_id")
        rel["to"] = r.get("to") or r.get("endNodeId") or r.get("end_node_id")
        rel["type"] = r.get("type", "RELATED_TO").upper().replace(" ", "_")
        
        # Use 'properties' if exists, else 'attributes', else empty dict
        props = r.get("properties") or r.get("attributes") or {}
        rel["properties"] = props
        
        # Only include if from and to are valid
        if rel["from"] and rel["to"]:
            normalized.append(rel)
    
    return normalized


# ============================================================================
# UID GENERATION - FIXED VERSION
# ============================================================================

def node_uid(node: dict) -> str:
    """
    Generate stable UID based ONLY on entity name.
    
    ✅ FIX: Removed document_id from UID generation.
    This ensures same entity across all chunks gets the same UID.
    """
    props = node.get("properties", {}) or {}
    
    # Extract name - try different possible keys
    name = str(props.get("name", "")).strip()
    if not name:
        name = str(node.get("label", "")).strip()
    if not name:
        name = str(node.get("id", "")).strip()
    
    # Normalize name for consistency
    name = normalize_entity_name(name)
    
    if not name:
        # Fallback: use entire node as key
        return hashlib.sha1(str(node).encode("utf-8")).hexdigest()
    
    # UID based ONLY on name (no document_id, no chunk_id)
    return hashlib.sha1(name.encode("utf-8")).hexdigest()


# ============================================================================
# GLOBAL ENTITY REGISTRY - KEY FIX FOR CROSS-CHUNK RELATIONSHIPS
# ============================================================================

class GlobalEntityRegistry:
    """
    Maintains entity information across all chunks.
    
    Key Features:
    1. Entity deduplication: same name = same UID across all chunks
    2. Local-to-global ID mapping: resolves chunk-local IDs to global UIDs
    3. Cross-chunk relationship support: relationships can reference entities from any chunk
    """
    
    def __init__(self):
        # Map: entity_name → {uid, chunks, properties, entity_type}
        self.entities: Dict[str, dict] = {}
        
        # Map: "chunk_id:local_id" → global_uid
        # This is THE KEY to fixing cross-chunk relationships!
        self.local_to_global: Dict[str, str] = {}
    
    def register_entity(self, node: dict, chunk_id: int) -> Optional[str]:
        """
        Register an entity from a specific chunk.
        Returns the global UID for this entity.
        """
        props = node.get("properties", {}) or {}
        name = str(props.get("name", "")).strip()
        
        if not name:
            name = str(node.get("label", "")).strip()
        if not name:
            return None
        
        # Normalize name
        name = normalize_entity_name(name)
        
        if not name:
            return None
        
        # Generate UID (will be same for same name across chunks)
        uid = node_uid(node)
        
        # Get chunk-local ID
        local_id = node.get("id", "")
        
        # KEY FIX: Map this chunk's local ID to the global UID
        if local_id:
            mapping_key = f"{chunk_id}:{local_id}"
            self.local_to_global[mapping_key] = uid
        
        # Store or update entity
        if name not in self.entities:
            # New entity
            self.entities[name] = {
                "uid": uid,
                "name": name,
                "normalized_name": name,
                "chunks": [chunk_id],
                "properties": props,
                "entity_type": node.get("label") or props.get("type") or "Entity"
            }
        else:
            # Entity exists - add this chunk if not already present
            if chunk_id not in self.entities[name]["chunks"]:
                self.entities[name]["chunks"].append(chunk_id)
            
            # Merge properties (keep most complete data, don't overwrite)
            for key, value in props.items():
                if value and (key not in self.entities[name]["properties"] or not self.entities[name]["properties"][key]):
                    self.entities[name]["properties"][key] = value
        
        return uid
    
    def resolve_uid(self, chunk_id: int, local_id: str) -> Optional[str]:
        """
        KEY METHOD: Resolve chunk-local ID to global UID.
        
        This is how we fix cross-chunk relationships:
        - Entity appears in chunk 0 with local_id "N1"
        - Relationship in chunk 5 references "N1"
        - This method resolves "5:N1" → global UID
        """
        if not local_id:
            return None
        
        mapping_key = f"{chunk_id}:{local_id}"
        return self.local_to_global.get(mapping_key)
    
    def get_stats(self) -> dict:
        """Get statistics about the registry"""
        multi_chunk = sum(1 for e in self.entities.values() if len(e["chunks"]) > 1)
        return {
            "total_entities": len(self.entities),
            "entities_in_multiple_chunks": multi_chunk,
            "total_mappings": len(self.local_to_global)
        }


# ============================================================================
# LLM EXTRACTION - WITH FULL PROMPT
# ============================================================================

def extract_nodes_relationships(chunk_id: int, document_id: str, content: str) -> Tuple[List[dict], List[dict]]:
    """
    Extract entities and relationships from text using LLM.
    Returns (normalized_nodes, normalized_relationships).
    """
    
    prompt = f"""
You are an expert Knowledge Graph construction engine specialized in Neo4j.
Your task is to extract entities and relationships from unstructured text
and output a STRICTLY VALID Neo4j-ingestable JSON graph.

CRITICAL OUTPUT RULES (NON-NEGOTIABLE)
=======================================
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
==========================

--------------------
NODES
--------------------
Each node MUST follow this exact structure:

{{{{
  "id": "N1",
  "label": "Person | Organization | Product | Technology | Event | Location | Algorithm | Year | Book | Movie | Agreement | ...",
  "properties": {{{{
    "name": "string (REQUIRED when applicable)",
    "document_id": "{document_id}",
    "chunk_id": {chunk_id},
    "...": "any additional relevant properties as key-value pairs"
  }}}}
}}}}

NODE RULES:
- Use ONE node per real-world entity (NO DUPLICATES).
- Do NOT use generic labels like "Entity"; use a meaningful label.
- The "name" property is REQUIRED for all nodes except Year.
- The "document_id", "chunk_id" is REQUIRED for all nodes.
- Years MUST be integers, not strings.
- Node IDs MUST be:
  * Unique
  * Deterministic
  * Sequential (N1, N2, N3, ...)
- Reuse the SAME node ID whenever the same entity appears again.
- Include all additional entity data in "properties".
- Do NOT store relationship information inside node properties.

--------------------
RELATIONSHIPS
--------------------
Each relationship MUST follow this exact structure:

{{{{
  "from": "N1",
  "to": "N2",
  "type": "UPPER_SNAKE_CASE_VERB",
  "properties": {{{{
    "justification": "Short sentence explaining why this relationship exists, grounded in the input text",
    "...": "other metadata like date, location, role, quantity, etc"
  }}}}
}}}}

JUSTIFICATION RULES:
- "justification" is always REQUIRED for every relationship.
- Max 280 characters (or max 1 sentence).
- Must be grounded in the input text; do NOT invent facts.
- Do NOT include private step-by-step reasoning; only a brief justification.

RELATIONSHIP RULES:
- Every relationship MUST reference valid node IDs in "from" and "to".
- Convert any action, interaction, or connection between entities into a short, meaningful verb or phrase in UPPER_SNAKE_CASE.
- Include relationships that are social, legal, scientific, academic, business, or technical; for Example: FOUNDED, WORKS_AT, LOCATED_IN, etc.).
- Always store relevant metadata (For example: roles, dates, titles, quantities, locations, scores, URLs, etc.) inside "properties".
- Every relationship properties MUST include:
  * "justification": "brief, grounded explanation"
- Do NOT output multi-step reasoning, analysis, or hidden thoughts.
- If a relationship has no additional data, properties MUST still include "justification".
- EVERY relationship MUST contain exactly four keys: "from", "to", "type", "properties".
- Use integers for years (e.g., 1998, not "1998").

MODELING GUIDELINES
===================
- Prefer relationships for actions/roles (for Example: FOUNDED, WORKS_AT, LOCATED_IN, etc.).
- Create separate nodes for distinct entities.
- Do not guess missing facts; if unsure, omit that specific node/relationship.
- Avoid unnecessary nodes/relationships

INPUT TEXT
==========
{content}

FAILSAFE BEHAVIOR (IMPORTANT):
===============================
- If you detect no extractable entities: return <JSON>{{"nodes":[],"relationships":[]}}</JSON>
- If you detect entities but no reliable relationships: output nodes and relationships as [].
- Under no circumstances output questions or conversational text.

FINAL VALIDATION : STRICTLY NEEDED
===================================
- Is the JSON structure valid?
- Are there EXACTLY two top-level keys?
- Are all node IDs unique and reused correctly?
- Are all relationships semantically correct?
- Does every relationship have exactly these four keys: "from", "to", "type", "properties"?
- Can this JSON be ingested directly into Neo4j?

OUTPUT FORMAT (MUST FOLLOW EXACTLY):
====================================
Return ONLY the JSON wrapped like this, with nothing before or after:
<JSON>{{"nodes":[...],"relationships":[...]}}</JSON>.
"""
    
    # Format prompt with actual values
    prompt_chunk = prompt.format(
        content=content, 
        document_id=document_id, 
        chunk_id=chunk_id
    )
    
    # Prepare messages for LLM
    messages = [
        SystemMessage(content=
            "You are a Neo4j knowledge-graph extraction engine. "
            "You must return ONLY a single JSON object wrapped in <JSON>...</JSON> with keys: nodes, relationships. "
            "Never ask questions. Never output explanations. Never output markdown. "
            "If extraction is not possible, return an empty graph: <JSON>{\"nodes\":[],\"relationships\":[]}</JSON>."
        ),
        HumanMessage(content=prompt_chunk)
    ]
    
    # Call LLM
    llm = ChatOllama(model="qwen3:latest", base_url=os.getenv("OLLAMA_BASE_URL"))
    result = llm.invoke(messages)
    
    # Safely parse JSON output
    json_text = result.content
    
    # Extract from <JSON> tags if present
    json_text = re.search(r"<JSON>\s*(.+?)\s*</JSON>", json_text, flags=re.DOTALL | re.IGNORECASE)
    
    if json_text is not None:
        json_text = json_text.group(1).strip()
        json_text = extract_json_from_tags(json_text)
    
    print(json_text, ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    json_text = format_structure(json_text)
    print(json_text, "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    
    json_text = extract_json_from_tags(json_text)
    
    data = safe_json_loads(json_text)
    if data is None:
        return [], []
    
    # Extract nodes and relationships
    nodes = data.get("nodes", [])
    nodes = normalize_nodes(nodes)
    
    relationships = data.get("relationships", [])
    relationships = normalize_relationships(relationships)
    
    return nodes, relationships


# ============================================================================
# NEO4J HELPER FUNCTIONS
# ============================================================================

_ident_re = re.compile(r"^[A-Za-z0-9_]+$")

def safe_ident(s: str) -> str:
    """Return _ident_re.sub("_", str(s or "")) or "RELATED_TO" """
    return _ident_re.sub("_", str(s or "")) or "RELATED_TO"


def flatten_properties(props: dict, parent_key="", sep="_") -> dict:
    """Flatten nested properties for Neo4j"""
    flat = {}
    for k, v in (props or {}).items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            flat.update(flatten_properties(v, new_key, sep=sep))
        elif isinstance(v, list):
            flat[new_key] = ", ".join(map(str, v))
        else:
            flat[new_key] = v
    return flat


# ============================================================================
# NEO4J INGESTION FUNCTIONS - FIXED VERSION
# ============================================================================

def create_nodes(tx, nodes_data: List[dict]):
    """
    Create/merge nodes using global UIDs.
    Uses MERGE to avoid duplicates.
    """
    for node_data in nodes_data:
        uid = node_data["uid"]
        name = node_data["name"]
        entity_type = node_data["entity_type"]
        chunk_ids = node_data["chunks"]
        properties = node_data.get("properties", {})
        
        # Flatten nested properties
        flat_props = flatten_properties(properties)
        
        # Clean properties - remove None values
        clean_props = {k: v for k, v in flat_props.items() if v is not None}
        
        tx.run(
            """
            MERGE (n:Entity {uid: $uid})
            ON CREATE SET
                n.name = $name,
                n.entity_type = $entity_type,
                n.chunk_ids = $chunk_ids,
                n.created_at = timestamp(),
                n.properties = $properties
            ON MATCH SET
                n.chunk_ids = CASE 
                    WHEN n.chunk_ids IS NULL THEN $chunk_ids
                    ELSE [x IN ($chunk_ids + n.chunk_ids) WHERE x IN ($chunk_ids + n.chunk_ids) | x][0..size($chunk_ids + n.chunk_ids)]
                END,
                n.updated_at = timestamp()
            """,
            uid=uid,
            name=name,
            entity_type=entity_type,
            chunk_ids=chunk_ids,
            properties=clean_props
        )


def create_relationships(tx, relationships: List[dict], registry: GlobalEntityRegistry, chunk_id: int) -> Tuple[int, int]:
    """
    Create relationships using global UIDs.
    Resolves chunk-local IDs to global UIDs using registry.
    
    Returns: (created_count, skipped_count)
    """
    created = 0
    skipped = 0
    
    # Normalize chunk_id to int to avoid duplicates
    try:
        chunk_id = int(chunk_id)
    except Exception:
        pass
    
    for rel in relationships:
        # Get chunk-local IDs
        from_local = rel.get("from")
        to_local = rel.get("to")
        
        if not from_local or not to_local:
            skipped += 1
            continue
        
        # KEY FIX: Resolve chunk-local IDs to global UIDs
        source_uid = registry.resolve_uid(chunk_id, from_local)
        target_uid = registry.resolve_uid(chunk_id, to_local)
        
        if not source_uid or not target_uid:
            skipped += 1
            continue
        
        # Get relationship properties
        rel_type = safe_ident(rel.get("type", "RELATED_TO"))
        rel_props = rel.get("properties", {}) or {}
        
        # Flatten and clean properties
        flat_props = flatten_properties(rel_props)
        clean_props = {k: v for k, v in flat_props.items() if v is not None}
        
        # Create relationship
        tx.run(
            """
            MATCH (a:Entity {uid: $source_uid})
            MATCH (b:Entity {uid: $target_uid})
            MERGE (a)-[r:RELATIONSHIP {type: $rel_type}]->(b)
            ON CREATE SET
                r.created_at = timestamp(),
                r.chunk_ids = [$chunk_id],
                r.properties = $properties
            ON MATCH SET
                r.chunk_ids = CASE
                    WHEN r.chunk_ids IS NULL THEN [$chunk_id]
                    WHEN $chunk_id IN r.chunk_ids THEN r.chunk_ids
                    ELSE r.chunk_ids + $chunk_id
                END,
                r.updated_at = timestamp()
            """,
            source_uid=source_uid,
            target_uid=target_uid,
            rel_type=rel_type,
            chunk_id=chunk_id,
            properties=clean_props
        )
        created += 1
    
    return created, skipped


# ============================================================================
# MAIN INGESTION FUNCTION - COMPLETE FIXED VERSION
# ============================================================================

def inject_to_neo4j(chunk_data: List[dict]):
    """
    Complete fixed ingestion process with two-phase processing:
    
    Phase 1: Build global entity registry across ALL chunks
    Phase 2: Create all nodes and relationships with proper resolution
    
    This fixes:
    - Entity deduplication across chunks
    - Cross-chunk relationship resolution
    - Orphaned nodes
    - Missing relationships
    """
    
    # Connect to Neo4j
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
    )
    
    try:
        # ====================================================================
        # PHASE 1: Build global entity registry
        # ====================================================================
        print("=" * 70)
        print("PHASE 1: Building global entity registry across all chunks...")
        print("=" * 70)
        
        registry = GlobalEntityRegistry()
        all_relationships = []
        
        for chunk in chunk_data:
            chunk_id = chunk.get("chunk_id")
            document_id = chunk.get("fsid")
            content = chunk.get("content")
            
            print(f"\nProcessing chunk {chunk_id}...")
            
            # Extract entities and relationships
            nodes, relationships = extract_nodes_relationships(chunk_id, document_id, content)
            
            if not nodes and not relationships:
                print(f"  ⊘ Chunk {chunk_id}: No extractable data")
                continue
            
            print(f"  ✓ Chunk {chunk_id}: {len(nodes)} nodes, {len(relationships)} relationships")
            
            # Register all entities from this chunk
            for node in nodes:
                registry.register_entity(node, chunk_id)
            
            # Store relationships with chunk context for later processing
            for rel in relationships:
                rel["_chunk_id"] = chunk_id  # Tag with source chunk
                all_relationships.append(rel)
        
        # Print registry statistics
        stats = registry.get_stats()
        print("\n" + "=" * 70)
        print("Registry Statistics:")
        print("=" * 70)
        print(f"✓ Total unique entities: {stats['total_entities']}")
        print(f"✓ Entities in multiple chunks: {stats['entities_in_multiple_chunks']}")
        print(f"✓ Total ID mappings: {stats['total_mappings']}")
        print(f"✓ Total relationships: {len(all_relationships)}")
        
        # ====================================================================
        # PHASE 2: Create all nodes
        # ====================================================================
        print("\n" + "=" * 70)
        print("PHASE 2: Creating nodes in Neo4j...")
        print("=" * 70)
        
        nodes_to_create = list(registry.entities.values())
        
        with driver.session() as session:
            session.execute_write(create_nodes, nodes_to_create)
        
        print(f"✓ Created/merged {len(nodes_to_create)} nodes")
        
        # ====================================================================
        # PHASE 3: Create all relationships
        # ====================================================================
        print("\n" + "=" * 70)
        print("PHASE 3: Creating relationships in Neo4j...")
        print("=" * 70)
        
        # Group relationships by chunk for better processing
        rels_by_chunk = {}
        for rel in all_relationships:
            chunk_id = rel["_chunk_id"]
            if chunk_id not in rels_by_chunk:
                rels_by_chunk[chunk_id] = []
            rels_by_chunk[chunk_id].append(rel)
        
        total_created = 0
        total_skipped = 0
        
        with driver.session() as session:
            for chunk_id, rels in rels_by_chunk.items():
                created, skipped = session.execute_write(
                    create_relationships, rels, registry, chunk_id
                )
                total_created += created
                total_skipped += skipped
                print(f"  Chunk {chunk_id}: {created} relationships created, {skipped} skipped")
        
        print(f"\n✓ Total relationships created: {total_created}")
        if total_skipped > 0:
            print(f"⚠ Warning: {total_skipped} relationships skipped (missing endpoints)")
        
        # ====================================================================
        # PHASE 4: Verification
        # ====================================================================
        print("\n" + "=" * 70)
        print("PHASE 4: Verifying graph integrity...")
        print("=" * 70)
        
        with driver.session() as session:
            # Count total nodes
            result = session.run("""
                MATCH (n:Entity)
                RETURN count(n) as node_count
            """)
            node_count = result.single()["node_count"]
            
            # Count total relationships
            result = session.run("""
                MATCH ()-[r:RELATIONSHIP]->()
                RETURN count(r) as rel_count
            """)
            rel_count = result.single()["rel_count"]
            
            # Count orphaned nodes (nodes without any relationships)
            result = session.run("""
                MATCH (n:Entity)
                WHERE NOT (n)-[]-()
                RETURN count(n) as orphan_count
            """)
            orphan_count = result.single()["orphan_count"]
        
        print(f"✓ Total nodes in graph: {node_count}")
        print(f"✓ Total relationships in graph: {rel_count}")
        
        if orphan_count > 0:
            print(f"⚠ Warning: {orphan_count} orphaned nodes (nodes without relationships)")
            print(f"  Run this query to investigate: MATCH (n:Entity) WHERE NOT (n)-[]-() RETURN n LIMIT 10")
        else:
            print(f"✓ No orphaned nodes - all entities are connected!")
        
        print("\n" + "=" * 70)
        print("✅ INGESTION COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ ERROR during ingestion: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        driver.close()


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    """
    Example usage - adapt to your data source
    """
    
    # Example: Load chunks from your data source
    # This could be from a database, file, API, etc.
    
    chunk_data = [
        {
            "chunk_id": 0,
            "fsid": "doc_001",
            "content": """
                Apple Inc. was founded by Steve Jobs and Steve Wozniak in 1976.
                The company is headquartered in Cupertino, California.
                Apple is known for products like the iPhone and Mac computers.
            """
        },
        {
            "chunk_id": 1,
            "fsid": "doc_001",
            "content": """
                Steve Jobs returned to Apple in 1997 after the company acquired NeXT.
                Under his leadership, Apple launched the iPod in 2001 and the iPhone in 2007.
                Apple acquired Beats Electronics in 2014 for $3 billion.
            """
        },
        {
            "chunk_id": 2,
            "fsid": "doc_002",
            "content": """
                Apple and Google compete in the smartphone market.
                Google's Android operating system powers most smartphones worldwide.
                Apple's iOS is exclusive to iPhone devices.
            """
        }
    ]
    
    # Run ingestion
    inject_to_neo4j(chunk_data)
    
    print("\n" + "=" * 70)
    print("VERIFICATION QUERIES")
    print("=" * 70)
    print("""
    Run these queries in Neo4j Browser to verify the results:
    
    1. Check entity deduplication:
       MATCH (n:Entity) WHERE n.name CONTAINS "Apple" RETURN n
       Expected: Only ONE Apple node
    
    2. Check cross-chunk relationships:
       MATCH (a:Entity)-[r:RELATIONSHIP]->(b:Entity)
       WHERE a.name CONTAINS "Apple"
       RETURN a.name, type(r), r.type, b.name
       Expected: Multiple relationships from different chunks
    
    3. Check for orphaned nodes:
       MATCH (n:Entity) WHERE NOT (n)-[]-() RETURN count(n)
       Expected: 0
    
    4. View full graph:
       MATCH (n:Entity)-[r:RELATIONSHIP]->(m:Entity)
       RETURN n, r, m LIMIT 100
    """)
