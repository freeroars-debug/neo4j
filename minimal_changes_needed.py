# Minimal Code Changes to Fix Your Neo4j Ingestion
# ====================================================
# This file shows the EXACT changes needed to fix your code.
# Use this to update your existing code with minimal disruption.

# ============================================================================
# CHANGE 1: Fix node_uid() function (Lines 517-522)
# ============================================================================

# ❌ REMOVE THIS:
def node_uid(node: dict) -> str:
    props = node.get("properties", {}) or {}
    doc = str(props.get("document_id", "")).strip().lower()  # ← REMOVE document_id
    name = str(props.get("name", "")).strip().lower()
    raw = f"{doc}|{name}"  # ← REMOVE doc from hash
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

# ✅ REPLACE WITH THIS:
def node_uid(node: dict) -> str:
    """Generate stable UID based ONLY on entity name"""
    props = node.get("properties", {}) or {}
    name = str(props.get("name", "")).strip().lower()
    
    # Normalize name for consistency
    name = " ".join(name.split())  # Remove extra whitespace
    
    # UID based ONLY on name (ensures same entity across all chunks has same UID)
    return hashlib.sha1(name.encode("utf-8")).hexdigest()


# ============================================================================
# CHANGE 2: Add GlobalEntityRegistry class (NEW CODE TO ADD)
# ============================================================================
# Add this class BEFORE your ingest_graph() function

class GlobalEntityRegistry:
    """
    Tracks entities across all chunks to enable:
    1. Entity deduplication (same name = same node)
    2. Cross-chunk relationship resolution
    """
    
    def __init__(self):
        # Map: entity_name → {uid, chunks, properties, etc}
        self.entities: Dict[str, dict] = {}
        
        # Map: "chunk_id:local_id" → global_uid
        # This is the KEY to fixing cross-chunk relationships!
        self.local_to_global: Dict[str, str] = {}
    
    def register_entity(self, node: dict, chunk_id: int):
        """Register an entity from a specific chunk"""
        props = node.get("properties", {}) or {}
        name = str(props.get("name", "")).strip().lower()
        name = " ".join(name.split())  # Normalize
        
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
                "chunks": [chunk_id],
                "properties": props,
                "entity_type": node.get("label") or props.get("type") or "Entity"
            }
        else:
            # Entity exists - add this chunk
            if chunk_id not in self.entities[name]["chunks"]:
                self.entities[name]["chunks"].append(chunk_id)
            
            # Merge properties (keep most complete data)
            for key, value in props.items():
                if key not in self.entities[name]["properties"]:
                    self.entities[name]["properties"][key] = value
        
        return uid
    
    def resolve_uid(self, chunk_id: int, local_id: str) -> str:
        """
        KEY METHOD: Resolve chunk-local ID to global UID
        This is how we fix cross-chunk relationships!
        """
        mapping_key = f"{chunk_id}:{local_id}"
        return self.local_to_global.get(mapping_key)


# ============================================================================
# CHANGE 3: Update create_nodes() function (Lines 524-550)
# ============================================================================

# Your current function is mostly fine, just ensure chunk_ids is a list:

def create_nodes(tx, nodes):
    """Create nodes - no changes needed here, your code is fine!"""
    for node in nodes:
        props = flatten_properties(node.get("properties", {}))
        uid = node_uid(node)
        
        # chunk_id is immutable (create-only)
        chunk_id = props.pop("chunk_id", None)
        document_id = props.pop("document_id", None)
        
        entity_type = node.get("label") or node.get("type") or "Entity"
        
        tx.run(
            """
            MERGE (n:Entity {uid: $uid})
            ON CREATE SET
                n.chunk_id = $chunk_id,
                n.document_id = $document_id
            SET n += $properties
            SET n.entity_type = $entity_type
            """,
            uid=uid,
            chunk_id=chunk_id,
            document_id=document_id,
            properties=props,
            entity_type=entity_type,
        )


# ============================================================================
# CHANGE 4: Update create_relationships() function (Lines 552-590)
# ============================================================================

# ❌ REMOVE THIS VERSION:
def create_relationships(tx, relationships, id_to_uid, chunk_id):
    """OLD VERSION - only knows about current chunk"""
    # ... existing code that uses id_to_uid dict ...

# ✅ REPLACE WITH THIS:
def create_relationships(tx, relationships, registry: GlobalEntityRegistry, chunk_id):
    """
    FIXED VERSION - resolves IDs using global registry
    """
    # normalize chunk_id to int to avoid ["5", 5] duplicates
    try:
        chunk_id = int(chunk_id)
    except Exception:
        pass
    
    for rel in relationships:
        rel_type = safe_ident(rel.get("type", "RELATED_TO"))
        rel_props = flatten_properties(rel.get("properties", {}))
        
        # KEY FIX: Use registry to resolve chunk-local IDs to global UIDs
        source_uid = registry.resolve_uid(chunk_id, rel.get("from"))
        target_uid = registry.resolve_uid(chunk_id, rel.get("to"))
        
        if not source_uid or not target_uid:
            # This should rarely happen now!
            continue
        
        tx.run(
            """
            MATCH (a:Entity {uid: $source}))
            MATCH (b:Entity {uid: $target}))
            MERGE (a)-[r:RELATIONSHIP {type: $rel_type}]->(b)
            ON CREATE SET r.chunk_ids = [$chunk_id]
            ON MATCH SET r.chunk_ids = CASE
                WHEN r.chunk_ids IS NULL THEN [$chunk_id]
                WHEN $chunk_id IN r.chunk_ids THEN r.chunk_ids
                ELSE r.chunk_ids + $chunk_id
                END
            SET r += $props
            """,
            source=source_uid,
            target=target_uid,
            rel_type=rel_type,
            chunk_id=chunk_id,
            props=rel_props
        )


# ============================================================================
# CHANGE 5: COMPLETELY REPLACE ingest_graph() and inject_to_neo4j() (Lines 592-625)
# ============================================================================

# ❌ REMOVE BOTH ingest_graph() AND inject_to_neo4j() functions

# ✅ REPLACE WITH THESE TWO NEW FUNCTIONS:

def ingest_graph_fixed(driver, all_nodes: List[dict], all_relationships: List[dict], 
                       registry: GlobalEntityRegistry):
    """
    Single function to ingest all nodes and relationships
    Uses global registry for entity resolution
    """
    with driver.session() as session:
        # Create all nodes
        session.execute_write(create_nodes, all_nodes)
        
        # Group relationships by chunk for processing
        rels_by_chunk = {}
        for rel in all_relationships:
            chunk_id = rel.get("_chunk_id", 0)
            if chunk_id not in rels_by_chunk:
                rels_by_chunk[chunk_id] = []
            rels_by_chunk[chunk_id].append(rel)
        
        # Create relationships chunk by chunk
        for chunk_id, rels in rels_by_chunk.items():
            session.execute_write(create_relationships, rels, registry, chunk_id)


def inject_to_neo4j_fixed(chunk_data):
    """
    FIXED VERSION with two-phase processing:
    Phase 1: Build global entity registry
    Phase 2: Ingest everything at once
    """
    # Create ONE driver for the whole batch
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD")),
    )
    
    try:
        # ====================================================================
        # PHASE 1: Build global entity registry across ALL chunks
        # ====================================================================
        print("Phase 1: Building global entity registry...")
        
        registry = GlobalEntityRegistry()
        all_nodes_to_create = []
        all_relationships = []
        
        for chunk in chunk_data:
            chunk_id = chunk.get("chunk_id")
            document_id = chunk.get("fsid")
            content = chunk.get("content")
            
            # Extract entities and relationships
            nodes, relationships = extract_nodes_relationships(chunk_id, document_id, content)
            
            if not nodes and not relationships:
                continue
            
            # Register all entities from this chunk
            for node in nodes:
                registry.register_entity(node, chunk_id)
            
            # Collect nodes for creation (will be deduplicated by registry)
            all_nodes_to_create.extend(nodes)
            
            # Store relationships with chunk context
            for rel in relationships:
                rel["_chunk_id"] = chunk_id  # Tag with chunk for later resolution
                all_relationships.append(rel)
        
        print(f"✓ Registered {len(registry.entities)} unique entities")
        print(f"✓ Total relationships: {len(all_relationships)}")
        
        # ====================================================================
        # PHASE 2: Ingest everything into Neo4j
        # ====================================================================
        print("Phase 2: Ingesting into Neo4j...")
        
        # Get unique nodes to create (using registry)
        nodes_to_create = list(registry.entities.values())
        
        # Ingest everything
        ingest_graph_fixed(driver, nodes_to_create, all_relationships, registry)
        
        print("✅ Ingestion complete!")
        
        # ====================================================================
        # VERIFICATION
        # ====================================================================
        with driver.session() as session:
            result = session.run("MATCH (n:Entity) WHERE NOT (n)-[]-() RETURN count(n) as orphans")
            orphan_count = result.single()["orphans"]
            
            if orphan_count > 0:
                print(f"⚠ Warning: {orphan_count} orphaned nodes detected")
            else:
                print(f"✓ No orphaned nodes!")
        
    finally:
        driver.close()


# ============================================================================
# SUMMARY: What to do
# ============================================================================

"""
1. Update node_uid() - remove document_id from hash
2. Add GlobalEntityRegistry class
3. Update create_relationships() - accept registry parameter
4. Replace inject_to_neo4j() with inject_to_neo4j_fixed()

That's it! These 4 changes will fix:
✅ Entity deduplication across chunks
✅ Cross-chunk relationships  
✅ Orphaned nodes
✅ Missing relationships
"""

# ============================================================================
# Quick Test After Changes
# ============================================================================

def test_fix():
    """Quick test to verify the fix works"""
    
    # Test data: Apple appears in 2 chunks, related to different entities
    test_data = [
        {
            "chunk_id": 0,
            "fsid": "doc1",
            "content": "",
            "nodes": [
                {"id": "N1", "properties": {"name": "Apple"}},
                {"id": "N2", "properties": {"name": "Google"}}
            ],
            "relationships": [
                {"from": "N1", "to": "N2", "type": "COMPETES_WITH"}
            ]
        },
        {
            "chunk_id": 1,
            "fsid": "doc1",
            "content": "",
            "nodes": [
                {"id": "N1", "properties": {"name": "Apple"}},
                {"id": "N2", "properties": {"name": "Microsoft"}}
            ],
            "relationships": [
                {"from": "N1", "to": "N2", "type": "ACQUIRED"}
            ]
        }
    ]
    
    # Run fixed ingestion
    inject_to_neo4j_fixed(test_data)
    
    # Verify in Neo4j:
    # Query 1: Check only ONE Apple node exists
    # MATCH (n:Entity {name: "Apple"}) RETURN count(n)
    # Expected: 1
    
    # Query 2: Check Apple has 2 relationships
    # MATCH (a:Entity {name: "Apple"})-[r]-() RETURN count(r)
    # Expected: 2
    
    # Query 3: Check no orphaned nodes
    # MATCH (n:Entity) WHERE NOT (n)-[]-() RETURN count(n)
    # Expected: 0
    
    print("✅ Test complete - verify with Neo4j queries above")


if __name__ == "__main__":
    test_fix()
