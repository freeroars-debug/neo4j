import re
import json
import hashlib
from typing import Dict, List, Tuple, Any


def node_uid(node: dict) -> str:
    """Generate a stable UID for a node based on its properties."""
    props = node.get("properties", {})
    doc = str(props.get("document_id", ""))
    name = str(props.get("name", "")).strip().lower()
    raw = f"{doc}|{name}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def flatten_properties(props, parent_key="", sep="_"):
    """Flatten nested properties into a single level dictionary."""
    flat = {}
    for k, v in (props or {}).items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            flat.update(flatten_properties(v, new_key, sep))
        elif isinstance(v, list):
            flat[new_key] = ", ".join(map(str, v))
        else:
            flat[new_key] = v
    return flat


def create_nodes(tx, nodes: List[dict], chunk_id: int, global_uid_to_props: Dict[str, dict]):
    """
    Create or merge nodes, tracking global properties across chunks.
    
    Args:
        tx: Neo4j transaction
        nodes: List of nodes from current chunk
        chunk_id: Current chunk ID
        global_uid_to_props: Global dictionary tracking all properties for each UID
    
    Returns:
        uid_to_local_id: Mapping from stable UID to the local ID (N1, N2, etc.) in this chunk
    """
    uid_to_local_id = {}
    
    for node in nodes:
        props = flatten_properties(node.get("properties", {}))
        uid = node_uid(node)
        local_id = node.get("id")  # This is N1, N2, etc. from LLM
        
        # Track the mapping from UID to local ID for relationship creation
        uid_to_local_id[uid] = local_id
        
        # Immutable fields (set once)
        chunk_id_val = props.pop("chunk_id", None)
        document_id = props.pop("document_id", None)
        
        # Get entity type
        entity_type = node.get("label") or node.get("type") or "Entity"
        
        # Update global tracking of properties for this UID
        if uid not in global_uid_to_props:
            global_uid_to_props[uid] = {
                "entity_type": entity_type,
                "chunk_ids": set(),
                "properties": {}
            }
        
        # Add this chunk to the list
        global_uid_to_props[uid]["chunk_ids"].add(chunk_id)
        
        # Merge properties (later chunks can add new properties)
        for k, v in props.items():
            if k not in global_uid_to_props[uid]["properties"]:
                global_uid_to_props[uid]["properties"][k] = v
        
        # Create/merge the node in Neo4j
        tx.run(
            """
            MERGE (n:Entity {uid: $uid})
            ON CREATE SET 
                n.chunk_id = $chunk_id,
                n.document_id = $document_id,
                n.entity_type = $entity_type
            SET n += $properties,
                n.chunk_ids = CASE 
                    WHEN n.chunk_ids IS NULL THEN [$chunk_id]
                    WHEN $chunk_id IN n.chunk_ids THEN n.chunk_ids
                    ELSE n.chunk_ids + $chunk_id
                END
            """,
            uid=uid,
            chunk_id=chunk_id_val or chunk_id,
            document_id=document_id,
            entity_type=entity_type,
            properties=global_uid_to_props[uid]["properties"]
        )
    
    return uid_to_local_id


def create_relationships(
    tx, 
    relationships: List[dict], 
    nodes: List[dict],
    chunk_id: int
):
    """
    Create relationships between nodes using stable UIDs.
    
    Args:
        tx: Neo4j transaction
        relationships: List of relationships with local IDs (N1, N2, etc.)
        nodes: List of nodes to build local_id -> UID mapping
        chunk_id: Current chunk ID
    """
    # Build mapping from local ID (N1, N2, etc.) to stable UID
    local_id_to_uid = {}
    for node in nodes:
        local_id = node.get("id")
        uid = node_uid(node)
        local_id_to_uid[local_id] = uid
    
    # Process relationships
    for rel in relationships:
        rel_type = rel.get("type", "RELATED_TO").upper().replace(" ", "_")
        rel_props = flatten_properties(rel.get("properties", {}))
        
        # Get local IDs from relationship
        from_local_id = rel.get("from")
        to_local_id = rel.get("to")
        
        # Convert to stable UIDs
        source_uid = local_id_to_uid.get(from_local_id)
        target_uid = local_id_to_uid.get(to_local_id)
        
        if not source_uid or not target_uid:
            print(f"Warning: Could not resolve relationship {from_local_id} -> {to_local_id}")
            print(f"  Available local IDs: {list(local_id_to_uid.keys())}")
            continue
        
        # Add chunk_id to relationship properties
        rel_props["chunk_id"] = chunk_id
        
        # Create the relationship
        tx.run(
            f"""
            MATCH (a:Entity {{uid: $source_uid}})
            MATCH (b:Entity {{uid: $target_uid}})
            MERGE (a)-[r:{rel_type}]->(b)
            ON CREATE SET r.chunk_ids = [$chunk_id]
            ON MATCH SET r.chunk_ids = CASE 
                WHEN $chunk_id IN r.chunk_ids THEN r.chunk_ids
                ELSE r.chunk_ids + $chunk_id
            END
            SET r += $props
            """,
            source_uid=source_uid,
            target_uid=target_uid,
            chunk_id=chunk_id,
            props=rel_props
        )


def ingest_graph(driver, nodes: List[dict], relationships: List[dict], chunk_id: int, 
                 global_uid_to_props: Dict[str, dict] = None):
    """
    Ingest a knowledge graph chunk into Neo4j.
    
    Args:
        driver: Neo4j driver
        nodes: List of nodes
        relationships: List of relationships
        chunk_id: Chunk ID
        global_uid_to_props: Global tracking dictionary (pass the same instance across chunks)
    """
    if global_uid_to_props is None:
        global_uid_to_props = {}
    
    with driver.session() as session:
        # Create nodes and get UID mappings
        session.execute_write(create_nodes, nodes, chunk_id, global_uid_to_props)
        
        # Create relationships using the nodes from this chunk
        session.execute_write(create_relationships, relationships, nodes, chunk_id)
    
    return global_uid_to_props


def ingest_to_neo4j(chunk_data: List[dict], driver):
    """
    Process all chunks and ingest into Neo4j.
    
    Args:
        chunk_data: List of dicts with keys: chunk_id, document_id, content, nodes, relationships
        driver: Neo4j driver
    """
    # Global tracking across all chunks
    global_uid_to_props = {}
    
    for chunk in chunk_data:
        chunk_id = chunk.get("chunk_id")
        document_id = chunk.get("document_id")
        nodes = chunk.get("nodes", [])
        relationships = chunk.get("relationships", [])
        
        # Skip empty graphs
        if not nodes and not relationships:
            continue
        
        print(f"Processing chunk {chunk_id} from document {document_id}")
        print(f"  Nodes: {len(nodes)}, Relationships: {len(relationships)}")
        
        # Ingest this chunk
        global_uid_to_props = ingest_graph(
            driver, 
            nodes, 
            relationships, 
            chunk_id,
            global_uid_to_props
        )
    
    print(f"\nTotal unique entities across all chunks: {len(global_uid_to_props)}")


# Example usage:
if __name__ == "__main__":
    from neo4j import GraphDatabase
    import os
    
    # Connect to Neo4j
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
    )
    
    # Example chunk data
    chunk_data = [
        {
            "chunk_id": 1,
            "document_id": "doc1",
            "nodes": [
                {
                    "id": "N1",
                    "label": "Person",
                    "properties": {
                        "name": "John Doe",
                        "document_id": "doc1",
                        "chunk_id": 1,
                        "role": "CEO"
                    }
                },
                {
                    "id": "N2",
                    "label": "Organization",
                    "properties": {
                        "name": "Acme Corp",
                        "document_id": "doc1",
                        "chunk_id": 1
                    }
                }
            ],
            "relationships": [
                {
                    "from": "N1",
                    "to": "N2",
                    "type": "WORKS_AT",
                    "properties": {
                        "justification": "John Doe is CEO of Acme Corp"
                    }
                }
            ]
        },
        {
            "chunk_id": 2,
            "document_id": "doc1",
            "nodes": [
                {
                    "id": "N1",  # Same person, different chunk
                    "label": "Person",
                    "properties": {
                        "name": "John Doe",
                        "document_id": "doc1",
                        "chunk_id": 2,
                        "age": 45  # New property
                    }
                },
                {
                    "id": "N2",
                    "label": "Product",
                    "properties": {
                        "name": "Widget Pro",
                        "document_id": "doc1",
                        "chunk_id": 2
                    }
                }
            ],
            "relationships": [
                {
                    "from": "N1",
                    "to": "N2",
                    "type": "CREATED",
                    "properties": {
                        "justification": "John Doe created Widget Pro"
                    }
                }
            ]
        }
    ]
    
    # Process all chunks
    ingest_to_neo4j(chunk_data, driver)
    
    driver.close()
