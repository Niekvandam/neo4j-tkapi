def merge_node(tx, label: str, key: str, props: dict):
    cypher = (
        f"MERGE (n:{label} {{{key}: $key_val}})\n"
        f"SET n += $props"
    )
    tx.run(cypher, key_val=props[key], props=props)


def merge_rel(tx,
              from_label: str, from_key: str, from_val,
              to_label:   str, to_key:   str, to_val,
              rel_type:   str):
    cypher = (
        f"MATCH (a:{from_label} {{{from_key}: $from_val}})\n"
        f"MATCH (b:{to_label}   {{{to_key}:   $to_val}})\n"
        f"MERGE (a)-[:{rel_type}]->(b)"
    )
    tx.run(cypher, from_val=from_val, to_val=to_val)


def check_nodes_exist(tx, label: str, key: str, ids: list) -> set:
    """
    Check which nodes already exist in Neo4j.
    
    Args:
        tx: Neo4j transaction
        label: Node label to check
        key: Property key to match on (e.g., 'id')
        ids: List of IDs to check
    
    Returns:
        Set of IDs that already exist in Neo4j
    """
    if not ids:
        return set()
    
    cypher = f"MATCH (n:{label}) WHERE n.{key} IN $ids RETURN n.{key} as existing_id"
    result = tx.run(cypher, ids=ids)
    return set(record["existing_id"] for record in result)


def batch_check_nodes_exist(session, label: str, key: str, ids: list, batch_size: int = 1000) -> set:
    """
    Check which nodes exist in Neo4j, processing in batches for large ID lists.
    
    Args:
        session: Neo4j session
        label: Node label to check
        key: Property key to match on
        ids: List of IDs to check
        batch_size: Size of batches for processing large lists
    
    Returns:
        Set of IDs that already exist in Neo4j
    """
    existing_ids = set()
    
    # Process in batches to avoid large query parameters
    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i:i + batch_size]
        batch_existing = session.execute_read(check_nodes_exist, label, key, batch_ids)
        existing_ids.update(batch_existing)
    
    return existing_ids

