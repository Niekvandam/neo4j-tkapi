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

