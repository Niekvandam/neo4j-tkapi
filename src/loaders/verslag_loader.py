from tkapi import TKApi
from tkapi.verslag import Verslag
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel

api = TKApi()

def load_verslagen(conn: Neo4jConnection, batch_size: int = 50):
    api = TKApi()
    Verslag.expand_params = ['Vergadering']
    verslagen = api.get_items(Verslag, max_items=batch_size)
    print(f"→ Fetched {len(verslagen)} Verslagen")

    with conn.driver.session(database=conn.database) as session:
        for idx, v in enumerate(verslagen, 1):
            props = {
                'id': v.id,
                'soort': v.soort.name if v.soort else None,
                'status': v.status.name if v.status else None
            }
            session.execute_write(merge_node, 'Verslag', 'id', props)

            if v.vergadering:
                session.execute_write(
                    merge_node, 'Vergadering', 'id', {'id': v.vergadering.id}
                )
                session.execute_write(
                    merge_rel, 'Verslag', 'id', v.id, 'Vergadering', 'id', v.vergadering.id, 'RECORDED_IN'
                )

            if idx % 100 == 0 or idx == len(verslagen):
                print(f"→ Processed Verslag {idx}/{len(verslagen)}")

    print("✅ Loaded Verslagen.")
