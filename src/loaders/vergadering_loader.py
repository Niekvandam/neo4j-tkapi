from tkapi import TKApi
from tkapi.vergadering import Vergadering
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel

api = TKApi()

def load_vergaderingen(conn: Neo4jConnection, batch_size: int = 50):
    api = TKApi()
    Vergadering.expand_params = ['Verslag']
    vergaderingen = api.get_items(Vergadering, max_items=batch_size)
    print(f"→ Fetched {len(vergaderingen)} Vergaderingen")

    with conn.driver.session(database=conn.database) as session:
        for idx, v in enumerate(vergaderingen, 1):
            props = {
                'id': v.id,
                'titel': v.titel,
                'nummer': v.nummer,
                'zaal': v.zaal,
                'soort': v.soort.name if v.soort else None,
                'datum': str(v.datum) if v.datum else None,
                'begin': str(v.begin) if v.begin else None,
                'einde': str(v.einde) if v.einde else None,
                'samenstelling': v.samenstelling
            }
            session.execute_write(merge_node, 'Vergadering', 'id', props)

            # Link to verslag (optional; may be inverse of Verslag loader)
            if v.verslag:
                session.execute_write(merge_node, 'Verslag', 'id', {'id': v.verslag.id})
                session.execute_write(merge_rel, 'Verslag', 'id', v.verslag.id, 'Vergadering', 'id', v.id, 'RECORDED_IN')

            if idx % 100 == 0 or idx == len(vergaderingen):
                print(f"→ Processed Vergadering {idx}/{len(vergaderingen)}")

    print("✅ Loaded Vergaderingen.")
