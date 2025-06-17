from tkapi import TKApi
from tkapi.stemming import Stemming
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel

api = TKApi()

def load_stemmingen(conn: Neo4jConnection, batch_size: int = 50):
    api = TKApi()
    Stemming.expand_params = ['Persoon', 'Fractie', 'Besluit']
    stemmingen = api.get_items(Stemming, max_items=batch_size)
    print(f"→ Fetched {len(stemmingen)} Stemmingen")
    with conn.driver.session(database=conn.database) as session:
        for i, s in enumerate(stemmingen, 1):
            if i % 100 == 0 or i == len(stemmingen):
                print(f"  → Processing Stemming {i}/{len(stemmingen)}")
            props = {
                'id': s.id,
                'soort': s.soort,
                'vergissing': s.vergissing,
                'fractie_size': s.fractie_size,
                'actor_naam': s.actor_naam,
                'actor_fractie': s.actor_fractie,
                'persoon_id': s.persoon_id,
                'fractie_id': s.fractie_id,
                'is_hoofdelijk': s.is_hoofdelijk,
            }
            session.execute_write(merge_node, 'Stemming', 'id', props)
            if s.persoon:
                session.execute_write(merge_node, 'Persoon', 'id', {'id': s.persoon.id})
                session.execute_write(merge_rel, 'Stemming', 'id', s.id, 'Persoon', 'id', s.persoon.id, 'CAST_BY')
            if s.fractie:
                session.execute_write(merge_node, 'Fractie', 'id', {'id': s.fractie.id})
                session.execute_write(merge_rel, 'Stemming', 'id', s.id, 'Fractie', 'id', s.fractie.id, 'REPRESENTS')
            if s.besluit:
                session.execute_write(merge_node, 'Besluit', 'id', {'id': s.besluit.id})
                session.execute_write(merge_rel, 'Stemming', 'id', s.id, 'Besluit', 'id', s.besluit.id, 'PART_OF_BESLUIT')
    print("✅ Loaded Stemmingen.")
