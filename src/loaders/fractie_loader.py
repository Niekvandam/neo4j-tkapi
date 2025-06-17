from tkapi import TKApi
from tkapi.fractie import Fractie
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel

api = TKApi()

def load_fracties(conn: Neo4jConnection, batch_size: int = 50):
    api = TKApi()
    fracties = api.get_items(Fractie, max_items=batch_size)
    print(f"→ Fetched {len(fracties)} Fracties")

    with conn.driver.session(database=conn.database) as session:
        for i, f in enumerate(fracties, 1):
            if i % 100 == 0 or i == len(fracties):
                print(f"  → Processing Fractie {i}/{len(fracties)}")
            print(f.id)
            print(f.naam)
            print(f.afkorting)
            print(f.zetels_aantal)
            print(f.datum_actief)
            print(f.datum_inactief)
            print(f.organisatie)
            props = {
                'id': f.id,
                'naam': f.naam,
                'afkorting': f.afkorting,
                'zetels_aantal': f.zetels_aantal,
                'datum_actief': str(f.datum_actief) if f.datum_actief else None,
                'datum_inactief': str(f.datum_inactief) if f.datum_inactief else None,
                'organisatie': f.organisatie
            }

            session.execute_write(merge_node, 'Fractie', 'id', props)

    print("✅ Loaded Fracties.")
