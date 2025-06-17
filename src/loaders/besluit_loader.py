from tkapi import TKApi
from tkapi.besluit import Besluit
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel
from constants import REL_MAP_BESLUIT

api = TKApi()

def load_besluiten(conn: Neo4jConnection, batch_size: int = 50):
    api = TKApi()
    Besluit.expand_params = ['Stemming']
    besluiten = api.get_items(Besluit, max_items=batch_size)
    print(f"→ Fetched {len(besluiten)} Besluiten")
    with conn.driver.session(database=conn.database) as session:
        for i, b in enumerate(besluiten, 1):
            if i % 100 == 0 or i == len(besluiten):
                print(f"  → Processing Besluit {i}/{len(besluiten)}")
            props = {
                'id': b.id,
                'soort': b.soort,
                'status': b.status.name if b.status else None,
                'tekst': b.tekst,
                'stemming_soort': b.stemming_soort,
                'opmerking': b.opmerking,
            }
            session.execute_write(merge_node, 'Besluit', 'id', props)
            if b.agendapunt:
                session.execute_write(merge_node, 'Agendapunt', 'id', {'id': b.agendapunt.id})
                session.execute_write(merge_rel, 'Besluit', 'id', b.id, 'Agendapunt', 'id', b.agendapunt.id, 'FROM_AGENDAPUNT')
            for stemming in b.stemmingen:
                session.execute_write(merge_node, 'Stemming', 'id', {'id': stemming.id})
                session.execute_write(merge_rel, 'Besluit', 'id', b.id, 'Stemming', 'id', stemming.id, 'HAS_STEMMING')
            for zaak in b.zaken:
                session.execute_write(merge_node, 'Zaak', 'nummer', {'nummer': zaak.nummer})
                session.execute_write(merge_rel, 'Besluit', 'id', b.id, 'Zaak', 'nummer', zaak.nummer, 'ABOUT_ZAAK')
            for attr, (label, rel, key) in REL_MAP_BESLUIT.items():
                items = getattr(b, attr, [])
                if not isinstance(items, list): items = [items]
                for it in items:
                    val = getattr(it, key)
                    session.execute_write(merge_node, label, key, {key: val})
                    session.execute_write(merge_rel, 'Besluit', 'id', b.id, label, key, val, rel)
    
    print("✅ Loaded Besluiten.")