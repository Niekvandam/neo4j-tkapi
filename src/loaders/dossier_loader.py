from tkapi import TKApi
from tkapi.dossier import Dossier
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel

api = TKApi()

def load_dossiers(conn: Neo4jConnection, batch_size: int = 50):
    api = TKApi()
    dossiers = api.get_items(Dossier, max_items=batch_size)
    print(f"→ Fetched {len(dossiers)} Dossiers")
    with conn.driver.session(database=conn.database) as session:
        for i, d in enumerate(dossiers, 1):
            if i % 100 == 0 or i == len(dossiers):
                print(f"  → Processing Dossier {i}/{len(dossiers)}")
            props = {
                'id': d.id,
                'nummer': d.nummer,
                'toevoeging': d.toevoeging,
                'titel': d.titel,
                'afgesloten': d.afgesloten,
                'organisatie': d.organisatie
            }
            session.execute_write(merge_node, 'Dossier', 'id', props)
            for doc in d.documenten:
                session.execute_write(merge_node, 'Document', 'id', {'id': doc.id})
                session.execute_write(merge_rel, 'Dossier', 'id', d.id, 'Document', 'id', doc.id, 'HAS_DOCUMENT')
            for zaak in d.zaken:
                session.execute_write(merge_node, 'Zaak', 'nummer', {'nummer': zaak.nummer})
                session.execute_write(merge_rel, 'Dossier', 'id', d.id, 'Zaak', 'nummer', zaak.nummer, 'HAS_ZAAK')
    print("✅ Loaded Dossiers.")
