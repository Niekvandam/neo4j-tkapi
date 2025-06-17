from tkapi import TKApi
from tkapi.agendapunt import Agendapunt
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel

api = TKApi()

def load_agendapunten(conn: Neo4jConnection, batch_size: int = 50):
    api = TKApi()
    Agendapunt.expand_params = ['Activiteit', 'Besluit', 'Document']
    agendapunten = api.get_items(Agendapunt, max_items=batch_size)
    print(f"→ Fetched {len(agendapunten)} Agendapunten")
    with conn.driver.session(database=conn.database) as session:
        for i, ap in enumerate(agendapunten, 1):
            if i % 100 == 0 or i == len(agendapunten):
                print(f"  → Processing Agendapunt {i}/{len(agendapunten)}")
            props = {
                'id': ap.id,
                'onderwerp': ap.onderwerp,
                'volgorde': ap.volgorde,
                'rubriek': ap.rubriek,
                'noot': ap.noot,
                'begin': str(ap.begin) if ap.begin else None,
                'einde': str(ap.einde) if ap.einde else None
            }
            session.execute_write(merge_node, 'Agendapunt', 'id', props)
            if ap.activiteit:
                session.execute_write(merge_node, 'Activiteit', 'id', {'id': ap.activiteit.id})
                session.execute_write(merge_rel, 'Agendapunt', 'id', ap.id, 'Activiteit', 'id', ap.activiteit.id, 'BELONGS_TO_ACTIVITEIT')
            if ap.besluit:
                session.execute_write(merge_node, 'Besluit', 'id', {'id': ap.besluit.id})
                session.execute_write(merge_rel, 'Agendapunt', 'id', ap.id, 'Besluit', 'id', ap.besluit.id, 'HAS_BESLUIT')
            for doc in ap.documenten:
                session.execute_write(merge_node, 'Document', 'id', {'id': doc.id})
                session.execute_write(merge_rel, 'Agendapunt', 'id', ap.id, 'Document', 'id', doc.id, 'HAS_DOCUMENT')
            for zaak in ap.zaken:
                session.execute_write(merge_node, 'Zaak', 'nummer', {'nummer': zaak.nummer})
                session.execute_write(merge_rel, 'Agendapunt', 'id', ap.id, 'Zaak', 'nummer', zaak.nummer, 'ABOUT_ZAAK')
    print("✅ Loaded Agendapunten.")
