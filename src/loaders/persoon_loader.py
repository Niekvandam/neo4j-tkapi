from tkapi import TKApi
from tkapi.persoon import Persoon
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel

api = TKApi()

def load_personen(conn: Neo4jConnection, batch_size: int = 50):
    api = TKApi()
    personen = api.get_items(Persoon, max_items=batch_size)
    print(f"→ Fetched {len(personen)} Personen")
    with conn.driver.session(database=conn.database) as session:
        for i, p in enumerate(personen, 1):
            if i % 100 == 0 or i == len(personen):
                print(f"  → Processing Persoon {i}/{len(personen)}")
            props = {
                'id': p.id,
                'achternaam': p.achternaam,
                'tussenvoegsel': p.tussenvoegsel,
                'initialen': p.initialen,
                'roepnaam': p.roepnaam,
                'voornamen': p.voornamen,
                'functie': p.functie,
                'geslacht': p.geslacht,
                'woonplaats': p.woonplaats,
                'land': p.land,
                'geboortedatum': str(p.geboortedatum) if p.geboortedatum else None,
                'geboorteland': p.geboorteland,
                'geboorteplaats': p.geboorteplaats,
                'overlijdensdatum': str(p.overlijdensdatum) if p.overlijdensdatum else None,
                'overlijdensplaats': p.overlijdensplaats,
                'titels': p.titels
            }
            session.execute_write(merge_node, 'Persoon', 'id', props)
    print("✅ Loaded Personen.")
