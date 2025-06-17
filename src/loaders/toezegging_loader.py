from tkapi import TKApi
from tkapi.toezegging import Toezegging
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel
from constants import REL_MAP_TOEZEGGING

api = TKApi()

def load_toezeggingen(conn: Neo4jConnection, batch_size: int = 50):
    api = TKApi()
    Toezegging.expand_params = ['Activiteit', 'ToegezegdAanPersoon', 'ToegezegdAanFractie']
    toezeggingen = api.get_items(Toezegging, max_items=batch_size)
    print(f"→ Fetched {len(toezeggingen)} Toezeggingen")

    with conn.driver.session(database=conn.database) as session:
        for idx, t in enumerate(toezeggingen, 1):
            props = {
                'id': t.id,
                'nummer': t.nummer,
                'tekst': t.tekst,
                'status': t.status.name if t.status else None,
                'functie': t.functie,
                'ministerie': t.ministerie,
                'naam': t.naam_bewindspersoon,
            }
            session.execute_write(merge_node, 'Toezegging', 'id', props)
            if t.activiteit:
                session.execute_write(merge_node, 'Activiteit', 'id', {'id': t.activiteit.id})
                session.execute_write(merge_rel, 'Toezegging', 'id', t.id, 'Activiteit', 'id', t.activiteit.id, 'MADE_DURING')
            for persoon_dict in t.toegezegd_aan_persoon:
                if "Id" in persoon_dict:
                    persoon_id = persoon_dict["Id"]
                session.execute_write(merge_node, "Persoon", "id", {"id": persoon_id})
                session.execute_write(
                    merge_rel,
                    "Toezegging", "id", t.id,
                    "Persoon", "id", persoon_id,
                    "ADDRESSED_TO"
                )


            for fractie_dict in t.toegezegd_aan_fractie:
                if "Id" in fractie_dict:
                    fractie_id = fractie_dict["Id"]
                session.execute_write(merge_node, "Fractie", "id", {"id": fractie_id})
                session.execute_write(
                    merge_rel,
                    "Toezegging", "id", t.id,
                    "Fractie", "id", fractie_id,
                    "ADDRESSED_TO"
                )
                
            # In load_toezeggingen, replace manual if-blocks with:
            for attr, (label, rel, key) in REL_MAP_TOEZEGGING.items():
                targets = getattr(t, attr, None)
                if not targets:
                    continue
                if not isinstance(targets, list):
                    targets = [targets]
                for target in targets:
                    if not target or key not in target:
                        continue
                    target_val = target[key]
                    session.execute_write(merge_node, label, key.lower(), {key.lower(): target_val})
                    session.execute_write(merge_rel, 'Toezegging', 'id', t.id, label, key.lower(), target_val, rel)
            if idx % 100 == 0 or idx == len(toezeggingen):
                print(f"→ Processed Toezegging {idx}/{len(toezeggingen)}")

    print("✅ Loaded Toezeggingen.")