from tkapi import TKApi
from tkapi.zaak import Zaak
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel
from constants import REL_MAP_ZAAK

api = TKApi()

def load_zaken(conn: Neo4jConnection, batch_size: int = 50,):
    api = TKApi()
    Zaak.expand_params = ['Document','Agendapunt','Activiteit','Besluit','ZaakActor','VervangenDoor']
    zaken = api.get_zaken(max_items=batch_size)
    print(f"→ Fetched {len(zaken)} Zaken")
    with conn.driver.session(database=conn.database) as session:
        for i, z in enumerate(zaken, 1):
            if i % 100 == 0 or i == len(zaken):
                print(f"  → Processing Zaak {i}/{len(zaken)}")
            props = {'nummer': z.nummer, 'onderwerp': z.onderwerp, 'afgedaan': z.afgedaan}
            session.execute_write(merge_node,'Zaak','nummer',props)
            if z.soort:
                session.execute_write(merge_rel,'Zaak','nummer',z.nummer,'ZaakSoort','key',z.soort.name,'HAS_SOORT')
            if z.kabinetsappreciatie:
                session.execute_write(merge_rel,'Zaak','nummer',z.nummer,'Kabinetsappreciatie','key',z.kabinetsappreciatie.name,'HAS_KABINETSAPPRECIATIE')
            if z.vervangen_door:
                vd = z.vervangen_door
                session.execute_write(merge_node, 'Zaak', 'nummer', {'nummer': vd.nummer})
                session.execute_write(merge_rel, 'Zaak', 'nummer', z.nummer, 'Zaak', 'nummer', vd.nummer, 'REPLACED_BY')

            for attr,(label,rel,key) in REL_MAP_ZAAK.items():
                items = getattr(z,attr) or []
                if not isinstance(items,list): items=[items]
                for it in items:
                    val = getattr(it,key)
                    session.execute_write(merge_node,label,key,{key:val})
                    session.execute_write(merge_rel,'Zaak','nummer',z.nummer,label,key,val,rel)
    print("✅ Loaded Zaken.")

