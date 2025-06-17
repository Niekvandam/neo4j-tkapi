from tkapi import TKApi
from tkapi.activiteit import Activiteit
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel
from constants import REL_MAP_ACTIVITEIT

api = TKApi()

def load_activiteiten(conn: Neo4jConnection, batch_size: int = 50):
    api = TKApi()
    Activiteit.expand_params = ['Document','Agendapunt','ActiviteitActor']
    activiteiten = api.get_items(Activiteit, max_items=batch_size)
    print(f"→ Fetched {len(activiteiten)} Activiteiten")
    with conn.driver.session(database=conn.database) as session:
        for i, a in enumerate(activiteiten, 1):
            if i % 100 == 0 or i == len(activiteiten):
                print(f"  → Processing Activiteit {i}/{len(activiteiten)}")
            props = {'id':a.id,'nummer':a.nummer,'onderwerp':a.onderwerp or '',
                     'begin':str(a.begin) if a.begin else None,'einde':str(a.einde) if a.einde else None}
            session.execute_write(merge_node,'Activiteit','id',props)
            if a.soort:
                session.execute_write(merge_rel,'Activiteit','id',a.id,'ActiviteitSoort','key',a.soort.name,'HAS_SOORT')
            if a.status:
                session.execute_write(merge_rel,'Activiteit','id',a.id,'ActiviteitStatus','key',a.status.name,'HAS_STATUS')
            if a.datum_soort:
                session.execute_write(merge_rel,'Activiteit','id',a.id,'DatumSoort','key',a.datum_soort.name,'HAS_DATUMSOORT')
            for attr,(label,rel,key) in REL_MAP_ACTIVITEIT.items():
                items = getattr(a,attr) or []
                if not isinstance(items,list): items=[items]
                for it in items:
                    val = getattr(it,key)
                    session.execute_write(merge_node,label,key,{key:val})
                    session.execute_write(merge_rel,'Activiteit','id',a.id,label,key,val,rel)
    print("✅ Loaded Activiteiten.")

