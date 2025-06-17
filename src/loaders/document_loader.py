from tkapi import TKApi
from tkapi.document import Document
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel
from constants import REL_MAP_DOC

api = TKApi()

def load_documents(conn: Neo4jConnection, batch_size: int = 50):
    api = TKApi()
    Document.expand_params = ['DocumentActor','Activiteit','Zaak','DocumentVersie','Agendapunt']
    documents = api.get_items(Document, max_items=batch_size)
    print(f"→ Fetched {len(documents)} Documents")
    with conn.driver.session(database=conn.database) as session:
        for i, d in enumerate(documents, 1):
            if i % 100 == 0 or i == len(documents):
                print(f"  → Processing Document {i}/{len(documents)}")
            props = {'id':d.id,'nummer':d.nummer,'volgnummer':d.volgnummer,'titel':d.titel or '', 'datum':str(d.datum) if d.datum else None}
            session.execute_write(merge_node,'Document','id',props)
            if d.soort:
                session.execute_write(merge_rel,'Document','id',d.id,'DocumentSoort','key',d.soort.name,'HAS_SOORT')
            for attr,(label,rel,key) in REL_MAP_DOC.items():
                items = getattr(d,attr) or []
                if not isinstance(items,list): items=[items]
                for it in items:
                    val = getattr(it,key)
                    session.execute_write(merge_node,label,key,{key:val})
                    session.execute_write(merge_rel,'Document','id',d.id,label,key,val,rel)
    print("✅ Loaded Documents.")