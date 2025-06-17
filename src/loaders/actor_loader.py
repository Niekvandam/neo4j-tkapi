from tkapi import TKApi
from tkapi.activiteit import ActiviteitActor
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel
from constants import REL_MAP_ACTOR

api = TKApi()

def load_activiteit_actors(conn: Neo4jConnection, batch_size: int = 50):
    api = TKApi()
    ActiviteitActor.expand_params = ['Activiteit','Persoon','Fractie','Commissie']
    actors = api.get_items(ActiviteitActor, max_items=batch_size)
    print(f"→ Fetched {len(actors)} ActiviteitActors")
    with conn.driver.session(database=conn.database) as session:
        for i, act in enumerate(actors, 1):
            if i % 100 == 0 or i == len(actors):
                print(f"  → Processing ActiviteitActor {i}/{len(actors)}")
            # merge actor node
            props = {
                'id': act.id,
                'naam': act.naam,
                'functie': act.functie,
                'fractie_naam': act.fractie_naam,
                'spreektijd': act.spreektijd,
                'volgorde': act.volgorde
            }
            session.execute_write(merge_node, 'ActiviteitActor', 'id', props)
            # link enum relatie
            if act.relatie:
                session.execute_write(merge_rel,
                    'ActiviteitActor','id',act.id,
                    'ActiviteitRelatieSoort','key',act.relatie.name,
                    'HAS_RELATIE'
                )
            # link related entities
            for attr,(label,rel,key) in REL_MAP_ACTOR.items():
                related = getattr(act,attr, None)
                if not related: continue
                items = [related] if not isinstance(related,list) else related
                for it in items:
                    val = getattr(it,key)
                    session.execute_write(merge_node, label, key, {key: val})
                    session.execute_write(
                        merge_rel,
                        'ActiviteitActor','id',act.id,
                        label,key,val,
                        rel
                    )
    print("✅ Loaded ActiviteitActors.")
