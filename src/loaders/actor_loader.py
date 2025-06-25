import datetime
from tkapi import TKApi
from tkapi.activiteit import ActiviteitActor
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel
from core.config.constants import REL_MAP_ACTOR

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry
import time

api = TKApi()


class ActorLoader(BaseLoader):
    """Loader for ActiviteitActor entities with full interface support"""
    
    def __init__(self):
        super().__init__(
            name="actor_loader",
            description="Loads ActiviteitActors from TK API with related entities"
        )
        self._capabilities = [
            LoaderCapability.BATCH_PROCESSING,
            LoaderCapability.RELATIONSHIP_PROCESSING
        ]
    
    def load(self, conn: Neo4jConnection, config: LoaderConfig, 
             checkpoint_manager=None) -> LoaderResult:
        """Main loading method implementing the interface"""
        start_time = time.time()
        result = LoaderResult(
            success=False,
            processed_count=0,
            failed_count=0,
            skipped_count=0,
            total_items=0,
            execution_time_seconds=0.0,
            error_messages=[],
            warnings=[]
        )
        
        try:
            # Validate configuration
            validation_errors = self.validate_config(config)
            if validation_errors:
                result.error_messages.extend(validation_errors)
                return result
            
            # Use the existing function for actual loading
            load_activiteit_actors(conn, config.batch_size)
            
            # For now, we'll mark as successful if no exceptions occurred
            result.success = True
            result.execution_time_seconds = time.time() - start_time
            
        except Exception as e:
            result.error_messages.append(f"Loading failed: {str(e)}")
            result.execution_time_seconds = time.time() - start_time
        
        return result


# Register the loader
actor_loader_instance = ActorLoader()
loader_registry.register(actor_loader_instance)


def load_activiteit_actors(conn: Neo4jConnection, batch_size: int = 50):
    api = TKApi()
    ActiviteitActor.expand_params = ['Activiteit','Persoon','Fractie','Commissie']
    filter = ActiviteitActor.create_filter()
    filter.add_filter_str("Datum ge 2024-01-01")
    actors = api.get_items(ActiviteitActor, filter=filter)
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
