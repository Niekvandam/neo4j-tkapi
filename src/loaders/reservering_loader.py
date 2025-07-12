import time
from tkapi import TKApi
from tkapi.activiteit import Reservering
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel
from core.config.constants import REL_MAP_RESERVERING

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry

# Import checkpoint functionality
from core.checkpoint.checkpoint_decorator import checkpoint_loader


class ReserveringLoader(BaseLoader):
    """Loader for Reservering entities with full interface support"""
    
    def __init__(self):
        super().__init__(
            name="reservering_loader",
            description="Loads Reservering from TK API with related entities"
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
            load_reserveringen(conn)
            result.success = True
            result.execution_time_seconds = time.time() - start_time
            
        except Exception as e:
            result.error_messages.append(f"Loading failed: {str(e)}")
            result.execution_time_seconds = time.time() - start_time
        
        return result


# Register the loader
reservering_loader_instance = ReserveringLoader()
loader_registry.register(reservering_loader_instance)


@checkpoint_loader(checkpoint_interval=25)
def load_reserveringen(conn: Neo4jConnection, batch_size: int = 50, _checkpoint_context=None):
    """
    Load Reservering entities with automatic checkpoint support.
    
    Args:
        conn: Neo4j connection
        batch_size: Number of items to process in each batch
    """
    api = TKApi()
    
    # Fetch all Reservering entities
    reserveringen = api.get_items(Reservering, max_items=batch_size) if batch_size else api.get_items(Reservering)
    print(f"→ Fetched {len(reserveringen)} Reserveringen")
    
    if not reserveringen:
        print("No Reserveringen found.")
        return
    
    with conn.driver.session(database=conn.database) as session:
        for idx, reservering in enumerate(reserveringen, 1):
            if idx % 25 == 0 or idx == len(reserveringen):
                print(f"  → Processing Reservering {idx}/{len(reserveringen)}: {reservering.id}")
            
            # Create main Reservering node
            props = {
                'id': reservering.id,
                'naam': getattr(reservering, 'naam', None),
                'omschrijving': getattr(reservering, 'omschrijving', None),
                'van': str(getattr(reservering, 'van', None)) if getattr(reservering, 'van', None) else None,
                'tot_en_met': str(getattr(reservering, 'tot_en_met', None)) if getattr(reservering, 'tot_en_met', None) else None,
                'status': getattr(reservering, 'status', None),
                'soort': getattr(reservering, 'soort', None)
            }
            session.execute_write(merge_node, 'Reservering', 'id', props)
            
            # Process relationships
            for rel_name, (target_label, rel_type, target_key) in REL_MAP_RESERVERING.items():
                rel_obj = getattr(reservering, rel_name, None)
                if rel_obj:
                    target_value = getattr(rel_obj, target_key, None)
                    if target_value:
                        # Ensure target node exists (minimal)
                        target_props = {target_key: target_value}
                        if hasattr(rel_obj, 'naam'):
                            target_props['naam'] = getattr(rel_obj, 'naam', None)
                        if hasattr(rel_obj, 'titel'):
                            target_props['titel'] = getattr(rel_obj, 'titel', None)
                        if hasattr(rel_obj, 'onderwerp'):
                            target_props['onderwerp'] = getattr(rel_obj, 'onderwerp', None)
                        
                        session.execute_write(merge_node, target_label, target_key, target_props)
                        session.execute_write(merge_rel, 'Reservering', 'id', reservering.id, 
                                            target_label, target_key, target_value, rel_type)
    
    print("✅ Loaded Reserveringen and related entities.") 