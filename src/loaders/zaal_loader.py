import time
from tkapi import TKApi
from tkapi.activiteit import Zaal
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel
from core.config.constants import REL_MAP_ZAAL

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry

# Import checkpoint functionality
from core.checkpoint.checkpoint_decorator import checkpoint_loader


class ZaalLoader(BaseLoader):
    """Loader for Zaal entities with full interface support"""
    
    def __init__(self):
        super().__init__(
            name="zaal_loader",
            description="Loads Zaal from TK API with related entities"
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
            load_zalen(conn)
            result.success = True
            result.execution_time_seconds = time.time() - start_time
            
        except Exception as e:
            result.error_messages.append(f"Loading failed: {str(e)}")
            result.execution_time_seconds = time.time() - start_time
        
        return result


# Register the loader
zaal_loader_instance = ZaalLoader()
loader_registry.register(zaal_loader_instance)


@checkpoint_loader(checkpoint_interval=25)
def load_zalen(conn: Neo4jConnection, batch_size: int = 50, _checkpoint_context=None):
    """
    Load Zaal entities with automatic checkpoint support.
    
    Args:
        conn: Neo4j connection
        batch_size: Number of items to process in each batch
    """
    api = TKApi()
    
    # Fetch all Zaal entities
    zalen = api.get_items(Zaal, max_items=batch_size) if batch_size else api.get_items(Zaal)
    print(f"→ Fetched {len(zalen)} Zalen")
    
    if not zalen:
        print("No Zalen found.")
        return
    
    with conn.driver.session(database=conn.database) as session:
        for idx, zaal in enumerate(zalen, 1):
            if idx % 25 == 0 or idx == len(zalen):
                print(f"  → Processing Zaal {idx}/{len(zalen)}: {zaal.id}")
            
            # Create main Zaal node
            props = {
                'id': zaal.id,
                'naam': getattr(zaal, 'naam', None),
                'nummer': getattr(zaal, 'nummer', None),
                'verdieping': getattr(zaal, 'verdieping', None),
                'gebouw': getattr(zaal, 'gebouw', None),
                'capaciteit': getattr(zaal, 'capaciteit', None),
                'openbaar': getattr(zaal, 'openbaar', None)
            }
            session.execute_write(merge_node, 'Zaal', 'id', props)
            
            # Process relationships
            for rel_name, (target_label, rel_type, target_key) in REL_MAP_ZAAL.items():
                rel_items = getattr(zaal, rel_name, [])
                if not isinstance(rel_items, (list, tuple)):
                    rel_items = [rel_items] if rel_items else []
                
                for rel_obj in rel_items:
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
                            session.execute_write(merge_rel, 'Zaal', 'id', zaal.id, 
                                                target_label, target_key, target_value, rel_type)
    
    print("✅ Loaded Zalen and related entities.") 