import time
from tkapi import TKApi
from tkapi.document import Kamerstukdossier
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel
from core.config.constants import REL_MAP_KAMERSTUKDOSSIER

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry

# Import checkpoint functionality
from core.checkpoint.checkpoint_decorator import checkpoint_loader


class KamerstukdossierLoader(BaseLoader):
    """Loader for Kamerstukdossier entities with full interface support"""
    
    def __init__(self):
        super().__init__(
            name="kamerstukdossier_loader",
            description="Loads Kamerstukdossier from TK API with related entities"
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
            load_kamerstukdossiers(conn)
            result.success = True
            result.execution_time_seconds = time.time() - start_time
            
        except Exception as e:
            result.error_messages.append(f"Loading failed: {str(e)}")
            result.execution_time_seconds = time.time() - start_time
        
        return result


# Register the loader
kamerstukdossier_loader_instance = KamerstukdossierLoader()
loader_registry.register(kamerstukdossier_loader_instance)


@checkpoint_loader(checkpoint_interval=25)
def load_kamerstukdossiers(conn: Neo4jConnection, batch_size: int = 50, _checkpoint_context=None):
    """
    Load Kamerstukdossier entities with automatic checkpoint support.
    
    Args:
        conn: Neo4j connection
        batch_size: Number of items to process in each batch
    """
    api = TKApi()
    
    # Fetch all Kamerstukdossier entities
    dossiers = api.get_items(Kamerstukdossier, max_items=batch_size) if batch_size else api.get_items(Kamerstukdossier)
    print(f"→ Fetched {len(dossiers)} Kamerstukdossiers")
    
    if not dossiers:
        print("No Kamerstukdossiers found.")
        return
    
    with conn.driver.session(database=conn.database) as session:
        for idx, dossier in enumerate(dossiers, 1):
            if idx % 25 == 0 or idx == len(dossiers):
                print(f"  → Processing Kamerstukdossier {idx}/{len(dossiers)}: {dossier.id}")
            
            # Create main Kamerstukdossier node
            props = {
                'id': dossier.id,
                'nummer': getattr(dossier, 'nummer', None),
                'toevoegingsnummer': getattr(dossier, 'toevoegingsnummer', None),
                'titel': getattr(dossier, 'titel', None),
                'type': getattr(dossier, 'type', None),
                'afgedaan': getattr(dossier, 'afgedaan', None),
                'status': getattr(dossier, 'status', None),
                'aangemaakt': str(getattr(dossier, 'aangemaakt', None)) if getattr(dossier, 'aangemaakt', None) else None,
                'gewijzigd': str(getattr(dossier, 'gewijzigd', None)) if getattr(dossier, 'gewijzigd', None) else None
            }
            session.execute_write(merge_node, 'Kamerstukdossier', 'id', props)
            
            # Process relationships
            for rel_name, (target_label, rel_type, target_key) in REL_MAP_KAMERSTUKDOSSIER.items():
                rel_items = getattr(dossier, rel_name, [])
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
                            
                            session.execute_write(merge_node, target_label, target_key, target_props)
                            session.execute_write(merge_rel, 'Kamerstukdossier', 'id', dossier.id, 
                                                target_label, target_key, target_value, rel_type)
    
    print("✅ Loaded Kamerstukdossiers and related entities.") 