import time
from tkapi import TKApi
from tkapi.persoon import PersoonFunctie
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel
from core.config.constants import REL_MAP_PERSOON_FUNCTIE

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry

# Import checkpoint functionality
from core.checkpoint.checkpoint_decorator import checkpoint_loader


class PersoonFunctieLoader(BaseLoader):
    """Loader for PersoonFunctie entities with full interface support"""
    
    def __init__(self):
        super().__init__(
            name="persoonfunctie_loader",
            description="Loads PersoonFunctie from TK API with related entities"
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
            load_persoon_functies(conn)
            result.success = True
            result.execution_time_seconds = time.time() - start_time
            
        except Exception as e:
            result.error_messages.append(f"Loading failed: {str(e)}")
            result.execution_time_seconds = time.time() - start_time
        
        return result


# Register the loader
persoonfunctie_loader_instance = PersoonFunctieLoader()
loader_registry.register(persoonfunctie_loader_instance)


@checkpoint_loader(checkpoint_interval=25)
def load_persoon_functies(conn: Neo4jConnection, batch_size: int = 50, _checkpoint_context=None):
    """
    Load PersoonFunctie entities with automatic checkpoint support.
    
    Args:
        conn: Neo4j connection
        batch_size: Number of items to process in each batch
    """
    api = TKApi()
    
    # Fetch all PersoonFunctie entities
    persoon_functies = api.get_items(PersoonFunctie, max_items=batch_size) if batch_size else api.get_items(PersoonFunctie)
    print(f"→ Fetched {len(persoon_functies)} PersoonFuncties")
    
    if not persoon_functies:
        print("No PersoonFuncties found.")
        return
    
    with conn.driver.session(database=conn.database) as session:
        for idx, functie in enumerate(persoon_functies, 1):
            if idx % 25 == 0 or idx == len(persoon_functies):
                print(f"  → Processing PersoonFunctie {idx}/{len(persoon_functies)}: {functie.id}")
            
            # Create main PersoonFunctie node
            props = {
                'id': functie.id,
                'functie': getattr(functie, 'functie', None),
                'omschrijving': getattr(functie, 'omschrijving', None),
                'van': str(getattr(functie, 'van', None)) if getattr(functie, 'van', None) else None,
                'tot_en_met': str(getattr(functie, 'tot_en_met', None)) if getattr(functie, 'tot_en_met', None) else None,
                'soort': getattr(functie, 'soort', None)
            }
            session.execute_write(merge_node, 'PersoonFunctie', 'id', props)
            
            # Process relationships
            for rel_name, (target_label, rel_type, target_key) in REL_MAP_PERSOON_FUNCTIE.items():
                rel_obj = getattr(functie, rel_name, None)
                if rel_obj:
                    target_value = getattr(rel_obj, target_key, None)
                    if target_value:
                        # Ensure target node exists (minimal)
                        target_props = {target_key: target_value}
                        if hasattr(rel_obj, 'naam'):
                            target_props['naam'] = getattr(rel_obj, 'naam', None)
                        
                        session.execute_write(merge_node, target_label, target_key, target_props)
                        session.execute_write(merge_rel, 'PersoonFunctie', 'id', functie.id, 
                                            target_label, target_key, target_value, rel_type)
    
    print("✅ Loaded PersoonFuncties and related entities.") 