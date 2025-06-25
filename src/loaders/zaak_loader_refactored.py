import datetime
from tkapi import TKApi
from tkapi.zaak import Zaak
from tkapi.document import Document # For expand_params
from tkapi.agendapunt import Agendapunt # For expand_params
from tkapi.activiteit import Activiteit # For expand_params
from tkapi.besluit import Besluit # For expand_params
from tkapi.dossier import Dossier # For expand_params (newly added)
# ZaakActor is also in Zaak.expand_params by default
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel
from core.config.constants import REL_MAP_ZAAK
import time

# Import processors for related entities that might need full processing
from .common_processors import process_and_load_besluit, PROCESSED_BESLUIT_IDS
from .common_processors import process_and_load_dossier, PROCESSED_DOSSIER_IDS
from tkapi.util import util as tkapi_util
from datetime import timezone

# Import the new checkpoint decorator
from core.checkpoint.checkpoint_decorator import checkpoint_zaak_loader

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry


class ZaakLoaderRefactored(BaseLoader):
    """Refactored loader for Zaken entities with full interface support"""
    
    def __init__(self):
        super().__init__(
            name="zaak_loader_refactored",
            description="Refactored Zaken loader from TK API with related entities and checkpoint support"
        )
        self._capabilities = [
            LoaderCapability.BATCH_PROCESSING,
            LoaderCapability.DATE_FILTERING,
            LoaderCapability.SKIP_FUNCTIONALITY,
            LoaderCapability.INCREMENTAL_LOADING,
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
            
            # Use the decorated function for actual loading
            load_result = load_zaken(
                conn=conn,
                batch_size=config.batch_size,
                start_date_str=config.start_date or "2024-01-01"
            )
            
            # For now, we'll mark as successful if no exceptions occurred
            result.success = True
            result.execution_time_seconds = time.time() - start_time
            
        except Exception as e:
            result.error_messages.append(f"Loading failed: {str(e)}")
            result.execution_time_seconds = time.time() - start_time
        
        return result


# Register the loader
zaak_loader_refactored_instance = ZaakLoaderRefactored()
loader_registry.register(zaak_loader_refactored_instance)


# Import processor functions
from .processors.zaak_loader_processor import process_and_load_zaak, setup_zaak_api_filter, restore_zaak_expand_params


@checkpoint_zaak_loader(checkpoint_interval=25)
def load_zaken(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", _checkpoint_context=None):
    """
    Load Zaken with automatic checkpoint support using decorator.
    
    The @checkpoint_zaak_loader decorator automatically handles:
    - Progress tracking every 25 items
    - Skipping already processed items
    - Error handling and logging
    - Final progress reporting
    """
    api = TKApi()
    filter_obj, original_zaak_expand_params = setup_zaak_api_filter(start_date_str)
    
    zaken_api = api.get_zaken(filter=filter_obj)
    print(f"→ Fetched {len(zaken_api)} Zaken since {start_date_str} (with expanded relations)")

    # --- Restore expand_params ---
    Zaak.expand_params = original_zaak_expand_params
    # ---

    if not zaken_api:
        print("No zaken found for the date range.")
        return

    # Define the processing function for a single Zaak
    def process_single_zaak(zaak_obj):
        with conn.driver.session(database=conn.database) as session:
            # Use the new processor function
            process_and_load_zaak(session, zaak_obj)

            # After process_and_load_zaak handles its internal relations,
            # explicitly process its direct Dossier if it's an expanded property
            if zaak_obj.dossier: # Access the single related dossier object
                dossier_obj_single = zaak_obj.dossier
                if process_and_load_dossier(session, dossier_obj_single):
                    pass # Dossier processed
                session.execute_write(merge_rel, 'Zaak', 'nummer', zaak_obj.nummer,
                                      'Dossier', 'id', dossier_obj_single.id, 'BELONGS_TO_DOSSIER')

    # Clear processed IDs at the beginning
    PROCESSED_BESLUIT_IDS.clear() # Reset for this scope
    PROCESSED_DOSSIER_IDS.clear() # Reset for this scope

    # Use the checkpoint context to process items automatically
    if _checkpoint_context:
        _checkpoint_context.process_items(zaken_api, process_single_zaak)
    else:
        # Fallback for when decorator is not used
        for zaak_obj in zaken_api:
            process_single_zaak(zaak_obj)

    print("✅ Loaded Zaken and their related Dossiers, Besluiten, etc.")


# Keep the original function for backward compatibility
def load_zaken_original(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", checkpoint_manager=None):
    """
    Original load_zaken function for backward compatibility.
    Use load_zaken() for the new decorator-based version.
    """
    # Import the original implementation if needed
    from .zaak_loader import load_zaken as original_load_zaken
    return original_load_zaken(conn, batch_size, start_date_str, checkpoint_manager) 