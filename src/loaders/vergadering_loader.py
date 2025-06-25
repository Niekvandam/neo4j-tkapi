import datetime
import time
from tkapi import TKApi
from tkapi.vergadering import Vergadering, VergaderingFilter, VergaderingSoort # Ensure VergaderingFilter & Soort are imported
from tkapi.verslag import Verslag
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel
from .common_processors import process_and_load_verslag, PROCESSED_VERSLAG_IDS, download_verslag_xml, process_and_load_zaak, PROCESSED_ZAAK_IDS
from tkapi.util import util as tkapi_util
from datetime import timezone, timedelta

# Import checkpoint functionality
from core.checkpoint.checkpoint_manager import LoaderCheckpoint, CheckpointManager

# Import the checkpoint decorator
from core.checkpoint.checkpoint_decorator import checkpoint_loader

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry

# Timezone handling for creating API query filters
LOCAL_TIMEZONE_OFFSET_HOURS_API = 2 # Example: CEST

# Import processor functions
from .processors.vergadering_processor import (
    process_and_load_vergadering, 
    process_single_vergadering, 
    setup_vergadering_api_filter,
    REL_MAP_VERGADERING
)


class VergaderingLoader(BaseLoader):
    """Loader for Vergadering entities with full interface support"""
    
    def __init__(self):
        super().__init__(
            name="vergadering_loader",
            description="Loads Vergaderingen from TK API with related entities and checkpoint support"
        )
        self._capabilities = [
            LoaderCapability.BATCH_PROCESSING,
            LoaderCapability.DATE_FILTERING,
            LoaderCapability.SKIP_FUNCTIONALITY,
            LoaderCapability.INCREMENTAL_LOADING,
            LoaderCapability.RELATIONSHIP_PROCESSING
        ]
    
    def validate_config(self, config: LoaderConfig) -> list[str]:
        """Validate configuration specific to VergaderingLoader"""
        errors = super().validate_config(config)
        
        # Add specific validation for Vergadering loader
        if config.custom_params and 'process_xml' in config.custom_params:
            if not isinstance(config.custom_params['process_xml'], bool):
                errors.append("custom_params.process_xml must be a boolean")
        
        return errors
    
    def load(self, conn: Neo4jConnection, config: LoaderConfig, 
             checkpoint_manager: CheckpointManager = None) -> LoaderResult:
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
            load_result = load_vergaderingen(
                conn=conn,
                batch_size=config.batch_size,
                start_date_str=config.start_date or "2024-01-01",
                skip_count=config.skip_count
            )
            
            # For now, we'll mark as successful if no exceptions occurred
            result.success = True
            result.execution_time_seconds = time.time() - start_time
            
        except Exception as e:
            result.error_messages.append(f"Loading failed: {str(e)}")
            result.execution_time_seconds = time.time() - start_time
        
        return result


# Register the loader
vergadering_loader_instance = VergaderingLoader()
loader_registry.register(vergadering_loader_instance)


# Processor function moved to processors/vergadering_processor.py


@checkpoint_loader(checkpoint_interval=25)
def load_vergaderingen(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", skip_count: int = 0, _checkpoint_context=None):
    """
    Load Vergaderingen with automatic checkpoint support using decorator.
    
    The @checkpoint_loader decorator automatically handles:
    - Progress tracking every 25 items
    - Skipping already processed items
    - Error handling and logging
    - Final progress reporting
    """
    api = TKApi()
    filter_obj = setup_vergadering_api_filter(start_date_str)
    vergaderingen_api = api.get_items(Vergadering, filter=filter_obj)
    print(f"→ Fetched {len(vergaderingen_api)} Vergaderingen since {start_date_str}")

    if not vergaderingen_api:
        print("No vergaderingen found for the date range.")
        return

    # Apply skip_count if specified
    if skip_count > 0:
        if skip_count >= len(vergaderingen_api):
            print(f"⚠️ Skip count ({skip_count}) is greater than or equal to total items ({len(vergaderingen_api)}). Nothing to process.")
            return
        vergaderingen_api = vergaderingen_api[skip_count:]
        print(f"⏭️ Skipping first {skip_count} items. Processing {len(vergaderingen_api)} remaining items.")

    def process_vergadering_wrapper(vergadering_obj):
        process_single_vergadering(conn, vergadering_obj)

    # Clear processed IDs at the beginning
    PROCESSED_ZAAK_IDS.clear()

    # Use the checkpoint context to process items automatically
    if _checkpoint_context:
        _checkpoint_context.process_items(vergaderingen_api, process_vergadering_wrapper)
    else:
        # Fallback for when decorator is not used
        for vergadering_obj in vergaderingen_api:
            process_vergadering_wrapper(vergadering_obj)

    print("✅ Loaded Vergaderingen and their related entities.")


# Keep the original function for backward compatibility (if needed)
def load_vergaderingen_original(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", checkpoint_manager=None):
    """
    Original load_vergaderingen function for backward compatibility.
    This version is deprecated - use load_vergaderingen() for the new decorator-based version.
    """
    # This could import the old implementation if needed, but for now we'll use the new one
    return load_vergaderingen(conn, batch_size, start_date_str)