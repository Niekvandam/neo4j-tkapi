"""
Zaak Loader - Loads Zaken from TK API with interface support
"""
import datetime
import time
from tkapi import TKApi
from tkapi.zaak import Zaak, ZaakFilter
from core.config.tkapi_config import create_tkapi_with_timeout
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import batch_check_nodes_exist
from tkapi.util import util as tkapi_util
from datetime import timezone

# Import checkpoint functionality
from core.checkpoint.checkpoint_manager import CheckpointManager
from core.checkpoint.checkpoint_decorator import checkpoint_loader

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry

# Import processors and threading utilities
from .processors.zaak_processor import process_single_zaak, process_single_zaak_threaded
from .processors.common_processors import PROCESSED_DOCUMENT_IDS
from .threading.threaded_loader import process_items_threaded


class ZaakLoader(BaseLoader):
    """Loader for Zaak entities with full interface support"""
    
    def __init__(self):
        super().__init__(
            name="zaak_loader",
            description="Loads Zaken from TK API with related entities and checkpoint support"
        )
        self._capabilities = [
            LoaderCapability.THREADING,
            LoaderCapability.BATCH_PROCESSING,
            LoaderCapability.DATE_FILTERING,
            LoaderCapability.SKIP_FUNCTIONALITY,
            LoaderCapability.INCREMENTAL_LOADING,
            LoaderCapability.RELATIONSHIP_PROCESSING
        ]
    
    def validate_config(self, config: LoaderConfig) -> list[str]:
        """Validate configuration specific to ZaakLoader"""
        errors = super().validate_config(config)
        
        # Add specific validation for Zaak loader
        if config.custom_params and 'overwrite' in config.custom_params:
            if not isinstance(config.custom_params['overwrite'], bool):
                errors.append("custom_params.overwrite must be a boolean")
        
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
            
            # Extract parameters
            overwrite = config.custom_params.get('overwrite', False) if config.custom_params else False
            
            # Use the appropriate loading function
            if config.enable_threading:
                stats = load_zaken_threaded(
                    conn=conn,
                    batch_size=config.batch_size,
                    start_date_str=config.start_date or "2024-01-01",
                    max_workers=config.max_workers,
                    skip_count=config.skip_count,
                    overwrite=overwrite,
                    checkpoint_manager=checkpoint_manager
                )
            else:
                load_zaken(
                    conn=conn,
                    batch_size=config.batch_size,
                    start_date_str=config.start_date or "2024-01-01",
                    skip_count=config.skip_count,
                    overwrite=overwrite
                )
                stats = {"processed": 0, "failed": 0}  # Placeholder for non-threaded version
            
            # Update result with statistics
            result.success = True
            result.processed_count = stats.get("processed", 0)
            result.failed_count = stats.get("failed", 0)
            result.skipped_count = stats.get("skipped", 0)
            result.total_items = stats.get("total", 0)
            result.execution_time_seconds = time.time() - start_time
            
        except Exception as e:
            result.error_messages.append(f"Loading failed: {str(e)}")
            result.execution_time_seconds = time.time() - start_time
        
        return result


# Register the loader
zaak_loader_instance = ZaakLoader()
loader_registry.register(zaak_loader_instance)


def _fetch_zaken_from_api(start_date_str: str = "2024-01-01"):
    """Fetch zaken from TK API with date filtering"""
    api = create_tkapi_with_timeout(
        connect_timeout=15.0,
        read_timeout=300.0,
        max_retries=3
    )
    start_datetime_obj = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    odata_start_date_str = tkapi_util.datetime_to_odata(start_datetime_obj.replace(tzinfo=timezone.utc))

    filter = Zaak.create_filter()
    # Zaak entities use 'GestartOp' field for start date filtering
    filter.add_filter_str(f"GestartOp ge {odata_start_date_str}")
    
    zaken_api = api.get_items(Zaak, filter=filter)
    print(f"→ Fetched {len(zaken_api)} Zaken since {start_date_str}")
    
    return zaken_api


@checkpoint_loader(checkpoint_interval=25)
def load_zaken(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", 
               skip_count: int = 0, overwrite: bool = False, _checkpoint_context=None):
    """
    Load Zaken with automatic checkpoint support using decorator.
    """
    zaken_api = _fetch_zaken_from_api(start_date_str)
    
    if not zaken_api:
        print("No zaken found for the date range.")
        return

    # Apply skip_count if specified
    if skip_count > 0:
        if skip_count >= len(zaken_api):
            print(f"⚠️ Skip count ({skip_count}) is greater than or equal to total items ({len(zaken_api)}). Nothing to process.")
            return
        zaken_api = zaken_api[skip_count:]
        print(f"⏭️ Skipping first {skip_count} items. Processing {len(zaken_api)} remaining items.")

    # Check which zaken already exist in Neo4j (unless overwrite is enabled)
    if not overwrite and zaken_api:
        print("🔍 Checking which Zaken already exist in Neo4j...")
        zaak_nummers = [zaak.nummer for zaak in zaken_api if zaak and zaak.nummer]
        
        with conn.driver.session(database=conn.database) as session:
            existing_nummers = batch_check_nodes_exist(session, "Zaak", "nummer", zaak_nummers)
        
        if existing_nummers:
            original_count = len(zaken_api)
            zaken_api = [zaak for zaak in zaken_api if zaak.nummer not in existing_nummers]
            filtered_count = len(zaken_api)
            print(f"📊 Found {len(existing_nummers)} existing Zaken in Neo4j")
            print(f"⏭️ Skipping {original_count - filtered_count} existing items. Processing {filtered_count} new items.")
            
            if not zaken_api:
                print("✅ All Zaken already exist in Neo4j. Nothing to process.")
                return
        else:
            print("📊 No existing Zaken found in Neo4j. Processing all items.")
    elif overwrite:
        print("🔄 Overwrite mode enabled - processing all items regardless of existing data")

    def process_wrapper(zaak_obj):
        with conn.driver.session(database=conn.database) as session:
            return process_single_zaak(session, zaak_obj)

    # Clear processed IDs at the beginning
    PROCESSED_DOCUMENT_IDS.clear()

    # Use the checkpoint context to process items automatically
    if _checkpoint_context:
        _checkpoint_context.process_items(zaken_api, process_wrapper)
    else:
        # Fallback for when decorator is not used
        for zaak_obj in zaken_api:
            process_wrapper(zaak_obj)

    print("✅ Loaded Zaken and their related entities.")


# -------------------------------------------------------------
# Helper exposed for nested processors (to avoid circular import)
# -------------------------------------------------------------


def process_and_load_zaak(session, zaak_obj, related_entity_id: str | None = None, related_entity_type: str | None = None):
    """Lightweight wrapper so other loaders can process a single Zaak.

    It simply delegates to :pyfunc:`process_single_zaak` which already
    creates/merges the node and its direct relationships.  The optional
    *related_entity_id* / *related_entity_type* arguments are accepted for
    signature compatibility but are not used at this point.
    """
    try:
        return process_single_zaak(session, zaak_obj)
    except Exception as exc:
        print(f"    ❌ Failed to process nested Zaak {getattr(zaak_obj, 'nummer', 'UNKNOWN')}: {exc}")
        return False


def load_zaken_threaded(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", 
                       max_workers: int = 10, skip_count: int = 0, overwrite: bool = False, 
                       checkpoint_manager: CheckpointManager = None):
    """
    Load Zaken using multithreading for faster processing.
    """
    zaken_api = _fetch_zaken_from_api(start_date_str)
    
    if not zaken_api:
        print("No zaken found for the date range.")
        return {"processed": 0, "failed": 0, "skipped": 0, "total": 0}

    # Clear processed IDs at the beginning
    PROCESSED_DOCUMENT_IDS.clear()

    # Use the generic threaded processor
    return process_items_threaded(
        items=zaken_api,
        process_func=process_single_zaak_threaded,
        conn=conn,
        max_workers=max_workers,
        checkpoint_manager=checkpoint_manager,
        loader_name="load_zaken_threaded",
        skip_count=skip_count,
        overwrite=overwrite,
        node_label="Zaak"
    )


# Backward compatibility function
def load_zaken_original(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", checkpoint_manager=None):
    """Original load_zaken function for backward compatibility."""
    return load_zaken(conn, batch_size, start_date_str)