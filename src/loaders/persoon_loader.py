from tkapi import TKApi
from tkapi.persoon import Persoon
from core.connection.neo4j_connection import Neo4jConnection
# Helpers
from utils.helpers import merge_node, merge_rel, batch_check_nodes_exist

# Relationship map
from core.config.constants import REL_MAP_PERSOON, REL_MAP_PERSOON_NEVENFUNCTIE

from tkapi.fractie import FractieZetelPersoon
# Monkey-patch: some tkapi versions expect `FractieZetelPersoonOk`; alias it if absent
import tkapi.fractie as _tk_fractie
if not hasattr(_tk_fractie, 'FractieZetelPersoonOk'):
    _tk_fractie.FractieZetelPersoonOk = FractieZetelPersoon
from tkapi.persoon import (
    PersoonContactinformatie,
    PersoonGeschenk,
    PersoonLoopbaan,
    PersoonNevenfunctie,
    PersoonNevenfunctieInkomsten,
    PersoonOnderwijs,
    PersoonReis,
)

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry

# Import checkpoint functionality
from core.checkpoint.checkpoint_manager import CheckpointManager
from core.checkpoint.checkpoint_decorator import checkpoint_loader

# Import processors and threading utilities
from .processors.persoon_processor import process_single_persoon, process_single_persoon_threaded
from .threading.threaded_loader import process_items_threaded

import time
import datetime
import json
from pathlib import Path

api = TKApi()


class PersoonLoader(BaseLoader):
    """Loader for Persoon entities with full interface support"""
    
    def __init__(self):
        super().__init__(
            name="persoon_loader",
            description="Loads Personen from TK API with threading support"
        )
        self._capabilities = [
            LoaderCapability.THREADING,
            LoaderCapability.BATCH_PROCESSING,
            LoaderCapability.SKIP_FUNCTIONALITY,
            LoaderCapability.ID_CHECKING,
            LoaderCapability.INCREMENTAL_LOADING,
            LoaderCapability.RELATIONSHIP_PROCESSING
        ]
    
    def validate_config(self, config: LoaderConfig) -> list[str]:
        """Validate configuration specific to PersoonLoader"""
        errors = super().validate_config(config)
        
        # Add specific validation for Persoon loader
        if config.custom_params and 'overwrite' in config.custom_params:
            if not isinstance(config.custom_params['overwrite'], bool):
                errors.append("custom_params.overwrite must be a boolean")
        
        return errors
    
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
            
            # Extract parameters
            overwrite = config.custom_params.get('overwrite', False) if config.custom_params else False
            
            # Use the appropriate loading function
            if config.enable_threading:
                stats = load_personen_threaded(
                    conn=conn,
                    batch_size=config.batch_size,
                    max_workers=config.max_workers,
                    skip_count=config.skip_count,
                    overwrite=overwrite,
                    checkpoint_manager=checkpoint_manager
                )
            else:
                load_personen(
                    conn=conn,
                    batch_size=config.batch_size,
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
persoon_loader_instance = PersoonLoader()
loader_registry.register(persoon_loader_instance)


def _fetch_personen_from_api(batch_size: int | None = None):
    """Fetch personen from TK API"""
    api = TKApi()

    if batch_size is not None and batch_size > 0:
        personen = api.get_items(Persoon, max_items=batch_size)
    else:
        # Fetch everything (no limit)
        personen = api.get_items(Persoon)
    
    print(f"→ Fetched {len(personen)} Personen")
    return personen


@checkpoint_loader(checkpoint_interval=25)
def load_personen(conn: Neo4jConnection, batch_size: int | None = None, 
                  skip_count: int = 0, overwrite: bool = False, _checkpoint_context=None):
    """Load all Personen with automatic checkpoint support using decorator."""
    personen = _fetch_personen_from_api(batch_size)
    
    if not personen:
        print("No personen found.")
        return

    # Apply skip_count if specified
    if skip_count > 0:
        if skip_count >= len(personen):
            print(f"⚠️ Skip count ({skip_count}) is greater than or equal to total items ({len(personen)}). Nothing to process.")
            return
        personen = personen[skip_count:]
        print(f"⏭️ Skipping first {skip_count} items. Processing {len(personen)} remaining items.")

    # Check which personen already exist in Neo4j (unless overwrite is enabled)
    if not overwrite and personen:
        print("🔍 Checking which Personen already exist in Neo4j...")
        persoon_ids = [p.id for p in personen if p and p.id]
        
        with conn.driver.session(database=conn.database) as session:
            existing_ids = batch_check_nodes_exist(session, "Persoon", "id", persoon_ids)
        
        if existing_ids:
            original_count = len(personen)
            personen = [p for p in personen if p.id not in existing_ids]
            filtered_count = len(personen)
            print(f"📊 Found {len(existing_ids)} existing Personen in Neo4j")
            print(f"⏭️ Skipping {original_count - filtered_count} existing items. Processing {filtered_count} new items.")
            
            if not personen:
                print("✅ All Personen already exist in Neo4j. Nothing to process.")
                return
        else:
            print("📊 No existing Personen found in Neo4j. Processing all items.")
    elif overwrite:
        print("🔄 Overwrite mode enabled - processing all items regardless of existing data")

    # Open debug file once per run (overwrite existing)
    debug_path = Path('debug_personen.jsonl')
    with debug_path.open('w', encoding='utf-8') as dbg:
        def process_wrapper(persoon_obj):
            with conn.driver.session(database=conn.database) as session:
                return process_single_persoon(session, persoon_obj, debug_file=dbg)

        # Use the checkpoint context to process items automatically
        if _checkpoint_context:
            _checkpoint_context.process_items(personen, process_wrapper)
        else:
            # Fallback for when decorator is not used
            for i, p in enumerate(personen, 1):
                if i % 100 == 0 or i == len(personen):
                    print(f"  → Processing Persoon {i}/{len(personen)}")
                process_wrapper(p)

    print("✅ Loaded Personen and their related entities.")


def load_personen_threaded(conn: Neo4jConnection, batch_size: int | None = None, 
                          max_workers: int = 10, skip_count: int = 0, overwrite: bool = False, 
                          checkpoint_manager: CheckpointManager = None):
    """
    Load Personen using multithreading for faster processing.
    """
    personen = _fetch_personen_from_api(batch_size)
    
    if not personen:
        print("No personen found.")
        return {"processed": 0, "failed": 0, "skipped": 0, "total": 0}

    # Use the generic threaded processor
    return process_items_threaded(
        items=personen,
        process_func=process_single_persoon_threaded,
        conn=conn,
        max_workers=max_workers,
        checkpoint_manager=checkpoint_manager,
        loader_name="load_personen_threaded",
        skip_count=skip_count,
        overwrite=overwrite,
        node_label="Persoon"
    )


# Backward compatibility function
def load_personen_original(conn: Neo4jConnection, batch_size: int | None = None):
    """Original load_personen function for backward compatibility."""
    return load_personen(conn, batch_size)


# Helper to safely fetch dates that might be YYYY or YYYY-MM
def _safe_date_str(obj, attr: str):
    """Return a string representation of the date attr but survive malformed TKApi dates (e.g. YYYY-MM)."""
    try:
        val = getattr(obj, attr, None)
    except ValueError:
        # Fall back to raw JSON value if available
        raw = getattr(obj, 'json', {}).get(attr.capitalize()) if hasattr(obj, 'json') else None
        return raw
    return str(val) if val else None
