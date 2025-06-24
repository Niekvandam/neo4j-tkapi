# Removed incorrect ctypes import
import datetime
from tkapi import TKApi
from tkapi.activiteit import Activiteit, ActiviteitFilter # Import ActiviteitFilter
from tkapi.document import Document # For expand_params
from tkapi.agendapunt import Agendapunt # For expand_params
from tkapi.activiteit import ActiviteitActor # For expand_params
from tkapi.zaak import Zaak
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel, batch_check_nodes_exist
from core.config.constants import REL_MAP_ACTIVITEIT
from tkapi.util import util as tkapi_util # CORRECT IMPORT for datetime_to_odata
from datetime import timezone
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

# Import checkpoint functionality
from core.checkpoint.checkpoint_manager import LoaderCheckpoint, CheckpointManager

# Import the checkpoint decorator
from core.checkpoint.checkpoint_decorator import checkpoint_loader

# Import the common processors
from .common_processors import process_and_load_zaak, PROCESSED_ZAAK_IDS
from .agendapunt_loader import process_and_load_agendapunt

# Thread-safe lock for shared resources
_thread_lock = threading.Lock()
_processed_count = 0
_failed_count = 0

@checkpoint_loader(checkpoint_interval=25)
def load_activiteiten(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", skip_count: int = 0, overwrite: bool = False, _checkpoint_context=None):
    """
    Load Activiteiten with automatic checkpoint support using decorator.
    
    The @checkpoint_loader decorator automatically handles:
    - Progress tracking every 25 items
    - Skipping already processed items
    - Error handling and logging
    - Final progress reporting
    """
    api = TKApi()
    start_datetime_obj = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    odata_start_date_str = tkapi_util.datetime_to_odata(start_datetime_obj.replace(tzinfo=timezone.utc))

    filter = Activiteit.create_filter()
    filter.add_filter_str(f"Datum ge {odata_start_date_str}")
    
    activiteiten_api = api.get_items(Activiteit, filter=filter)
    print(f"→ Fetched {len(activiteiten_api)} Activiteiten since {start_date_str}")

    if not activiteiten_api:
        print("No activiteiten found for the date range.")
        return

    # Apply skip_count if specified
    if skip_count > 0:
        if skip_count >= len(activiteiten_api):
            print(f"⚠️ Skip count ({skip_count}) is greater than or equal to total items ({len(activiteiten_api)}). Nothing to process.")
            return
        activiteiten_api = activiteiten_api[skip_count:]
        print(f"⏭️ Skipping first {skip_count} items. Processing {len(activiteiten_api)} remaining items.")

    # Check which activiteiten already exist in Neo4j (unless overwrite is enabled)
    if not overwrite and activiteiten_api:
        print("🔍 Checking which Activiteiten already exist in Neo4j...")
        activiteit_ids = [act.id for act in activiteiten_api if act and act.id]
        
        with conn.driver.session(database=conn.database) as session:
            existing_ids = batch_check_nodes_exist(session, "Activiteit", "id", activiteit_ids)
        
        if existing_ids:
            # Filter out existing activiteiten
            original_count = len(activiteiten_api)
            activiteiten_api = [act for act in activiteiten_api if act.id not in existing_ids]
            filtered_count = len(activiteiten_api)
            print(f"📊 Found {len(existing_ids)} existing Activiteiten in Neo4j")
            print(f"⏭️ Skipping {original_count - filtered_count} existing items. Processing {filtered_count} new items.")
            
            if not activiteiten_api:
                print("✅ All Activiteiten already exist in Neo4j. Nothing to process.")
                return
        else:
            print("📊 No existing Activiteiten found in Neo4j. Processing all items.")
    elif overwrite:
        print("🔄 Overwrite mode enabled - processing all items regardless of existing data")

    def process_single_activiteit(activiteit_obj):
        with conn.driver.session(database=conn.database) as session:
            if not activiteit_obj or not activiteit_obj.id:
                return

            # Create Activiteit node
            props = {
                'id': activiteit_obj.id,
                'nummer': activiteit_obj.nummer,
                'onderwerp': activiteit_obj.onderwerp,
                'soort': activiteit_obj.soort,
                'datum': str(activiteit_obj.datum) if activiteit_obj.datum else None,
                'aanvangstijd': str(activiteit_obj.aanvangstijd) if activiteit_obj.aanvangstijd else None,
                'eindtijd': str(activiteit_obj.eindtijd) if activiteit_obj.eindtijd else None,
                'geplande_datum': str(activiteit_obj.geplande_datum) if activiteit_obj.geplande_datum else None,
                'voortouwcommissie': activiteit_obj.voortouwcommissie,
                'status': activiteit_obj.status
            }
            session.execute_write(merge_node, 'Activiteit', 'id', props)

            # Process related items
            for attr_name, (target_label, rel_type, target_key_prop) in REL_MAP_ACTIVITEIT.items():
                related_items = getattr(activiteit_obj, attr_name, []) or []
                if not isinstance(related_items, list):
                    related_items = [related_items]

                for related_item_obj in related_items:
                    if not related_item_obj:
                        continue
                    
                    related_item_key_val = getattr(related_item_obj, target_key_prop, None)
                    if related_item_key_val is None:
                        print(f"    ! Warning: Related item for '{attr_name}' in Activiteit {activiteit_obj.id} missing key '{target_key_prop}'.")
                        continue

                    if target_label == 'Zaak':
                        if process_and_load_zaak(session, related_item_obj, related_entity_id=activiteit_obj.id, related_entity_type="Activiteit"):
                            pass
                    elif target_label == 'Agendapunt':
                        # Process agendapunt fully since it belongs to this activiteit
                        if process_and_load_agendapunt(session, related_item_obj, related_activiteit_id=activiteit_obj.id):
                            pass
                    else:
                        session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val})

                    session.execute_write(merge_rel, 'Activiteit', 'id', activiteit_obj.id,
                                          target_label, target_key_prop, related_item_key_val, rel_type)

    # Clear processed IDs at the beginning
    PROCESSED_ZAAK_IDS.clear()

    # Use the checkpoint context to process items automatically
    if _checkpoint_context:
        _checkpoint_context.process_items(activiteiten_api, process_single_activiteit)
    else:
        # Fallback for when decorator is not used
        for activiteit_obj in activiteiten_api:
            process_single_activiteit(activiteit_obj)

    print("✅ Loaded Activiteiten and their related entities.")


# Keep the original function for backward compatibility (if needed)
def load_activiteiten_original(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", checkpoint_manager=None):
    """
    Original load_activiteiten function for backward compatibility.
    This version is deprecated - use load_activiteiten() for the new decorator-based version.
    """
    # This could import the old implementation if needed, but for now we'll use the new one
    return load_activiteiten(conn, batch_size, start_date_str)

def _process_single_activiteit_threaded(activiteit_obj, conn: Neo4jConnection, checkpoint_context=None):
    """
    Thread-safe version of processing a single activiteit.
    Each thread gets its own Neo4j session.
    """
    global _processed_count, _failed_count
    
    try:
        with conn.driver.session(database=conn.database) as session:
            if not activiteit_obj or not activiteit_obj.id:
                return False

            # Create Activiteit node
            props = {
                'id': activiteit_obj.id,
                'nummer': activiteit_obj.nummer,
                'onderwerp': activiteit_obj.onderwerp,
                'soort': activiteit_obj.soort,
                'datum': str(activiteit_obj.datum) if activiteit_obj.datum else None,
                'aanvangstijd': str(activiteit_obj.aanvangstijd) if activiteit_obj.aanvangstijd else None,
                'eindtijd': str(activiteit_obj.eindtijd) if activiteit_obj.eindtijd else None,
                'geplande_datum': str(activiteit_obj.geplande_datum) if activiteit_obj.geplande_datum else None,
                'voortouwcommissie': activiteit_obj.voortouwcommissie,
                'status': activiteit_obj.status
            }
            session.execute_write(merge_node, 'Activiteit', 'id', props)

            # Process related items
            for attr_name, (target_label, rel_type, target_key_prop) in REL_MAP_ACTIVITEIT.items():
                related_items = getattr(activiteit_obj, attr_name, []) or []
                if not isinstance(related_items, list):
                    related_items = [related_items]

                for related_item_obj in related_items:
                    if not related_item_obj:
                        continue
                    
                    related_item_key_val = getattr(related_item_obj, target_key_prop, None)
                    if related_item_key_val is None:
                        print(f"    ! Warning: Related item for '{attr_name}' in Activiteit {activiteit_obj.id} missing key '{target_key_prop}'.")
                        continue

                    if target_label == 'Zaak':
                        # Thread-safe zaak processing with lock
                        with _thread_lock:
                            if process_and_load_zaak(session, related_item_obj, related_entity_id=activiteit_obj.id, related_entity_type="Activiteit"):
                                pass
                    elif target_label == 'Agendapunt':
                        # Process agendapunt fully since it belongs to this activiteit
                        if process_and_load_agendapunt(session, related_item_obj, related_activiteit_id=activiteit_obj.id):
                            pass
                    else:
                        session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val})

                    session.execute_write(merge_rel, 'Activiteit', 'id', activiteit_obj.id,
                                          target_label, target_key_prop, related_item_key_val, rel_type)
        
        # Update counters thread-safely
        with _thread_lock:
            _processed_count += 1
            
        # Mark as processed in checkpoint if available
        if checkpoint_context:
            checkpoint_context.mark_processed(activiteit_obj)
            
        return True
        
    except Exception as e:
        with _thread_lock:
            _failed_count += 1
        
        error_msg = f"Failed to process Activiteit {activiteit_obj.id}: {str(e)}"
        print(f"    ❌ {error_msg}")
        
        if checkpoint_context:
            checkpoint_context.mark_failed(activiteit_obj, error_msg)
            
        return False


def load_activiteiten_threaded(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", 
                              max_workers: int = 10, skip_count: int = 0, overwrite: bool = False, checkpoint_manager: CheckpointManager = None):
    """
    Load Activiteiten using multithreading for faster processing.
    
    Args:
        conn: Neo4j connection
        batch_size: Not used in threaded version (kept for compatibility)
        start_date_str: Start date for filtering activiteiten
        max_workers: Number of threads to use (default: 10)
        skip_count: Number of items to skip from the beginning (default: 0)
        checkpoint_manager: Optional checkpoint manager for progress tracking
    """
    global _processed_count, _failed_count
    
    # Reset global counters
    _processed_count = 0
    _failed_count = 0
    
    # Initialize checkpoint if provided
    checkpoint = None
    if checkpoint_manager:
        checkpoint = LoaderCheckpoint(checkpoint_manager, "load_activiteiten_threaded")
    
    api = TKApi()
    start_datetime_obj = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    odata_start_date_str = tkapi_util.datetime_to_odata(start_datetime_obj.replace(tzinfo=timezone.utc))

    filter = Activiteit.create_filter()
    filter.add_filter_str(f"Datum ge {odata_start_date_str}")
    
    activiteiten_api = api.get_items(Activiteit, filter=filter)
    total_items = len(activiteiten_api)
    print(f"→ Fetched {total_items} Activiteiten since {start_date_str}")

    if not activiteiten_api:
        print("No activiteiten found for the date range.")
        return

    # Set up checkpoint context
    checkpoint_context = None
    if checkpoint:
        checkpoint.set_total_items(total_items)
        # Create a simple checkpoint context for thread compatibility
        class SimpleCheckpointContext:
            def __init__(self, checkpoint_obj):
                self.checkpoint = checkpoint_obj
                
            def mark_processed(self, item):
                if self.checkpoint:
                    self.checkpoint.mark_processed(item.id)
                    
            def mark_failed(self, item, error_msg):
                if self.checkpoint:
                    self.checkpoint.mark_failed(item.id, error_msg)
        
        checkpoint_context = SimpleCheckpointContext(checkpoint)

    # Clear processed IDs at the beginning (thread-safe)
    with _thread_lock:
        PROCESSED_ZAAK_IDS.clear()

    print(f"🚀 Starting threaded processing with {max_workers} workers...")
    start_time = time.time()

    # Apply skip_count if specified
    if skip_count > 0:
        if skip_count >= len(activiteiten_api):
            print(f"⚠️ Skip count ({skip_count}) is greater than or equal to total items ({len(activiteiten_api)}). Nothing to process.")
            return
        activiteiten_api = activiteiten_api[skip_count:]
        print(f"⏭️ Skipping first {skip_count} items. Processing {len(activiteiten_api)} remaining items.")

    # Check which activiteiten already exist in Neo4j (unless overwrite is enabled)
    if not overwrite and activiteiten_api:
        print("🔍 Checking which Activiteiten already exist in Neo4j...")
        activiteit_ids = [act.id for act in activiteiten_api if act and act.id]
        
        with conn.driver.session(database=conn.database) as session:
            existing_ids = batch_check_nodes_exist(session, "Activiteit", "id", activiteit_ids)
        
        if existing_ids:
            # Filter out existing activiteiten
            original_count = len(activiteiten_api)
            activiteiten_api = [act for act in activiteiten_api if act.id not in existing_ids]
            filtered_count = len(activiteiten_api)
            print(f"📊 Found {len(existing_ids)} existing Activiteiten in Neo4j")
            print(f"⏭️ Skipping {original_count - filtered_count} existing items. Processing {filtered_count} new items.")
            
            if not activiteiten_api:
                print("✅ All Activiteiten already exist in Neo4j. Nothing to process.")
                return
        else:
            print("📊 No existing Activiteiten found in Neo4j. Processing all items.")
    elif overwrite:
        print("🔄 Overwrite mode enabled - processing all items regardless of existing data")

    # Filter out already processed items if checkpoint exists
    items_to_process = []
    if checkpoint:
        for item in activiteiten_api:
            if not checkpoint.is_processed(item.id):
                items_to_process.append(item)
        print(f"→ {len(items_to_process)} items remaining to process (skipped {total_items - len(items_to_process)} already processed)")
    else:
        items_to_process = activiteiten_api

    # Process with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(_process_single_activiteit_threaded, activiteit_obj, conn, checkpoint_context): activiteit_obj 
            for activiteit_obj in items_to_process
        }
        
        # Process completed tasks and show progress
        completed = 0
        for future in as_completed(futures):
            completed += 1
            activiteit_obj = futures[future]
            
            try:
                success = future.result()
                if completed % 25 == 0:  # Progress update every 25 items
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    print(f"    📊 Progress: {completed}/{len(items_to_process)} ({completed/len(items_to_process)*100:.1f}%) - Rate: {rate:.1f} items/sec")
                    
                    # Save checkpoint progress
                    if checkpoint:
                        checkpoint.save_progress()
                        
            except Exception as e:
                print(f"    ❌ Unexpected error processing {activiteit_obj.id}: {e}")

    # Final statistics
    elapsed_time = time.time() - start_time
    avg_rate = len(items_to_process) / elapsed_time if elapsed_time > 0 else 0
    
    print(f"✅ Completed threaded processing!")
    print(f"📊 Final Stats:")
    print(f"   • Total processed: {_processed_count}")
    print(f"   • Failed: {_failed_count}")
    print(f"   • Time elapsed: {elapsed_time:.2f} seconds")
    print(f"   • Average rate: {avg_rate:.2f} items/second")
    
    # Final checkpoint save
    if checkpoint:
        checkpoint.save_progress()
        stats = checkpoint.get_progress_stats()
        print(f"📊 Checkpoint Stats: {stats['processed_count']}/{stats['total_items']} ({stats['completion_percentage']:.1f}%)")