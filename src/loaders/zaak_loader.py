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

# Import processors for related entities that might need full processing
from .common_processors import process_and_load_besluit, PROCESSED_BESLUIT_IDS
from .common_processors import process_and_load_dossier, PROCESSED_DOSSIER_IDS
from tkapi.util import util as tkapi_util
from datetime import timezone
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import the checkpoint decorator
from core.checkpoint.checkpoint_decorator import checkpoint_zaak_loader

# Import checkpoint functionality
from core.checkpoint.checkpoint_manager import LoaderCheckpoint, CheckpointManager

# Thread-safe lock for shared resources
_thread_lock = threading.Lock()
_processed_count = 0
_failed_count = 0

# from .common_processors import process_and_load_document # If documents from here need full processing
# from .agendapunt_loader import process_and_load_agendapunt # If agendapunten from here need full processing
# from .activiteit_loader import process_and_load_activiteit_from_zaak # If
# api = TKApi() # Not needed at module level

# New processor function for a single Zaak, if needed by other loaders
def process_and_load_zaak(session, zaak_obj: Zaak, related_entity_id: str = None, related_entity_type:str = None):
    # Add to a PROCESSED_ZAAK_IDS if you create one
    if not zaak_obj or not zaak_obj.nummer: # Assuming 'nummer' is the key
        return False

    # Key conflict: vlos_verslag_loader uses 'id' for Zaak, here we use 'nummer'.
    # This needs to be harmonized. For now, this function uses 'nummer'.
    # If a Zaak node with this 'nummer' already exists, its 'id' property might be overwritten
    # if the Zaak object from API has a different 'id' (GUID) than what VLOS provided.
    props = {
        'id': zaak_obj.id, # Store the GUID from API as a property
        'nummer': zaak_obj.nummer, # Use 'nummer' as the MERGE key
        'onderwerp': zaak_obj.onderwerp,
        'afgedaan': zaak_obj.afgedaan,
        'volgnummer': zaak_obj.volgnummer,
        'alias': zaak_obj.alias,
        'gestart_op': str(zaak_obj.gestart_op) if zaak_obj.gestart_op else None
    }
    session.execute_write(merge_node,'Zaak','nummer',props) # MERGE on 'nummer'
    # print(f"    ↳ Processing Zaak: {zaak_obj.nummer} (ID: {zaak_obj.id})")


    if zaak_obj.soort:
        session.execute_write(merge_rel,'Zaak','nummer',zaak_obj.nummer,
                              'ZaakSoort','key',zaak_obj.soort.name,'HAS_SOORT')
    if zaak_obj.kabinetsappreciatie:
        session.execute_write(merge_rel,'Zaak','nummer',zaak_obj.nummer,
                              'Kabinetsappreciatie','key',zaak_obj.kabinetsappreciatie.name,'HAS_KABINETSAPPRECIATIE')
    
    # Handle VervangenDoor
    if zaak_obj.vervangen_door:
        vd = zaak_obj.vervangen_door
        # Recursively process the 'vervangen_door' Zaak if it's new
        process_and_load_zaak(session, vd) # It will merge on its own 'nummer'
        session.execute_write(merge_rel, 'Zaak', 'nummer', zaak_obj.nummer,
                              'Zaak', 'nummer', vd.nummer, 'REPLACED_BY')

    # Handle Dossier relationship (if Zaak has a direct .dossier property that's expanded)
    # The default Zaak TKItem does not list 'dossier' as a direct expandable single item,
    # but 'Kamerstukdossier' which is a list. So this part might need adjustment based on TKApi behavior.
    # We will handle dossiers when iterating REL_MAP_ZAAK if 'dossiers' (plural) is a property.
    # If Zaak is related to Dossier via Kamerstukdossier (list):
    # for dossier_obj in zaak_obj.dossiers: # Assuming zaak_obj.dossiers exists and is expanded
    #    if process_and_load_dossier(session, dossier_obj):
    #        pass
    #    session.execute_write(merge_rel, 'Zaak', 'nummer', zaak_obj.nummer,
    #                          'Dossier', 'id', dossier_obj.id, 'PART_OF_DOSSIER')


    # Process other related items (Documenten, Agendapunten, Activiteiten, Besluiten, ZaakActors)
    # This assumes they are expanded on the zaak_obj
    for attr_name, (target_label, rel_type, target_key_prop) in REL_MAP_ZAAK.items():
        if attr_name == 'vervangen_door': continue # Handled above

        related_items = getattr(zaak_obj, attr_name, []) or []
        if not isinstance(related_items, list): related_items = [related_items]

        for related_item_obj in related_items:
            if not related_item_obj: continue
            
            related_item_key_val = getattr(related_item_obj, target_key_prop, None)
            if related_item_key_val is None:
                print(f"    ! Warning: Related item for '{attr_name}' in Zaak {zaak_obj.nummer} missing key '{target_key_prop}'.")
                continue

            if target_label == 'Besluit':
                if process_and_load_besluit(session, related_item_obj, related_zaak_nummer=zaak_obj.nummer):
                    pass # Processed new besluit
            elif target_label == 'Document':
                # from .common_processors import process_and_load_document # If full processing needed
                # process_and_load_document(session, related_item_obj)
                session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val, 'titel': related_item_obj.titel or ''})
            elif target_label == 'Agendapunt':
                # from .agendapunt_loader import process_and_load_agendapunt
                # process_and_load_agendapunt(session, related_item_obj)
                session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val, 'onderwerp': related_item_obj.onderwerp or ''})
            elif target_label == 'Activiteit':
                # process_and_load_activiteit_from_zaak(session, related_item_obj)
                session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val, 'onderwerp': related_item_obj.onderwerp or ''})
            elif target_label == 'ZaakActor':
                actor_props = {'id': related_item_obj.id, 'naam': related_item_obj.naam or ''}
                session.execute_write(merge_node, target_label, 'id', actor_props)
            else: # Default minimal node creation
                 session.execute_write(merge_node,target_label,target_key_prop,{target_key_prop:related_item_key_val})

            # Create the relationship from Zaak to the related item
            session.execute_write(merge_rel, 'Zaak', 'nummer', zaak_obj.nummer,
                                  target_label, target_key_prop, related_item_key_val, rel_type)
    return True


@checkpoint_zaak_loader(checkpoint_interval=25)
def load_zaken(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", skip_count: int = 0, _checkpoint_context=None):
    """
    Load Zaken with automatic checkpoint support using decorator.
    
    The @checkpoint_zaak_loader decorator automatically handles:
    - Progress tracking every 25 items
    - Skipping already processed items
    - Error handling and logging
    - Final progress reporting
    """
    api = TKApi()
    start_datetime_obj = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    odata_start_date_str = tkapi_util.datetime_to_odata(start_datetime_obj.replace(tzinfo=timezone.utc))

    filter_obj = Zaak.create_filter()
    filter_obj.add_filter_str(f"GestartOp ge {odata_start_date_str}")
    # --- Manage expand_params ---
    # Zaak.expand_params is ['Document','Agendapunt','Activiteit','Besluit','ZaakActor','VervangenDoor'] by default
    original_zaak_expand_params = list(Zaak.expand_params or [])
    current_expand_params = list(original_zaak_expand_params)

    # Add Dossier.type (which is 'Kamerstukdossier') if Zaken need to expand their Dossiers.
    # Zaak has a `dossier` property which is related_item(Dossier), this implies
    # 'Kamerstukdossier' might be the navigation link name if Zaak can directly link to one Dossier,
    # or it might be related via Documenten.
    # The Zaak TKItem has `dossier -> related_item(Dossier)`. This suggests a single related Dossier.
    # Let's assume 'Kamerstukdossier' is the correct expand string for this.
    # Or, if `Zaak.dossier` works without explicit expand because `Dossier.type` is 'Kamerstukdossier',
    # then direct access `zaak_obj.dossier` might yield the object if it was fetched as part of a broader call.
    # However, to be safe, if `Dossier` is a primary relation to process from `Zaak`, add its type:
    if Dossier.type not in current_expand_params: # Dossier.type is 'Kamerstukdossier'
        current_expand_params.append(Dossier.type)

    Zaak.expand_params = current_expand_params
    # ---

    filter = Zaak.create_filter()
    filter.add_filter_str(f"GestartOp ge {odata_start_date_str}") # Zaak has GestartOp
    
    zaken_api = api.get_zaken(filter=filter) # get_zaken is a method on TKApi, not get_items
    print(f"→ Fetched {len(zaken_api)} Zaken since {start_date_str} (with expanded relations)")

    # --- Restore expand_params ---
    Zaak.expand_params = original_zaak_expand_params
    # ---

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

    def process_single_zaak(zaak_obj):
        with conn.driver.session(database=conn.database) as session:
            # Use the processor function
            process_and_load_zaak(session, zaak_obj)

            # Process direct Dossier if it's an expanded property
            if zaak_obj.dossier:
                dossier_obj_single = zaak_obj.dossier
                if process_and_load_dossier(session, dossier_obj_single):
                    pass
                session.execute_write(merge_rel, 'Zaak', 'nummer', zaak_obj.nummer,
                                      'Dossier', 'id', dossier_obj_single.id, 'BELONGS_TO_DOSSIER')

    # Clear processed IDs at the beginning
    PROCESSED_BESLUIT_IDS.clear()
    PROCESSED_DOSSIER_IDS.clear()

    # Use the checkpoint context to process items automatically
    if _checkpoint_context:
        _checkpoint_context.process_items(zaken_api, process_single_zaak)
    else:
        # Fallback for when decorator is not used
        for zaak_obj in zaken_api:
            process_single_zaak(zaak_obj)

    print("✅ Loaded Zaken and their related Dossiers, Besluiten, etc.")


def _process_single_zaak_threaded(zaak_obj, conn: Neo4jConnection, checkpoint_context=None):
    """
    Thread-safe version of processing a single zaak.
    Each thread gets its own Neo4j session.
    """
    global _processed_count, _failed_count
    
    try:
        with conn.driver.session(database=conn.database) as session:
            if not zaak_obj or not zaak_obj.nummer:
                return False

            # Thread-safe processing of shared resources
            with _thread_lock:
                # Use the processor function
                process_and_load_zaak(session, zaak_obj)

                # Process direct Dossier if it's an expanded property
                if zaak_obj.dossier:
                    dossier_obj_single = zaak_obj.dossier
                    if process_and_load_dossier(session, dossier_obj_single):
                        pass
                    session.execute_write(merge_rel, 'Zaak', 'nummer', zaak_obj.nummer,
                                          'Dossier', 'id', dossier_obj_single.id, 'BELONGS_TO_DOSSIER')
        
        # Update counters thread-safely
        with _thread_lock:
            _processed_count += 1
            
        # Mark as processed in checkpoint if available
        if checkpoint_context:
            checkpoint_context.mark_processed(zaak_obj)
            
        return True
        
    except Exception as e:
        with _thread_lock:
            _failed_count += 1
        
        error_msg = f"Failed to process Zaak {zaak_obj.nummer}: {str(e)}"
        print(f"    ❌ {error_msg}")
        
        if checkpoint_context:
            checkpoint_context.mark_failed(zaak_obj, error_msg)
            
        return False


def load_zaken_threaded(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", 
                       max_workers: int = 10, skip_count: int = 0, checkpoint_manager: CheckpointManager = None):
    """
    Load Zaken using multithreading for faster processing.
    
    Args:
        conn: Neo4j connection
        batch_size: Not used in threaded version (kept for compatibility)
        start_date_str: Start date for filtering zaken
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
        checkpoint = LoaderCheckpoint(checkpoint_manager, "load_zaken_threaded")
    
    api = TKApi()
    start_datetime_obj = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    odata_start_date_str = tkapi_util.datetime_to_odata(start_datetime_obj.replace(tzinfo=timezone.utc))

    # --- Manage expand_params ---
    original_zaak_expand_params = list(Zaak.expand_params or [])
    current_expand_params = list(original_zaak_expand_params)

    if Dossier.type not in current_expand_params:
        current_expand_params.append(Dossier.type)

    Zaak.expand_params = current_expand_params
    # ---

    filter = Zaak.create_filter()
    filter.add_filter_str(f"GestartOp ge {odata_start_date_str}")
    
    zaken_api = api.get_zaken(filter=filter)
    total_items = len(zaken_api)
    print(f"→ Fetched {total_items} Zaken since {start_date_str} (with expanded relations)")

    # --- Restore expand_params ---
    Zaak.expand_params = original_zaak_expand_params
    # ---

    if not zaken_api:
        print("No zaken found for the date range.")
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
                    self.checkpoint.mark_processed(item.nummer)
                    
            def mark_failed(self, item, error_msg):
                if self.checkpoint:
                    self.checkpoint.mark_failed(item.nummer, error_msg)
        
        checkpoint_context = SimpleCheckpointContext(checkpoint)

    # Clear processed IDs at the beginning (thread-safe)
    with _thread_lock:
        PROCESSED_BESLUIT_IDS.clear()
        PROCESSED_DOSSIER_IDS.clear()

    print(f"🚀 Starting threaded processing with {max_workers} workers...")
    start_time = time.time()

    # Apply skip_count if specified
    if skip_count > 0:
        if skip_count >= len(zaken_api):
            print(f"⚠️ Skip count ({skip_count}) is greater than or equal to total items ({len(zaken_api)}). Nothing to process.")
            return
        zaken_api = zaken_api[skip_count:]
        print(f"⏭️ Skipping first {skip_count} items. Processing {len(zaken_api)} remaining items.")

    # Filter out already processed items if checkpoint exists
    items_to_process = []
    if checkpoint:
        for item in zaken_api:
            if not checkpoint.is_processed(item.nummer):
                items_to_process.append(item)
        print(f"→ {len(items_to_process)} items remaining to process (skipped {total_items - len(items_to_process)} already processed)")
    else:
        items_to_process = zaken_api

    # Process with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(_process_single_zaak_threaded, zaak_obj, conn, checkpoint_context): zaak_obj 
            for zaak_obj in items_to_process
        }
        
        # Process completed tasks and show progress
        completed = 0
        for future in as_completed(futures):
            completed += 1
            zaak_obj = futures[future]
            
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
                print(f"    ❌ Unexpected error processing {zaak_obj.nummer}: {e}")

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


# Keep the original function for backward compatibility (if needed)
def load_zaken_original(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", checkpoint_manager=None):
    """
    Original load_zaken function for backward compatibility.
    This version is deprecated - use load_zaken() for the new decorator-based version.
    """
    # This could import the old implementation if needed, but for now we'll use the new one
    return load_zaken(conn, batch_size, start_date_str)