import datetime
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

# Timezone handling for creating API query filters
LOCAL_TIMEZONE_OFFSET_HOURS_API = 2 # Example: CEST

def process_and_load_vergadering(session, driver, vergadering_obj: Vergadering, process_xml=True): # Added process_xml flag
    if not vergadering_obj or not vergadering_obj.id:
        return False

    props = {
        'id': vergadering_obj.id, # This is the canonical API ID
        'titel': vergadering_obj.titel,
        'nummer': vergadering_obj.nummer, # This is VergaderingNummer
        'zaal': vergadering_obj.zaal,
        'soort': vergadering_obj.soort.name if vergadering_obj.soort else None,
        'datum': str(vergadering_obj.datum) if vergadering_obj.datum else None, # API Datum field
        'begin': str(vergadering_obj.begin) if vergadering_obj.begin else None, # API Aanvangstijd
        'einde': str(vergadering_obj.einde) if vergadering_obj.einde else None,   # API Sluiting
        'samenstelling': vergadering_obj.samenstelling,
        'source': 'tkapi' # Mark as API sourced
    }
    session.execute_write(merge_node, 'Vergadering', 'id', props)
    print(f"    ↳ Processed API Vergadering: {vergadering_obj.id} - {vergadering_obj.titel}")

    # Process related Verslag from API
    if vergadering_obj.verslag:
        # process_and_load_verslag will create the :Verslag node from API data
        # and link it to this Vergadering.
        # It will also trigger the download and processing of the VLOS XML.
        if process_xml and process_and_load_verslag(session, driver, vergadering_obj.verslag, 
                                        related_vergadering_id=vergadering_obj.id,
                                        canonical_api_vergadering_id_for_vlos=vergadering_obj.id): # Pass canonical ID
            pass 
        elif not process_xml: # If only processing API verslag metadata without XML
             # Minimal Verslag node creation if not fully processed by process_and_load_verslag
            session.execute_write(merge_node, 'Verslag', 'id', {'id': vergadering_obj.verslag.id, 'source': 'tkapi_placeholder'})
            session.execute_write(merge_rel, 'Vergadering', 'id', vergadering_obj.id,
                                  'Verslag', 'id', vergadering_obj.verslag.id, 'HAS_API_VERSLAG')
    return True


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
    start_datetime_obj = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    odata_start_date_str = tkapi_util.datetime_to_odata(start_datetime_obj.replace(tzinfo=timezone.utc))

    filter = Vergadering.create_filter()
    filter.add_filter_str(f"Datum ge {odata_start_date_str}")
    
    vergaderingen_api = api.get_items(Vergadering, filter=filter)
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

    def process_single_vergadering(vergadering_obj):
        with conn.driver.session(database=conn.database) as session:
            if not vergadering_obj or not vergadering_obj.id:
                return

            # Create Vergadering node
            props = {
                'id': vergadering_obj.id,
                'nummer': vergadering_obj.nummer,
                'titel': vergadering_obj.titel or '',
                'datum': str(vergadering_obj.datum) if vergadering_obj.datum else None,
                'aanvangstijd': str(vergadering_obj.aanvangstijd) if vergadering_obj.aanvangstijd else None,
                'eindtijd': str(vergadering_obj.eindtijd) if vergadering_obj.eindtijd else None,
                'status': vergadering_obj.status,
                'soort': vergadering_obj.soort
            }
            session.execute_write(merge_node, 'Vergadering', 'id', props)

            # Process related items
            for attr_name, (target_label, rel_type, target_key_prop) in REL_MAP_VERGADERING.items():
                related_items = getattr(vergadering_obj, attr_name, []) or []
                if not isinstance(related_items, list):
                    related_items = [related_items]

                for related_item_obj in related_items:
                    if not related_item_obj:
                        continue
                    
                    related_item_key_val = getattr(related_item_obj, target_key_prop, None)
                    if related_item_key_val is None:
                        print(f"    ! Warning: Related item for '{attr_name}' in Vergadering {vergadering_obj.id} missing key '{target_key_prop}'.")
                        continue

                    if target_label == 'Zaak':
                        if process_and_load_zaak(session, related_item_obj, related_entity_id=vergadering_obj.id, related_entity_type="Vergadering"):
                            pass
                    elif target_label == 'Activiteit':
                        session.execute_write(merge_node, target_label, target_key_prop, {
                            target_key_prop: related_item_key_val, 
                            'onderwerp': related_item_obj.onderwerp or ''
                        })
                    elif target_label == 'Agendapunt':
                        session.execute_write(merge_node, target_label, target_key_prop, {
                            target_key_prop: related_item_key_val, 
                            'onderwerp': related_item_obj.onderwerp or ''
                        })
                    elif target_label == 'Document':
                        session.execute_write(merge_node, target_label, target_key_prop, {
                            target_key_prop: related_item_key_val, 
                            'titel': related_item_obj.titel or ''
                        })
                    else:
                        session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val})

                    session.execute_write(merge_rel, 'Vergadering', 'id', vergadering_obj.id,
                                          target_label, target_key_prop, related_item_key_val, rel_type)

    # Clear processed IDs at the beginning
    PROCESSED_ZAAK_IDS.clear()

    # Use the checkpoint context to process items automatically
    if _checkpoint_context:
        _checkpoint_context.process_items(vergaderingen_api, process_single_vergadering)
    else:
        # Fallback for when decorator is not used
        for vergadering_obj in vergaderingen_api:
            process_single_vergadering(vergadering_obj)

    print("✅ Loaded Vergaderingen and their related entities.")


# Keep the original function for backward compatibility (if needed)
def load_vergaderingen_original(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", checkpoint_manager=None):
    """
    Original load_vergaderingen function for backward compatibility.
    This version is deprecated - use load_vergaderingen() for the new decorator-based version.
    """
    # This could import the old implementation if needed, but for now we'll use the new one
    return load_vergaderingen(conn, batch_size, start_date_str)