import datetime
from tkapi import TKApi
from tkapi.vergadering import Vergadering, VergaderingFilter, VergaderingSoort # Ensure VergaderingFilter & Soort are imported
from tkapi.verslag import Verslag
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel
from .common_processors import process_and_load_verslag, PROCESSED_VERSLAG_IDS, download_verslag_xml
# Import the vlos_verslag_loader
from .vlos_verslag_loader import load_vlos_verslag # ADD THIS IMPORT

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


def load_vergaderingen(conn: Neo4jConnection, start_date_str: str = "2024-01-01", process_xml_content: bool = True): # Added flag
    api = TKApi()
    
    # Convert start_date_str to datetime object for filtering
    local_start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    
    # Adjust for timezone to create UTC filter range for API
    # We want meetings *on* local_start_date onwards
    local_timezone_delta = timedelta(hours=LOCAL_TIMEZONE_OFFSET_HOURS_API)
    utc_filter_start = local_start_date - local_timezone_delta
    # For an open-ended range from start_date onwards:
    # No end_datetime needs to be passed to filter_date_range if it handles None correctly,
    # or pass a far future date if it requires an end_datetime.

    # --- Manage expand_params ---
    original_vergadering_expand_params = list(Vergadering.expand_params or [])
    current_expand_params = list(original_vergadering_expand_params)
    if Verslag.type not in current_expand_params:
         current_expand_params.append(Verslag.type)
    Vergadering.expand_params = current_expand_params
    # ---

    filter = Vergadering.create_filter()
    # Use the filter_date_range which now uses 'Datum' correctly.
    # This filters for meetings whose 'Datum' field is on or after the UTC equivalent of local_start_date 00:00.
    filter.filter_date_range(begin_datetime=utc_filter_start) 
    # If you want to filter by Aanvangstijd instead:
    # filter.add_filter_str(f"Aanvangstijd ge {tkapi_util.datetime_to_odata(utc_filter_start)}")


    vergaderingen_api = api.get_items(Vergadering, filter=filter)
    print(f"→ Fetched {len(vergaderingen_api)} API Vergaderingen since {start_date_str} (with expanded Verslagen)")

    Vergadering.expand_params = original_vergadering_expand_params # Restore

    if not vergaderingen_api:
        print("No vergaderingen found for the date range from API.")
        return

    with conn.driver.session(database=conn.database) as session:
        if process_xml_content: # Only clear if we are processing XMLs
            PROCESSED_VERSLAG_IDS.clear() 

        for idx, v_obj in enumerate(vergaderingen_api, 1):
            if idx % 100 == 0 or idx == len(vergaderingen_api):
                print(f"  → Processing API Vergadering {idx}/{len(vergaderingen_api)}: {v_obj.id}")
            process_and_load_vergadering(session, conn.driver, v_obj, process_xml=process_xml_content)

    print("✅ Loaded API Vergaderingen and potentially their related VLOS Verslagen content.")