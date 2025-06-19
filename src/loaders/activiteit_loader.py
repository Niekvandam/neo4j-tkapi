from ctypes import util
import datetime
from tkapi import TKApi
from tkapi.activiteit import Activiteit
from tkapi.document import Document # For expand_params
from tkapi.agendapunt import Agendapunt # For expand_params
from tkapi.activiteit import ActiviteitActor # For expand_params
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel
from constants import REL_MAP_ACTIVITEIT
# Import processors if Activiteiten can create other entities like Zaken that need full processing
# from .common_processors import process_and_load_zaak, process_and_load_document, etc.

# api = TKApi() # Not needed at module level if instantiated in function

def load_activiteiten(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01"):
    api = TKApi()
    
    start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")

    # --- Manage expand_params ---
    original_activiteit_expand_params = list(Activiteit.expand_params or [])
    current_expand_params = list(original_activiteit_expand_params)

    # Ensure necessary related types are in expand_params
    # Example: if an Activiteit links to a Document that needs to be fully processed:
    # if Document.type not in current_expand_params:
    #     current_expand_params.append(Document.type)
    # Activiteit.expand_params already includes Document, Agendapunt, ActiviteitActor by default in your setup.
    # Add Zaak if activities processed here should also process their related Zaken.
    # from tkapi.zaak import Zaak
    # if Zaak.type not in current_expand_params:
    #    current_expand_params.append(Zaak.type)
    
    Activiteit.expand_params = current_expand_params
    # ---

    filter = Activiteit.create_filter()
    # Using 'Datum' which is available on Activiteit. If 'Aanvangstijd' is more reliable, use that.
    # Activiteit.begin_date_key() returns 'Aanvangstijd'
    filter_str_begin_date = Activiteit.begin_date_key() or 'Datum' # Fallback to Datum
    filter.add_filter_str(f"{filter_str_begin_date} ge {start_date.isoformat()}") # OData format
    
    activiteiten = api.get_items(Activiteit, filter=filter)
    print(f"→ Fetched {len(activiteiten)} Activiteiten since {start_date_str}")

    # --- Restore expand_params ---
    Activiteit.expand_params = original_activiteit_expand_params
    # ---

    with conn.driver.session(database=conn.database) as session:
        for i, a_obj in enumerate(activiteiten, 1):
            if i % 100 == 0 or i == len(activiteiten):
                print(f"  → Processing Activiteit {i}/{len(activiteiten)}: {a_obj.id}")
            props = {'id':a_obj.id,'nummer':a_obj.nummer,'onderwerp':a_obj.onderwerp or '',
                     'begin':str(a_obj.begin) if a_obj.begin else None,'einde':str(a_obj.einde) if a_obj.einde else None}
            session.execute_write(merge_node,'Activiteit','id',props)
            
            if a_obj.soort:
                session.execute_write(merge_rel,'Activiteit','id',a_obj.id,'ActiviteitSoort','key',a_obj.soort.name,'HAS_SOORT')
            if a_obj.status:
                session.execute_write(merge_rel,'Activiteit','id',a_obj.id,'ActiviteitStatus','key',a_obj.status.name,'HAS_STATUS')
            if a_obj.datum_soort:
                session.execute_write(merge_rel,'Activiteit','id',a_obj.id,'DatumSoort','key',a_obj.datum_soort.name,'HAS_DATUMSOORT')

            # Process related entities using REL_MAP_ACTIVITEIT
            # This part assumes that related Document, Zaak, Agendapunt, ActiviteitActor
            # are either simple links or their full processing is handled elsewhere / not needed here.
            # If they need full processing *initiated from here*, you'd call their specific
            # process_and_load_* functions.

            for attr,(label,rel,key_prop_name) in REL_MAP_ACTIVITEIT.items():
                items = getattr(a_obj,attr, []) or [] # Use getattr with a default
                if not isinstance(items,list): items=[items]
                
                for item_obj in items:
                    if not item_obj: continue
                    
                    # Example: If 'zaken' are expanded and need full processing
                    # if attr == 'zaken' and label == 'Zaak':
                    #     from .common_processors import process_and_load_zaak # Assuming Zaak has 'nummer' as key_prop_name
                    #     process_and_load_zaak(session, item_obj) # item_obj is a Zaak instance
                    #     session.execute_write(merge_rel,'Activiteit','id',a_obj.id, label, key_prop_name, getattr(item_obj, key_prop_name), rel)
                    #     continue # Skip generic handling below for this attribute

                    # Generic handling for simple links (creates placeholder nodes if not existing)
                    item_key_value = getattr(item_obj, key_prop_name, None)
                    if item_key_value is not None:
                        # Create a minimal node for the related item if it doesn't exist
                        session.execute_write(merge_node,label,key_prop_name,{key_prop_name:item_key_value})
                        # Create the relationship
                        session.execute_write(merge_rel,'Activiteit','id',a_obj.id,label,key_prop_name,item_key_value,rel)
                    else:
                        print(f"    ! Warning: Item for attribute '{attr}' in Activiteit {a_obj.id} missing key '{key_prop_name}'. Skipping relation.")

    print("✅ Loaded Activiteiten and their direct relations.")