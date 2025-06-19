import datetime
import time
from tkapi import TKApi
from tkapi.document import Document
from tkapi.dossier import Dossier # For expand_params
from tkapi.zaak import Zaak # For expand_params
from tkapi.activiteit import Activiteit # For expand_params
from tkapi.agendapunt import Agendapunt # For expand_params
# DocumentActor is also a related type in Document.expand_params
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel
from constants import REL_MAP_DOC

# Import processors for related entities
from .common_processors import process_and_load_dossier, PROCESSED_DOSSIER_IDS
# from .common_processors import process_and_load_zaak, PROCESSED_ZAAK_IDS # If Zaken from here need full processing
# from .agendapunt_loader import process_and_load_agendapunt # If Agendapunten from here need full processing
# from .activiteit_loader import process_and_load_activiteit_from_doc # You'd need a specific processor

# api = TKApi() # Not needed at module level

def load_documents(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01"):
    api = TKApi()
    start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")

    # --- Manage expand_params ---
    # Document.expand_params is ['DocumentActor','Activiteit','Zaak','DocumentVersie','Agendapunt'] by default
    original_doc_expand_params = list(Document.expand_params or [])
    current_expand_params = list(original_doc_expand_params) # Make a mutable copy

    # Add Dossier.type if not already present and needed for expansion here
    if Dossier.type not in current_expand_params:
        current_expand_params.append(Dossier.type)
    # Ensure other types from REL_MAP_DOC that need to be *processed* (not just linked) are expanded
    # Zaak, Activiteit, Agendapunt are already in default Document.expand_params
    
    Document.expand_params = current_expand_params
    # ---

    documents_api = []
    # try 3 times
    for _ in range(3):
        try:
            filter = Document.create_filter()
            filter.filter_date_range(start_datetime=start_date, end_datetime=datetime.datetime.now() + datetime.timedelta(days=1)) # Ensure end_datetime is in future
            documents_api = api.get_items(Document, filter=filter)
            print(f"→ Fetched {len(documents_api)} Documents since {start_date_str} (with expanded relations)")
            break
        except Exception as e:
            print(f"Error fetching documents: {e}")
            time.sleep(15)
    
    # --- Restore expand_params ---
    Document.expand_params = original_doc_expand_params
    # ---

    if not documents_api:
        print("No documents found for the date range.")
        return

    with conn.driver.session(database=conn.database) as session:
        PROCESSED_DOSSIER_IDS.clear() # Reset for this run/scope
        # PROCESSED_ZAAK_IDS.clear() # If managing Zaken processed here

        for i, doc_obj in enumerate(documents_api, 1):
            if i % 100 == 0 or i == len(documents_api):
                print(f"  → Processing Document {i}/{len(documents_api)}: {doc_obj.id}")
            
            doc_props = {'id':doc_obj.id, 'nummer':doc_obj.nummer, 'volgnummer':doc_obj.volgnummer, 
                         'titel':doc_obj.titel or '', 'datum':str(doc_obj.datum) if doc_obj.datum else None}
            session.execute_write(merge_node,'Document','id',doc_props)
            
            if doc_obj.soort:
                session.execute_write(merge_rel,'Document','id',doc_obj.id,
                                      'DocumentSoort','key',doc_obj.soort.name,'HAS_SOORT')

            # Process expanded Dossiers
            # doc_obj.dossiers should be populated if Dossier.type was in expand_params
            for dossier_item in doc_obj.dossiers: # .dossiers is the property on Document TKItem
                if process_and_load_dossier(session, dossier_item):
                    pass # Dossier processed
                # Create relationship from Document to Dossier
                session.execute_write(merge_rel, 'Document', 'id', doc_obj.id,
                                      'Dossier', 'id', dossier_item.id, 'PART_OF_DOSSIER') # Or use REL_MAP_DOC's relation type

            # Handle other relations from REL_MAP_DOC
            for attr_name, (target_label, rel_type, target_key_prop) in REL_MAP_DOC.items():
                if attr_name == 'dossiers': # Already handled above
                    continue

                related_items = getattr(doc_obj, attr_name, []) or []
                if not isinstance(related_items, list): related_items = [related_items]

                for related_item_obj in related_items:
                    if not related_item_obj: continue
                    
                    related_item_key_val = getattr(related_item_obj, target_key_prop, None)
                    if related_item_key_val is None:
                        print(f"    ! Warning: Related item for '{attr_name}' in Doc {doc_obj.id} missing key '{target_key_prop}'.")
                        continue

                    # If this related item (e.g., Zaak, Activiteit) needs full processing:
                    if target_label == 'Zaak':
                        # from .common_processors import process_and_load_zaak # Ensure this exists
                        # process_and_load_zaak(session, related_item_obj)
                        # For now, just merge node and link:
                        session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val, 'onderwerp': related_item_obj.onderwerp or ''})
                    elif target_label == 'Activiteit':
                        # process_and_load_activiteit_from_doc(session, related_item_obj) # If needed
                        session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val, 'onderwerp': related_item_obj.onderwerp or ''})
                    elif target_label == 'Agendapunt': # Agendapunt already in default expand
                        # from .agendapunt_loader import process_and_load_agendapunt
                        # process_and_load_agendapunt(session, related_item_obj)
                        session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val, 'onderwerp': related_item_obj.onderwerp or ''})
                    elif target_label == 'DocumentActor':
                        # DocumentActor is simpler, usually just needs its own node and link
                        actor_props = {'id': related_item_obj.id, 'naam': related_item_obj.naam or ''} # Add more props if needed
                        session.execute_write(merge_node, target_label, 'id', actor_props)
                    elif target_label == 'DocumentVersie':
                        # DocumentVersie is also simpler
                        versie_props = {'id': related_item_obj.id, 'nummer': related_item_obj.nummer} # 'nummer' is versienummer here
                        # DocumentVersie key is 'nummer' in REL_MAP_DOC. Ensure it's unique enough or use 'id'.
                        # Let's assume 'id' is better for DocumentVersie node key.
                        # session.execute_write(merge_node, target_label, 'id', {'id': related_item_obj.id, 'nummer': related_item_obj.nummer})
                        # session.execute_write(merge_rel,'Document','id',doc_obj.id, target_label, 'id', related_item_obj.id, rel_type)
                        # Continue with REL_MAP_DOC specified key for now.
                        session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val})

                    else: # Default: create minimal node
                        session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val})
                    
                    # Create the relationship
                    session.execute_write(merge_rel, 'Document', 'id', doc_obj.id,
                                          target_label, target_key_prop, related_item_key_val, rel_type)

    print("✅ Loaded Documents and their related date-relevant Dossiers, Zaken, etc.")