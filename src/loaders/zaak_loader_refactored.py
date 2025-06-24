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

# Import the new checkpoint decorator
from core.checkpoint.checkpoint_decorator import checkpoint_zaak_loader


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
    start_datetime_obj = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    odata_start_date_str = tkapi_util.datetime_to_odata(start_datetime_obj.replace(tzinfo=timezone.utc))

    filter_obj = Zaak.create_filter()
    filter_obj.add_filter_str(f"GestartOp ge {odata_start_date_str}")
    
    # --- Manage expand_params ---
    original_zaak_expand_params = list(Zaak.expand_params or [])
    current_expand_params = list(original_zaak_expand_params)

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