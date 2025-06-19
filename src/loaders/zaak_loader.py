import datetime
from tkapi import TKApi
from tkapi.zaak import Zaak
from tkapi.document import Document # For expand_params
from tkapi.agendapunt import Agendapunt # For expand_params
from tkapi.activiteit import Activiteit # For expand_params
from tkapi.besluit import Besluit # For expand_params
from tkapi.dossier import Dossier # For expand_params (newly added)
# ZaakActor is also in Zaak.expand_params by default
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel
from constants import REL_MAP_ZAAK

# Import processors for related entities that might need full processing
from .common_processors import process_and_load_besluit, PROCESSED_BESLUIT_IDS
from .common_processors import process_and_load_dossier, PROCESSED_DOSSIER_IDS
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


def load_zaken(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01"):
    api = TKApi()
    start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")

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
    filter.add_filter_str(f"GestartOp ge {start_date.isoformat()}") # Zaak has GestartOp
    
    zaken_api = api.get_zaken(filter=filter) # get_zaken is a method on TKApi, not get_items
    print(f"→ Fetched {len(zaken_api)} Zaken since {start_date_str} (with expanded relations)")

    # --- Restore expand_params ---
    Zaak.expand_params = original_zaak_expand_params
    # ---

    if not zaken_api:
        print("No zaken found for the date range.")
        return

    with conn.driver.session(database=conn.database) as session:
        PROCESSED_BESLUIT_IDS.clear() # Reset for this scope
        PROCESSED_DOSSIER_IDS.clear() # Reset for this scope
        # PROCESSED_ZAAK_IDS.clear() # If managing already processed zaken in this run

        for i, z_obj in enumerate(zaken_api, 1):
            if i % 100 == 0 or i == len(zaken_api):
                print(f"  → Processing Zaak {i}/{len(zaken_api)}: {z_obj.nummer} (ID: {z_obj.id})")
            
            # Use the new processor function
            process_and_load_zaak(session, z_obj)

            # After process_and_load_zaak handles its internal relations,
            # explicitly process its direct Dossier if it's an expanded property
            # not covered by REL_MAP_ZAAK or if REL_MAP_ZAAK needs adjustment.
            # Zaak.dossier is a single related item.
            if z_obj.dossier: # Access the single related dossier object
                dossier_obj_single = z_obj.dossier
                if process_and_load_dossier(session, dossier_obj_single):
                    pass # Dossier processed
                session.execute_write(merge_rel, 'Zaak', 'nummer', z_obj.nummer,
                                      'Dossier', 'id', dossier_obj_single.id, 'BELONGS_TO_DOSSIER')


    print("✅ Loaded Zaken and their related Dossiers, Besluiten, etc.")