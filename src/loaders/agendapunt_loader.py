import datetime
from tkapi import TKApi
from tkapi.agendapunt import Agendapunt
from tkapi.besluit import Besluit # For expand_params
from tkapi.document import Document # For expand_params
from tkapi.activiteit import Activiteit # For expand_params
from tkapi.zaak import Zaak # For expand_params
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel
from .common_processors import process_and_load_besluit, PROCESSED_BESLUIT_IDS
# from .common_processors import process_and_load_document # If documents linked here need full processing
# from .common_processors import process_and_load_zaak # If zaken linked here need full processing

# api = TKApi() # Not needed at module level

# New processor function for a single Agendapunt
def process_and_load_agendapunt(session, ap_obj: Agendapunt, related_activiteit_id: str = None):
    if not ap_obj or not ap_obj.id: # Add to a PROCESSED_AGENDAPUNT_IDS if you have one
        return False

    props = {
        'id': ap_obj.id,
        'onderwerp': ap_obj.onderwerp,
        'volgorde': ap_obj.volgorde,
        'rubriek': ap_obj.rubriek,
        'noot': ap_obj.noot,
        'begin': str(ap_obj.begin) if ap_obj.begin else None,
        'einde': str(ap_obj.einde) if ap_obj.einde else None
    }
    session.execute_write(merge_node, 'Agendapunt', 'id', props)
    # print(f"    ↳ Processing Agendapunt: {ap_obj.id}")

    # Link to parent Activiteit (if called from Activiteit loader, this is done there)
    # If Agendapunt itself has an expanded Activiteit different from the caller:
    if ap_obj.activiteit and ap_obj.activiteit.id != related_activiteit_id:
        # This Activiteit might not be date-filtered.
        # Minimal node creation, full processing should be done by load_activiteiten
        session.execute_write(merge_node, 'Activiteit', 'id', {'id': ap_obj.activiteit.id})
        session.execute_write(merge_rel, 'Agendapunt', 'id', ap_obj.id,
                              'Activiteit', 'id', ap_obj.activiteit.id, 'BELONGS_TO_ACTIVITEIT')

    # Process related Besluit
    if ap_obj.besluit: # Assuming ap_obj.besluit is an expanded Besluit object
        if process_and_load_besluit(session, ap_obj.besluit, related_agendapunt_id=ap_obj.id):
            pass # Processed new besluit
        session.execute_write(merge_rel, 'Agendapunt', 'id', ap_obj.id,
                              'Besluit', 'id', ap_obj.besluit.id, 'HAS_BESLUIT')

    # Process related Documenten
    for doc_obj in ap_obj.documenten: # Assuming ap_obj.documenten contains expanded Document objects
        # from .common_processors import process_and_load_document # If needed
        # process_and_load_document(session, doc_obj) # For full processing
        # For now, just create node and link:
        session.execute_write(merge_node, 'Document', 'id', {'id': doc_obj.id, 'titel': doc_obj.titel or ''})
        session.execute_write(merge_rel, 'Agendapunt', 'id', ap_obj.id,
                              'Document', 'id', doc_obj.id, 'HAS_DOCUMENT')

    # Process related Zaken
    for zaak_obj in ap_obj.zaken: # Assuming ap_obj.zaken contains expanded Zaak objects
        # from .common_processors import process_and_load_zaak
        # process_and_load_zaak(session, zaak_obj) # For full processing
        # For now, just create node and link:
        session.execute_write(merge_node, 'Zaak', 'nummer', {'nummer': zaak_obj.nummer, 'onderwerp': zaak_obj.onderwerp or ''})
        session.execute_write(merge_rel, 'Agendapunt', 'id', ap_obj.id,
                              'Zaak', 'nummer', zaak_obj.nummer, 'ABOUT_ZAAK')
    return True


def load_agendapunten(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01"):
    api = TKApi()
    start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")

    # --- Manage expand_params ---
    original_agendapunt_expand_params = list(Agendapunt.expand_params or [])
    current_expand_params = list(original_agendapunt_expand_params)

    # Ensure Besluit, Document, Activiteit, Zaak are in expand_params
    # Default Agendapunt.expand_params = ['Activiteit', 'Besluit', 'Document']
    if Besluit.type not in current_expand_params: current_expand_params.append(Besluit.type)
    if Document.type not in current_expand_params: current_expand_params.append(Document.type)
    if Activiteit.type not in current_expand_params: current_expand_params.append(Activiteit.type)
    if Zaak.type not in current_expand_params: current_expand_params.append(Zaak.type) # Add Zaak

    Agendapunt.expand_params = current_expand_params
    # ---

    filter = Agendapunt.create_filter()
    # Agendapunt has 'Aanvangstijd'
    filter.add_filter_str(f"Aanvangstijd ge {start_date.isoformat()}")
    
    agendapunten = api.get_items(Agendapunt, filter=filter)
    print(f"→ Fetched {len(agendapunten)} Agendapunten since {start_date_str}")

    # --- Restore expand_params ---
    Agendapunt.expand_params = original_agendapunt_expand_params
    # ---
    
    if not agendapunten:
        print("No agendapunten found for the date range.")
        return

    with conn.driver.session(database=conn.database) as session:
        PROCESSED_BESLUIT_IDS.clear() # Clear for this specific scope if needed, or manage globally

        for i, ap_obj in enumerate(agendapunten, 1):
            if i % 100 == 0 or i == len(agendapunten):
                print(f"  → Processing Agendapunt {i}/{len(agendapunten)}: {ap_obj.id}")
            process_and_load_agendapunt(session, ap_obj)

    print("✅ Loaded Agendapunten and their related Besluiten, Documenten, Zaken.")