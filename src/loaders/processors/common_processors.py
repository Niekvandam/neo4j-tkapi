from tkapi.dossier import Dossier
from tkapi.besluit import Besluit
from tkapi.stemming import Stemming
from tkapi.verslag import Verslag
from tkapi.vergadering import Vergadering # For type hinting
from tkapi.persoon import Persoon
from tkapi.fractie import Fractie
from tkapi.agendapunt import Agendapunt
from tkapi.zaak import Zaak
from utils.helpers import merge_node, merge_rel
from loaders.vlos_verslag_loader import load_vlos_verslag # Legacy loader
from loaders.enhanced_vlos_verslag_loader import load_enhanced_vlos_verslag # Enhanced loader
import requests
import concurrent.futures # For verslag XML download
from typing import Optional

# --- Processed ID Sets ---
PROCESSED_DOSSIER_IDS = set()
PROCESSED_BESLUIT_IDS = set()
PROCESSED_STEMMING_IDS = set()
PROCESSED_VERSLAG_IDS = set()
PROCESSED_ZAAK_IDS = set()  # For Zaken processed as nested entities
PROCESSED_DOCUMENT_IDS = set()

# --- Helper to download XML for Verslag ---
def download_verslag_xml(verslag_id):
    """Downloads the XML content for a given Verslag ID."""
    url = f"https://gegevensmagazijn.tweedekamer.nl/OData/v4/2.0/Verslag({verslag_id})/resource"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"  ✕ ERROR downloading XML for Verslag {verslag_id}: {e}")
        return None

# --- Processor Functions ---

def process_and_load_dossier(session, dossier_obj: Dossier):
    if not dossier_obj or not dossier_obj.id or dossier_obj.id in PROCESSED_DOSSIER_IDS:
        return False

    props = {
        'id': dossier_obj.id,
        'nummer': dossier_obj.nummer,
        'toevoeging': dossier_obj.toevoeging,
        'titel': dossier_obj.titel,
        'afgesloten': dossier_obj.afgesloten,
        'organisatie': dossier_obj.organisatie
    }
    session.execute_write(merge_node, 'Dossier', 'id', props)
    PROCESSED_DOSSIER_IDS.add(dossier_obj.id)
    # print(f"    ↳ Processed related Dossier: {dossier_obj.id} - {dossier_obj.nummer}")
    return True


def process_and_load_besluit(session, besluit_obj: Besluit, related_agendapunt_id: str = None, related_zaak_nummer: str = None):
    if not besluit_obj or not besluit_obj.id or besluit_obj.id in PROCESSED_BESLUIT_IDS:
        return False

    props = {
        'id': besluit_obj.id,
        'soort': besluit_obj.soort,
        'status': besluit_obj.status.name if besluit_obj.status else None,
        'tekst': besluit_obj.tekst,
        'stemming_soort': besluit_obj.stemming_soort,
        'opmerking': besluit_obj.opmerking,
    }
    session.execute_write(merge_node, 'Besluit', 'id', props)
    PROCESSED_BESLUIT_IDS.add(besluit_obj.id)
    # print(f"    ↳ Processed related Besluit: {besluit_obj.id}")

    # Link to parent if provided (the caller, e.g., Agendapunt or Zaak loader, does this)
    # However, if Besluit itself has expanded relationships like Agendapunt or Zaak, handle them here.
    # From tkapi.besluit.Besluit, it can have 'zaken' and 'agendapunt' as properties.

    if besluit_obj.agendapunt:  # Always (re)link to its Agendapunt, avoid circular re-processing
        # Process Agendapunt if we did not arrive here FROM that same Agendapunt to prevent recursion
        if besluit_obj.agendapunt.id != related_agendapunt_id:
            from .agendapunt_loader import process_and_load_agendapunt  # circular-import safe
            if process_and_load_agendapunt(session, besluit_obj.agendapunt):
                pass

        # Canonical forward edge: Besluit -> Agendapunt
        session.execute_write(
            merge_rel,
            'Besluit', 'id', besluit_obj.id,
            'Agendapunt', 'id', besluit_obj.agendapunt.id,
            'BELONGS_TO_AGENDAPUNT'
        )

    for zaak_obj in besluit_obj.zaken: # Assuming besluit_obj.zaken is expanded
        if zaak_obj.nummer != related_zaak_nummer: # Avoid circular if called from zaak
            # This Zaak might not be date-filtered, so process it if new
            from ..zaak_loader import process_and_load_zaak  # corrected import path
            if process_and_load_zaak(session, zaak_obj):
                pass # Processed new Zaak
            session.execute_write(merge_rel, 'Besluit', 'id', besluit_obj.id,
                                  'Zaak', 'nummer', zaak_obj.nummer, 'ABOUT_ZAAK')


    # Process Stemmingen related to this Besluit
    # Besluit.expand_params should include Stemming.type when Besluit itself is expanded
    for stemming_obj in besluit_obj.stemmingen:
        # Pass besluit_obj.id and besluit_obj.tekst
        if process_and_load_stemming(session, stemming_obj, besluit_obj.id, besluit_obj.tekst): # MODIFIED CALL
            pass 
        session.execute_write(merge_rel, 'Besluit', 'id', besluit_obj.id,
                              'Stemming', 'id', stemming_obj.id, 'HAS_STEMMING')
    return True


# src/loaders/common_processor.py

# Modify process_and_load_stemming to accept the parent besluit_obj's text
def process_and_load_stemming(session, stemming_obj: Stemming, parent_besluit_id: str, parent_besluit_tekst: Optional[str]):
    if not stemming_obj or not stemming_obj.id or stemming_obj.id in PROCESSED_STEMMING_IDS:
        return False

    # Determine is_hoofdelijk using the passed parent_besluit_tekst
    is_hoofdelijk_val = False
    if parent_besluit_tekst:
        is_hoofdelijk_val = 'hoofdelijk' in parent_besluit_tekst.lower()


    props = {
        'id': stemming_obj.id,
        'soort': stemming_obj.soort,
        'vergissing': stemming_obj.vergissing,
        'fractie_size': stemming_obj.fractie_size,
        'actor_naam': stemming_obj.actor_naam,
        'actor_fractie': stemming_obj.actor_fractie,
        'persoon_id_prop': stemming_obj.persoon_id,
        'fractie_id_prop': stemming_obj.fractie_id,
        'is_hoofdelijk': is_hoofdelijk_val, # Use the calculated value
    }
    session.execute_write(merge_node, 'Stemming', 'id', props)
    PROCESSED_STEMMING_IDS.add(stemming_obj.id)
    # print(f"      ↳ Processed related Stemming: {stemming_obj.id}")

    # Link to parent Besluit (done by caller: process_and_load_besluit)

    # Link to Persoon if exists
    if stemming_obj.persoon:
        # Assuming Persoon nodes are created by load_personen
        session.execute_write(merge_node, 'Persoon', 'id', {'id': stemming_obj.persoon.id})
        session.execute_write(merge_rel, 'Stemming', 'id', stemming_obj.id,
                              'Persoon', 'id', stemming_obj.persoon.id, 'CAST_BY')

    # Link to Fractie if exists
    if stemming_obj.fractie:
        # Assuming Fractie nodes are created by load_fracties
        session.execute_write(merge_node, 'Fractie', 'id', {'id': stemming_obj.fractie.id})
        session.execute_write(merge_rel, 'Stemming', 'id', stemming_obj.id,
                              'Fractie', 'id', stemming_obj.fractie.id, 'REPRESENTS_FRACTIE_VOTE')
    return True


def process_and_load_verslag(session, driver, verslag_obj: Verslag, 
                             related_vergadering_id: str = None, 
                             canonical_api_vergadering_id_for_vlos: str = None,
                             defer_vlos_processing: bool = False):
    if not verslag_obj or not verslag_obj.id or verslag_obj.id in PROCESSED_VERSLAG_IDS:
        return False

    # This node represents the API's view of the Verslag
    props = {
        'id': verslag_obj.id, # API ID for the Verslag
        'soort': verslag_obj.soort.name if verslag_obj.soort else None,
        'status': verslag_obj.status.name if verslag_obj.status else None,
        'source': 'tkapi'
    }
    session.execute_write(merge_node, 'Verslag', 'id', props)
    PROCESSED_VERSLAG_IDS.add(verslag_obj.id)
    print(f"    ↳ Processed API Verslag: {verslag_obj.id}")

    # Link to parent Vergadering (the one that expanded this verslag_obj)
    if related_vergadering_id:
         session.execute_write(merge_rel, 'Vergadering', 'id', related_vergadering_id,
                               'Verslag', 'id', verslag_obj.id, 'HAS_API_VERSLAG')


    # If this verslag_obj has an expanded vergadering (it always should if Verslag.expand_params includes Vergadering)
    # And it's different from the caller (less likely if called from vergadering_loader which IS the caller)
    # This part might be redundant if called correctly from vergadering_loader
    if verslag_obj.vergadering and verslag_obj.vergadering.id != related_vergadering_id:
        from .vergadering_loader import process_and_load_vergadering 
        # When processing a vergadering found via an expanded verslag, don't re-process its XML from here
        if process_and_load_vergadering(session, driver, verslag_obj.vergadering, process_xml=False):
            pass 
        session.execute_write(merge_rel, 'Verslag', 'id', verslag_obj.id,
                              'Vergadering', 'id', verslag_obj.vergadering.id, 'REPORT_OF_VERGADERING') # More specific relation


    # Download and process VLOS XML for this specific API verslag_obj
    # The canonical_api_vergadering_id_for_vlos IS the related_vergadering_id in this flow
    if canonical_api_vergadering_id_for_vlos:
        print(f"      - Downloading XML resource for API Verslag {verslag_obj.id} (linked to Vergadering {canonical_api_vergadering_id_for_vlos})...")
        xml_content = download_verslag_xml(verslag_obj.id) # API Verslag ID is used to get the resource URL
        if xml_content:
            if defer_vlos_processing:
                # Queue for later processing at end of ETL
                xml_str = xml_content.decode('utf-8') if isinstance(xml_content, bytes) else xml_content
                DEFERRED_VLOS_ITEMS.append((xml_str, canonical_api_vergadering_id_for_vlos, verslag_obj.id))
                print(f"      ⏩ Deferred VLOS processing for Verslag {verslag_obj.id} (queued for later)")
            else:
                print(f"      - Parsing VLOS XML with Enhanced Matching for Verslag {verslag_obj.id}...")
                # Use enhanced VLOS loader with sophisticated matching
                xml_str = xml_content.decode('utf-8') if isinstance(xml_content, bytes) else xml_content
                counts = load_enhanced_vlos_verslag(driver, xml_str, canonical_api_vergadering_id_for_vlos, verslag_obj.id)
                print(f"      ✔ Enhanced VLOS processing complete for Verslag {verslag_obj.id}.")
                print(f"        📊 Match rates: Activities {counts['matched_activities']}/{counts['activities']}, "
                      f"Speakers {counts['matched_speakers']}/{counts['speakers']}, "
                      f"Zaken {counts['matched_zaken']}/{counts['zaken']}")
            # Optional: Add a property to Verslag node indicating its VLOS XML has been processed
            session.run("MATCH (vs:Verslag {id: $id}) SET vs.vlos_xml_processed = true", id=verslag_obj.id)

        else:
            print(f"      ! No XML content found/downloaded for API Verslag {verslag_obj.id}.")
    else:
        print(f"    ! Warning: No canonical_api_vergadering_id_for_vlos provided for Verslag {verslag_obj.id}, cannot process VLOS XML.")
        
    return True

def process_and_load_zaak(session, zaak_obj, related_entity_id: str = None, related_entity_type: str = None):
    """
    Process and load a Zaak object. This is a wrapper that imports from zaak_loader
    to avoid circular imports.
    """
    if not zaak_obj or not zaak_obj.nummer or zaak_obj.nummer in PROCESSED_ZAAK_IDS:
        return False
    
    # Use parent package relative import (loaders.zaak_loader)
    from ..zaak_loader import process_and_load_zaak as _process_zaak
    result = _process_zaak(session, zaak_obj, related_entity_id, related_entity_type)
    
    if result:
        PROCESSED_ZAAK_IDS.add(zaak_obj.nummer)
    
    return result

def process_and_load_document(session, doc_obj, related_entity_id: str = None, related_entity_type: str = None):
    """
    Process and load a Document object, intended for nested calls.
    This is a shallow processor and does not handle the document's own relationships.
    """
    if not doc_obj or not doc_obj.id or doc_obj.id in PROCESSED_DOCUMENT_IDS:
        return False
    
    props = {
        'id': doc_obj.id,
        'titel': doc_obj.titel or '',
        'datum': str(doc_obj.datum) if doc_obj.datum else None,
        'soort': doc_obj.soort.name if hasattr(doc_obj.soort, 'name') else doc_obj.soort
    }
    session.execute_write(merge_node, 'Document', 'id', props)
    PROCESSED_DOCUMENT_IDS.add(doc_obj.id)
    return True


def clear_processed_ids():
    """Clears all global processed ID sets. Call at the beginning of a full run."""
    PROCESSED_DOSSIER_IDS.clear()
    PROCESSED_BESLUIT_IDS.clear()
    PROCESSED_STEMMING_IDS.clear()
    PROCESSED_VERSLAG_IDS.clear()
    PROCESSED_ZAAK_IDS.clear()
    print("🧹 Cleared processed ID sets.")


# Insert after PROCESSED_VERSLAG_IDS set definition
DEFERRED_VLOS_ITEMS = []  # Tuples of (xml_string, canonical_vergadering_id, verslag_id)

