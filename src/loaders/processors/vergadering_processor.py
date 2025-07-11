"""
Processing logic extracted from vergadering_loader.py
"""
import datetime
from tkapi import TKApi
from tkapi.vergadering import Vergadering
from tkapi.verslag import Verslag
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel
from .common_processors import process_and_load_verslag
from tkapi.util import util as tkapi_util
from datetime import timezone

# Define relationship mapping for Vergadering
REL_MAP_VERGADERING = {
    'activiteiten': ('Activiteit', 'HAS_ACTIVITEIT', 'id'),
    'agendapunten': ('Agendapunt', 'HAS_AGENDAPUNT', 'id'),
    'zaken': ('Zaak', 'DISCUSSES_ZAAK', 'nummer'),
    'documenten': ('Document', 'HAS_DOCUMENT', 'id'),
}


def process_and_load_vergadering(session, driver, vergadering_obj: Vergadering, process_xml=True):
    """Process and load a single Vergadering entity"""
    if not vergadering_obj or not vergadering_obj.id:
        return False

    props = {
        'id': vergadering_obj.id,
        'titel': vergadering_obj.titel,
        'nummer': vergadering_obj.nummer,
        'zaal': vergadering_obj.zaal,
        'soort': vergadering_obj.soort.name if vergadering_obj.soort else None,
        'datum': str(vergadering_obj.datum) if vergadering_obj.datum else None,
        'begin': str(vergadering_obj.begin) if vergadering_obj.begin else None,
        'einde': str(vergadering_obj.einde) if vergadering_obj.einde else None,
        'samenstelling': vergadering_obj.samenstelling,
        'source': 'tkapi'
    }
    session.execute_write(merge_node, 'Vergadering', 'id', props)
    print(f"    ↳ Processed API Vergadering: {vergadering_obj.id} - {vergadering_obj.titel}")

    # ------------------------------------------------------------
    # NEW: Link Vergadering to its direct child entities so that
    #       follow-up matchers (VLOS, etc.) can traverse the graph.
    #       This logic used to exist only in process_single_vergadering
    #       and was therefore skipped in normal loader runs.
    # ------------------------------------------------------------

    for attr_name, (target_label, rel_type, target_key_prop) in REL_MAP_VERGADERING.items():
        related_items = getattr(vergadering_obj, attr_name, []) or []

        # Normalise to list
        if not isinstance(related_items, list):
            related_items = [related_items]

        for rel_obj in related_items:
            if not rel_obj:
                continue
            key_val = getattr(rel_obj, target_key_prop, None)
            if not key_val:
                continue

            # Ensure the child node exists (minimal merge)
            session.execute_write(
                merge_node,
                target_label,
                target_key_prop,
                {target_key_prop: key_val}
            )

            # Create / merge the relationship
            session.execute_write(
                merge_rel,
                'Vergadering', 'id', vergadering_obj.id,
                target_label,  target_key_prop, key_val,
                rel_type
            )

    # Process related Verslag from API
    if vergadering_obj.verslag:
        if process_xml and process_and_load_verslag(session, driver, vergadering_obj.verslag, 
                                        related_vergadering_id=vergadering_obj.id,
                                        canonical_api_vergadering_id_for_vlos=vergadering_obj.id,
                                        defer_vlos_processing=True):
            pass 
        elif not process_xml:
            # Minimal Verslag node creation if not fully processed
            session.execute_write(merge_node, 'Verslag', 'id', {'id': vergadering_obj.verslag.id, 'source': 'tkapi_placeholder'})
            session.execute_write(merge_rel, 'Vergadering', 'id', vergadering_obj.id,
                                  'Verslag', 'id', vergadering_obj.verslag.id, 'HAS_API_VERSLAG')
    return True


def process_single_vergadering(session, driver, vergadering_obj: Vergadering, process_xml: bool = True):
    """Process a single Vergadering object"""
    print(f"  🏛️ Processing Vergadering {vergadering_obj.id} ({vergadering_obj.titel})")
    
    # Create or update the Vergadering node
    props = {
        'id': vergadering_obj.id,
        'titel': vergadering_obj.titel,
        'soort': vergadering_obj.soort.name if vergadering_obj.soort else None,
        'begin': tkapi_util.datetime_to_neo4j_string(vergadering_obj.begin),
        'einde': tkapi_util.datetime_to_neo4j_string(vergadering_obj.einde),
        'nummer': vergadering_obj.nummer,
        'datum': tkapi_util.datetime_to_neo4j_string(vergadering_obj.datum),
        'kamer': vergadering_obj.kamer,
        'bijgewerkt': tkapi_util.datetime_to_neo4j_string(vergadering_obj.bijgewerkt),
        'source': 'tkapi'
    }
    
    session.execute_write(merge_node, 'Vergadering', 'id', props)
    
    # Process expanded Verslag objects
    verslag_count = 0
    zaak_count = 0
    
    for verslag_obj in vergadering_obj.verslagen:
        # Note: VLOS processing has been deprecated
        # The canonical_api_vergadering_id_for_vlos parameter is kept for compatibility
        # but XML processing is disabled in the deprecated system
        if process_and_load_verslag(session, driver, verslag_obj, 
                                   related_vergadering_id=vergadering_obj.id,
                                   canonical_api_vergadering_id_for_vlos=vergadering_obj.id,
                                   defer_vlos_processing=True):
            verslag_count += 1
    
    # Process expanded Zaak objects
    for zaak_obj in vergadering_obj.zaken:
        if process_and_load_zaak(session, zaak_obj, 
                                related_entity_id=vergadering_obj.id,
                                related_entity_type='Vergadering'):
            zaak_count += 1
        
        # Create relationship
        session.execute_write(merge_rel, 'Vergadering', 'id', vergadering_obj.id,
                              'Zaak', 'nummer', zaak_obj.nummer, 
                              REL_MAP_VERGADERING['vergadering_to_zaak'])
    
    return {
        'verslagen_processed': verslag_count,
        'zaken_processed': zaak_count
    }

def setup_vergadering_api_filter(start_date: datetime, end_date: datetime):
    """Set up the Vergadering API filter for a date range"""
    from tkapi.vergadering import VergaderingFilter
    
    # Convert to UTC for API call
    start_utc = start_date.astimezone(timezone.utc)
    end_utc = end_date.astimezone(timezone.utc)
    
    v_filter = VergaderingFilter()
    v_filter.filter_date_range(begin_datetime=start_utc, end_datetime=end_utc)
    
    return v_filter 