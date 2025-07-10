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


def process_single_vergadering(conn, vergadering_obj):
    """Process a single vergadering with all related items"""
    with conn.driver.session(database=conn.database) as session:
        if not vergadering_obj or not vergadering_obj.id:
            return

        # Create Vergadering node
        props = {
            'id': vergadering_obj.id,
            'nummer': vergadering_obj.nummer,
            'titel': vergadering_obj.titel or '',
            'datum': str(vergadering_obj.datum) if vergadering_obj.datum else None,
            'begin': str(vergadering_obj.begin) if vergadering_obj.begin else None,
            'einde': str(vergadering_obj.einde) if vergadering_obj.einde else None,
            'zaal': vergadering_obj.zaal,
            'soort': vergadering_obj.soort.name if vergadering_obj.soort else None,
            'samenstelling': vergadering_obj.samenstelling
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
                    continue

                # Create minimal node for related item
                session.execute_write(merge_node, target_label, target_key_prop, 
                                    {target_key_prop: related_item_key_val})
                
                # Create relationship
                session.execute_write(merge_rel, 'Vergadering', 'id', vergadering_obj.id,
                                      target_label, target_key_prop, related_item_key_val, rel_type)


def setup_vergadering_api_filter(start_date_str: str):
    """Setup API filter for Vergadering queries"""
    start_datetime_obj = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    odata_start_date_str = tkapi_util.datetime_to_odata(start_datetime_obj.replace(tzinfo=timezone.utc))

    filter_obj = Vergadering.create_filter()
    filter_obj.add_filter_str(f"Datum ge {odata_start_date_str}")
    
    return filter_obj 