"""
Processing logic extracted from zaak_loader_refactored.py
"""
import datetime
from tkapi import TKApi
from tkapi.zaak import Zaak
from tkapi.document import Document
from tkapi.agendapunt import Agendapunt
from tkapi.activiteit import Activiteit
from tkapi.besluit import Besluit
from tkapi.dossier import Dossier
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel
from core.config.constants import REL_MAP_ZAAK
from .common_processors import process_and_load_besluit, process_and_load_dossier
from tkapi.util import util as tkapi_util
from datetime import timezone


def process_and_load_zaak(session, zaak_obj: Zaak, related_entity_id: str = None, related_entity_type: str = None):
    """Process and load a single Zaak entity"""
    if not zaak_obj or not zaak_obj.nummer:
        return False

    # Key conflict: vlos_verslag_loader uses 'id' for Zaak, here we use 'nummer'.
    # This needs to be harmonized. For now, this function uses 'nummer'.
    props = {
        'id': zaak_obj.id,  # Store the GUID from API as a property
        'nummer': zaak_obj.nummer,  # Use 'nummer' as the MERGE key
        'onderwerp': zaak_obj.onderwerp,
        'afgedaan': zaak_obj.afgedaan,
        'volgnummer': zaak_obj.volgnummer,
        'alias': zaak_obj.alias,
        'gestart_op': str(zaak_obj.gestart_op) if zaak_obj.gestart_op else None
    }
    session.execute_write(merge_node, 'Zaak', 'nummer', props)

    if zaak_obj.soort:
        session.execute_write(merge_rel, 'Zaak', 'nummer', zaak_obj.nummer,
                              'ZaakSoort', 'key', zaak_obj.soort.name, 'HAS_SOORT')
    if zaak_obj.kabinetsappreciatie:
        session.execute_write(merge_rel, 'Zaak', 'nummer', zaak_obj.nummer,
                              'Kabinetsappreciatie', 'key', zaak_obj.kabinetsappreciatie.name, 'HAS_KABINETSAPPRECIATIE')
    
    # Handle VervangenDoor
    if zaak_obj.vervangen_door:
        vd = zaak_obj.vervangen_door
        process_and_load_zaak(session, vd)
        session.execute_write(merge_rel, 'Zaak', 'nummer', zaak_obj.nummer,
                              'Zaak', 'nummer', vd.nummer, 'REPLACED_BY')

    # Process other related items
    for attr_name, (target_label, rel_type, target_key_prop) in REL_MAP_ZAAK.items():
        if attr_name == 'vervangen_door':
            continue

        related_items = getattr(zaak_obj, attr_name, []) or []
        if not isinstance(related_items, list):
            related_items = [related_items]

        for related_item_obj in related_items:
            if not related_item_obj:
                continue
            
            related_item_key_val = getattr(related_item_obj, target_key_prop, None)
            if related_item_key_val is None:
                print(f"    ! Warning: Related item for '{attr_name}' in Zaak {zaak_obj.nummer} missing key '{target_key_prop}'.")
                continue

            if target_label == 'Besluit':
                if process_and_load_besluit(session, related_item_obj, related_zaak_nummer=zaak_obj.nummer):
                    pass
            elif target_label == 'Document':
                session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val, 'titel': related_item_obj.titel or ''})
            elif target_label == 'Agendapunt':
                session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val, 'onderwerp': related_item_obj.onderwerp or ''})
            elif target_label == 'Activiteit':
                session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val, 'onderwerp': related_item_obj.onderwerp or ''})
            elif target_label == 'ZaakActor':
                actor_props = {'id': related_item_obj.id, 'naam': related_item_obj.naam or ''}
                session.execute_write(merge_node, target_label, 'id', actor_props)
            else:
                session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val})

            # Create the relationship from Zaak to the related item
            session.execute_write(merge_rel, 'Zaak', 'nummer', zaak_obj.nummer,
                                  target_label, target_key_prop, related_item_key_val, rel_type)
    return True


def setup_zaak_api_filter(start_date_str: str):
    """Setup API filter for Zaak queries"""
    start_datetime_obj = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    odata_start_date_str = tkapi_util.datetime_to_odata(start_datetime_obj.replace(tzinfo=timezone.utc))

    # Manage expand_params
    original_zaak_expand_params = list(Zaak.expand_params or [])
    current_expand_params = list(original_zaak_expand_params)

    if Dossier.type not in current_expand_params:
        current_expand_params.append(Dossier.type)

    Zaak.expand_params = current_expand_params

    filter_obj = Zaak.create_filter()
    filter_obj.add_filter_str(f"GestartOp ge {odata_start_date_str}")
    
    return filter_obj, original_zaak_expand_params


def restore_zaak_expand_params(original_params):
    """Restore original expand params"""
    Zaak.expand_params = original_params 