"""
Activiteit processing logic extracted from activiteit_loader.py
"""
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel
from core.config.constants import REL_MAP_ACTIVITEIT
from .common_processors import process_and_load_zaak, PROCESSED_ZAAK_IDS
from ..agendapunt_loader import process_and_load_agendapunt
import threading

# Thread-safe lock for shared resources
_thread_lock = threading.Lock()


def process_single_activiteit(session, activiteit_obj):
    """
    Process a single Activiteit object and create Neo4j nodes/relationships.
    
    Args:
        session: Neo4j session
        activiteit_obj: Activiteit object from TK API
    """
    if not activiteit_obj or not activiteit_obj.id:
        return False

    # Create Activiteit node
    props = {
        'id': activiteit_obj.id,
        'nummer': activiteit_obj.nummer,
        'onderwerp': activiteit_obj.onderwerp,
        'soort': activiteit_obj.soort.name if hasattr(activiteit_obj.soort, 'name') else activiteit_obj.soort,
        'datum': str(activiteit_obj.datum) if activiteit_obj.datum else None,
        'begin': str(activiteit_obj.begin) if activiteit_obj.begin else None,
        'einde': str(activiteit_obj.einde) if activiteit_obj.einde else None,
        # Some activiteiten do not expose 'geplande_datum'; use getattr to avoid AttributeError
        'geplande_datum': str(getattr(activiteit_obj, 'geplande_datum', None)) if getattr(activiteit_obj, 'geplande_datum', None) else None,
        'datum_soort': activiteit_obj.datum_soort.name if getattr(activiteit_obj, 'datum_soort', None) else None,
        'vergaderjaar': activiteit_obj.vergaderjaar,
        # 'voortouwcommissies' are handled as relationships below
        'status': activiteit_obj.status.name if hasattr(activiteit_obj.status, 'name') else activiteit_obj.status
    }
    session.execute_write(merge_node, 'Activiteit', 'id', props)

    # Process related items
    for attr_name, (target_label, rel_type, target_key_prop) in REL_MAP_ACTIVITEIT.items():
        related_items = getattr(activiteit_obj, attr_name, []) or []
        if not isinstance(related_items, list):
            related_items = [related_items]

        for related_item_obj in related_items:
            if not related_item_obj:
                continue
            
            related_item_key_val = getattr(related_item_obj, target_key_prop, None)
            if related_item_key_val is None:
                print(f"    ! Warning: Related item for '{attr_name}' in Activiteit {activiteit_obj.id} missing key '{target_key_prop}'.")
                continue

            if target_label == 'Zaak':
                if process_and_load_zaak(session, related_item_obj, related_entity_id=activiteit_obj.id, related_entity_type="Activiteit"):
                    pass
            elif target_label == 'Reservering':
                # Store full Reservering metadata
                res_props = {
                    'id': related_item_obj.id,
                    'nummer': related_item_obj.nummer,
                    'activiteit_nummer': related_item_obj.activiteit_nummer,
                    'status_code': related_item_obj.status_code.name if getattr(related_item_obj, 'status_code', None) else None,
                    'status_naam': related_item_obj.status_naam.name if getattr(related_item_obj, 'status_naam', None) else None,
                }
                session.execute_write(merge_node, 'Reservering', 'id', res_props)

                # Ensure linked Zaal is stored and linked
                zaal_obj = getattr(related_item_obj, 'zaal', None)
                if zaal_obj and zaal_obj.id:
                    zaal_props = {
                        'id': zaal_obj.id,
                        'naam': zaal_obj.naam,
                    }
                    session.execute_write(merge_node, 'Zaal', 'id', zaal_props)
                    session.execute_write(merge_rel, 'Reservering', 'id', related_item_obj.id,
                                              'Zaal', 'id', zaal_obj.id, 'IN_ZAAL')
                # Create relation from Activiteit to Reservering handled below
            elif target_label == 'Agendapunt':
                # Process agendapunt fully since it belongs to this activiteit
                if process_and_load_agendapunt(session, related_item_obj, related_activiteit_id=activiteit_obj.id):
                    pass
            else:
                session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val})

            session.execute_write(merge_rel, 'Activiteit', 'id', activiteit_obj.id,
                                  target_label, target_key_prop, related_item_key_val, rel_type)
    
    return True


def process_single_activiteit_threaded(activiteit_obj, conn: Neo4jConnection, checkpoint_context=None):
    """
    Thread-safe version of processing a single activiteit.
    Each thread gets its own Neo4j session.
    
    Args:
        activiteit_obj: Activiteit object from TK API
        conn: Neo4j connection
        checkpoint_context: Optional checkpoint context for progress tracking
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with conn.driver.session(database=conn.database) as session:
            if not activiteit_obj or not activiteit_obj.id:
                return False

            # Create Activiteit node
            props = {
                'id': activiteit_obj.id,
                'nummer': activiteit_obj.nummer,
                'onderwerp': activiteit_obj.onderwerp,
                'soort': activiteit_obj.soort.name if hasattr(activiteit_obj.soort, 'name') else str(activiteit_obj.soort),
                'datum': str(activiteit_obj.datum) if activiteit_obj.datum else None,
                'begin': str(activiteit_obj.begin) if activiteit_obj.begin else None,
                'einde': str(activiteit_obj.einde) if activiteit_obj.einde else None,
                'geplande_datum': str(getattr(activiteit_obj, 'geplande_datum', None)) if getattr(activiteit_obj, 'geplande_datum', None) else None,
                'datum_soort': activiteit_obj.datum_soort.name if getattr(activiteit_obj, 'datum_soort', None) else None,
                'vergaderjaar': activiteit_obj.vergaderjaar,
                'status': activiteit_obj.status.name if hasattr(activiteit_obj.status, 'name') else str(activiteit_obj.status)
            }
            session.execute_write(merge_node, 'Activiteit', 'id', props)

            # Process related items
            for attr_name, (target_label, rel_type, target_key_prop) in REL_MAP_ACTIVITEIT.items():
                related_items = getattr(activiteit_obj, attr_name, []) or []
                if not isinstance(related_items, list):
                    related_items = [related_items]

                for related_item_obj in related_items:
                    if not related_item_obj:
                        continue
                    
                    related_item_key_val = getattr(related_item_obj, target_key_prop, None)
                    if related_item_key_val is None:
                        print(f"    ! Warning: Related item for '{attr_name}' in Activiteit {activiteit_obj.id} missing key '{target_key_prop}'.")
                        continue

                    if target_label == 'Zaak':
                        # Thread-safe zaak processing with lock
                        with _thread_lock:
                            if process_and_load_zaak(session, related_item_obj, related_entity_id=activiteit_obj.id, related_entity_type="Activiteit"):
                                pass
                    elif target_label == 'Reservering':
                        # Store full Reservering metadata
                        res_props = {
                            'id': related_item_obj.id,
                            'nummer': related_item_obj.nummer,
                            'activiteit_nummer': related_item_obj.activiteit_nummer,
                            'status_code': related_item_obj.status_code.name if getattr(related_item_obj, 'status_code', None) else None,
                            'status_naam': related_item_obj.status_naam.name if getattr(related_item_obj, 'status_naam', None) else None,
                        }
                        session.execute_write(merge_node, 'Reservering', 'id', res_props)

                        # Ensure linked Zaal is stored and linked
                        zaal_obj = getattr(related_item_obj, 'zaal', None)
                        if zaal_obj and zaal_obj.id:
                            zaal_props = {
                                'id': zaal_obj.id,
                                'naam': zaal_obj.naam,
                            }
                            session.execute_write(merge_node, 'Zaal', 'id', zaal_props)
                            session.execute_write(merge_rel, 'Reservering', 'id', related_item_obj.id,
                                                      'Zaal', 'id', zaal_obj.id, 'IN_ZAAL')
                        # Create relation from Activiteit to Reservering handled below
                    elif target_label == 'Agendapunt':
                        # Process agendapunt fully since it belongs to this activiteit
                        if process_and_load_agendapunt(session, related_item_obj, related_activiteit_id=activiteit_obj.id):
                            pass
                    else:
                        session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val})

                    session.execute_write(merge_rel, 'Activiteit', 'id', activiteit_obj.id,
                                          target_label, target_key_prop, related_item_key_val, rel_type)
            
        # Mark as processed in checkpoint if available
        if checkpoint_context:
            checkpoint_context.mark_processed(activiteit_obj)
            
        return True
        
    except Exception as e:
        error_msg = f"Failed to process Activiteit {activiteit_obj.id}: {str(e)}"
        print(f"    ❌ {error_msg}")
        
        if checkpoint_context:
            checkpoint_context.mark_failed(activiteit_obj, error_msg)
            
        return False 