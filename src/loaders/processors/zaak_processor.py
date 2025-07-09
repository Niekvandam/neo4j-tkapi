"""
Zaak processing logic extracted from zaak_loader.py
"""
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel
from core.config.constants import REL_MAP_ZAAK
from loaders.processors.common_processors import process_and_load_document, process_and_load_zaak, PROCESSED_DOCUMENT_IDS
import threading

# Thread-safe lock for shared resources
_thread_lock = threading.Lock()


def process_single_zaak(session, zaak_obj):
    """
    Process a single Zaak object and create Neo4j nodes/relationships.
    
    Args:
        session: Neo4j session
        zaak_obj: Zaak object from TK API
    """
    if not zaak_obj or not zaak_obj.nummer:
        return False

    # Create Zaak node
    props = {
        'nummer': zaak_obj.nummer,
        'onderwerp': zaak_obj.onderwerp or '',
        'soort': zaak_obj.soort,
        # Use 'gestart_op' (start date) since Zaak objects have no 'datum' attribute
        'datum': str(getattr(zaak_obj, 'gestart_op', None)) if getattr(zaak_obj, 'gestart_op', None) else None,
        'afgedaan': zaak_obj.afgedaan,
        'status': getattr(zaak_obj, 'status', None),
        'dossier_id': zaak_obj.dossier.id if getattr(zaak_obj, 'dossier', None) else None,
        'source': 'tkapi'
    }
    session.execute_write(merge_node, 'Zaak', 'nummer', props)

    # Process related items
    for attr_name, (target_label, rel_type, target_key_prop) in REL_MAP_ZAAK.items():
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

            if target_label == 'Document':
                if process_and_load_document(session, related_item_obj, related_entity_id=zaak_obj.nummer, related_entity_type="Zaak"):
                    pass
            else:
                session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val})

            session.execute_write(merge_rel, 'Zaak', 'nummer', zaak_obj.nummer,
                                  target_label, target_key_prop, related_item_key_val, rel_type)
    
    return True


def process_single_zaak_threaded(zaak_obj, conn: Neo4jConnection, checkpoint_context=None):
    """
    Thread-safe version of processing a single zaak.
    Each thread gets its own Neo4j session.
    
    Args:
        zaak_obj: Zaak object from TK API
        conn: Neo4j connection
        checkpoint_context: Optional checkpoint context for progress tracking
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with conn.driver.session(database=conn.database) as session:
            if not zaak_obj or not zaak_obj.nummer:
                return False

            # Create Zaak node
            props = {
                'nummer': zaak_obj.nummer,
                'onderwerp': zaak_obj.onderwerp or '',
                'soort': zaak_obj.soort,
                # Use 'gestart_op' (start date) since Zaak objects have no 'datum' attribute
                'datum': str(getattr(zaak_obj, 'gestart_op', None)) if getattr(zaak_obj, 'gestart_op', None) else None,
                'afgedaan': zaak_obj.afgedaan,
                'status': getattr(zaak_obj, 'status', None),
                'dossier_id': zaak_obj.dossier.id if getattr(zaak_obj, 'dossier', None) else None,
                'source': 'tkapi'
            }
            session.execute_write(merge_node, 'Zaak', 'nummer', props)

            # Process related items
            for attr_name, (target_label, rel_type, target_key_prop) in REL_MAP_ZAAK.items():
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

                    if target_label == 'Document':
                        # Thread-safe document processing with lock
                        with _thread_lock:
                            if process_and_load_document(session, related_item_obj, related_entity_id=zaak_obj.nummer, related_entity_type="Zaak"):
                                pass
                    else:
                        session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val})

                    session.execute_write(merge_rel, 'Zaak', 'nummer', zaak_obj.nummer,
                                          target_label, target_key_prop, related_item_key_val, rel_type)
            
        # Mark as processed in checkpoint if available
        if checkpoint_context:
            checkpoint_context.mark_processed(zaak_obj)
            
        return True
        
    except Exception as e:
        error_msg = f"Failed to process Zaak {zaak_obj.nummer}: {str(e)}"
        print(f"    ❌ {error_msg}")
        
        if checkpoint_context:
            checkpoint_context.mark_failed(zaak_obj, error_msg)
            
        return False 