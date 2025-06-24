import datetime
import time
from tkapi import TKApi
from tkapi.document import Document
from tkapi.dossier import Dossier # For expand_params
from tkapi.zaak import Zaak # For expand_params
from tkapi.activiteit import Activiteit # For expand_params
from tkapi.agendapunt import Agendapunt # For expand_params
# DocumentActor is also a related type in Document.expand_params
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel

# Import processors for related entities
from .common_processors import process_and_load_dossier, PROCESSED_DOSSIER_IDS, process_and_load_zaak, PROCESSED_ZAAK_IDS
# from .common_processors import process_and_load_zaak, PROCESSED_ZAAK_IDS # If Zaken from here need full processing
# from .agendapunt_loader import process_and_load_agendapunt # If Agendapunten from here need full processing
# from .activiteit_loader import process_and_load_activiteit_from_doc # You'd need a specific processor

# Import checkpoint functionality
from core.checkpoint.checkpoint_manager import LoaderCheckpoint, CheckpointManager

# Import the checkpoint decorator
from core.checkpoint.checkpoint_decorator import checkpoint_loader

# api = TKApi() # Not needed at module level

@checkpoint_loader(checkpoint_interval=25)
def load_documents(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", skip_count: int = 0, _checkpoint_context=None):
    """
    Load Documents with automatic checkpoint support using decorator.
    
    The @checkpoint_loader decorator automatically handles:
    - Progress tracking every 25 items
    - Skipping already processed items
    - Error handling and logging
    - Final progress reporting
    """
    from tkapi.util import util as tkapi_util
    from datetime import timezone
    
    api = TKApi()
    start_datetime_obj = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    odata_start_date_str = tkapi_util.datetime_to_odata(start_datetime_obj.replace(tzinfo=timezone.utc))

    filter = Document.create_filter()
    filter.add_filter_str(f"Datum ge {odata_start_date_str}")
    
    documenten_api = api.get_items(Document, filter=filter)
    print(f"→ Fetched {len(documenten_api)} Documents since {start_date_str}")

    if not documenten_api:
        print("No documents found for the date range.")
        return

    # Apply skip_count if specified
    if skip_count > 0:
        if skip_count >= len(documenten_api):
            print(f"⚠️ Skip count ({skip_count}) is greater than or equal to total items ({len(documenten_api)}). Nothing to process.")
            return
        documenten_api = documenten_api[skip_count:]
        print(f"⏭️ Skipping first {skip_count} items. Processing {len(documenten_api)} remaining items.")

    def process_single_document(document_obj):
        with conn.driver.session(database=conn.database) as session:
            if not document_obj or not document_obj.id:
                return

            # Create Document node
            props = {
                'id': document_obj.id,
                'titel': document_obj.titel or '',
                'datum': str(document_obj.datum) if document_obj.datum else None,
                'soort': document_obj.soort,
                'onderwerp': document_obj.onderwerp,
                'alias': document_obj.alias,
                'volgnummer': document_obj.volgnummer
            }
            session.execute_write(merge_node, 'Document', 'id', props)

            # Process related items
            for attr_name, (target_label, rel_type, target_key_prop) in REL_MAP_DOCUMENT.items():
                related_items = getattr(document_obj, attr_name, []) or []
                if not isinstance(related_items, list):
                    related_items = [related_items]

                for related_item_obj in related_items:
                    if not related_item_obj:
                        continue
                    
                    related_item_key_val = getattr(related_item_obj, target_key_prop, None)
                    if related_item_key_val is None:
                        print(f"    ! Warning: Related item for '{attr_name}' in Document {document_obj.id} missing key '{target_key_prop}'.")
                        continue

                    if target_label == 'Zaak':
                        if process_and_load_zaak(session, related_item_obj, related_entity_id=document_obj.id, related_entity_type="Document"):
                            pass
                    elif target_label == 'Activiteit':
                        session.execute_write(merge_node, target_label, target_key_prop, {
                            target_key_prop: related_item_key_val, 
                            'onderwerp': related_item_obj.onderwerp or ''
                        })
                    elif target_label == 'Agendapunt':
                        session.execute_write(merge_node, target_label, target_key_prop, {
                            target_key_prop: related_item_key_val, 
                            'onderwerp': related_item_obj.onderwerp or ''
                        })
                    else:
                        session.execute_write(merge_node, target_label, target_key_prop, {target_key_prop: related_item_key_val})

                    session.execute_write(merge_rel, 'Document', 'id', document_obj.id,
                                          target_label, target_key_prop, related_item_key_val, rel_type)

    # Clear processed IDs at the beginning
    PROCESSED_ZAAK_IDS.clear()

    # Use the checkpoint context to process items automatically
    if _checkpoint_context:
        _checkpoint_context.process_items(documenten_api, process_single_document)
    else:
        # Fallback for when decorator is not used
        for document_obj in documenten_api:
            process_single_document(document_obj)

    print("✅ Loaded Documents and their related entities.")


# Keep the original function for backward compatibility (if needed)
def load_documents_original(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", checkpoint_manager=None):
    """
    Original load_documents function for backward compatibility.
    This version is deprecated - use load_documents() for the new decorator-based version.
    """
    # This could import the old implementation if needed, but for now we'll use the new one
    return load_documents(conn, batch_size, start_date_str)