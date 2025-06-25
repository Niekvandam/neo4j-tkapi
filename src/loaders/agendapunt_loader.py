import datetime
from tkapi import TKApi
from tkapi.agendapunt import Agendapunt
from tkapi.besluit import Besluit # For expand_params
from tkapi.document import Document # For expand_params
from tkapi.activiteit import Activiteit # For expand_params
from tkapi.zaak import Zaak # For expand_params
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel
from .common_processors import process_and_load_besluit, PROCESSED_BESLUIT_IDS, process_and_load_zaak, PROCESSED_ZAAK_IDS
from .vlos_verslag_loader import load_vlos_verslag
from tkapi.util import util as tkapi_util
from datetime import timezone
import time

# Import checkpoint functionality
from core.checkpoint.checkpoint_manager import LoaderCheckpoint, CheckpointManager

# Import the checkpoint decorator
from core.checkpoint.checkpoint_decorator import checkpoint_loader

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry

# from .common_processors import process_and_load_document # If documents linked here need full processing
# from .common_processors import process_and_load_zaak # If zaken linked here need full processing

# api = TKApi() # Not needed at module level


class AgendapuntLoader(BaseLoader):
    """Loader for Agendapunt entities with full interface support"""
    
    def __init__(self):
        super().__init__(
            name="agendapunt_loader",
            description="Loads Agendapunten from TK API with related entities (DEPRECATED - use via Activiteiten)"
        )
        self._capabilities = [
            LoaderCapability.BATCH_PROCESSING,
            LoaderCapability.DATE_FILTERING,
            LoaderCapability.SKIP_FUNCTIONALITY,
            LoaderCapability.INCREMENTAL_LOADING,
            LoaderCapability.RELATIONSHIP_PROCESSING
        ]
    
    def load(self, conn: Neo4jConnection, config: LoaderConfig, 
             checkpoint_manager: CheckpointManager = None) -> LoaderResult:
        """Main loading method implementing the interface"""
        start_time = time.time()
        result = LoaderResult(
            success=False,
            processed_count=0,
            failed_count=0,
            skipped_count=0,
            total_items=0,
            execution_time_seconds=0.0,
            error_messages=[],
            warnings=["This loader is deprecated - Agendapunten should be processed through Activiteiten"]
        )
        
        try:
            # Validate configuration
            validation_errors = self.validate_config(config)
            if validation_errors:
                result.error_messages.extend(validation_errors)
                return result
            
            # Use the decorated function for actual loading
            load_result = load_agendapunten(
                conn=conn,
                batch_size=config.batch_size,
                start_date_str=config.start_date or "2024-01-01",
                skip_count=config.skip_count
            )
            
            # For now, we'll mark as successful if no exceptions occurred
            result.success = True
            result.execution_time_seconds = time.time() - start_time
            
        except Exception as e:
            result.error_messages.append(f"Loading failed: {str(e)}")
            result.execution_time_seconds = time.time() - start_time
        
        return result


# Register the loader
agendapunt_loader_instance = AgendapuntLoader()
loader_registry.register(agendapunt_loader_instance)


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


@checkpoint_loader(checkpoint_interval=25)
def load_agendapunten(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", skip_count: int = 0, _checkpoint_context=None):
    """
    DEPRECATED: Load Agendapunten with automatic checkpoint support using decorator.
    
    NOTE: Agendapunten should be processed through Activiteiten since every Agendapunt 
    belongs to exactly one Activiteit. This standalone loader is kept for compatibility
    but should not be used in normal operation.
    
    The @checkpoint_loader decorator automatically handles:
    - Progress tracking every 25 items
    - Skipping already processed items
    - Error handling and logging
    - Final progress reporting
    """
    print("⚠️ WARNING: Using deprecated standalone agendapunt loader.")
    print("   Agendapunten should be processed through Activiteiten instead.")
    print("   This loader is kept for compatibility but may be removed in the future.")
    api = TKApi()
    start_datetime_obj = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    odata_start_date_str = tkapi_util.datetime_to_odata(start_datetime_obj.replace(tzinfo=timezone.utc))

    filter = Agendapunt.create_filter()
    filter.add_filter_str(f"GeplandeDatum ge {odata_start_date_str}")
    
    agendapunten_api = api.get_items(Agendapunt, filter=filter)
    print(f"→ Fetched {len(agendapunten_api)} Agendapunten since {start_date_str}")

    if not agendapunten_api:
        print("No agendapunten found for the date range.")
        return

    # Apply skip_count if specified
    if skip_count > 0:
        if skip_count >= len(agendapunten_api):
            print(f"⚠️ Skip count ({skip_count}) is greater than or equal to total items ({len(agendapunten_api)}). Nothing to process.")
            return
        agendapunten_api = agendapunten_api[skip_count:]
        print(f"⏭️ Skipping first {skip_count} items. Processing {len(agendapunten_api)} remaining items.")

    def process_single_agendapunt(agendapunt_obj):
        with conn.driver.session(database=conn.database) as session:
            if not agendapunt_obj or not agendapunt_obj.id:
                return

            # Create Agendapunt node
            props = {
                'id': agendapunt_obj.id,
                'nummer': agendapunt_obj.nummer,
                'onderwerp': agendapunt_obj.onderwerp or '',
                'toelichting': agendapunt_obj.toelichting,
                'status': agendapunt_obj.status,
                'datum': str(agendapunt_obj.datum) if agendapunt_obj.datum else None,
                'geplande_datum': str(agendapunt_obj.geplande_datum) if agendapunt_obj.geplande_datum else None,
                'volgorde': agendapunt_obj.volgorde
            }
            session.execute_write(merge_node, 'Agendapunt', 'id', props)

            # Note: Since this is a deprecated standalone loader, we only process the agendapunt itself
            # Related items should be processed through the proper activiteit -> agendapunt hierarchy
            # If you need full relationship processing, use the activiteit loader instead
            
            # Just call the processor function which handles relationships properly
            process_and_load_agendapunt(session, agendapunt_obj)

    # Clear processed IDs at the beginning
    PROCESSED_ZAAK_IDS.clear()

    # Use the checkpoint context to process items automatically
    if _checkpoint_context:
        _checkpoint_context.process_items(agendapunten_api, process_single_agendapunt)
    else:
        # Fallback for when decorator is not used
        for agendapunt_obj in agendapunten_api:
            process_single_agendapunt(agendapunt_obj)

    print("✅ Loaded Agendapunten and their related entities.")


# Keep the original function for backward compatibility (if needed)
def load_agendapunten_original(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", checkpoint_manager=None):
    """
    Original load_agendapunten function for backward compatibility.
    This version is deprecated - use load_agendapunten() for the new decorator-based version.
    """
    # This could import the old implementation if needed, but for now we'll use the new one
    return load_agendapunten(conn, batch_size, start_date_str)