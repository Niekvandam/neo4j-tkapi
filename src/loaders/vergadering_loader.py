"""
Simple Vergadering Loader - Creates foundation Vergadering nodes for VLOS analysis

This loader creates the basic Vergadering nodes from TK API that the VLOS analysis
loader can reference. It's kept simple to avoid circular dependencies.
"""

import datetime
from tkapi import TKApi
from tkapi.vergadering import Vergadering, VergaderingSoort
from core.connection.neo4j_connection import Neo4jConnection
from core.checkpoint.checkpoint_decorator import checkpoint_loader
from utils.helpers import merge_node, merge_rel
from datetime import timezone


def setup_vergadering_api_filter(start_date_str: str):
    """Set up the Vergadering API filter for a date range"""
    # Parse the start date
    start_datetime = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    start_utc = start_datetime.replace(tzinfo=timezone.utc)
    
    # Create filter for vergaderingen from the start date
    filter_obj = Vergadering.create_filter()
    filter_obj.add_filter_str(f"Datum ge {start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    
    return filter_obj


@checkpoint_loader(checkpoint_interval=25)
def load_vergaderingen(conn: Neo4jConnection, batch_size: int = 50, start_date_str: str = "2024-01-01", 
                       skip_count: int = 0, _checkpoint_context=None):
    """
    Load Vergaderingen from TK API with automatic checkpoint support.
    
    This creates the foundation Vergadering nodes that the VLOS analysis loader
    can reference via canonical_api_vergadering_id.
    """
    print("🏛️ Loading Vergaderingen from TK API...")
    
    api = TKApi()
    filter_obj = setup_vergadering_api_filter(start_date_str)
    
    # Get vergaderingen from API
    vergaderingen_api = api.get_items(Vergadering, filter=filter_obj)
    print(f"→ Fetched {len(vergaderingen_api)} Vergaderingen since {start_date_str}")

    if not vergaderingen_api:
        print("No vergaderingen found for the date range.")
        return

    # Apply skip_count if specified
    if skip_count > 0:
        if skip_count >= len(vergaderingen_api):
            print(f"⚠️ Skip count ({skip_count}) is greater than or equal to total items ({len(vergaderingen_api)}). Nothing to process.")
            return
        vergaderingen_api = vergaderingen_api[skip_count:]
        print(f"⏭️ Skipping first {skip_count} items. Processing {len(vergaderingen_api)} remaining items.")

    def process_single_vergadering(vergadering_obj):
        """Process a single Vergadering object"""
        with conn.driver.session(database=conn.database) as session:
            if not vergadering_obj or not vergadering_obj.id:
                return

            # Create basic Vergadering node
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
            
            # Create minimal links to related entities (IDs only)
            # This provides the foundation for VLOS analysis to reference
            
            # Link to activiteiten (if expanded)
            if hasattr(vergadering_obj, 'activiteiten') and vergadering_obj.activiteiten:
                for activiteit in vergadering_obj.activiteiten:
                    # Create minimal Activiteit node
                    session.execute_write(merge_node, 'Activiteit', 'id', {'id': activiteit.id})
                    session.execute_write(merge_rel, 'Vergadering', 'id', vergadering_obj.id,
                                          'Activiteit', 'id', activiteit.id, 'HAS_ACTIVITEIT')
            
            # Link to zaken (if expanded)
            if hasattr(vergadering_obj, 'zaken') and vergadering_obj.zaken:
                for zaak in vergadering_obj.zaken:
                    # Create minimal Zaak node
                    session.execute_write(merge_node, 'Zaak', 'id', {'id': zaak.id})
                    session.execute_write(merge_rel, 'Vergadering', 'id', vergadering_obj.id,
                                          'Zaak', 'id', zaak.id, 'DISCUSSES_ZAAK')

    # Use the checkpoint context to process items automatically
    if _checkpoint_context:
        _checkpoint_context.process_items(vergaderingen_api, process_single_vergadering)
    else:
        # Fallback for when decorator is not used
        for vergadering_obj in vergaderingen_api:
            process_single_vergadering(vergadering_obj)

    print("✅ Loaded Vergaderingen successfully!")
    print(f"   These foundation nodes can now be referenced by VLOS analysis") 