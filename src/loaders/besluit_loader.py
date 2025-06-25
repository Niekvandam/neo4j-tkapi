# This file is now mostly superseded by the process_and_load_besluit function
# in common_processors.py.
# You can keep this file empty or remove it if no top-level load_besluiten is needed.
# For completeness, if you ever needed to load ALL besluiten (not recommended without date filter):

# import datetime
# from tkapi import TKApi
# from tkapi.besluit import Besluit
# from neo4j_connection import Neo4jConnection
# from helpers import merge_node, merge_rel
# from constants import REL_MAP_BESLUIT
# from .common_processors import process_and_load_besluit, PROCESSED_BESLUIT_IDS

# def load_all_besluiten_independently(conn: Neo4jConnection, batch_size: int = 50):
#     """
#     Example of loading besluiten independently.
#     WARNING: This will fetch ALL besluiten, potentially very large.
#     Use with caution and preferably with a date filter if Besluit had one.
#     """
#     api = TKApi()
#     # Besluit itself does not have a direct date field for filtering.
#     # Fetching all is generally not what you want for the current strategy.
#     # This function is here as an example if ever needed.

#     # To fetch related items if processing independently:
#     # Besluit.expand_params should include Stemming, Agendapunt, Zaak
#     # from tkapi.stemming import Stemming
#     # from tkapi.agendapunt import Agendapunt
#     # from tkapi.zaak import Zaak
#     # original_besluit_expand_params = list(Besluit.expand_params or [])
#     # current_expand_params = list(original_besluit_expand_params)
#     # if Stemming.type not in current_expand_params: current_expand_params.append(Stemming.type)
#     # if Agendapunt.type not in current_expand_params: current_expand_params.append(Agendapunt.type)
#     # if Zaak.type not in current_expand_params: current_expand_params.append(Zaak.type)
#     # Besluit.expand_params = current_expand_params
    
#     # This filter is problematic as Besluit has no primary date.
#     # filter = Besluit.create_filter()
#     # filter.add_filter_str("...") # Some other filter if available
    
#     besluiten = api.get_items(Besluit, max_items=1000) # Example limit
#     print(f"→ Fetched {len(besluiten)} Besluiten (example independent load)")

#     # Besluit.expand_params = original_besluit_expand_params


#     with conn.driver.session(database=conn.database) as session:
#         PROCESSED_BESLUIT_IDS.clear() # Reset for this run
#         for i, b_obj in enumerate(besluiten, 1):
#             if i % 100 == 0 or i == len(besluiten):
#                 print(f"  → Processing Besluit {i}/{len(besluiten)} (independent): {b_obj.id}")
#             process_and_load_besluit(session, b_obj) # Calls the common processor
    
#     print("✅ Loaded Besluiten (example independent load).")

import time
from core.connection.neo4j_connection import Neo4jConnection

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry


class BesluitLoader(BaseLoader):
    """Placeholder loader for Besluit entities - superseded by common_processors"""
    
    def __init__(self):
        super().__init__(
            name="besluit_loader",
            description="Placeholder for Besluiten - now processed via common_processors from Agendapunten/Zaken"
        )
        self._capabilities = []  # No capabilities as this is superseded
    
    def load(self, conn: Neo4jConnection, config: LoaderConfig, 
             checkpoint_manager=None) -> LoaderResult:
        """Main loading method implementing the interface"""
        start_time = time.time()
        result = LoaderResult(
            success=False,
            processed_count=0,
            failed_count=0,
            skipped_count=0,
            total_items=0,
            execution_time_seconds=0.0,
            error_messages=["BesluitLoader is superseded - use common_processors.process_and_load_besluit via Agendapunten/Zaken"],
            warnings=["This loader is deprecated and non-functional"]
        )
        
        result.execution_time_seconds = time.time() - start_time
        return result


# Register the loader
besluit_loader_instance = BesluitLoader()
loader_registry.register(besluit_loader_instance)

print("Note: besluit_loader.py is mostly superseded by common_processors.process_and_load_besluit.")
print("Besluiten are now primarily loaded via related dated entities (Agendapunten, Zaken).")