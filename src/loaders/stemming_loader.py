# This file is now mostly superseded by the process_and_load_stemming function
# in common_processors.py.
# Stemmingen are primarily loaded when encountered via dated Besluiten.

# import datetime
# from tkapi import TKApi
# from tkapi.stemming import Stemming
# from neo4j_connection import Neo4jConnection
# from helpers import merge_node, merge_rel
# from .common_processors import process_and_load_stemming, PROCESSED_STEMMING_IDS

# def load_all_stemmingen_independently(conn: Neo4jConnection, batch_size: int = 50):
#     """
#     Example of loading stemmingen independently.
#     WARNING: This will fetch ALL stemmingen, potentially very large.
#     Stemming has no direct date field.
#     """
#     api = TKApi()
#     # Stemming.expand_params is ['Persoon', 'Fractie', 'Besluit'] by default

#     # filter = Stemming.create_filter() # No date filter for Stemming
#     stemmingen = api.get_items(Stemming, max_items=1000) # Example limit
#     print(f"→ Fetched {len(stemmingen)} Stemmingen (example independent load)")
    
#     with conn.driver.session(database=conn.database) as session:
#         PROCESSED_STEMMING_IDS.clear() # Reset for this run
#         for i, s_obj in enumerate(stemmingen, 1):
#             if i % 100 == 0 or i == len(stemmingen):
#                 print(f"  → Processing Stemming {i}/{len(stemmingen)} (independent): {s_obj.id}")
            
#             # The besluit_id parameter for process_and_load_stemming is crucial
#             # if the Stemming object itself has its Besluit expanded.
#             besluit_id_for_processor = s_obj.besluit.id if s_obj.besluit else None
            
#             if process_and_load_stemming(session, s_obj, besluit_id=besluit_id_for_processor):
#                 # If s_obj.besluit is expanded, and it needs processing:
#                 if s_obj.besluit:
#                     from .common_processors import process_and_load_besluit # Careful with circular
#                     if process_and_load_besluit(session, s_obj.besluit, related_stemming_id_is_not_a_param=True): # Made up param
#                         pass
#                     session.execute_write(merge_rel, 'Stemming', 'id', s_obj.id,
#                                           'Besluit', 'id', s_obj.besluit.id, 'PART_OF_BESLUIT')
#     print("✅ Loaded Stemmingen (example independent load).")

import time
from core.connection.neo4j_connection import Neo4jConnection

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry


class StemmingLoader(BaseLoader):
    """Placeholder loader for Stemming entities - superseded by common_processors"""
    
    def __init__(self):
        super().__init__(
            name="stemming_loader",
            description="Placeholder for Stemmingen - now processed via common_processors from Besluiten"
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
            error_messages=["StemmingLoader is superseded - use common_processors.process_and_load_stemming via Besluiten"],
            warnings=["This loader is deprecated and non-functional"]
        )
        
        result.execution_time_seconds = time.time() - start_time
        return result


# Register the loader
stemming_loader_instance = StemmingLoader()
loader_registry.register(stemming_loader_instance)

print("Note: stemming_loader.py is mostly superseded by common_processors.process_and_load_stemming.")
print("Stemmingen are now primarily loaded via related dated entities (Besluiten from Agendapunten/Zaken).")