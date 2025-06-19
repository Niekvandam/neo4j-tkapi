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

print("Note: stemming_loader.py is mostly superseded by common_processors.process_and_load_stemming.")
print("Stemmingen are now primarily loaded via related dated entities (Besluiten from Agendapunten/Zaken).")