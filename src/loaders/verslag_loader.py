# This file is now mostly superseded by the process_and_load_verslag function
# in common_processors.py.
# Verslagen are primarily loaded when encountered via dated Vergaderingen.

# import datetime
# from tkapi import TKApi
# from tkapi.verslag import Verslag
# from neo4j_connection import Neo4jConnection
# from helpers import merge_node, merge_rel
# from .vlos_verslag_loader import load_vlos_verslag
# import requests
# import xml.etree.ElementTree as ET
# import concurrent.futures
# from .common_processors import process_and_load_verslag, PROCESSED_VERSLAG_IDS, download_verslag_xml


# def load_all_verslagen_independently(conn: Neo4jConnection, batch_size: int = 50):
#     """
#     Example of loading verslagen independently.
#     WARNING: This will fetch ALL verslagen. Verslag has no direct date field for filtering.
#     """
#     api = TKApi()
#     # Verslag.expand_params is ['Vergadering'] by default

#     verslagen = api.get_items(Verslag, max_items=100) # Example limit, remove for full independent load
#     print(f"→ Fetched metadata for {len(verslagen)} Verslagen (example independent load).")

#     # Using ThreadPoolExecutor for XML download and processing if done independently
#     # If called from vergadering_loader, XML processing happens there.

#     def process_single_independent_verslag(v_obj):
#         with conn.driver.session(database=conn.database) as session:
#             # The related_vergadering_id parameter for process_and_load_verslag is crucial
#             # if the Verslag object itself has its Vergadering expanded.
#             related_vergadering_id_for_processor = v_obj.vergadering.id if v_obj.vergadering else None
            
#             if process_and_load_verslag(session, conn.driver, v_obj, related_vergadering_id=related_vergadering_id_for_processor):
#                 # If v_obj.vergadering is expanded, and it needs processing:
#                 if v_obj.vergadering:
#                     from .vergadering_loader import process_and_load_vergadering # Careful with circular
#                     if process_and_load_vergadering(session, conn.driver, v_obj.vergadering):
#                         pass
#                     # Link from Verslag to Vergadering (common_processor might also do this if vergadering is expanded on verslag)
#                     session.execute_write(merge_rel, 'Verslag', 'id', v_obj.id,
#                                           'Vergadering', 'id', v_obj.vergadering.id, 'RECORDED_IN')

#     with conn.driver.session(database=conn.database) as s: # Outer session for clearing set
#         PROCESSED_VERSLAG_IDS.clear()

#     # Use a ThreadPoolExecutor for independent processing to speed up XML downloads
#     with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor: # Adjust max_workers
#         futures = [executor.submit(process_single_independent_verslag, v) for v in verslagen]
#         for i, future in enumerate(concurrent.futures.as_completed(futures)):
#             if (i+1) % 10 == 0 or (i+1) == len(verslagen):
#                 print(f"  → Completed processing independent Verslag {i+1}/{len(verslagen)}")
#             try:
#                 future.result()
#             except Exception as e:
#                 print(f"  ✕ Error processing independent Verslag: {e}")

#     print("✅ Loaded Verslagen and their detailed XML content (example independent load).")


print("Note: verslag_loader.py is mostly superseded by common_processors.process_and_load_verslag.")
print("Verslagen are now primarily loaded via related dated entities (Vergaderingen).")