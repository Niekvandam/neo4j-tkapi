# This file is now mostly superseded by the process_and_load_dossier function
# in common_processors.py.
# Dossiers are primarily loaded when encountered via dated Documents or Zaken.

# import datetime
# from tkapi import TKApi
# from tkapi.dossier import Dossier
# from neo4j_connection import Neo4jConnection
# from helpers import merge_node, merge_rel
# from .common_processors import process_and_load_dossier, PROCESSED_DOSSIER_IDS


# def load_all_dossiers_independently(conn: Neo4jConnection, batch_size: int = 50):
#     """
#     Example of loading dossiers independently.
#     WARNING: This will fetch ALL dossiers, potentially very large.
#     Dossier has no direct date field for filtering.
#     """
#     api = TKApi()
#     # To fetch related items if processing independently:
#     # Dossier.expand_params should include Document, Zaak
#     # from tkapi.document import Document
#     # from tkapi.zaak import Zaak
#     # original_dossier_expand_params = list(Dossier.expand_params or [])
#     # current_expand_params = list(original_dossier_expand_params)
#     # if Document.type not in current_expand_params: current_expand_params.append(Document.type)
#     # if Zaak.type not in current_expand_params: current_expand_params.append(Zaak.type)
#     # Dossier.expand_params = current_expand_params
    
#     dossiers = api.get_items(Dossier, max_items=1000) # Example limit
#     print(f"→ Fetched {len(dossiers)} Dossiers (example independent load)")
    
#     # Dossier.expand_params = original_dossier_expand_params

#     with conn.driver.session(database=conn.database) as session:
#         PROCESSED_DOSSIER_IDS.clear() # Reset for this run
#         for i, d_obj in enumerate(dossiers, 1):
#             if i % 100 == 0 or i == len(dossiers):
#                 print(f"  → Processing Dossier {i}/{len(dossiers)} (independent): {d_obj.id}")
#             if process_and_load_dossier(session, d_obj):
#                 # If dossier has its own expanded documenten/zaken, process them here
#                 # from .common_processors import process_and_load_document, process_and_load_zaak
#                 # for doc_item in d_obj.documenten:
#                 #     if process_and_load_document(session, doc_item): # You'd need this function
#                 #         session.execute_write(merge_rel, 'Dossier', 'id', d_obj.id, 'Document', 'id', doc_item.id, 'HAS_DOCUMENT')
#                 # for zaak_item in d_obj.zaken:
#                 #     if process_and_load_zaak(session, zaak_item): # You'd need this function
#                 #         session.execute_write(merge_rel, 'Dossier', 'id', d_obj.id, 'Zaak', 'nummer', zaak_item.nummer, 'HAS_ZAAK')
#                 pass
    
#     print("✅ Loaded Dossiers (example independent load).")


print("Note: dossier_loader.py is mostly superseded by common_processors.process_and_load_dossier.")
print("Dossiers are now primarily loaded via related dated entities (Documents, Zaken).")