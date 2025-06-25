# This file is now mostly superseded by the process_and_load_dossier function
# in common_processors.py.
# Dossiers are primarily loaded when encountered via dated Documents or Zaken.

# import datetime
# from tkapi import TKApi
# from tkapi.dossier import Dossier
# from neo4j_connection import Neo4jConnection
# from helpers import merge_node, merge_rel
# from .common_processors import process_and_load_dossier, PROCESSED_DOSSIER_IDS

import time
from core.connection.neo4j_connection import Neo4jConnection

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry


class DossierLoader(BaseLoader):
    """Placeholder loader for Dossier entities - superseded by common_processors"""
    
    def __init__(self):
        super().__init__(
            name="dossier_loader",
            description="Placeholder for Dossiers - now processed via common_processors from Documents/Zaken"
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
            error_messages=["DossierLoader is superseded - use common_processors.process_and_load_dossier via Documents/Zaken"],
            warnings=["This loader is deprecated and non-functional"]
        )
        
        result.execution_time_seconds = time.time() - start_time
        return result


# Register the loader
dossier_loader_instance = DossierLoader()
loader_registry.register(dossier_loader_instance)

print("Note: dossier_loader.py is mostly superseded by common_processors.process_and_load_dossier.")
print("Dossiers are now primarily loaded via related dated entities (Documents, Zaken).")