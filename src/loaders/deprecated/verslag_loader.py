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

import time
from core.connection.neo4j_connection import Neo4jConnection

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry


class VerslagLoader(BaseLoader):
    """Placeholder loader for Verslag entities - superseded by common_processors"""
    
    def __init__(self):
        super().__init__(
            name="verslag_loader",
            description="Placeholder for Verslagen - now processed via common_processors from Vergaderingen"
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
            error_messages=["VerslagLoader is superseded - use common_processors.process_and_load_verslag via Vergaderingen"],
            warnings=["This loader is deprecated and non-functional"]
        )
        
        result.execution_time_seconds = time.time() - start_time
        return result


# Register the loader
verslag_loader_instance = VerslagLoader()
loader_registry.register(verslag_loader_instance)

print("Note: verslag_loader.py is mostly superseded by common_processors.process_and_load_verslag.")
print("Verslagen are now primarily loaded via related dated entities (Vergaderingen).")