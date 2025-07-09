from tkapi import TKApi
from tkapi.fractie import Fractie
from core.connection.neo4j_connection import Neo4jConnection
# Helpers
from utils.helpers import merge_node, merge_rel

# Relationship maps
from core.config.constants import REL_MAP_FRACTIE, REL_MAP_FRACTIE_ZETEL

from tkapi.fractie import FractieZetel
from tkapi.persoon import Persoon

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry
import time

api = TKApi()


class FractieLoader(BaseLoader):
    """Loader for Fractie entities with full interface support"""
    
    def __init__(self):
        super().__init__(
            name="fractie_loader",
            description="Loads Fracties from TK API"
        )
        self._capabilities = [
            LoaderCapability.BATCH_PROCESSING
        ]
    
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
            error_messages=[],
            warnings=[]
        )
        
        try:
            # Validate configuration
            validation_errors = self.validate_config(config)
            if validation_errors:
                result.error_messages.extend(validation_errors)
                return result
            
            # Fetch all Fracties (no artificial limit)
            load_fracties(conn)
            
            # For now, we'll mark as successful if no exceptions occurred
            result.success = True
            result.execution_time_seconds = time.time() - start_time
            
        except Exception as e:
            result.error_messages.append(f"Loading failed: {str(e)}")
            result.execution_time_seconds = time.time() - start_time
        
        return result


# Register the loader
fractie_loader_instance = FractieLoader()
loader_registry.register(fractie_loader_instance)


def load_fracties(conn: Neo4jConnection, batch_size: int | None = None):
    """Load all Fracties unless a positive batch_size is explicitly provided."""
    api = TKApi()

    if batch_size is not None and batch_size > 0:
        fracties = api.get_items(Fractie, max_items=batch_size)
    else:
        fracties = api.get_items(Fractie)
    print(f"→ Fetched {len(fracties)} Fracties")

    with conn.driver.session(database=conn.database) as session:
        for i, f in enumerate(fracties, 1):
            if i % 100 == 0 or i == len(fracties):
                print(f"  → Processing Fractie {i}/{len(fracties)}")
            print(f.id)
            print(f.naam)
            print(f.afkorting)
            print(f.zetels_aantal)
            print(f.datum_actief)
            print(f.datum_inactief)
            print(f.organisatie)
            props = {
                'id': f.id,
                'naam': f.naam,
                'afkorting': f.afkorting,
                'zetels_aantal': f.zetels_aantal,
                'datum_actief': str(f.datum_actief) if f.datum_actief else None,
                'datum_inactief': str(f.datum_inactief) if f.datum_inactief else None,
                'organisatie': f.organisatie
            }

            session.execute_write(merge_node, 'Fractie', 'id', props)

    print("✅ Loaded Fracties.")
