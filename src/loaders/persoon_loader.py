from tkapi import TKApi
from tkapi.persoon import Persoon
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry
import time

api = TKApi()


class PersoonLoader(BaseLoader):
    """Loader for Persoon entities with full interface support"""
    
    def __init__(self):
        super().__init__(
            name="persoon_loader",
            description="Loads Personen from TK API"
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
            
            # Use the existing function for actual loading
            load_personen(conn, config.batch_size)
            
            # For now, we'll mark as successful if no exceptions occurred
            result.success = True
            result.execution_time_seconds = time.time() - start_time
            
        except Exception as e:
            result.error_messages.append(f"Loading failed: {str(e)}")
            result.execution_time_seconds = time.time() - start_time
        
        return result


# Register the loader
persoon_loader_instance = PersoonLoader()
loader_registry.register(persoon_loader_instance)


def load_personen(conn: Neo4jConnection, batch_size: int = 50):
    api = TKApi()
    personen = api.get_items(Persoon, max_items=batch_size)
    print(f"→ Fetched {len(personen)} Personen")
    with conn.driver.session(database=conn.database) as session:
        for i, p in enumerate(personen, 1):
            if i % 100 == 0 or i == len(personen):
                print(f"  → Processing Persoon {i}/{len(personen)}")
            props = {
                'id': p.id,
                'achternaam': p.achternaam,
                'tussenvoegsel': p.tussenvoegsel,
                'initialen': p.initialen,
                'roepnaam': p.roepnaam,
                'voornamen': p.voornamen,
                'functie': p.functie,
                'geslacht': p.geslacht,
                'woonplaats': p.woonplaats,
                'land': p.land,
                'geboortedatum': str(p.geboortedatum) if p.geboortedatum else None,
                'geboorteland': p.geboorteland,
                'geboorteplaats': p.geboorteplaats,
                'overlijdensdatum': str(p.overlijdensdatum) if p.overlijdensdatum else None,
                'overlijdensplaats': p.overlijdensplaats,
                'titels': p.titels
            }
            session.execute_write(merge_node, 'Persoon', 'id', props)
    print("✅ Loaded Personen.")
