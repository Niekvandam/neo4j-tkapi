import datetime
from tkapi import TKApi
from tkapi.toezegging import Toezegging
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel
from core.config.constants import REL_MAP_TOEZEGGING
import time

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry

api = TKApi()


class ToezeggingenLoader(BaseLoader):
    """Loader for Toezegging entities with full interface support"""
    
    def __init__(self):
        super().__init__(
            name="toezegging_loader",
            description="Loads Toezeggingen from TK API with related entities"
        )
        self._capabilities = [
            LoaderCapability.BATCH_PROCESSING,
            LoaderCapability.DATE_FILTERING,
            LoaderCapability.RELATIONSHIP_PROCESSING
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
            load_toezeggingen(conn)
            
            # For now, we'll mark as successful if no exceptions occurred
            result.success = True
            result.execution_time_seconds = time.time() - start_time
            
        except Exception as e:
            result.error_messages.append(f"Loading failed: {str(e)}")
            result.execution_time_seconds = time.time() - start_time
        
        return result


# Register the loader
toezegging_loader_instance = ToezeggingenLoader()
loader_registry.register(toezegging_loader_instance)


def load_toezeggingen(conn: Neo4jConnection):
    api = TKApi()
    Toezegging.expand_params = ['Activiteit', 'ToegezegdAanPersoon', 'ToegezegdAanFractie']
    filter = Toezegging.create_filter()
    
    filter.add_filter_str("AanmaakDatum ge 2024-01-01")

    toezeggingen = api.get_items(Toezegging, filter=filter)
    print(f"→ Fetched {len(toezeggingen)} Toezeggingen")

    with conn.driver.session(database=conn.database) as session:
        for idx, t in enumerate(toezeggingen, 1):
            props = {
                'id': t.id,
                'nummer': t.nummer,
                'tekst': t.tekst,
                'status': t.status.name if t.status else None,
                'functie': t.functie,
                'ministerie': t.ministerie,
                'naam': t.naam_bewindspersoon,
            }
            session.execute_write(merge_node, 'Toezegging', 'id', props)
            if t.activiteit:
                session.execute_write(merge_node, 'Activiteit', 'id', {'id': t.activiteit.id})
                session.execute_write(merge_rel, 'Toezegging', 'id', t.id, 'Activiteit', 'id', t.activiteit.id, 'MADE_DURING')
            for persoon_dict in t.toegezegd_aan_persoon:
                if "Id" in persoon_dict:
                    persoon_id = persoon_dict["Id"]
                session.execute_write(merge_node, "Persoon", "id", {"id": persoon_id})
                session.execute_write(
                    merge_rel,
                    "Toezegging", "id", t.id,
                    "Persoon", "id", persoon_id,
                    "ADDRESSED_TO"
                )


            for fractie_dict in t.toegezegd_aan_fractie:
                if "Id" in fractie_dict:
                    fractie_id = fractie_dict["Id"]
                session.execute_write(merge_node, "Fractie", "id", {"id": fractie_id})
                session.execute_write(
                    merge_rel,
                    "Toezegging", "id", t.id,
                    "Fractie", "id", fractie_id,
                    "ADDRESSED_TO"
                )
                
            # In load_toezeggingen, replace manual if-blocks with:
            for attr, (label, rel, key) in REL_MAP_TOEZEGGING.items():
                targets = getattr(t, attr, None)
                if not targets:
                    continue
                if not isinstance(targets, list):
                    targets = [targets]
                for target in targets:
                    if not target or key not in target:
                        continue
                    target_val = target[key]
                    session.execute_write(merge_node, label, key.lower(), {key.lower(): target_val})
                    session.execute_write(merge_rel, 'Toezegging', 'id', t.id, label, key.lower(), target_val, rel)
            if idx % 100 == 0 or idx == len(toezeggingen):
                print(f"→ Processed Toezegging {idx}/{len(toezeggingen)}")

    print("✅ Loaded Toezeggingen.")