from tkapi import TKApi
from tkapi.fractie import Fractie
from core.connection.neo4j_connection import Neo4jConnection
# Helpers
from utils.helpers import merge_node, merge_rel

# Relationship maps
from core.config.constants import REL_MAP_FRACTIE, REL_MAP_FRACTIE_ZETEL, REL_MAP_FRACTIE_ZETEL_PERSOON

from tkapi.fractie import FractieZetel, FractieZetelPersoon
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
            LoaderCapability.BATCH_PROCESSING,
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

            # Merge the Fractie node itself
            session.execute_write(merge_node, 'Fractie', 'id', props)

            # ------------------------------------------------------------------
            # Process related entities (currently only FractieZetel and its person)
            # ------------------------------------------------------------------
            for attr_name, (target_label, rel_type, target_key_prop) in REL_MAP_FRACTIE.items():
                related_items = getattr(f, attr_name, []) or []

                # Ensure list semantics
                if not isinstance(related_items, (list, tuple)):
                    related_items = [related_items]

                for related_item in related_items:
                    if not related_item:
                        continue

                    related_key_val = getattr(related_item, target_key_prop, None)
                    if related_key_val is None:
                        print(f"    ! Warning: Related item for '{attr_name}' in Fractie {f.id} missing key '{target_key_prop}'.")
                        continue

                    # Merge FractieZetel node with minimal props
                    zetel_props = {
                        'id': related_key_val,
                        'gewicht': getattr(related_item, 'gewicht', None),
                    }
                    session.execute_write(merge_node, target_label, target_key_prop, zetel_props)

                    # Link Fractie → FractieZetel
                    session.execute_write(
                        merge_rel,
                        'Fractie', 'id', f.id,
                        target_label, target_key_prop, related_key_val,
                        rel_type
                    )

                    # ---------------- Process FractieZetelPersoon (incumbent) ----------------
                    for zetel_attr, (fzp_label, fzp_rel_type, fzp_key_prop) in REL_MAP_FRACTIE_ZETEL.items():
                        fzp_obj = getattr(related_item, zetel_attr, None)
                        if not fzp_obj:
                            continue
                        fzp_key_val = getattr(fzp_obj, fzp_key_prop, None)
                        if fzp_key_val is None:
                            continue

                        # Merge FractieZetelPersoon node with timing information
                        fzp_props = {
                            'id': fzp_key_val,
                            'functie': getattr(fzp_obj, 'functie', None),
                            'van': str(getattr(fzp_obj, 'van', None)) if getattr(fzp_obj, 'van', None) else None,
                            'tot_en_met': str(getattr(fzp_obj, 'tot_en_met', None)) if getattr(fzp_obj, 'tot_en_met', None) else None,
                        }
                        session.execute_write(merge_node, fzp_label, fzp_key_prop, fzp_props)

                        # Link FractieZetel -> FractieZetelPersoon
                        session.execute_write(
                            merge_rel,
                            target_label, target_key_prop, related_key_val,
                            fzp_label, fzp_key_prop, fzp_key_val,
                            fzp_rel_type
                        )

                        # --------- Link FractieZetelPersoon to underlying Person ---------
                        for fzp_attr, (p_label, p_rel_type, p_key_prop) in REL_MAP_FRACTIE_ZETEL_PERSOON.items():
                            person_obj = getattr(fzp_obj, fzp_attr, None)
                            if not person_obj:
                                continue
                            person_key_val = getattr(person_obj, p_key_prop, None)
                            if person_key_val is None:
                                continue

                            # Merge Person node (basic props, full data comes from persoon_loader)
                            session.execute_write(merge_node, p_label, p_key_prop, {
                                p_key_prop: person_key_val,
                                'naam': getattr(person_obj, 'naam', None),
                            })

                            session.execute_write(
                                merge_rel,
                                fzp_label, fzp_key_prop, fzp_key_val,
                                p_label, p_key_prop, person_key_val,
                                p_rel_type
                            )

    print("✅ Loaded Fracties.")
