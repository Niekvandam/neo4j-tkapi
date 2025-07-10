from tkapi import TKApi
from tkapi.persoon import Persoon
from core.connection.neo4j_connection import Neo4jConnection
# Helpers
from utils.helpers import merge_node, merge_rel

# Relationship map
from core.config.constants import REL_MAP_PERSOON, REL_MAP_PERSOON_NEVENFUNCTIE

from tkapi.fractie import FractieZetelPersoon
# Monkey-patch: some tkapi versions expect `FractieZetelPersoonOk`; alias it if absent
import tkapi.fractie as _tk_fractie
if not hasattr(_tk_fractie, 'FractieZetelPersoonOk'):
    _tk_fractie.FractieZetelPersoonOk = FractieZetelPersoon
from tkapi.persoon import (
    PersoonContactinformatie,
    PersoonGeschenk,
    PersoonLoopbaan,
    PersoonNevenfunctie,
    PersoonNevenfunctieInkomsten,
    PersoonOnderwijs,
    PersoonReis,
)

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry
import time
import datetime
import json
from pathlib import Path

api = TKApi()


class PersoonLoader(BaseLoader):
    """Loader for Persoon entities with full interface support"""
    
    def __init__(self):
        super().__init__(
            name="persoon_loader",
            description="Loads Personen from TK API"
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
            
            # Fetch all Personen (no artificial limit)
            load_personen(conn)
            
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


def load_personen(conn: Neo4jConnection, batch_size: int | None = None):
    """Load all Personen unless a positive batch_size is explicitly provided."""
    api = TKApi()

    if batch_size is not None and batch_size > 0:
        personen = api.get_items(Persoon, max_items=batch_size)
    else:
        # Fetch everything (no limit)
        personen = api.get_items(Persoon)
    print(f"→ Fetched {len(personen)} Personen")
    with conn.driver.session(database=conn.database) as session:
        # open debug file once per run (overwrite existing)
        debug_path = Path('debug_personen.jsonl')
        with debug_path.open('w', encoding='utf-8') as dbg:
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
                # Dump raw props for debugging
                dbg.write(json.dumps(props, ensure_ascii=False) + "\n")

                # Merge the Person node itself
                session.execute_write(merge_node, 'Persoon', 'id', props)

                # ---------------- Process relationships (e.g. FractieZetelPersoon) ----------------
                for attr_name, (target_label, rel_type, target_key_prop) in REL_MAP_PERSOON.items():
                    related_items = getattr(p, attr_name, []) or []

                    if not isinstance(related_items, (list, tuple)):
                        related_items = [related_items]

                    for rel_obj in related_items:
                        if not rel_obj:
                            continue
                        rel_key_val = getattr(rel_obj, target_key_prop, None)
                        if rel_key_val is None:
                            continue

                        # Specialised props per related label
                        if target_label == 'FractieZetelPersoon':
                            props_rel = {
                                'id': rel_key_val,
                                'functie': getattr(rel_obj, 'functie', None),
                                'van': _safe_date_str(rel_obj, 'van'),
                                'tot_en_met': _safe_date_str(rel_obj, 'tot_en_met'),
                            }
                        elif target_label == 'PersoonContactinformatie':
                            props_rel = {
                                'id': rel_key_val,
                                'soort': getattr(rel_obj, 'soort', None).name if getattr(rel_obj, 'soort', None) else None,
                                'waarde': getattr(rel_obj, 'waarde', None),
                            }
                        elif target_label == 'PersoonGeschenk':
                            props_rel = {
                                'id': rel_key_val,
                                'omschrijving': getattr(rel_obj, 'omschrijving', None),
                                'datum': str(getattr(rel_obj, 'datum', None)) if getattr(rel_obj, 'datum', None) else None,
                            }
                        elif target_label == 'PersoonLoopbaan':
                            props_rel = {
                                'id': rel_key_val,
                                'functie': getattr(rel_obj, 'functie', None),
                                'werkgever': getattr(rel_obj, 'werkgever', None),
                                'van': _safe_date_str(rel_obj, 'van'),
                                'tot_en_met': _safe_date_str(rel_obj, 'tot_en_met'),
                            }
                        elif target_label == 'PersoonOnderwijs':
                            props_rel = {
                                'id': rel_key_val,
                                'opleiding_nl': getattr(rel_obj, 'opleiding_nl', None),
                                'instelling': getattr(rel_obj, 'instelling', None),
                                'van': _safe_date_str(rel_obj, 'van'),
                                'tot_en_met': _safe_date_str(rel_obj, 'tot_en_met'),
                            }
                        elif target_label == 'PersoonReis':
                            props_rel = {
                                'id': rel_key_val,
                                'doel': getattr(rel_obj, 'doel', None),
                                'bestemming': getattr(rel_obj, 'bestemming', None),
                                'van': _safe_date_str(rel_obj, 'van'),
                                'tot_en_met': _safe_date_str(rel_obj, 'tot_en_met'),
                                'betaald_door': getattr(rel_obj, 'betaald_door', None),
                            }
                        elif target_label == 'PersoonNevenfunctie':
                            props_rel = {
                                'id': rel_key_val,
                                'omschrijving': getattr(rel_obj, 'omschrijving', None),
                                'van': _safe_date_str(rel_obj, 'van'),
                                'tot_en_met': _safe_date_str(rel_obj, 'tot_en_met'),
                                'soort': getattr(rel_obj, 'soort', None),
                                'toelichting': getattr(rel_obj, 'toelichting', None),
                            }
                        else:
                            props_rel = {target_key_prop: rel_key_val}

                        session.execute_write(merge_node, target_label, target_key_prop, props_rel)

                        # If PersoonNevenfunctie, process inkomsten nested
                        if target_label == 'PersoonNevenfunctie':
                            for inc_attr, (inc_label, inc_rel_type, inc_key_prop) in REL_MAP_PERSOON_NEVENFUNCTIE.items():
                                inc_items = getattr(rel_obj, inc_attr, []) or []
                                if not isinstance(inc_items, (list, tuple)):
                                    inc_items = [inc_items]
                                for inc in inc_items:
                                    if not inc:
                                        continue
                                    inc_key_val = getattr(inc, inc_key_prop, None)
                                    if inc_key_val is None:
                                        continue
                                    inc_props = {
                                        'id': inc_key_val,
                                        'omschrijving': getattr(inc, 'omschrijving', None),
                                        'datum': str(getattr(inc, 'datum', None)) if getattr(inc, 'datum', None) else None,
                                    }
                                    session.execute_write(merge_node, inc_label, inc_key_prop, inc_props)
                                    session.execute_write(
                                        merge_rel,
                                        target_label, target_key_prop, rel_key_val,
                                        inc_label, inc_key_prop, inc_key_val,
                                        inc_rel_type
                                    )
 
                        # Link Person -> related node
                        session.execute_write(
                            merge_rel,
                            'Persoon', 'id', p.id,
                            target_label, target_key_prop, rel_key_val,
                            rel_type
                        )
        print("✅ Loaded Personen.")


# Helper to safely fetch dates that might be YYYY or YYYY-MM

def _safe_date_str(obj, attr: str):
    """Return a string representation of the date attr but survive malformed TKApi dates (e.g. YYYY-MM)."""
    try:
        val = getattr(obj, attr, None)
    except ValueError:
        # Fall back to raw JSON value if available
        raw = getattr(obj, 'json', {}).get(attr.capitalize()) if hasattr(obj, 'json') else None
        return raw
    return str(val) if val else None
