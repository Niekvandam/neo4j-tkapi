"""
Persoon Processor - Handles processing of individual Persoon entities
"""
import json
from pathlib import Path
from tkapi.fractie import FractieZetelPersoon
from tkapi.persoon import (
    PersoonContactinformatie,
    PersoonGeschenk,
    PersoonLoopbaan,
    PersoonNevenfunctie,
    PersoonNevenfunctieInkomsten,
    PersoonOnderwijs,
    PersoonReis,
)
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel
from core.config.constants import REL_MAP_PERSOON, REL_MAP_PERSOON_NEVENFUNCTIE


def process_single_persoon(session, persoon_obj, debug_file=None):
    """
    Process a single Persoon entity and create all related nodes and relationships.
    
    Args:
        session: Neo4j session
        persoon_obj: Persoon object from TK API
        debug_file: Optional file handle for debugging output
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        p = persoon_obj
        
        # Build Persoon properties
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
        
        # Write debug info if debug file is provided
        if debug_file:
            debug_file.write(json.dumps(props, ensure_ascii=False) + "\n")

        # Merge the Person node itself
        session.execute_write(merge_node, 'Persoon', 'id', props)

        # Process relationships (e.g. FractieZetelPersoon)
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

                # Build specialized props per related label
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
        
        return True
        
    except Exception as e:
        print(f"    ❌ Failed to process Persoon {getattr(persoon_obj, 'id', 'UNKNOWN')}: {e}")
        return False


def process_single_persoon_threaded(persoon_obj, conn: Neo4jConnection, checkpoint_context=None):
    """
    Thread-safe processing function for a single Persoon entity.
    
    Args:
        persoon_obj: Persoon object from TK API
        conn: Neo4j connection
        checkpoint_context: Optional checkpoint context for progress tracking
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with conn.driver.session(database=conn.database) as session:
            success = process_single_persoon(session, persoon_obj)
            
            # Update checkpoint if provided
            if checkpoint_context:
                if success:
                    checkpoint_context.mark_processed(persoon_obj)
                else:
                    checkpoint_context.mark_failed(persoon_obj, "Processing failed")
            
            return success
            
    except Exception as e:
        print(f"    ❌ Unexpected error processing Persoon {getattr(persoon_obj, 'id', 'UNKNOWN')}: {e}")
        
        if checkpoint_context:
            checkpoint_context.mark_failed(persoon_obj, str(e))
        
        return False


def _safe_date_str(obj, attr: str):
    """Return a string representation of the date attr but survive malformed TKApi dates (e.g. YYYY-MM)."""
    try:
        val = getattr(obj, attr, None)
    except ValueError:
        # Fall back to raw JSON value if available
        raw = getattr(obj, 'json', {}).get(attr.capitalize()) if hasattr(obj, 'json') else None
        return raw
    return str(val) if val else None 