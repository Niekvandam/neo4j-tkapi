"""
VLOS XML processing logic extracted from vlos_verslag_loader.py
"""
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from neo4j.graph import Node as Neo4jNode
from thefuzz import fuzz
from tkapi.util import util as tkapi_util
from utils.helpers import merge_node, merge_rel

# Import matching utilities
from .vlos_matching import (
    evaluate_vlos_time_match, 
    get_candidate_api_activities,
    calculate_vlos_activity_match_score,
    MIN_MATCH_SCORE_FOR_VLOS_ACTIVITEIT,
    FUZZY_SIMILARITY_THRESHOLD_HIGH_VLOS,
    FUZZY_SIMILARITY_THRESHOLD_MEDIUM_VLOS,
    SCORE_ONDERWERP_EXACT_VLOS,
    SCORE_ONDERWERP_FUZZY_HIGH_VLOS,
    SCORE_ONDERWERP_FUZZY_MEDIUM_VLOS,
    SCORE_TITEL_EXACT_VS_API_ONDERWERP_VLOS,
    SCORE_TITEL_FUZZY_HIGH_VS_API_ONDERWERP_VLOS,
    SCORE_TITEL_FUZZY_MEDIUM_VS_API_ONDERWERP_VLOS
)

# Namespace for the vlosCoreDocument XML
NS_VLOS = {'vlos': 'http://www.tweedekamer.nl/ggm/vergaderverslag/v1.0'}


def parse_vlos_xml_datetime(datetime_val: Optional[str]) -> Optional[datetime]:
    """Parse VLOS XML datetime strings into Python datetime objects"""
    if not datetime_val or not isinstance(datetime_val, str):
        return None
    
    datetime_str = datetime_val.strip()
    try:
        if datetime_str.endswith('Z'):
            return datetime.fromisoformat(datetime_str[:-1] + '+00:00')
        if len(datetime_str) >= 24 and (datetime_str[19] == '+' or datetime_str[19] == '-') and datetime_str[22] == ':':
            return datetime.fromisoformat(datetime_str)
        if len(datetime_str) >= 23 and (datetime_str[19] == '+' or datetime_str[19] == '-') and datetime_str[22] != ':':
            dt_str_fixed = datetime_str[:22] + ":" + datetime_str[22:]
            return datetime.fromisoformat(dt_str_fixed)
        return datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        try:
            return tkapi_util.odatedatetime_to_datetime(datetime_str)
        except Exception:
            return None


# Matching functions moved to vlos_matching.py


def process_vlos_activity_element(session, element: ET.Element, canonical_vergadering_node: Neo4jNode,
                                 parent_vlos_node_id: Optional[str], api_activities_for_vergadering: List[Dict[str, Any]]):
    """Process a single VLOS activity element and create Neo4j nodes"""
    # Extract activity data from XML
    activity_id = element.get('id', f"vlos_activity_{hash(ET.tostring(element))}")
    title = element.get('title', 'Unnamed Activity')
    soort = element.get('soort', 'Unknown')
    start_time_str = element.get('start')
    end_time_str = element.get('end')
    
    # Parse times
    start_time = parse_vlos_xml_datetime(start_time_str)
    end_time = parse_vlos_xml_datetime(end_time_str)
    
    # Create VLOS activity node
    vlos_props = {
        'id': activity_id,
        'title': title,
        'soort': soort,
        'start_time': str(start_time) if start_time else None,
        'end_time': str(end_time) if end_time else None,
        'source': 'vlos_xml'
    }
    session.execute_write(merge_node, 'VlosActivity', 'id', vlos_props)
    
    # Link to parent if exists
    if parent_vlos_node_id:
        session.execute_write(merge_rel, 'VlosActivity', 'id', parent_vlos_node_id,
                              'VlosActivity', 'id', activity_id, 'CONTAINS')
    
    # Link to Vergadering
    session.execute_write(merge_rel, 'Vergadering', 'id', canonical_vergadering_node['id'],
                          'VlosActivity', 'id', activity_id, 'HAS_VLOS_ACTIVITY')
    
    # Try to match with API activities
    best_match = None
    best_score = 0.0
    
    for api_activity in api_activities_for_vergadering:
        score = 0.0
        reasons = []
        
        # Time matching
        api_start = parse_vlos_xml_datetime(api_activity.get('begin')) if api_activity.get('begin') else None
        api_end = parse_vlos_xml_datetime(api_activity.get('einde')) if api_activity.get('einde') else None
        time_score, time_reason = evaluate_vlos_time_match(start_time, end_time, api_start, api_end)
        score += time_score
        if time_score > 0:
            reasons.append(time_reason)
        
        # Soort matching
        api_soort = api_activity.get('soort', '').lower()
        xml_soort = soort.lower()
        if api_soort == xml_soort:
            score += SCORE_SOORT_EXACT_VLOS
            reasons.append(f"Soort EXACT match: '{soort}'")
        elif xml_soort in api_soort:
            score += SCORE_SOORT_PARTIAL_XML_IN_API_VLOS
            reasons.append(f"Soort PARTIAL match (XML in API): '{xml_soort}' in '{api_soort}'")
        elif api_soort in xml_soort:
            score += SCORE_SOORT_PARTIAL_API_IN_XML_VLOS
            reasons.append(f"Soort PARTIAL match (API in XML): '{api_soort}' in '{xml_soort}'")
        
        # Onderwerp/Title matching
        api_onderwerp = api_activity.get('onderwerp', '').lower()
        xml_title = title.lower()
        
        if api_onderwerp and xml_title:
            if api_onderwerp == xml_title:
                score += SCORE_ONDERWERP_EXACT_VLOS
                reasons.append("Onderwerp/Title EXACT match")
            else:
                fuzzy_score = fuzz.ratio(api_onderwerp, xml_title)
                if fuzzy_score >= FUZZY_SIMILARITY_THRESHOLD_HIGH_VLOS:
                    score += SCORE_ONDERWERP_FUZZY_HIGH_VLOS
                    reasons.append(f"Onderwerp/Title HIGH fuzzy match ({fuzzy_score}%)")
                elif fuzzy_score >= FUZZY_SIMILARITY_THRESHOLD_MEDIUM_VLOS:
                    score += SCORE_ONDERWERP_FUZZY_MEDIUM_VLOS
                    reasons.append(f"Onderwerp/Title MEDIUM fuzzy match ({fuzzy_score}%)")
        
        if score > best_score:
            best_score = score
            best_match = api_activity
    
    # Create relationship if good match found
    if best_match and best_score >= MIN_MATCH_SCORE_FOR_VLOS_ACTIVITEIT:
        session.execute_write(merge_rel, 'VlosActivity', 'id', activity_id,
                              'Activiteit', 'id', best_match['id'], 'MATCHES_API_ACTIVITY')
        print(f"    🔗 Matched VLOS activity '{title}' to API activity {best_match['id']} (score: {best_score:.1f})")
    
    return activity_id


def process_vlos_speakers(session, element: ET.Element, vlos_parent_section_id: str, vlos_parent_label: str):
    """Process speakers from VLOS XML element"""
    for speaker_elem in element.findall('.//vlos:speaker', NS_VLOS):
        speaker_id = speaker_elem.get('id', f"vlos_speaker_{hash(ET.tostring(speaker_elem))}")
        speaker_name = speaker_elem.get('name', 'Unknown Speaker')
        speaker_role = speaker_elem.get('role', '')
        
        # Create speaker node
        speaker_props = {
            'id': speaker_id,
            'name': speaker_name,
            'role': speaker_role,
            'source': 'vlos_xml'
        }
        session.execute_write(merge_node, 'VlosSpeaker', 'id', speaker_props)
        
        # Link to parent section
        session.execute_write(merge_rel, vlos_parent_label, 'id', vlos_parent_section_id,
                              'VlosSpeaker', 'id', speaker_id, 'HAS_SPEAKER')


def process_vlos_zaken(session, element: ET.Element, vlos_parent_section_id: str, vlos_parent_label: str):
    """Process zaken from VLOS XML element"""
    for zaak_elem in element.findall('.//vlos:zaak', NS_VLOS):
        zaak_nummer = zaak_elem.get('nummer')
        if not zaak_nummer:
            continue
            
        zaak_titel = zaak_elem.get('titel', '')
        zaak_type = zaak_elem.get('type', '')
        
        # Create or update Zaak node (merge with existing if present)
        zaak_props = {
            'nummer': zaak_nummer,
            'titel': zaak_titel,
            'type': zaak_type,
            'source': 'vlos_xml'
        }
        session.execute_write(merge_node, 'Zaak', 'nummer', zaak_props)
        
        # Link to parent section
        session.execute_write(merge_rel, vlos_parent_label, 'id', vlos_parent_section_id,
                              'Zaak', 'nummer', zaak_nummer, 'DISCUSSES_ZAAK') 