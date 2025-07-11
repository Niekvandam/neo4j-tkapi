"""
VLOS XML matching logic extracted from vlos_processor.py
"""
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from neo4j.graph import Node as Neo4jNode
from thefuzz import fuzz
from tkapi.util import util as tkapi_util

# --- Configuration for VLOS Matching ---
LOCAL_TIMEZONE_OFFSET_HOURS_VLOS = 2

# Scoring weights for matching XML activities to API activities
SCORE_TIME_START_PROXIMITY_VLOS = 3.0
SCORE_TIME_OVERLAP_ONLY_VLOS = 1.5
SCORE_SOORT_EXACT_VLOS = 2.0
SCORE_SOORT_PARTIAL_XML_IN_API_VLOS = 1.5
SCORE_SOORT_PARTIAL_API_IN_XML_VLOS = 1.0
SCORE_ONDERWERP_EXACT_VLOS = 4.0
SCORE_ONDERWERP_FUZZY_HIGH_VLOS = 3.0
SCORE_ONDERWERP_FUZZY_MEDIUM_VLOS = 2.0
SCORE_TITEL_EXACT_VS_API_ONDERWERP_VLOS = 1.5
SCORE_TITEL_FUZZY_HIGH_VS_API_ONDERWERP_VLOS = 1.0
SCORE_TITEL_FUZZY_MEDIUM_VS_API_ONDERWERP_VLOS = 0.5

MIN_MATCH_SCORE_FOR_VLOS_ACTIVITEIT = 4.0
TIME_START_PROXIMITY_TOLERANCE_SECONDS_VLOS = 300
TIME_GENERAL_OVERLAP_BUFFER_SECONDS_VLOS = 600

FUZZY_SIMILARITY_THRESHOLD_HIGH_VLOS = 90
FUZZY_SIMILARITY_THRESHOLD_MEDIUM_VLOS = 75

# --- Additional configuration for Agendapunt matching ---
SCORE_ZAAK_LINK_AGP = 3.0  # Bonus if Agendapunt is linked to a Zaak mentioned in XML
MIN_MATCH_SCORE_FOR_VLOS_AGENDAPUNT = 4.0


# ------------------------------------------------------
# Helper to fetch candidate Agendapunt nodes for a vergadering
# ------------------------------------------------------


def get_vlos_utc_datetime(dt_obj: Optional[datetime]) -> Optional[datetime]:
    """Convert datetime to UTC for VLOS processing"""
    if not dt_obj:
        return None
    if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
        return (dt_obj - timedelta(hours=LOCAL_TIMEZONE_OFFSET_HOURS_VLOS)).replace(tzinfo=timezone.utc)
    return dt_obj.astimezone(timezone.utc)


def evaluate_vlos_time_match(xml_start: Optional[datetime], xml_end: Optional[datetime],
                            api_start: Optional[datetime], api_end: Optional[datetime]) -> tuple[float, str]:
    """Evaluate time matching between XML and API activities"""
    score = 0.0
    reason = "No significant time match"
    if not (xml_start and api_start and api_end):
        return score, reason

    xml_start_utc = get_vlos_utc_datetime(xml_start)
    api_start_utc = get_vlos_utc_datetime(api_start)
    api_end_utc = get_vlos_utc_datetime(api_end)

    xml_end_for_check = xml_end or (xml_start + timedelta(minutes=1))
    xml_end_utc = get_vlos_utc_datetime(xml_end_for_check)
    
    if not (xml_start_utc and api_start_utc and api_end_utc and xml_end_utc):
        return score, "Missing converted UTC time data"

    start_proximity_ok = abs((xml_start_utc - api_start_utc).total_seconds()) <= TIME_START_PROXIMITY_TOLERANCE_SECONDS_VLOS
    overlap_exists = (max(xml_start_utc, api_start_utc - timedelta(seconds=TIME_GENERAL_OVERLAP_BUFFER_SECONDS_VLOS)) <
                      min(xml_end_utc, api_end_utc + timedelta(seconds=TIME_GENERAL_OVERLAP_BUFFER_SECONDS_VLOS)))

    if start_proximity_ok:
        score = SCORE_TIME_START_PROXIMITY_VLOS
        reason = f"XML_start ({xml_start_utc.time()}) PROXIMATE to API_start ({api_start_utc.time()})"
        if overlap_exists:
            reason += " & timeframes overlap"
    elif overlap_exists:
        score = SCORE_TIME_OVERLAP_ONLY_VLOS
        reason = f"Timeframes OVERLAP (XML: {xml_start_utc.time()}-{xml_end_utc.time()}, API: {api_start_utc.time()}-{api_end_utc.time()})"
    
    return score, reason


def get_candidate_api_activities(session, canonical_vergadering_node: Neo4jNode) -> List[Dict[str, Any]]:
    """Fetch Activiteit nodes linked to the Vergadering from Neo4j"""
    query = """
    MATCH (verg:Vergadering {id: $vergadering_id})
    OPTIONAL MATCH (verg)-[:HAS_AGENDAPUNT]->(ap:Agendapunt)<-[:HAS_AGENDAPUNT]-(act_via_ap:Activiteit)
    OPTIONAL MATCH (verg)-[:HAS_ACTIVITEIT]->(act_direct:Activiteit)
    WITH verg,
         collect(DISTINCT act_via_ap)   +
         collect(DISTINCT act_direct)   AS activities_linked
    OPTIONAL MATCH (act_by_time:Activiteit)
    WHERE verg.begin IS NOT NULL AND verg.einde IS NOT NULL
      AND act_by_time.begin >= verg.begin 
      AND act_by_time.einde <= verg.einde 
    WITH activities_linked,
         [act IN collect(DISTINCT act_by_time) WHERE act IS NOT NULL] AS activities_by_time
    WITH [act IN activities_linked WHERE act IS NOT NULL] + activities_by_time AS all_acts_list
    UNWIND all_acts_list AS act_node
    RETURN DISTINCT act_node.id AS id, act_node.soort_api AS soort, act_node.onderwerp AS onderwerp,
           act_node.begin AS begin, act_node.einde AS einde
    """
    results = session.run(query, vergadering_id=canonical_vergadering_node['id'])
    candidates = []
    for record in results:
        candidates.append({
            "id": record["id"],
            "soort": record["soort"],
            "onderwerp": record["onderwerp"],
            "begin": record["begin"],
            "einde": record["einde"]
        })
    return candidates


def calculate_vlos_activity_match_score(xml_activity_data: Dict[str, Any], api_activity: Dict[str, Any]) -> tuple[float, List[str]]:
    """Calculate match score between XML activity and API activity"""
    score = 0.0
    reasons = []
    
    # Extract data from XML activity (handle None values)
    title = str(xml_activity_data.get('title', '') or '')
    soort = str(xml_activity_data.get('soort', '') or '')
    start_time = xml_activity_data.get('start_time')
    end_time = xml_activity_data.get('end_time')
    
    # Time matching – use begin/end from api_activity dict
    api_start = api_activity.get('begin')
    api_end = api_activity.get('einde')
    time_score, time_reason = evaluate_vlos_time_match(start_time, end_time, api_start, api_end)
    score += time_score
    if time_score > 0:
        reasons.append(time_reason)
    
    # Soort matching (handle None values) - soort_api is mapped to 'soort' in candidate dict
    api_soort = str(api_activity.get('soort', '') or '').lower()
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
    
    # Onderwerp/Title matching (handle None values)
    api_onderwerp = str(api_activity.get('onderwerp', '') or '').lower()
    xml_title = title.lower()
    
    if api_onderwerp and xml_title:
        if api_onderwerp == xml_title:
            score += SCORE_ONDERWERP_EXACT_VLOS
            reasons.append(f"Onderwerp EXACT match: '{title}'")
        else:
            fuzzy_score = fuzz.ratio(api_onderwerp, xml_title)
            if fuzzy_score >= FUZZY_SIMILARITY_THRESHOLD_HIGH_VLOS:
                score += SCORE_ONDERWERP_FUZZY_HIGH_VLOS
                reasons.append(f"Onderwerp HIGH fuzzy match: {fuzzy_score}% - '{xml_title}' ≈ '{api_onderwerp}'")
            elif fuzzy_score >= FUZZY_SIMILARITY_THRESHOLD_MEDIUM_VLOS:
                score += SCORE_ONDERWERP_FUZZY_MEDIUM_VLOS
                reasons.append(f"Onderwerp MEDIUM fuzzy match: {fuzzy_score}% - '{xml_title}' ≈ '{api_onderwerp}'")
    
    return score, reasons 


# ------------------------------------------------------
# Helper to fetch candidate Agendapunt nodes for a vergadering
# ------------------------------------------------------


def get_candidate_api_agendapunten(session, canonical_vergadering_node: Neo4jNode) -> List[Dict[str, Any]]:
    """Return Agendapunt dicts relevant to the vergadering (directly linked or via its activiteiten)."""
    query = """
    MATCH (verg:Vergadering {id: $verg_id})
    OPTIONAL MATCH (verg)-[:HAS_AGENDAPUNT]->(ap_direct:Agendapunt)
    OPTIONAL MATCH (verg)-[:HAS_ACTIVITEIT]->(:Activiteit)-[:HAS_AGENDAPUNT]->(ap_via_act:Agendapunt)
    WITH collect(ap_direct) + collect(ap_via_act) AS all_ap
    UNWIND all_ap AS ap
    WITH DISTINCT ap WHERE ap IS NOT NULL
    RETURN ap.id   AS id,
           ap.onderwerp AS onderwerp,
           ap.begin AS begin,
           ap.einde AS einde
    """
    res = session.run(query, verg_id=canonical_vergadering_node["id"])
    candidates = []
    for rec in res:
        candidates.append({
            "id": rec["id"],
            "onderwerp": rec["onderwerp"],
            "begin": rec["begin"],
            "einde": rec["einde"],
        })
    return candidates


def calculate_vlos_agendapunt_match_score(xml_activity: Dict[str, Any], api_agp: Dict[str, Any]) -> tuple[float, List[str]]:
    """Compute a similarity score between a VLOS activity (which acts as agenda item) and API Agendapunt."""
    score = 0.0
    reasons: List[str] = []

    # Time
    api_start = api_agp.get("begin")
    api_end = api_agp.get("einde")
    if api_start and api_end and xml_activity.get("start_time"):
        time_score, time_reason = evaluate_vlos_time_match(
            xml_activity.get("start_time"), xml_activity.get("end_time"), api_start, api_end
        )
        score += time_score
        if time_score > 0:
            reasons.append(time_reason)

    # Titel/Onderwerp similarity
    api_onderwerp = (api_agp.get("onderwerp") or "").lower()
    xml_title = (xml_activity.get("title") or "").lower()

    if api_onderwerp and xml_title:
        if api_onderwerp == xml_title:
            score += SCORE_ONDERWERP_EXACT_VLOS
            reasons.append("Onderwerp EXACT match")
        else:
            fuzz_val = fuzz.ratio(api_onderwerp, xml_title)
            if fuzz_val >= FUZZY_SIMILARITY_THRESHOLD_HIGH_VLOS:
                score += SCORE_ONDERWERP_FUZZY_HIGH_VLOS
                reasons.append(f"Onderwerp HIGH fuzzy match ({fuzz_val}%)")
            elif fuzz_val >= FUZZY_SIMILARITY_THRESHOLD_MEDIUM_VLOS:
                score += SCORE_ONDERWERP_FUZZY_MEDIUM_VLOS
                reasons.append(f"Onderwerp MEDIUM fuzzy match ({fuzz_val}%)")

    # Future: Zaak link bonus (xml_activity may include 'zaak_nummers' list)
    if xml_activity.get("zaak_nummers") and isinstance(xml_activity["zaak_nummers"], list):
        # We would need candidate Agendapunt's zaak nummers; skip for now
        pass

    return score, reasons 