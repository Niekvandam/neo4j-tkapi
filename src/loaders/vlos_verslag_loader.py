import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from neo4j.graph import Node as Neo4jNode # For type hinting

from tkapi import TKApi # For fetching API activities if needed directly here (optional)
from tkapi.activiteit import Activiteit as TKApiActiviteit, ActiviteitFilter # For type hinting and filtering
from tkapi.util import util as tkapi_util

from helpers import merge_node, merge_rel
# No Neo4jConnection needed here directly if driver is passed

# For fuzzy matching
from thefuzz import fuzz

# --- Configuration for VLOS Matching ---
# (These could also be moved to constants.py if preferred)
LOCAL_TIMEZONE_OFFSET_HOURS_VLOS = 2 # For parsing XML datetimes if they are naive local

# Scoring weights for matching XML activities to API activities
SCORE_TIME_START_PROXIMITY_VLOS = 3.0
SCORE_TIME_OVERLAP_ONLY_VLOS = 1.5
SCORE_SOORT_EXACT_VLOS = 2.0
SCORE_SOORT_PARTIAL_XML_IN_API_VLOS = 1.5
SCORE_SOORT_PARTIAL_API_IN_XML_VLOS = 1.0
SCORE_ONDERWERP_EXACT_VLOS = 2.5
SCORE_ONDERWERP_FUZZY_HIGH_VLOS = 2.0
SCORE_ONDERWERP_FUZZY_MEDIUM_VLOS = 1.0
SCORE_TITEL_EXACT_VS_API_ONDERWERP_VLOS = 1.5
SCORE_TITEL_FUZZY_HIGH_VS_API_ONDERWERP_VLOS = 1.0
SCORE_TITEL_FUZZY_MEDIUM_VS_API_ONDERWERP_VLOS = 0.5

MIN_MATCH_SCORE_FOR_VLOS_ACTIVITEIT = 4.0
TIME_START_PROXIMITY_TOLERANCE_SECONDS_VLOS = 300 # +/- 5 minutes
TIME_GENERAL_OVERLAP_BUFFER_SECONDS_VLOS = 600 # +/- 10 minutes

FUZZY_SIMILARITY_THRESHOLD_HIGH_VLOS = 90
FUZZY_SIMILARITY_THRESHOLD_MEDIUM_VLOS = 75

# Namespace for the vlosCoreDocument XML
NS_VLOS = {'vlos': 'http://www.tweedekamer.nl/ggm/vergaderverslag/v1.0'}

# --- Helper Functions for VLOS Processing (similar to test.py) ---
def parse_vlos_xml_datetime(datetime_val: Optional[str]) -> Optional[datetime]:
    if not datetime_val: return None
    if not isinstance(datetime_val, str): return None
    datetime_str = datetime_val.strip()
    try:
        if datetime_str.endswith('Z'): return datetime.fromisoformat(datetime_str[:-1] + '+00:00')
        if len(datetime_str) >= 24 and (datetime_str[19] == '+' or datetime_str[19] == '-') and datetime_str[22] == ':': return datetime.fromisoformat(datetime_str)
        if len(datetime_str) >= 23 and (datetime_str[19] == '+' or datetime_str[19] == '-') and datetime_str[22] != ':':
             dt_str_fixed = datetime_str[:22] + ":" + datetime_str[22:]
             return datetime.fromisoformat(dt_str_fixed)
        return datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S") # Returns naive
    except ValueError:
        try: return tkapi_util.odatedatetime_to_datetime(datetime_str) # Fallback
        except Exception: return None

def get_vlos_utc_datetime(dt_obj: Optional[datetime]) -> Optional[datetime]:
    if not dt_obj: return None
    if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
        return (dt_obj - timedelta(hours=LOCAL_TIMEZONE_OFFSET_HOURS_VLOS)).replace(tzinfo=timezone.utc)
    return dt_obj.astimezone(timezone.utc)

def evaluate_vlos_time_match(
    xml_start: Optional[datetime], xml_end: Optional[datetime],
    api_start: Optional[datetime], api_end: Optional[datetime]
) -> tuple[float, str]:
    score = 0.0
    reason = "No significant time match"
    if not (xml_start and api_start and api_end): return score, reason

    xml_start_utc = get_vlos_utc_datetime(xml_start)
    api_start_utc = get_vlos_utc_datetime(api_start) # API times are already aware
    api_end_utc = get_vlos_utc_datetime(api_end)

    xml_end_for_check = xml_end or (xml_start + timedelta(minutes=1)) # Give a small duration if no end
    xml_end_utc = get_vlos_utc_datetime(xml_end_for_check)
    
    if not (xml_start_utc and api_start_utc and api_end_utc and xml_end_utc):
         return score, "Missing converted UTC time data"

    start_proximity_ok = abs((xml_start_utc - api_start_utc).total_seconds()) <= TIME_START_PROXIMITY_TOLERANCE_SECONDS_VLOS
    overlap_exists = (max(xml_start_utc, api_start_utc - timedelta(seconds=TIME_GENERAL_OVERLAP_BUFFER_SECONDS_VLOS)) <
                      min(xml_end_utc, api_end_utc + timedelta(seconds=TIME_GENERAL_OVERLAP_BUFFER_SECONDS_VLOS)))

    if start_proximity_ok:
        score = SCORE_TIME_START_PROXIMITY_VLOS
        reason = f"XML_start ({xml_start_utc.time()}) PROXIMATE to API_start ({api_start_utc.time()})"
        if overlap_exists: reason += " & timeframes overlap"
    elif overlap_exists:
        score = SCORE_TIME_OVERLAP_ONLY_VLOS
        reason = f"Timeframes OVERLAP (XML: {xml_start_utc.time()}-{xml_end_utc.time()}, API: {api_start_utc.time()}-{api_end_utc.time()})"
    return score, reason

def _get_candidate_api_activities(session, canonical_vergadering_node: Neo4jNode) -> List[Dict[str, Any]]:
    """Fetches Activiteit nodes linked to the Vergadering from Neo4j."""
    query = """
    MATCH (verg:Vergadering {id: $vergadering_id})
    // Path 1: Vergadering -> Agendapunt -> Activiteit (typical for Plenary)
    OPTIONAL MATCH (verg)<-[:HAS_AGENDAPUNT]-(ap:Agendapunt)-[:BELONGS_TO_ACTIVITEIT]->(act_via_ap:Activiteit)
    WITH verg, collect(DISTINCT act_via_ap) AS activities_from_agendapunten
    // Path 2: Activiteiten directly within the Vergadering's timeframe (more general, or for commissie)
    // Requires Vergadering node to have reliable 'begin' and 'einde' properties from API
    OPTIONAL MATCH (act_by_time:Activiteit)
    WHERE verg.begin IS NOT NULL AND verg.einde IS NOT NULL
      AND act_by_time.begin >= verg.begin 
      AND act_by_time.einde <= verg.einde 
    WITH activities_from_agendapunten + collect(DISTINCT act_by_time) AS all_acts_list
    UNWIND all_acts_list AS act_node
    RETURN DISTINCT act_node.id AS id, act_node.soort AS soort, act_node.onderwerp AS onderwerp,
           act_node.begin AS begin, act_node.einde AS einde
    """
    results = session.run(query, vergadering_id=canonical_vergadering_node['id'])
    candidates = []
    for record in results:
        # Convert Neo4j datetime strings back to Python datetime objects if necessary
        # For simplicity here, assuming they are readily usable or string comparisons work if stored as strings.
        # Ideally, store datetimes in Neo4j as datetime type.
        candidates.append({
            "id": record["id"],
            "soort": record["soort"], # This would be the enum key name string from API
            "onderwerp": record["onderwerp"],
            "begin": tkapi_util.odatedatetime_to_datetime(record["begin"]) if record["begin"] else None, # tkapi util parses OData strings
            "einde": tkapi_util.odatedatetime_to_datetime(record["einde"]) if record["einde"] else None
        })
    return candidates


def _process_vlos_activity_element(session, element: ET.Element, 
                                   canonical_vergadering_node: Neo4jNode,
                                   parent_vlos_node_id: Optional[str], 
                                   api_activities_for_vergadering: List[Dict[str, Any]]):
    """
    Recursively processes an activity-like XML element, creates a :VlosReportSection,
    and tries to link it to a canonical API :Activiteit.
    """
    vlos_act_id = element.get('objectid')
    if not vlos_act_id:
        return

    xml_act_soort_str = element.get('soort')
    xml_act_titel = element.findtext('vlos:titel', default='', namespaces=NS_VLOS)
    xml_act_onderwerp = element.findtext('vlos:onderwerp', default='', namespaces=NS_VLOS)
    
    val_aanvangstijd = element.findtext('vlos:aanvangstijd', default=None, namespaces=NS_VLOS)
    val_markeertijdbegin = element.findtext('vlos:markeertijdbegin', default=None, namespaces=NS_VLOS)
    xml_start_str = val_aanvangstijd or val_markeertijdbegin
    
    val_eindtijd = element.findtext('vlos:eindtijd', default=None, namespaces=NS_VLOS)
    val_markeertijdeind = element.findtext('vlos:markeertijdeind', default=None, namespaces=NS_VLOS)
    xml_end_str = val_eindtijd or val_markeertijdeind

    xml_act_start_dt = parse_vlos_xml_datetime(xml_start_str)
    xml_act_end_dt = parse_vlos_xml_datetime(xml_end_str)

    props = {
        'vlos_id': vlos_act_id, # Use vlos_id as the key for these nodes
        'soort': xml_act_soort_str,
        'titel': xml_act_titel,
        'onderwerp': xml_act_onderwerp,
        'begin_str': xml_start_str, # Store original string
        'einde_str': xml_end_str,   # Store original string
        'begin_dt': str(xml_act_start_dt) if xml_act_start_dt else None, # Store parsed datetime as string
        'einde_dt': str(xml_act_end_dt) if xml_act_end_dt else None,     # Store parsed datetime as string
        'source': 'vlos'
    }
    # More specific label based on the XML tag?
    label = "VlosReportSection"
    if "activiteitdeel" in element.tag: label = "VlosActiviteitDeel"
    elif "activiteithoofd" in element.tag: label = "VlosActiviteitHoofd"
    elif "woordvoerder" in element.tag: label = "VlosWoordvoerder" # Example
    elif "interrumpant" in element.tag: label = "VlosInterrumpant" # Example

    session.execute_write(merge_node, label, 'vlos_id', props)

    if parent_vlos_node_id: # Link to parent VLOS section
        # Determine parent label dynamically if needed, or use a generic one
        session.execute_write(merge_rel, label, 'vlos_id', vlos_act_id,
                              "VlosReportSection", 'vlos_id', parent_vlos_node_id, 'IS_PART_OF_VLOS_SECTION')
    else: # Top-level XML activity, link to canonical Vergadering
        session.execute_write(merge_rel, label, 'vlos_id', vlos_act_id,
                              "Vergadering", 'id', canonical_vergadering_node['id'], 'SECTION_OF_REPORTED_VERGADERING')

    # Try to match top-level XML <activiteit> elements to API Activiteiten
    if element.tag == f"{{{NS_VLOS['vlos']}}}activiteit": # Only match direct <activiteit> children of <vergadering>
        best_match_api_activity_id = None
        highest_score = 0.0

        for api_act_data in api_activities_for_vergadering:
            current_score = 0.0
            time_score, _ = evaluate_vlos_time_match(xml_act_start_dt, xml_act_end_dt, api_act_data["begin"], api_act_data["einde"])
            current_score += time_score
            
            api_s_lower = (api_act_data["soort"] or "").lower()
            xml_s_lower = (xml_act_soort_str or "").lower()
            if xml_s_lower and api_s_lower:
                if xml_s_lower == api_s_lower: current_score += SCORE_SOORT_EXACT_VLOS
                elif xml_s_lower in api_s_lower: current_score += SCORE_SOORT_PARTIAL_XML_IN_API_VLOS
                elif api_s_lower in xml_s_lower: current_score += SCORE_SOORT_PARTIAL_API_IN_XML_VLOS
            
            api_onderwerp_lower = (api_act_data["onderwerp"] or "").strip().lower()
            xml_onderwerp_from_xml_lower = (xml_act_onderwerp or "").strip().lower()
            xml_titel_from_xml_lower = (xml_act_titel or "").strip().lower()

            if xml_onderwerp_from_xml_lower and api_onderwerp_lower:
                if xml_onderwerp_from_xml_lower == api_onderwerp_lower: current_score += SCORE_ONDERWERP_EXACT_VLOS
                else:
                    ratio = fuzz.ratio(xml_onderwerp_from_xml_lower, api_onderwerp_lower)
                    if ratio >= FUZZY_SIMILARITY_THRESHOLD_HIGH_VLOS: current_score += SCORE_ONDERWERP_FUZZY_HIGH_VLOS
                    elif ratio >= FUZZY_SIMILARITY_THRESHOLD_MEDIUM_VLOS: current_score += SCORE_ONDERWERP_FUZZY_MEDIUM_VLOS
            
            if xml_titel_from_xml_lower and api_onderwerp_lower:
                if xml_titel_from_xml_lower == api_onderwerp_lower: current_score += SCORE_TITEL_EXACT_VS_API_ONDERWERP_VLOS
                else:
                    ratio = fuzz.ratio(xml_titel_from_xml_lower, api_onderwerp_lower)
                    if ratio >= FUZZY_SIMILARITY_THRESHOLD_HIGH_VLOS: current_score += SCORE_TITEL_FUZZY_HIGH_VS_API_ONDERWERP_VLOS
                    elif ratio >= FUZZY_SIMILARITY_THRESHOLD_MEDIUM_VLOS: current_score += SCORE_TITEL_FUZZY_MEDIUM_VS_API_ONDERWERP_VLOS

            if current_score > highest_score:
                highest_score = current_score
                best_match_api_activity_id = api_act_data["id"]

        if best_match_api_activity_id and highest_score >= MIN_MATCH_SCORE_FOR_VLOS_ACTIVITEIT:
            print(f"    🔗 Linking VLOS section {vlos_act_id} (Soort: {xml_act_soort_str}) to API Activiteit {best_match_api_activity_id} (Score: {highest_score:.2f})")
            session.execute_write(merge_rel, label, 'vlos_id', vlos_act_id,
                                  "Activiteit", 'id', best_match_api_activity_id, 'CORRESPONDS_TO_API_ACTIVITY')

    # Process direct children (speakers, zaken) for this VLOS element
    _process_vlos_speakers(session, element, vlos_act_id, label)
    _process_vlos_zaken(session, element, vlos_act_id, label)

    # Recursive Step: Process sub-activity elements
    sub_activity_tags = ['vlos:activiteit', 'vlos:activiteithoofd', 'vlos:activiteitdeel', 'vlos:woordvoerder', 'vlos:interrumpant']
    for tag_name_str in sub_activity_tags:
        for sub_element in element.findall(tag_name_str, NS_VLOS):
            _process_vlos_activity_element(session, sub_element, canonical_vergadering_node, vlos_act_id, api_activities_for_vergadering)


def _process_vlos_speakers(session, element: ET.Element, vlos_parent_section_id: str, vlos_parent_label: str):
    """Finds speakers and links them to the VlosReportSection."""
    for spreker_el in element.findall('.//vlos:spreker', NS_VLOS): # Find all descendant sprekers
        vlos_persoon_id = spreker_el.get('objectid')
        if not vlos_persoon_id: continue

        # Create a placeholder :VlosSpreker node or link to existing :Persoon
        # For simplicity, creating a VlosSpreker linked to Persoon if matched by vlos_id
        # This assumes Persoon loader might add vlos_id from other sources or match by name.
        
        # Option 1: Try to match to an existing Persoon via vlos_id (if Persoon loader stores it)
        # OR match by name after API load (more complex, deferred)
        # For now, create/merge a Persoon with vlos_id as a potential linkable ID
        
        persoon_props = {
            'id': vlos_persoon_id, # Tentatively use VLOS ID for Persoon if no API ID known yet
                                   # This WILL clash if API Person also uses 'id' and they differ.
                                   # Safer: 'vlos_person_id': vlos_persoon_id and merge on API Persoon.id if found
            'achternaam': spreker_el.findtext('vlos:achternaam', default='', namespaces=NS_VLOS),
            'voornaam': spreker_el.findtext('vlos:voornaam', default='', namespaces=NS_VLOS).strip(),
            'verslagnaam': spreker_el.findtext('vlos:verslagnaam', default='', namespaces=NS_VLOS),
            'functie_vlos': spreker_el.findtext('vlos:functie', default='', namespaces=NS_VLOS), # distinguish from API functie
            'soort_vlos': spreker_el.get('soort'),
            'source_info': 'vlos_spreker_element'
        }
        # MERGE Persoon on a more stable key if possible, or plan to reconcile these Persoon nodes later
        # Using vlos_persoon_id for now for simplicity of this example, but this needs care.
        session.execute_write(merge_node, 'Persoon', 'id', persoon_props) # DANGER: Re-evaluate Persoon keying strategy!

        # Link the VLOS parent section to this Persoon
        session.execute_write(merge_rel, vlos_parent_label, 'vlos_id', vlos_parent_section_id,
                              'Persoon', 'id', vlos_persoon_id, 'HAS_SPREKER')
        
        # Additionally, if fractie info exists in VLOS:
        fractie_naam = spreker_el.findtext('vlos:fractie', default=None, namespaces=NS_VLOS)
        if fractie_naam:
            # Merge Fractie based on 'naam' from VLOS. This can also cause duplicates if API uses 'id'.
            # This assumes fractie_loader uses 'id'. Reconciliation needed.
            session.execute_write(merge_node, 'Fractie', 'naam', {'naam': fractie_naam, 'source_info': 'vlos_fractie_mention'})
            session.execute_write(merge_rel, 'Persoon', 'id', vlos_persoon_id,
                                  'Fractie', 'naam', fractie_naam, 'HAD_VLOS_FRACTIE')


def _process_vlos_zaken(session, element: ET.Element, vlos_parent_section_id: str, vlos_parent_label: str):
    """Finds zaken and links them to the VlosReportSection."""
    for zaak_el in element.findall('./vlos:zaken/vlos:zaak', NS_VLOS): # Direct children only
        vlos_zaak_id = zaak_el.get('objectid')
        if not vlos_zaak_id: continue
        
        xml_zaak_soort = zaak_el.get('soort')
        xml_zaak_onderwerp = zaak_el.findtext('vlos:onderwerp', default='', namespaces=NS_VLOS)
        xml_zaak_dossiernummer = zaak_el.findtext('vlos:dossiernummer', default=None, namespaces=NS_VLOS)

        # Strategy:
        # 1. If dossiernummer exists, try to MATCH on canonical :Zaak {nummer: dossiernummer}
        # 2. If found, link VlosReportSection to it. Add vlos_zaak_id as a secondary_id to :Zaak.
        # 3. If not found by dossiernummer, create a :VlosZaakPlaceholder {vlos_id: vlos_zaak_id} and link to that.
        
        if xml_zaak_dossiernummer:
            # Check if canonical Zaak exists
            check_query = "MATCH (z:Zaak {nummer: $nummer}) RETURN z.id AS id LIMIT 1"
            result = session.run(check_query, nummer=xml_zaak_dossiernummer)
            canonical_zaak_record = result.single()

            if canonical_zaak_record:
                canonical_zaak_api_id = canonical_zaak_record["id"]
                session.execute_write(merge_rel, vlos_parent_label, 'vlos_id', vlos_parent_section_id,
                                      'Zaak', 'id', canonical_zaak_api_id, 'MENTIONS_ZAAK')
                # Add vlos_id to the list of vlos_ids on the canonical Zaak
                update_zaak_query = """
                MATCH (z:Zaak {id: $api_id})
                SET z.vlos_ids = coalesce(z.vlos_ids, []) + $vlos_id_to_add
                WHERE NOT $vlos_id_to_add IN coalesce(z.vlos_ids, [])
                """
                session.run(update_zaak_query, api_id=canonical_zaak_api_id, vlos_id_to_add=vlos_zaak_id)
            else:
                # Canonical Zaak not found by nummer, create placeholder based on VLOS ID
                zaak_props = {
                    'vlos_id': vlos_zaak_id, # Key for this placeholder
                    'soort_vlos': xml_zaak_soort,
                    'onderwerp_vlos': xml_zaak_onderwerp,
                    'dossiernummer_vlos': xml_zaak_dossiernummer,
                    'source': 'vlos_placeholder'
                }
                session.execute_write(merge_node, 'VlosZaakPlaceholder', 'vlos_id', zaak_props)
                session.execute_write(merge_rel, vlos_parent_label, 'vlos_id', vlos_parent_section_id,
                                      'VlosZaakPlaceholder', 'vlos_id', vlos_zaak_id, 'MENTIONS_VLOS_ZAAK')
        else: # No dossiernummer, create placeholder based on VLOS ID
            zaak_props = {
                'vlos_id': vlos_zaak_id,
                'soort_vlos': xml_zaak_soort,
                'onderwerp_vlos': xml_zaak_onderwerp,
                'source': 'vlos_placeholder_no_nummer'
            }
            session.execute_write(merge_node, 'VlosZaakPlaceholder', 'vlos_id', zaak_props)
            session.execute_write(merge_rel, vlos_parent_label, 'vlos_id', vlos_parent_section_id,
                                  'VlosZaakPlaceholder', 'vlos_id', vlos_zaak_id, 'MENTIONS_VLOS_ZAAK')


def load_vlos_verslag(driver, xml_content: str, canonical_api_vergadering_id: str):
    """
    Parses and loads a VLOS Verslag into Neo4j, linking it to a pre-existing canonical Vergadering node.
    :param driver: Neo4j driver instance.
    :param xml_content: The XML string content of the VLOS verslag.
    :param canonical_api_vergadering_id: The API ID of the Vergadering this XML report corresponds to.
    """
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        print(f"  ✕ ERROR: Could not parse VLOS XML. {e}")
        return

    xml_vergadering_el = root.find('vlos:vergadering', NS_VLOS)
    if xml_vergadering_el is None:
        print("  ✕ ERROR: <vergadering> tag not found in VLOS XML.")
        return

    vlos_report_id = xml_vergadering_el.get('objectid') # This is the ID of the XML document/report itself

    with driver.session() as session:
        # 1. Find the canonical Vergadering node (should have been created by vergadering_loader.py)
        verg_node_result = session.run("MATCH (v:Vergadering {id: $id}) RETURN v", id=canonical_api_vergadering_id).single()
        if not verg_node_result:
            print(f"  ✕ ERROR: Canonical Vergadering with API ID '{canonical_api_vergadering_id}' not found in Neo4j. VLOS XML for report '{vlos_report_id}' cannot be processed further without it.")
            return
        
        canonical_vergadering_node = verg_node_result['v']
        print(f"  ℹ️  Found canonical Vergadering: {canonical_vergadering_node['id']} for VLOS report {vlos_report_id}")

        # 2. Add VLOS-specific attributes to the canonical Vergadering node
        session.run("""
            MATCH (v:Vergadering {id: $api_id})
            SET v.vlos_report_id = $vlos_report_id,
                v.vlos_kamer = $vlos_kamer,
                v.vlos_zaal = $vlos_zaal,
                v.vlos_datum_str = $vlos_datum_str,
                v.vlos_aanvangstijd_str = $vlos_aanvangstijd_str,
                v_sluiting_str = $vlos_sluiting_str
            """, 
            api_id=canonical_api_vergadering_id,
            vlos_report_id=vlos_report_id,
            vlos_kamer=xml_vergadering_el.get('kamer'),
            vlos_zaal=xml_vergadering_el.findtext('vlos:zaal', default='', namespaces=NS_VLOS),
            vlos_datum_str=xml_vergadering_el.findtext('vlos:datum', default='', namespaces=NS_VLOS),
            vlos_aanvangstijd_str=xml_vergadering_el.findtext('vlos:aanvangstijd', default='', namespaces=NS_VLOS),
            vlos_sluiting_str=xml_vergadering_element.findtext('vlos:sluiting', default='', namespaces=NS_VLOS)
        )
        
        # 3. Get candidate API activities for this vergadering from Neo4j
        api_activities_for_vergadering = _get_candidate_api_activities(session, canonical_vergadering_node)
        print(f"  ℹ️  Fetched {len(api_activities_for_vergadering)} candidate API activities from Neo4j for this Vergadering.")

        # 4. Process the VLOS XML structure, starting with top-level <activiteit> elements
        for vlos_activiteit_el in xml_vergadering_el.findall('vlos:activiteit', NS_VLOS):
            _process_vlos_activity_element(session, vlos_activiteit_el, 
                                           canonical_vergadering_node, 
                                           None, # No parent_vlos_node_id for top-level
                                           api_activities_for_vergadering)
        
        print(f"  ✅ Successfully processed VLOS XML structure for report ID {vlos_report_id} and linked to Vergadering {canonical_api_vergadering_id}.")