"""
Enhanced VLOS XML matching logic incorporating sophisticated algorithms from test file.
This module provides comprehensive matching between VLOS XML content and TK-API entities.
"""

import xml.etree.ElementTree as ET
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple
from thefuzz import fuzz
from tkapi.util import util as tkapi_util
from tkapi.persoon import Persoon
from tkapi.zaak import Zaak
from tkapi.dossier import Dossier
from tkapi.document import Document
from utils.helpers import merge_node, merge_rel

# Namespace for VLOS XML
NS_VLOS = {'vlos': 'http://www.tweedekamer.nl/ggm/vergaderverslag/v1.0'}

# Enhanced Configuration
LOCAL_TIMEZONE_OFFSET_HOURS = 2  # CEST for summer samples

# Scoring weights for activity matching
SCORE_TIME_START_PROXIMITY = 3.0
SCORE_TIME_OVERLAP_ONLY = 1.5
SCORE_SOORT_EXACT = 2.0
SCORE_SOORT_PARTIAL_XML_IN_API = 2.0
SCORE_SOORT_PARTIAL_API_IN_XML = 1.5
SCORE_ONDERWERP_EXACT = 4.0
SCORE_ONDERWERP_FUZZY_HIGH = 2.5
SCORE_ONDERWERP_FUZZY_MEDIUM = 2.0
SCORE_TITEL_EXACT_VS_API_ONDERWERP = 1.5
SCORE_TITEL_FUZZY_HIGH_VS_API_ONDERWERP = 1.25
SCORE_TITEL_FUZZY_MEDIUM_VS_API_ONDERWERP = 0.5

MIN_MATCH_SCORE_FOR_ACTIVITEIT = 3.0
TIME_START_PROXIMITY_TOLERANCE_SECONDS = 300  # 5 minutes
TIME_GENERAL_OVERLAP_BUFFER_SECONDS = 600     # 10 minutes

FUZZY_SIMILARITY_THRESHOLD_HIGH = 85
FUZZY_SIMILARITY_THRESHOLD_MEDIUM = 70

# Speaker matching thresholds
FUZZY_FIRSTNAME_THRESHOLD = 80
FUZZY_SURNAME_THRESHOLD = 80

# Common topic prefixes for normalization
COMMON_TOPIC_PREFIXES = [
    'tweeminutendebat',
    'procedurevergadering',
    'wetgevingsoverleg',
    'plenaire afronding',
    'plenaire afronding in 1 termijn',
    'plenaire afronding in één termijn',
    'plenaire afronding in een termijn',
    'plenaire debat',
    'plenair debat',
    'debate over',
    'debate',
    'aanvang',
    'einde vergadering',
    'regeling van werkzaamheden',
    'stemmingen',
    'aanbieding',
    'technische briefing',
    'delegatievergadering',
    'commissiedebat',
    'inbreng schriftelijk overleg',
    'gesprek',
    'emailprocedure',
    'e-mailprocedure',
]

# Soort aliases
SOORT_ALIAS = {
    'opening': [
        'aanvang', 'regeling van werkzaamheden', 'reglementair'
    ],
    'sluiting': [
        'einde vergadering', 'stemmingen', 'stemmen'
    ],
    'mededelingen': [
        'procedurevergadering', 'procedures en brieven', 'uitstel brieven'
    ],
}

# Compile regex for topic normalization
_PREFIX_REGEX = re.compile(r'^(' + '|'.join(re.escape(p) for p in COMMON_TOPIC_PREFIXES) + r')[\s:,-]+', re.IGNORECASE)

# Dossier regex
_DOSSIER_REGEX = re.compile(r"^(\d+)(?:[-\s]?([A-Za-z0-9]+))?$")


def normalize_topic(text: str) -> str:
    """Lower-case, strip, and remove common boilerplate prefixes for fair fuzzy matching."""
    if not text:
        return ''
    text = text.strip().lower()
    # Remove prefix once
    text = _PREFIX_REGEX.sub('', text, count=1)
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text)
    return text


def parse_xml_datetime(datetime_val):
    """Parse XML datetime string with multiple format support."""
    if not datetime_val or not isinstance(datetime_val, str):
        return None
    dt_str = datetime_val.strip()
    try:
        if dt_str.endswith('Z'):
            return datetime.fromisoformat(dt_str[:-1] + '+00:00')
        if len(dt_str) >= 24 and (dt_str[19] in '+-') and dt_str[22] == ':':
            return datetime.fromisoformat(dt_str)
        if len(dt_str) >= 23 and (dt_str[19] in '+-') and dt_str[22] != ':':
            return datetime.fromisoformat(dt_str[:22] + ':' + dt_str[22:])
        return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        try:
            return tkapi_util.odatedatetime_to_datetime(dt_str)
        except Exception:
            return None


def get_utc_datetime(dt_obj, local_offset_hours):
    """Convert datetime to UTC."""
    if not dt_obj:
        return None
    if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
        return (dt_obj - timedelta(hours=local_offset_hours)).replace(tzinfo=timezone.utc)
    return dt_obj.astimezone(timezone.utc)


def evaluate_time_match(xml_start, xml_end, api_start, api_end):
    """Enhanced time matching evaluation."""
    score = 0.0
    reason = 'No significant time match'
    if not (xml_start and api_start and api_end):
        return score, reason

    xml_start_utc = get_utc_datetime(xml_start, LOCAL_TIMEZONE_OFFSET_HOURS)
    api_start_utc = get_utc_datetime(api_start, LOCAL_TIMEZONE_OFFSET_HOURS)
    api_end_utc = get_utc_datetime(api_end, LOCAL_TIMEZONE_OFFSET_HOURS)

    xml_end_eff = xml_end or (xml_start + timedelta(minutes=1))
    xml_end_utc = get_utc_datetime(xml_end_eff, LOCAL_TIMEZONE_OFFSET_HOURS)

    if not (xml_start_utc and api_start_utc and api_end_utc and xml_end_utc):
        return score, 'Missing converted UTC data'

    start_close = abs((xml_start_utc - api_start_utc).total_seconds()) <= TIME_START_PROXIMITY_TOLERANCE_SECONDS
    overlap = max(xml_start_utc, api_start_utc - timedelta(seconds=TIME_GENERAL_OVERLAP_BUFFER_SECONDS)) < \
              min(xml_end_utc, api_end_utc + timedelta(seconds=TIME_GENERAL_OVERLAP_BUFFER_SECONDS))

    if start_close:
        score = SCORE_TIME_START_PROXIMITY
        reason = 'Start times close'
        if overlap:
            reason += ' & overlap'
    elif overlap:
        score = SCORE_TIME_OVERLAP_ONLY
        reason = 'Timeframes overlap'
    
    return score, reason


def get_candidate_api_activities(session, canonical_vergadering_node) -> List[Dict[str, Any]]:
    """Enhanced candidate activity fetching with broader search."""
    query = """
    MATCH (verg:Vergadering {id: $vergadering_id})
    
    // Direct activities
    OPTIONAL MATCH (verg)-[:HAS_ACTIVITEIT]->(act_direct:Activiteit)
    
    // Activities via agendapunten
    OPTIONAL MATCH (verg)-[:HAS_AGENDAPUNT]->(ap:Agendapunt)<-[:HAS_AGENDAPUNT]-(act_via_ap:Activiteit)
    
    // Activities by time overlap (wider buffer)
    OPTIONAL MATCH (act_by_time:Activiteit)
    WHERE verg.begin IS NOT NULL AND verg.einde IS NOT NULL
      AND act_by_time.begin >= verg.begin - duration('PT1H')
      AND act_by_time.einde <= verg.einde + duration('PT1H')
    
    WITH collect(DISTINCT act_direct) + collect(DISTINCT act_via_ap) + collect(DISTINCT act_by_time) AS all_activities
    
    UNWIND all_activities AS act
    WHERE act IS NOT NULL
    
    RETURN DISTINCT 
        act.id AS id,
        act.soort_api AS soort,
        act.onderwerp AS onderwerp,
        act.begin AS begin,
        act.einde AS einde,
        act.nummer AS nummer
    """
    
    results = session.run(query, vergadering_id=canonical_vergadering_node['id'])
    candidates = []
    
    for record in results:
        candidates.append({
            'id': record['id'],
            'soort': record['soort'],
            'onderwerp': record['onderwerp'],
            'begin': record['begin'],
            'einde': record['einde'],
            'nummer': record['nummer']
        })
    
    return candidates


def calculate_activity_match_score(xml_activity_data: Dict[str, Any], api_activity: Dict[str, Any]) -> Tuple[float, List[str]]:
    """Enhanced activity matching with comprehensive scoring."""
    score = 0.0
    reasons = []
    
    # Extract XML data
    xml_soort = (xml_activity_data.get('soort') or '').lower()
    xml_titel = (xml_activity_data.get('titel') or '').lower()
    xml_onderwerp = (xml_activity_data.get('onderwerp') or '').lower()
    xml_start = xml_activity_data.get('start_time')
    xml_end = xml_activity_data.get('end_time')
    
    # Extract API data
    api_soort = (api_activity.get('soort') or '').lower()
    api_onderwerp = (api_activity.get('onderwerp') or '').lower()
    api_start = api_activity.get('begin')
    api_end = api_activity.get('einde')
    
    # Time matching
    time_score, time_reason = evaluate_time_match(xml_start, xml_end, api_start, api_end)
    score += time_score
    if time_score > 0:
        reasons.append(time_reason)
    
    # Soort matching with aliases
    if xml_soort and api_soort:
        if xml_soort == api_soort:
            score += SCORE_SOORT_EXACT
            reasons.append("Soort exact match")
        elif xml_soort in api_soort:
            score += SCORE_SOORT_PARTIAL_XML_IN_API
            reasons.append("Soort partial XML in API")
        elif api_soort in xml_soort:
            score += SCORE_SOORT_PARTIAL_API_IN_XML
            reasons.append("Soort partial API in XML")
        else:
            # Check aliases
            for alias in SOORT_ALIAS.get(xml_soort, []):
                if alias in api_soort:
                    score += SCORE_SOORT_PARTIAL_XML_IN_API
                    reasons.append(f"Soort alias match ('{alias}')")
                    break
    
    # Onderwerp matching with normalization
    norm_xml_onderwerp = normalize_topic(xml_onderwerp)
    norm_xml_titel = normalize_topic(xml_titel)
    norm_api_onderwerp = normalize_topic(api_onderwerp)
    
    if norm_xml_onderwerp and norm_api_onderwerp:
        if norm_xml_onderwerp == norm_api_onderwerp:
            score += SCORE_ONDERWERP_EXACT
            reasons.append("Onderwerp exact match")
        else:
            ratio = fuzz.ratio(norm_xml_onderwerp, norm_api_onderwerp)
            if ratio >= FUZZY_SIMILARITY_THRESHOLD_HIGH:
                score += SCORE_ONDERWERP_FUZZY_HIGH
                reasons.append(f"Onderwerp fuzzy high ({ratio}%)")
            elif ratio >= FUZZY_SIMILARITY_THRESHOLD_MEDIUM:
                score += SCORE_ONDERWERP_FUZZY_MEDIUM
                reasons.append(f"Onderwerp fuzzy medium ({ratio}%)")
    
    # Title vs API onderwerp matching
    if norm_xml_titel and norm_api_onderwerp:
        if norm_xml_titel == norm_api_onderwerp:
            score += SCORE_TITEL_EXACT_VS_API_ONDERWERP
            reasons.append("Title exact vs API onderwerp")
        else:
            ratio = fuzz.ratio(norm_xml_titel, norm_api_onderwerp)
            if ratio >= FUZZY_SIMILARITY_THRESHOLD_HIGH:
                score += SCORE_TITEL_FUZZY_HIGH_VS_API_ONDERWERP
                reasons.append(f"Title fuzzy high ({ratio}%)")
            elif ratio >= FUZZY_SIMILARITY_THRESHOLD_MEDIUM:
                score += SCORE_TITEL_FUZZY_MEDIUM_VS_API_ONDERWERP
                reasons.append(f"Title fuzzy medium ({ratio}%)")
    
    return score, reasons


# Speaker matching functions
def _build_full_surname(p: Dict[str, Any]) -> str:
    """Return full surname including tussenvoegsel (if any)."""
    tussenvoegsel = p.get('tussenvoegsel', '') or ''
    achternaam = p.get('achternaam', '') or ''
    full = f"{tussenvoegsel} {achternaam}".strip()
    return re.sub(r"\s+", " ", full).lower()


def calc_name_similarity(v_first: str, v_last: str, p: Dict[str, Any]) -> int:
    """Enhanced name similarity calculation with tussenvoegsel handling."""
    score = 0
    
    if not (v_last and p.get('achternaam')):
        return score
    
    v_last_lower = v_last.lower()
    
    bare_surname = p.get('achternaam', '').lower()
    full_surname = _build_full_surname(p)
    
    # Pick best of bare vs full surname similarity
    ratio_bare = fuzz.ratio(v_last_lower, bare_surname)
    ratio_full = fuzz.ratio(v_last_lower, full_surname)
    best_ratio = max(ratio_bare, ratio_full)
    
    # Exact match on either variant → big boost
    if v_last_lower in [bare_surname, full_surname]:
        score += 60
    else:
        # Convert fuzzy similarity to 0-60 scale
        score += max(best_ratio - 20, 0)
    
    # Firstname / roepnaam boost
    v_first_lower = (v_first or "").lower()
    if v_first_lower:
        roepnaam = p.get('roepnaam', '') or ''
        voornamen = p.get('voornamen', '') or ''
        first_candidates = [c for c in [roepnaam, voornamen] if c]
        if first_candidates:
            best_first = max((fuzz.ratio(v_first_lower, fc.lower()) for fc in first_candidates), default=0)
            if best_first >= FUZZY_FIRSTNAME_THRESHOLD:
                score += 40
            elif best_first >= 60:
                score += 20
    
    return min(score, 100)


def collapse_text(element: ET.Element) -> str:
    """Collapse XML text content."""
    if element is None:
        return ""
    
    text_parts = []
    if element.text:
        text_parts.append(element.text.strip())
    
    for child in element:
        if child.tail:
            text_parts.append(child.tail.strip())
    
    return " ".join(text_parts)


def find_best_persoon(session, first: str, last: str) -> Optional[Dict[str, Any]]:
    """Enhanced person finding with fuzzy matching."""
    if not last:
        return None
    
    # First try exact achternaam search
    query = """
    MATCH (p:Persoon)
    WHERE toLower(p.achternaam) = toLower($last)
    RETURN p.id as id, p.roepnaam as roepnaam, p.voornamen as voornamen, 
           p.achternaam as achternaam, p.tussenvoegsel as tussenvoegsel
    LIMIT 50
    """
    
    results = session.run(query, last=last)
    candidates = [dict(record) for record in results]
    
    if not candidates:
        # Fallback: search by contains main surname token
        main_last_token = last.strip().split()[-1]
        query = """
        MATCH (p:Persoon)
        WHERE toLower(p.achternaam) CONTAINS toLower($token)
        RETURN p.id as id, p.roepnaam as roepnaam, p.voornamen as voornamen, 
               p.achternaam as achternaam, p.tussenvoegsel as tussenvoegsel
        LIMIT 100
        """
        results = session.run(query, token=main_last_token)
        candidates = [dict(record) for record in results]
    
    if not candidates:
        return None
    
    # Find best match
    best_p = None
    best_score = 0
    
    for p in candidates:
        score = calc_name_similarity(first, last, p)
        if score > best_score:
            best_score = score
            best_p = p
    
    return best_p if best_score >= 60 else None


def best_persoon_from_actors(first: str, last: str, session, activity_id: str) -> Optional[Dict[str, Any]]:
    """Find best person from activity actors."""
    query = """
    MATCH (act:Activiteit {id: $activity_id})-[:HAS_ACTOR]->(actor:Actor)-[:ACTOR_PERSOON]->(p:Persoon)
    RETURN p.id as id, p.roepnaam as roepnaam, p.voornamen as voornamen, 
           p.achternaam as achternaam, p.tussenvoegsel as tussenvoegsel
    """
    
    results = session.run(query, activity_id=activity_id)
    candidates = [dict(record) for record in results]
    
    if not candidates:
        return None
    
    best_p = None
    best_score = 0
    
    for p in candidates:
        score = calc_name_similarity(first, last, p)
        if score > best_score:
            best_score = score
            best_p = p
    
    return best_p if best_score >= 60 else None


# Zaak matching functions
def _safe_int(val: str) -> Optional[int]:
    """Return int(val) if val represents an integer, else None."""
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def find_best_zaak(session, dossiernummer: str, stuknummer: str) -> Optional[Dict[str, Any]]:
    """Enhanced zaak finding with dossier and stuk nummer matching."""
    if not dossiernummer and not stuknummer:
        return None
    
    # Build dynamic query based on available parameters
    conditions = []
    params = {}
    
    dnr_int = _safe_int(dossiernummer)
    if dnr_int is not None:
        conditions.append("z.dossier_nummer = $dnr_int")
        params['dnr_int'] = dnr_int
    elif dossiernummer:
        conditions.append("z.nummer = $dossiernummer")
        params['dossiernummer'] = dossiernummer
    
    snr_int = _safe_int(stuknummer)
    if snr_int is not None:
        conditions.append("z.volgnummer = $snr_int")
        params['snr_int'] = snr_int
    elif stuknummer:
        conditions.append("z.volgnummer = $stuknummer")
        params['stuknummer'] = stuknummer
    
    if not conditions:
        return None
    
    query = f"""
    MATCH (z:Zaak)
    WHERE {' AND '.join(conditions)}
    RETURN z.id as id, z.soort as soort, z.nummer as nummer, z.volgnummer as volgnummer,
           z.dossier_nummer as dossier_nummer
    LIMIT 10
    """
    
    results = session.run(query, **params)
    candidates = [dict(record) for record in results]
    
    if not candidates:
        return None
    
    # Return first match or best match based on both criteria
    if len(candidates) == 1:
        return candidates[0]
    
    # Prefer exact matches on both criteria
    for z in candidates:
        if (dnr_int and _safe_int(z.get('dossier_nummer')) == dnr_int) and \
           (snr_int is None or _safe_int(z.get('volgnummer')) == snr_int):
            return z
    
    return candidates[0]


def find_best_zaak_or_fallback(session, dossiernummer: str, stuknummer: str) -> dict:
    """Enhanced zaak finding with dossier fallback logic.
    
    Returns a dict with:
    - 'zaak': Zaak dict if found, None otherwise
    - 'dossier': Dossier dict if zaak not found but dossier exists
    - 'document': Document dict if applicable
    - 'match_type': 'zaak', 'dossier_fallback', or 'no_match'
    - 'success': bool indicating if any match was found
    """
    result = {
        'zaak': None,
        'dossier': None, 
        'document': None,
        'match_type': 'no_match',
        'success': False
    }
    
    # First try to find a specific Zaak
    zaak = find_best_zaak(session, dossiernummer, stuknummer)
    if zaak:
        result['zaak'] = zaak
        result['match_type'] = 'zaak'
        result['success'] = True
        return result
    
    # No specific Zaak found - try dossier fallback
    if dossiernummer:
        dossier = find_best_dossier(session, dossiernummer)
        if dossier:
            result['dossier'] = dossier
            result['match_type'] = 'dossier_fallback'
            result['success'] = True
            
            # Also try to find the document within this dossier
            if stuknummer:
                num, toevoeg = _split_dossier_code(dossiernummer)
                document = find_best_document(session, num, toevoeg, stuknummer)
                if document:
                    result['document'] = document
            
            return result
    
    return result


def _split_dossier_code(code: str) -> Tuple[Optional[int], Optional[str]]:
    """Split dossier code into nummer and toevoeging."""
    if not code:
        return None, None
    
    m = _DOSSIER_REGEX.match(code.strip())
    if not m:
        return None, None
    
    nummer = _safe_int(m.group(1))
    toevoeging = m.group(2) or None
    return nummer, toevoeging


def find_best_dossier(session, dossier_code: str) -> Optional[Dict[str, Any]]:
    """Find best dossier match."""
    num, toevoeg = _split_dossier_code(dossier_code or "")
    if num is None:
        return None
    
    conditions = ["d.nummer = $num"]
    params = {'num': num}
    
    if toevoeg:
        conditions.append("d.toevoeging = $toevoeg")
        params['toevoeg'] = toevoeg
    
    query = f"""
    MATCH (d:Dossier)
    WHERE {' AND '.join(conditions)}
    RETURN d.id as id, d.nummer as nummer, d.toevoeging as toevoeging
    LIMIT 5
    """
    
    results = session.run(query, **params)
    candidates = [dict(record) for record in results]
    
    return candidates[0] if candidates else None


def find_best_document(session, dossier_num: int, dossier_toevoeging: str, stuknummer: str) -> Optional[Dict[str, Any]]:
    """Find best document match."""
    snr_int = _safe_int(stuknummer)
    if snr_int is None:
        return None
    
    conditions = ["doc.volgnummer = $snr_int"]
    params = {'snr_int': snr_int}
    
    if dossier_num:
        conditions.append("doc.dossier_nummer = $dossier_num")
        params['dossier_num'] = dossier_num
        
        if dossier_toevoeging:
            conditions.append("doc.dossier_toevoeging = $dossier_toevoeging")
            params['dossier_toevoeging'] = dossier_toevoeging
    
    query = f"""
    MATCH (doc:Document)
    WHERE {' AND '.join(conditions)}
    RETURN doc.id as id, doc.nummer as nummer, doc.volgnummer as volgnummer,
           doc.dossier_nummer as dossier_nummer, doc.dossier_toevoeging as dossier_toevoeging
    LIMIT 5
    """
    
    results = session.run(query, **params)
    candidates = [dict(record) for record in results]
    
    return candidates[0] if candidates else None


def process_enhanced_vlos_activity(session, xml_act: ET.Element, canonical_vergadering_node, 
                                  api_activities: List[Dict[str, Any]]) -> Optional[str]:
    """Process a single VLOS activity with enhanced matching."""
    # Extract activity data
    xml_id = xml_act.get('objectid')
    xml_soort = xml_act.get('soort')
    xml_titel = xml_act.findtext('vlos:titel', default='', namespaces=NS_VLOS)
    xml_onderwerp = xml_act.findtext('vlos:onderwerp', default='', namespaces=NS_VLOS)
    
    xml_start = parse_xml_datetime(
        xml_act.findtext('vlos:aanvangstijd', default=None, namespaces=NS_VLOS)
        or xml_act.findtext('vlos:markeertijdbegin', default=None, namespaces=NS_VLOS)
    )
    xml_end = parse_xml_datetime(
        xml_act.findtext('vlos:eindtijd', default=None, namespaces=NS_VLOS)
        or xml_act.findtext('vlos:markeertijdeind', default=None, namespaces=NS_VLOS)
    )
    
    # Fallback to vergadering timeframe if no explicit times
    if not xml_start:
        xml_start = canonical_vergadering_node.get('begin')
    if not xml_end:
        xml_end = canonical_vergadering_node.get('einde')
    
    # Create VLOS activity node
    activity_props = {
        'id': xml_id,
        'titel': xml_titel,
        'onderwerp': xml_onderwerp,
        'soort': xml_soort,
        'start_time': str(xml_start) if xml_start else None,
        'end_time': str(xml_end) if xml_end else None,
        'source': 'vlos_xml_enhanced'
    }
    
    session.execute_write(merge_node, 'VlosActivity', 'id', activity_props)
    
    # Link to vergadering
    session.execute_write(merge_rel, 'Vergadering', 'id', canonical_vergadering_node['id'],
                          'VlosActivity', 'id', xml_id, 'HAS_VLOS_ACTIVITY')
    
    # Find best API activity match
    best_match = None
    best_score = 0.0
    potential_matches = []
    
    xml_activity_data = {
        'soort': xml_soort,
        'titel': xml_titel,
        'onderwerp': xml_onderwerp,
        'start_time': xml_start,
        'end_time': xml_end
    }
    
    for api_act in api_activities:
        score, reasons = calculate_activity_match_score(xml_activity_data, api_act)
        
        potential_matches.append({
            'score': score,
            'reasons': reasons,
            'api_act': api_act,
        })
        
        if score > best_score:
            best_score = score
            best_match = api_act
    
    # Sort by score
    potential_matches.sort(key=lambda d: d['score'], reverse=True)
    
    # Determine acceptance
    accept_match = False
    if best_score >= MIN_MATCH_SCORE_FOR_ACTIVITEIT:
        accept_match = True
    else:
        runner_up_score = potential_matches[1]['score'] if len(potential_matches) > 1 else 0.0
        if best_score - runner_up_score >= 1.0 and best_score >= 1.0:
            accept_match = True
    
    if accept_match and best_match:
        # Create relationship
        session.execute_write(merge_rel, 'VlosActivity', 'id', xml_id,
                              'Activiteit', 'id', best_match['id'], 'MATCHES_API_ACTIVITY')
        
        print(f"✅ MATCHED: VLOS activity {xml_id} to API activity {best_match['id']} (score: {best_score:.2f})")
        
        # Process speakers within this activity
        process_vlos_speakers_for_activity(session, xml_act, xml_id, best_match['id'])
        
        # Process zaken within this activity
        process_vlos_zaken_for_activity(session, xml_act, xml_id)
    else:
        print(f"❌ NO MATCH: VLOS activity {xml_id} (best score: {best_score:.2f})")
        
        # Still process speakers and zaken even without activity match
        process_vlos_speakers_for_activity(session, xml_act, xml_id, None)
        process_vlos_zaken_for_activity(session, xml_act, xml_id)
    
    return xml_id


def process_vlos_speakers_for_activity(session, xml_act: ET.Element, vlos_activity_id: str, 
                                      api_activity_id: Optional[str]):
    """Process speakers within a VLOS activity."""
    speaker_count = 0
    
    # Find all draadboekfragment elements with speakers
    for frag in xml_act.findall(".//vlos:draadboekfragment", NS_VLOS):
        tekst_el = frag.find("vlos:tekst", NS_VLOS)
        if tekst_el is None:
            continue
            
        speech_text = collapse_text(tekst_el)
        if not speech_text:
            continue
        
        for sprek_el in frag.findall("vlos:sprekers/vlos:spreker", NS_VLOS):
            v_first = sprek_el.findtext("vlos:voornaam", default="", namespaces=NS_VLOS)
            v_last = (
                sprek_el.findtext("vlos:verslagnaam", default="", namespaces=NS_VLOS)
                or sprek_el.findtext("vlos:achternaam", default="", namespaces=NS_VLOS)
            )
            
            if not v_last:
                continue
            
            speaker_count += 1
            
            # Create VLOS speaker node
            speaker_id = f"vlos_speaker_{vlos_activity_id}_{speaker_count}"
            speaker_props = {
                'id': speaker_id,
                'voornaam': v_first,
                'achternaam': v_last,
                'speech_text': speech_text[:500],  # Truncate for storage
                'source': 'vlos_xml_enhanced'
            }
            
            session.execute_write(merge_node, 'VlosSpeaker', 'id', speaker_props)
            
            # Link to activity
            session.execute_write(merge_rel, 'VlosActivity', 'id', vlos_activity_id,
                                  'VlosSpeaker', 'id', speaker_id, 'HAS_SPEAKER')
            
            # Try to match to Persoon
            matched_persoon = None
            
            # First try from API activity actors if available
            if api_activity_id:
                matched_persoon = best_persoon_from_actors(v_first, v_last, session, api_activity_id)
            
            # Fallback to general search
            if not matched_persoon:
                matched_persoon = find_best_persoon(session, v_first, v_last)
            
            if matched_persoon:
                # Link to Persoon
                session.execute_write(merge_rel, 'VlosSpeaker', 'id', speaker_id,
                                      'Persoon', 'id', matched_persoon['id'], 'MATCHES_PERSOON')
                
                print(f"  ✅ Speaker: {v_first} {v_last} -> {matched_persoon['roepnaam']} {matched_persoon['achternaam']}")
            else:
                print(f"  ❌ Speaker: {v_first} {v_last} [NO MATCH]")


def process_vlos_zaken_for_activity(session, xml_act: ET.Element, vlos_activity_id: str):
    """Process zaken within a VLOS activity with enhanced fallback logic and speaker connections."""
    zaak_count = 0
    
    # Get all speakers in this activity for creating connections
    activity_speakers = session.run("""
        MATCH (va:VlosActivity {id: $activity_id})-[:HAS_SPEAKER]->(vs:VlosSpeaker)
        OPTIONAL MATCH (vs)-[:MATCHES_PERSOON]->(p:Persoon)
        RETURN vs.id as speaker_id, vs.voornaam as voornaam, vs.achternaam as achternaam,
               p.id as persoon_id, p.roepnaam as roepnaam, p.achternaam as persoon_achternaam
    """, activity_id=vlos_activity_id)
    
    activity_speaker_list = [dict(record) for record in activity_speakers]
    
    for xml_zaak in xml_act.findall(".//vlos:zaak", NS_VLOS):
        zaak_count += 1
        
        dossiernr = xml_zaak.findtext("vlos:dossiernummer", default="", namespaces=NS_VLOS).strip()
        stuknr = xml_zaak.findtext("vlos:stuknummer", default="", namespaces=NS_VLOS).strip()
        zaak_titel = xml_zaak.findtext("vlos:titel", default="", namespaces=NS_VLOS).strip()
        
        # Create VLOS zaak node
        zaak_id = f"vlos_zaak_{vlos_activity_id}_{zaak_count}"
        zaak_props = {
            'id': zaak_id,
            'dossiernummer': dossiernr,
            'stuknummer': stuknr,
            'titel': zaak_titel,
            'source': 'vlos_xml_enhanced'
        }
        
        session.execute_write(merge_node, 'VlosZaak', 'id', zaak_props)
        
        # Link to activity
        session.execute_write(merge_rel, 'VlosActivity', 'id', vlos_activity_id,
                              'VlosZaak', 'id', zaak_id, 'HAS_ZAAK')
        
        # Use enhanced matching with fallback logic
        match_result = find_best_zaak_or_fallback(session, dossiernr, stuknr)
        
        # Initialize variables for this zaak
        matched_object = None
        match_type = None
        zaak_label = None
        
        if match_result['success']:
            if match_result['match_type'] == 'zaak':
                matched_zaak = match_result['zaak']
                matched_object = matched_zaak
                match_type = 'zaak'
                zaak_label = f"{matched_zaak.get('soort', '')} {matched_zaak.get('nummer', '')}"
                
                # Link to Zaak
                session.execute_write(merge_rel, 'VlosZaak', 'id', zaak_id,
                                      'Zaak', 'id', matched_zaak['id'], 'MATCHES_API_ZAAK')
                
                print(f"  ✅ Zaak: {dossiernr}/{stuknr} -> {zaak_label}")
                
            elif match_result['match_type'] == 'dossier_fallback':
                matched_dossier = match_result['dossier']
                matched_object = matched_dossier
                match_type = 'dossier'
                zaak_label = f"Dossier {matched_dossier.get('nummer', '')}"
                if matched_dossier.get('toevoeging'):
                    zaak_label += f" {matched_dossier['toevoeging']}"
                zaak_label += " [FALLBACK]"
                
                # Link to Dossier
                session.execute_write(merge_rel, 'VlosZaak', 'id', zaak_id,
                                      'Dossier', 'id', matched_dossier['id'], 'RELATED_TO_DOSSIER')
                
                print(f"  ✅ Zaak (fallback to Dossier): {dossiernr}/{stuknr} -> {zaak_label}")
                
                # Also link to document if found
                if match_result['document']:
                    matched_doc = match_result['document']
                    session.execute_write(merge_rel, 'VlosZaak', 'id', zaak_id,
                                          'Document', 'id', matched_doc['id'], 'RELATED_TO_DOCUMENT')
                    
                    print(f"    ✅ Document: {stuknr} -> {matched_doc.get('nummer', '')}/{matched_doc.get('volgnummer', '')}")
            
            # Create speaker-zaak connections for all speakers in this activity
            for speaker_info in activity_speaker_list:
                speaker_id = speaker_info['speaker_id']
                persoon_id = speaker_info['persoon_id']
                
                # Create VlosSpeaker -> Zaak/Dossier connection
                if match_type == 'zaak':
                    session.execute_write(merge_rel, 'VlosSpeaker', 'id', speaker_id,
                                          'Zaak', 'id', matched_object['id'], 'SPOKE_ABOUT')
                elif match_type == 'dossier':
                    session.execute_write(merge_rel, 'VlosSpeaker', 'id', speaker_id,
                                          'Dossier', 'id', matched_object['id'], 'SPOKE_ABOUT')
                
                # Create Persoon -> Zaak/Dossier connection if persoon matched
                if persoon_id:
                    if match_type == 'zaak':
                        session.execute_write(merge_rel, 'Persoon', 'id', persoon_id,
                                              'Zaak', 'id', matched_object['id'], 'DISCUSSED')
                    elif match_type == 'dossier':
                        session.execute_write(merge_rel, 'Persoon', 'id', persoon_id,
                                              'Dossier', 'id', matched_object['id'], 'DISCUSSED')
            
            print(f"    🔗 Created {len(activity_speaker_list)} speaker-{match_type} connections")
            
        else:
            print(f"  ❌ Zaak: {dossiernr}/{stuknr} [NO MATCH - neither Zaak nor Dossier found]")
        
        # Process direct speaker links within this zaak element
        for sprek_el in xml_zaak.findall("vlos:sprekers/vlos:spreker", NS_VLOS):
            v_first = sprek_el.findtext("vlos:voornaam", default="", namespaces=NS_VLOS)
            v_last = (
                sprek_el.findtext("vlos:verslagnaam", default="", namespaces=NS_VLOS)
                or sprek_el.findtext("vlos:achternaam", default="", namespaces=NS_VLOS)
            )
            
            if not v_last:
                continue
            
            # Find matching persoon
            matched_persoon = find_best_persoon(session, v_first, v_last)
            
            if matched_persoon:
                person_display = f"{matched_persoon.get('roepnaam', '')} {matched_persoon.get('achternaam', '')}"
                print(f"    👤 Direct speaker link: {person_display}")
                
                # Create direct Persoon -> Zaak/Dossier connection if zaak/dossier matched
                if matched_object and match_type:
                    if match_type == 'zaak':
                        session.execute_write(merge_rel, 'Persoon', 'id', matched_persoon['id'],
                                              'Zaak', 'id', matched_object['id'], 'DISCUSSED_DIRECTLY')
                    elif match_type == 'dossier':
                        session.execute_write(merge_rel, 'Persoon', 'id', matched_persoon['id'],
                                              'Dossier', 'id', matched_object['id'], 'DISCUSSED_DIRECTLY')
            else:
                print(f"    ❌ Direct speaker link: {v_first} {v_last} [NO MATCH]")


def update_enhanced_zaak_statistics(session, vlos_activity_id: str):
    """Update enhanced zaak match statistics including fallback matches and speaker connections."""
    # Count total zaken in this activity
    total_zaken = session.run("""
        MATCH (va:VlosActivity {id: $activity_id})-[:HAS_ZAAK]->(vz:VlosZaak)
        RETURN count(vz) as total
    """, activity_id=vlos_activity_id).single()['total']
    
    # Count direct zaak matches
    direct_zaak_matches = session.run("""
        MATCH (va:VlosActivity {id: $activity_id})-[:HAS_ZAAK]->(vz:VlosZaak)
        WHERE EXISTS((vz)-[:MATCHES_API_ZAAK]->())
        RETURN count(vz) as matched
    """, activity_id=vlos_activity_id).single()['matched']
    
    # Count dossier fallback matches
    dossier_fallback_matches = session.run("""
        MATCH (va:VlosActivity {id: $activity_id})-[:HAS_ZAAK]->(vz:VlosZaak)
        WHERE EXISTS((vz)-[:RELATED_TO_DOSSIER]->()) 
          AND NOT EXISTS((vz)-[:MATCHES_API_ZAAK]->())
        RETURN count(vz) as matched
    """, activity_id=vlos_activity_id).single()['matched']
    
    # Count document matches
    document_matches = session.run("""
        MATCH (va:VlosActivity {id: $activity_id})-[:HAS_ZAAK]->(vz:VlosZaak)
        WHERE EXISTS((vz)-[:RELATED_TO_DOCUMENT]->())
        RETURN count(vz) as matched
    """, activity_id=vlos_activity_id).single()['matched']
    
    # Total successful matches = direct + fallback
    total_matched = direct_zaak_matches + dossier_fallback_matches
    
    # Count speaker-zaak connections
    speaker_zaak_connections = session.run("""
        MATCH (va:VlosActivity {id: $activity_id})-[:HAS_SPEAKER]->(vs:VlosSpeaker)
        WHERE EXISTS((vs)-[:SPOKE_ABOUT]->())
        RETURN count(vs) as connections
    """, activity_id=vlos_activity_id).single()['connections']
    
    # Count persoon-zaak connections
    persoon_zaak_connections = session.run("""
        MATCH (va:VlosActivity {id: $activity_id})-[:HAS_SPEAKER]->(vs:VlosSpeaker)-[:MATCHES_PERSOON]->(p:Persoon)
        WHERE EXISTS((p)-[:DISCUSSED]->()) OR EXISTS((p)-[:DISCUSSED_DIRECTLY]->())
        RETURN count(DISTINCT p) as connections
    """, activity_id=vlos_activity_id).single()['connections']
    
    return {
        'total_zaken': total_zaken,
        'direct_zaak_matches': direct_zaak_matches,
        'dossier_fallback_matches': dossier_fallback_matches,
        'document_matches': document_matches,
        'total_matched_zaken': total_matched,
        'zaak_match_rate': total_matched / total_zaken if total_zaken > 0 else 0.0,
        'speaker_zaak_connections': speaker_zaak_connections,
        'persoon_zaak_connections': persoon_zaak_connections
    }


def calculate_enhanced_vlos_statistics(session, document_id: str):
    """Calculate comprehensive statistics for enhanced VLOS processing including speaker-zaak connections."""
    stats = {}
    
    # Basic activity and speaker statistics
    basic_stats = session.run("""
        MATCH (doc:EnhancedVlosDocument {id: $doc_id})
        OPTIONAL MATCH (doc)-[:HAS_ACTIVITY]->(va:VlosActivity)
        OPTIONAL MATCH (va)-[:HAS_SPEAKER]->(vs:VlosSpeaker)
        OPTIONAL MATCH (vs)-[:MATCHES_PERSOON]->(p:Persoon)
        OPTIONAL MATCH (va)-[:MATCHES_API_ACTIVITY]->(api_act:Activiteit)
        RETURN 
            count(DISTINCT va) as total_activities,
            count(DISTINCT vs) as total_speakers,
            count(DISTINCT p) as matched_speakers,
            count(DISTINCT api_act) as matched_activities
    """, doc_id=document_id).single()
    
    stats.update(dict(basic_stats))
    
    # Enhanced zaak statistics with fallbacks
    zaak_stats = session.run("""
        MATCH (doc:EnhancedVlosDocument {id: $doc_id})-[:HAS_ACTIVITY]->(va:VlosActivity)
        OPTIONAL MATCH (va)-[:HAS_ZAAK]->(vz:VlosZaak)
        OPTIONAL MATCH (vz)-[:MATCHES_API_ZAAK]->(z:Zaak)
        OPTIONAL MATCH (vz)-[:RELATED_TO_DOSSIER]->(d:Dossier)
        WHERE NOT EXISTS((vz)-[:MATCHES_API_ZAAK]->())
        OPTIONAL MATCH (vz)-[:RELATED_TO_DOCUMENT]->(doc_rel:Document)
        RETURN 
            count(DISTINCT vz) as total_zaken,
            count(DISTINCT z) as direct_zaak_matches,
            count(DISTINCT d) as dossier_fallback_matches,
            count(DISTINCT doc_rel) as document_matches
    """, doc_id=document_id).single()
    
    stats.update(dict(zaak_stats))
    
    # Speaker-zaak connection statistics
    connection_stats = session.run("""
        MATCH (doc:EnhancedVlosDocument {id: $doc_id})-[:HAS_ACTIVITY]->(va:VlosActivity)
        OPTIONAL MATCH (va)-[:HAS_SPEAKER]->(vs:VlosSpeaker)
        OPTIONAL MATCH (vs)-[:SPOKE_ABOUT]->(target)
        OPTIONAL MATCH (vs)-[:MATCHES_PERSOON]->(p:Persoon)
        OPTIONAL MATCH (p)-[:DISCUSSED|DISCUSSED_DIRECTLY]->(discussion_target)
        RETURN 
            count(DISTINCT vs) as speakers_with_zaak_connections,
            count(DISTINCT target) as unique_zaken_discussed_by_speakers,
            count(DISTINCT p) as personen_with_zaak_connections,
            count(DISTINCT discussion_target) as unique_zaken_discussed_by_personen
    """, doc_id=document_id).single()
    
    stats.update(dict(connection_stats))
    
    # Calculate rates
    stats['activity_match_rate'] = stats['matched_activities'] / stats['total_activities'] if stats['total_activities'] > 0 else 0.0
    stats['speaker_match_rate'] = stats['matched_speakers'] / stats['total_speakers'] if stats['total_speakers'] > 0 else 0.0
    
    # Enhanced zaak match rate (including fallbacks)
    total_zaak_successes = stats['direct_zaak_matches'] + stats['dossier_fallback_matches']
    stats['total_zaak_successes'] = total_zaak_successes
    stats['zaak_match_rate'] = total_zaak_successes / stats['total_zaken'] if stats['total_zaken'] > 0 else 0.0
    
    # Speaker-zaak connection rates
    stats['speaker_zaak_connection_rate'] = stats['speakers_with_zaak_connections'] / stats['total_speakers'] if stats['total_speakers'] > 0 else 0.0
    stats['persoon_zaak_connection_rate'] = stats['personen_with_zaak_connections'] / stats['matched_speakers'] if stats['matched_speakers'] > 0 else 0.0
    
    return stats 