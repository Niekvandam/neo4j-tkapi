"""
Enhanced VLOS Matching Processor - Comprehensive parliamentary discourse analysis
Migrated from test_vlos_activity_matching_with_personen_and_zaken.py
"""

import xml.etree.ElementTree as ET
import time
from typing import Optional, Dict, List, Any, Tuple, Set
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import re

from utils.helpers import merge_node, merge_rel
from tkapi import TKApi
from tkapi.zaak import Zaak
from tkapi.vergadering import Vergadering, VergaderingSoort
from tkapi.activiteit import Activiteit, ActiviteitFilter
from tkapi.persoon import Persoon
from tkapi.stemming import Stemming
from tkapi.fractie import Fractie
from tkapi.besluit import Besluit
from tkapi.dossier import Dossier  # NEW – link <dossiernummer>
from tkapi.document import Document  # NEW – link <stuknummer> (volgnummer)
from thefuzz import fuzz

# XML namespaces
NS_VLOS = {'vlos': 'http://www.tweedekamer.nl/ggm/vergaderverslag/v1.0'}

# Local timezone offset (CEST = UTC+2)
LOCAL_TIMEZONE_OFFSET_HOURS = 2

# Scoring constants (from working test file)
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
TIME_START_PROXIMITY_TOLERANCE_SECONDS = 300
TIME_GENERAL_OVERLAP_BUFFER_SECONDS = 600
FUZZY_SIMILARITY_THRESHOLD_HIGH = 85
FUZZY_SIMILARITY_THRESHOLD_MEDIUM = 70
FUZZY_FIRSTNAME_THRESHOLD = 75

# Map frequent XML soorten to equivalent API soorten words
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

# Common topic prefixes to strip for better matching - FROM WORKING TEST FILE
COMMON_TOPIC_PREFIXES = [
    'tweeminutendebat',
    'procedurevergadering',
    'wetgevingsoverleg',
    'plenaire afronding',
    'plenaire afronding in 1 termijn',
    'plenaire afronding in één termijn',
    'plenaire afronding in een termijn',
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

_PREFIX_REGEX = re.compile(r'^(' + '|'.join(re.escape(p) for p in COMMON_TOPIC_PREFIXES) + r')[\s:,-]+', re.IGNORECASE)

# Dossier code regex for parsing dossier codes like '36725-VI'
_DOSSIER_REGEX = re.compile(r"^(\d+)(?:[-\s]?([A-Za-z0-9]+))?$")


# ===============================================================================
# CRITICAL MISSING FUNCTIONS - DOSSIER & DOCUMENT PROCESSING
# ===============================================================================

def _safe_int(val: str) -> Optional[int]:
    """Return int(val) if val represents an integer, else None."""
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _split_dossier_code(code: str) -> Tuple[Optional[int], Optional[str]]:
    """Return (nummer:int|None, toevoeging:str|None) for a dossier code like '36725-VI'."""
    m = _DOSSIER_REGEX.match(code.strip()) if code else None
    if not m:
        return None, None
    nummer = _safe_int(m.group(1))
    toevoeg = m.group(2) or None
    return nummer, toevoeg


def find_best_dossier(api: TKApi, dossier_code: str) -> Optional[Dossier]:
    """Find best matching Dossier by parsing dossier code."""
    num, toevoeg = _split_dossier_code(dossier_code or "")
    if num is None:
        return None
    df = Dossier.create_filter()
    df.filter_nummer(num)
    if toevoeg:
        df.filter_toevoeging(toevoeg)
    items = api.get_items(Dossier, filter=df, max_items=5)
    return items[0] if items else None


def find_best_document(api: TKApi, dossier_num: int, dossier_toevoeging: str, stuknummer: str) -> Optional[Document]:
    """Find best matching Document by dossier and stuknummer."""
    snr_int = _safe_int(stuknummer)
    if snr_int is None:
        return None
    df = Document.create_filter()
    df.filter_volgnummer(snr_int)
    # Narrow by dossier association
    if dossier_num:
        df.filter_dossier(dossier_num, dossier_toevoeging)
    docs = api.get_items(Document, filter=df, max_items=5)
    return docs[0] if docs else None


def find_best_zaak(api: TKApi, dossiernummer: str, stuknummer: str) -> Optional[Zaak]:
    """Retrieve a TK-API Zaak by dossier- and/or stuknummer (volgnummer).
    
    Uses the most restrictive combination of filters available.
    Returns the single best candidate (first hit) or None.
    """
    if not dossiernummer and not stuknummer:
        return None

    zf = Zaak.create_filter()

    dnr_int = _safe_int(dossiernummer)
    if dnr_int is not None:
        # Filter on Kamerstukdossier nummer
        zf.filter_kamerstukdossier(dnr_int)
    elif dossiernummer:
        # Fall back to generic Nummer filter (string equality)
        zf.filter_nummer(dossiernummer)

    snr_int = _safe_int(stuknummer)
    if snr_int is not None:
        # Prefer document/volgnummer (stuknummer) filter – narrower than volgnummer
        zf.filter_document(snr_int)
    elif stuknummer:
        zf.filter_volgnummer(stuknummer)

    candidates = api.get_items(Zaak, filter=zf, max_items=10)
    if not candidates:
        return None

    # If only one candidate, pick it. Otherwise favour exact dossier+stuk nummer combo.
    if len(candidates) == 1:
        return candidates[0]

    for z in candidates:
        if (dnr_int and _safe_int(z.dossier.nummer) == dnr_int) and (
            snr_int is None or _safe_int(z.volgnummer) == snr_int
        ):
            return z

    return candidates[0]


def find_best_zaak_or_fallback_enhanced(api: TKApi, dossiernummer: str, stuknummer: str) -> Dict[str, Any]:
    """Enhanced zaak finding with dossier fallback - EXACTLY FROM TEST FILE.
    
    Returns a dict with:
    - 'zaak': Zaak object if found, None otherwise
    - 'dossier': Dossier object if zaak not found but dossier exists
    - 'document': Document object if applicable
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
    zaak = find_best_zaak(api, dossiernummer, stuknummer)
    if zaak:
        result['zaak'] = zaak
        result['match_type'] = 'zaak'
        result['success'] = True
        return result
    
    # No specific Zaak found - try dossier fallback
    if dossiernummer:
        dossier = find_best_dossier(api, dossiernummer)
        if dossier:
            result['dossier'] = dossier
            result['match_type'] = 'dossier_fallback'
            result['success'] = True
            
            # Also try to find the document within this dossier
            if stuknummer:
                num, toevoeg = _split_dossier_code(dossiernummer)
                document = find_best_document(api, num, toevoeg, stuknummer)
                if document:
                    result['document'] = document
            
            return result
    
    return result


# ===============================================================================
# COMPREHENSIVE ANALYSIS FUNCTIONS - MISSING FROM ENHANCED MATCHER
# ===============================================================================

def analyze_interruption_patterns(all_interruptions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze interruption patterns to identify trends and key players - FROM TEST FILE."""
    if not all_interruptions:
        return {}
    
    # Who interrupts whom most
    interruption_pairs = {}
    for interruption in all_interruptions:
        # Handle different interruption types with different key structures
        if interruption['type'] == 'fragment_interruption':
            # Fragment interruptions have multiple speakers - treat as mutual interruptions
            speakers = interruption.get('speakers', [])
            for i, speaker1 in enumerate(speakers):
                for j, speaker2 in enumerate(speakers):
                    if i != j and speaker1.get('naam') and speaker2.get('naam'):
                        interrupter = speaker1['naam']
                        interrupted = speaker2['naam']
                        pair_key = f"{interrupter} → {interrupted}"
                        
                        if pair_key not in interruption_pairs:
                            interruption_pairs[pair_key] = {
                                'count': 0,
                                'interrupter': interrupter,
                                'interrupted': interrupted,
                                'topics': set(),
                                'examples': []
                            }
                        
                        interruption_pairs[pair_key]['count'] += 1
                        interruption_pairs[pair_key]['topics'].update(interruption.get('topics_discussed', []))
                        interruption_pairs[pair_key]['examples'].append(interruption)
        else:
            # Handle simple_interruption and interruption_with_response
            interrupter_info = interruption.get('interrupter', {})
            original_info = interruption.get('original_speaker', {})
            
            if interrupter_info.get('naam') and original_info.get('naam'):
                interrupter = interrupter_info['naam']
                interrupted = original_info['naam']
                pair_key = f"{interrupter} → {interrupted}"
                
                if pair_key not in interruption_pairs:
                    interruption_pairs[pair_key] = {
                        'count': 0,
                        'interrupter': interrupter,
                        'interrupted': interrupted,
                        'topics': set(),
                        'examples': []
                    }
                
                interruption_pairs[pair_key]['count'] += 1
                interruption_pairs[pair_key]['topics'].update(interruption.get('topics_discussed', []))
                interruption_pairs[pair_key]['examples'].append(interruption)
    
    # Most frequent interrupters
    interrupter_counts = {}
    interrupted_counts = {}
    
    for interruption in all_interruptions:
        if interruption['type'] == 'fragment_interruption':
            # Fragment interruptions have multiple speakers - count all as potential interrupters
            speakers = interruption.get('speakers', [])
            for speaker in speakers:
                if speaker.get('naam'):
                    speaker_name = speaker['naam']
                    interrupter_counts[speaker_name] = interrupter_counts.get(speaker_name, 0) + 1
                    interrupted_counts[speaker_name] = interrupted_counts.get(speaker_name, 0) + 1
        else:
            # Handle simple_interruption and interruption_with_response
            interrupter_info = interruption.get('interrupter', {})
            if interrupter_info.get('naam'):
                interrupter = interrupter_info['naam']
                interrupter_counts[interrupter] = interrupter_counts.get(interrupter, 0) + 1
            
            original_info = interruption.get('original_speaker', {})
            if original_info.get('naam'):
                interrupted = original_info['naam']
                interrupted_counts[interrupted] = interrupted_counts.get(interrupted, 0) + 1
    
    # Topics that generate most interruptions
    topic_interruption_counts = {}
    for interruption in all_interruptions:
        for topic in interruption.get('topics_discussed', []):
            if topic not in topic_interruption_counts:
                topic_interruption_counts[topic] = {
                    'count': 0,
                    'interruption_events': []
                }
            topic_interruption_counts[topic]['count'] += 1
            topic_interruption_counts[topic]['interruption_events'].append(interruption)
    
    # Response patterns (who responds to interruptions)
    response_patterns = {}
    for interruption in all_interruptions:
        if interruption['type'] == 'interruption_with_response':
            # The 'response' key contains the responder info
            responder_info = interruption.get('response', {})
            interrupter_info = interruption.get('interrupter', {})
            
            if responder_info.get('naam') and interrupter_info.get('naam'):
                responder = responder_info['naam']
                interrupter = interrupter_info['naam']
                response_key = f"{responder} responds to {interrupter}"
                
                if response_key not in response_patterns:
                    response_patterns[response_key] = {
                        'count': 0,
                        'responder': responder,
                        'interrupter': interrupter,
                        'topics': set()
                    }
                
                response_patterns[response_key]['count'] += 1
                response_patterns[response_key]['topics'].update(interruption.get('topics_discussed', []))
    
    return {
        'total_interruptions': len(all_interruptions),
        'interruption_pairs': dict(sorted(interruption_pairs.items(), key=lambda x: x[1]['count'], reverse=True)),
        'most_frequent_interrupters': dict(sorted(interrupter_counts.items(), key=lambda x: x[1], reverse=True)),
        'most_interrupted_speakers': dict(sorted(interrupted_counts.items(), key=lambda x: x[1], reverse=True)),
        'topics_causing_interruptions': dict(sorted(topic_interruption_counts.items(), key=lambda x: x[1]['count'], reverse=True)),
        'response_patterns': dict(sorted(response_patterns.items(), key=lambda x: x[1]['count'], reverse=True)),
        'interruption_types': {
            'fragment_interruptions': len([i for i in all_interruptions if i['type'] == 'fragment_interruption']),
            'simple_interruptions': len([i for i in all_interruptions if i['type'] == 'simple_interruption']),
            'interruptions_with_response': len([i for i in all_interruptions if i['type'] == 'interruption_with_response'])
        }
    }


def analyze_voting_patterns(all_voting_events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze voting patterns to identify political trends and party behaviors - FROM TEST FILE."""
    if not all_voting_events:
        return {}
    
    # Fractie voting behavior
    fractie_vote_counts = {}
    fractie_topic_votes = {}
    
    # Topic voting patterns
    topic_vote_patterns = {}
    
    # Vote type statistics
    vote_type_counts = {'voor': 0, 'tegen': 0, 'niet_deelgenomen': 0, 'onthouding': 0}
    
    for event in all_voting_events:
        topics = event.get('topics_discussed', [])
        
        for vote in event.get('fractie_votes', []):
            fractie = vote['fractie']
            vote_type = vote['vote_normalized']
            
            # Track overall fractie voting behavior
            if fractie not in fractie_vote_counts:
                fractie_vote_counts[fractie] = {'voor': 0, 'tegen': 0, 'onthouding': 0, 'niet_deelgenomen': 0, 'total': 0}
            
            if vote_type in fractie_vote_counts[fractie]:
                fractie_vote_counts[fractie][vote_type] += 1
            fractie_vote_counts[fractie]['total'] += 1
            
            # Track fractie votes on specific topics
            if fractie not in fractie_topic_votes:
                fractie_topic_votes[fractie] = {}
            
            for topic in topics:
                if topic not in fractie_topic_votes[fractie]:
                    fractie_topic_votes[fractie][topic] = {'voor': 0, 'tegen': 0, 'onthouding': 0}
                
                if vote_type in fractie_topic_votes[fractie][topic]:
                    fractie_topic_votes[fractie][topic][vote_type] += 1
            
            # Track topic voting patterns
            for topic in topics:
                if topic not in topic_vote_patterns:
                    topic_vote_patterns[topic] = {
                        'votes': {'voor': [], 'tegen': [], 'onthouding': []},
                        'consensus_level': 0,
                        'total_votes': 0
                    }
                
                if vote_type in topic_vote_patterns[topic]['votes']:
                    topic_vote_patterns[topic]['votes'][vote_type].append(fractie)
                topic_vote_patterns[topic]['total_votes'] += 1
            
            # Overall vote type counting
            if vote_type in vote_type_counts:
                vote_type_counts[vote_type] += 1
    
    # Calculate consensus levels for topics
    for topic, data in topic_vote_patterns.items():
        total = data['total_votes']
        if total > 0:
            voor_count = len(data['votes']['voor'])
            tegen_count = len(data['votes']['tegen'])
            
            # Consensus level: percentage of majority vote
            majority_count = max(voor_count, tegen_count)
            data['consensus_level'] = (majority_count / total) * 100
    
    # Calculate fractie alignment (how often they vote with majority)
    fractie_alignment = {}
    for fractie in fractie_vote_counts.keys():
        total_votes = fractie_vote_counts[fractie]['total']
        alignment_score = 0  # This would need more complex calculation across all votes
        fractie_alignment[fractie] = {
            'total_votes': total_votes,
            'voor_percentage': (fractie_vote_counts[fractie]['voor'] / total_votes * 100) if total_votes > 0 else 0,
            'tegen_percentage': (fractie_vote_counts[fractie]['tegen'] / total_votes * 100) if total_votes > 0 else 0
        }
    
    return {
        'total_voting_events': len(all_voting_events),
        'total_individual_votes': sum(len(event.get('fractie_votes', [])) for event in all_voting_events),
        'fractie_vote_counts': dict(sorted(fractie_vote_counts.items(), key=lambda x: x[1]['total'], reverse=True)),
        'fractie_alignment': dict(sorted(fractie_alignment.items(), key=lambda x: x[1]['voor_percentage'], reverse=True)),
        'topic_vote_patterns': dict(sorted(topic_vote_patterns.items(), key=lambda x: x[1]['consensus_level'], reverse=True)),
        'vote_type_distribution': vote_type_counts,
        'most_controversial_topics': dict(sorted(
            {k: v for k, v in topic_vote_patterns.items() if v['consensus_level'] < 80}.items(),
            key=lambda x: x[1]['consensus_level']
        )),
        'unanimous_topics': dict(sorted(
            {k: v for k, v in topic_vote_patterns.items() if v['consensus_level'] >= 95}.items(),
            key=lambda x: x[1]['total_votes'], reverse=True
        ))
    }


# ===============================================================================
# EXISTING FUNCTIONS CONTINUE BELOW...
# ===============================================================================

def normalize_topic(text: str) -> str:
    """Lower-case, strip, and remove common boilerplate prefixes for fair fuzzy matching - FROM WORKING TEST FILE"""
    if not text:
        return ''
    text = text.strip().lower()
    # remove prefix once
    text = _PREFIX_REGEX.sub('', text, count=1)
    # collapse whitespace
    text = re.sub(r'\s+', ' ', text)
    return text


def collapse_text(elem: ET.Element) -> str:
    """Return all inner text of an XML element, collapsed to single-spaced string."""
    texts: List[str] = []
    for t in elem.itertext():
        t = t.strip()
        if t:
            texts.append(t)
    return " ".join(texts)


# ===============================================================================
# MISSING FUNCTIONS FROM WORKING TEST FILE - ADD THESE TO ENSURE HIGH MATCH RATES
# ===============================================================================

def _build_full_surname(p: Persoon) -> str:
    """Return full surname including tussenvoegsel (if any) - FROM WORKING TEST FILE"""
    full = f"{p.tussenvoegsel} {p.achternaam}".strip()
    return re.sub(r"\s+", " ", full).lower()


def calc_name_similarity(v_first: str, v_last: str, p: Persoon) -> int:
    """Enhanced name similarity calculation with tussenvoegsel - FROM WORKING TEST FILE"""
    score = 0

    if not (v_last and p.achternaam):
        return score

    v_last_lower = v_last.lower()

    bare_surname = p.achternaam.lower()
    full_surname = _build_full_surname(p)

    # Pick best of bare vs full surname similarity
    ratio_bare = fuzz.ratio(v_last_lower, bare_surname)
    ratio_full = fuzz.ratio(v_last_lower, full_surname)
    best_ratio = max(ratio_bare, ratio_full)

    # Exact match on either variant → big boost
    if v_last_lower in [bare_surname, full_surname]:
        score += 60
    else:
        # Convert fuzzy similarity to 0-60 scale (same logic as earlier: dampen by 20)
        score += max(best_ratio - 20, 0)

    # ---------------------------------------------
    # Firstname / roepnaam boost (unchanged logic)
    # ---------------------------------------------
    v_first_lower = (v_first or "").lower()
    if v_first_lower:
        first_candidates = [c for c in [getattr(p, "roepnaam", None), getattr(p, "voornamen", None)] if c]
        best_first = max((fuzz.ratio(v_first_lower, fc.lower()) for fc in first_candidates), default=0)
        if best_first >= FUZZY_FIRSTNAME_THRESHOLD:
            score += 40
        elif best_first >= 60:
            score += 20

    return min(score, 100)  # cap


def collapse_text(tekst_el: ET.Element) -> str:
    """Extract and collapse text from a VLOS tekst element - FROM WORKING TEST FILE"""
    if tekst_el is None:
        return ""
    
    # Get all text content, preserving structure
    full_text = ET.tostring(tekst_el, encoding='unicode', method='text')
    
    if not full_text:
        return ""
    
    # Clean up whitespace
    full_text = re.sub(r'\s+', ' ', full_text.strip())
    
    return full_text


def parse_xml_datetime(datetime_val):
    """Parse XML datetime string - FROM WORKING TEST FILE"""
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
            # Try tkapi utility as fallback
            from tkapi.util import util as tkapi_util
            return tkapi_util.odatedatetime_to_datetime(dt_str)
        except Exception:
            return None


def get_utc_datetime(dt_obj, local_offset_hours):
    """Convert datetime to UTC - FROM WORKING TEST FILE"""
    if not dt_obj:
        return None
    if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
        return (dt_obj - timedelta(hours=local_offset_hours)).replace(tzinfo=timezone.utc)
    return dt_obj.astimezone(timezone.utc)


def evaluate_time_match(xml_start, xml_end, api_start, api_end):
    """Evaluate time match between XML and API activities - FROM WORKING TEST FILE"""
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


def find_best_persoon(v_first: str, v_last: str) -> Optional[Persoon]:
    """Enhanced Persoon finder - EXACTLY FROM WORKING TEST FILE"""
    if not v_last:
        return None

    # Create API instance outside session context (like in test file)
    api = TKApi(verbose=False)
    
    # Search by exact achternaam to limit results
    pf = Persoon.create_filter()
    safe_last = v_last.replace("'", "''")
    pf.add_filter_str(f"Achternaam eq '{safe_last}'")

    candidates = api.get_items(Persoon, filter=pf, max_items=100)
    if candidates:
        best_p = None
        best_sc = 0
        for p in candidates:
            s = calc_name_similarity(v_first, v_last, p)
            if s > best_sc:
                best_sc = s
                best_p = p
        if best_sc >= 60:
            return best_p

    # Fallback: search by *contains* main surname token (last word of v_last)
    main_last_token = v_last.strip().split()[-1]
    pf = Persoon.create_filter()
    safe_last = main_last_token.replace("'", "''")
    pf.add_filter_str(f"contains(tolower(Achternaam), '{safe_last.lower()}')")

    candidates = api.get_items(Persoon, filter=pf, max_items=100)
    if not candidates:
        return None

    best_p = None
    best_sc = 0
    for p in candidates:
        s = calc_name_similarity(v_first, v_last, p)
        if s > best_sc:
            best_sc = s
            best_p = p

    return best_p if best_sc >= 60 else None


def find_fractie_by_name(fractie_name: str) -> Optional[Fractie]:
    """Find matching Fractie by name using TKApi"""
    if not fractie_name:
        return None
    
    # Create API instance outside session context
    api = TKApi(verbose=False)
    
    # Search for exact name match first
    ff = Fractie.create_filter()
    safe_name = fractie_name.replace("'", "''")
    ff.add_filter_str(f"Naam eq '{safe_name}'")
    
    candidates = api.get_items(Fractie, filter=ff, max_items=10)
    if candidates:
        return candidates[0]
    
    # Try fuzzy matching with contains
    ff2 = Fractie.create_filter()
    ff2.add_filter_str(f"contains(tolower(Naam), '{safe_name.lower()}')")
    
    fuzzy_candidates = api.get_items(Fractie, filter=ff2, max_items=20)
    if fuzzy_candidates:
        # Pick best fuzzy match
        best_match = None
        best_ratio = 0
        for fractie in fuzzy_candidates:
            ratio = fuzz.ratio(fractie_name.lower(), fractie.naam.lower())
            if ratio > best_ratio and ratio >= 70:  # Minimum fuzzy threshold
                best_ratio = ratio
                best_match = fractie
        return best_match
    
    return None


def find_stemmingen_for_voting_event(voting_event: Dict[str, Any], related_zaak_ids: List[str]) -> List[Stemming]:
    """Find matching Stemming records for a VLOS voting event"""
    
    if not voting_event.get('fractie_votes'):
        return []
    
    # Create API instance outside session context
    api = TKApi(verbose=False)
    
    matched_stemmingen = []
    
    # Try to find stemmingen for each fractie vote
    for vote in voting_event['fractie_votes']:
        fractie_name = vote['fractie']
        vote_type = vote['vote_normalized']  # 'voor', 'tegen', etc.
        
        # Find the fractie first
        fractie = find_fractie_by_name(fractie_name)
        if not fractie:
            print(f"    ⚠️ Could not find fractie: {fractie_name}")
            continue
        
        # Search for Stemming records for this fractie
        sf = Stemming.create_filter()
        sf.filter_fractie(fractie.id)
        
        # If we have related zaak IDs, try to filter by them
        if related_zaak_ids:
            # This is more complex as we need to go through Besluit->Zaak relationship
            # For now, let's just get all stemmingen for the fractie and filter later
            pass
        
        stemmingen = api.get_items(Stemming, filter=sf, max_items=50)
        
        # Filter by vote type and timeframe if possible
        for stemming in stemmingen:
            # Check if the vote type matches
            if stemming.soort and vote_type in stemming.soort.lower():
                # Additional checks could include timing, related zaak, etc.
                matched_stemmingen.append({
                    'stemming': stemming,
                    'vlos_vote': vote,
                    'match_confidence': 0.8  # Could be improved with more sophisticated matching
                })
                print(f"    ✅ Matched stemming: {fractie_name} voted {vote_type} → Stemming {stemming.id}")
                break
    
    return matched_stemmingen


def best_persoon_from_actors(first: str, last: str, actors) -> Optional[Persoon]:
    """Pick the actor.persoon with highest similarity ≥60 - FROM WORKING TEST FILE"""
    best: Optional[Persoon] = None
    best_score = 0
    for a in actors or []:
        p = getattr(a, "persoon", None)
        if not p:
            continue
        s = calc_name_similarity(first, last, p)
        if s > best_score:
            best_score = s
            best = p
    return best if best_score >= 60 else None


def find_best_zaak_from_api(topics: List[str]) -> Optional[Tuple[str, str, bool, object]]:
    """🚀 API-FIRST: Find best matching Zaak from TK API directly, bypassing database issues"""
    
    if not topics:
        return None
    
    # Create API instance outside session context to avoid conflicts
    api = TKApi(verbose=False)
    
    for topic in topics:
        topic_normalized = normalize_topic(topic)
        if len(topic_normalized) < 3:
            continue
        
        print(f"🔍 DEBUG: Searching TK API for topic: '{topic_normalized[:50]}...'")
        
        try:
            # Strategy 1: Direct onderwerp filtering using built-in filter method
            try:
                filter_obj = Zaak.create_filter()
                filter_obj.filter_onderwerp(topic_normalized)
                zaken = api.get_zaken(filter=filter_obj)
                
                if zaken:
                    best_zaak = zaken[0]  # Take first match
                    print(f"    ✅ Found zaak via exact onderwerp: {best_zaak.nummer} - {best_zaak.onderwerp[:50]}...")
                    return (best_zaak.nummer, best_zaak.nummer, False, best_zaak)
            except Exception as e:
                print(f"    ⚠️ Exact onderwerp search failed: {e}")
            
            # Strategy 2: Search by nummer if topic looks like a zaak number
            if re.match(r'^\d{4}Z\d{5}$', topic.strip()):
                try:
                    filter_obj = Zaak.create_filter()
                    filter_obj.filter_nummer(topic.strip())
                    zaken = api.get_zaken(filter=filter_obj)
                    
                    if zaken:
                        best_zaak = zaken[0]
                        print(f"    ✅ Found zaak via nummer: {best_zaak.nummer}")
                        return (best_zaak.nummer, best_zaak.nummer, False, best_zaak)
                except Exception as e:
                    print(f"    ⚠️ Nummer search failed: {e}")
            
            # Strategy 3: Try partial onderwerp matches with keywords
            keywords = [word for word in topic_normalized.split() if len(word) >= 4]
            for keyword in keywords[:3]:  # Try up to 3 keywords
                try:
                    filter_obj = Zaak.create_filter()
                    filter_obj.filter_onderwerp(keyword)  # Use built-in method
                    zaken = api.get_zaken(filter=filter_obj)
                    
                    if zaken:
                        best_zaak = zaken[0]
                        print(f"    ✅ Found zaak via keyword '{keyword}': {best_zaak.nummer}")
                        return (best_zaak.nummer, best_zaak.nummer, False, best_zaak)
                except Exception as e:
                    print(f"    ⚠️ Keyword search failed for '{keyword}': {e}")
                    continue
                        
        except Exception as e:
            print(f"    ⚠️ API search error for topic '{topic_normalized[:30]}...': {e}")
            continue
    
    print(f"    ❌ No zaak found in API for any topics")
    return None


def create_or_update_zaak_from_api(session, zaak_obj) -> str:
    """🚀 Create or update Zaak + relationships in Neo4j from fresh TK API data"""
    
    print(f"      🏗️ Creating/updating Zaak {zaak_obj.nummer} from API...")
    
    # Create/update the main Zaak node with fresh API data
    zaak_props = {
        'id': zaak_obj.id,
        'nummer': zaak_obj.nummer,
        'onderwerp': zaak_obj.onderwerp,
        'afgedaan': zaak_obj.afgedaan,
        'volgnummer': zaak_obj.volgnummer,
        'alias': zaak_obj.alias,
        'gestart_op': str(zaak_obj.gestart_op) if zaak_obj.gestart_op else None,
        'soort': zaak_obj.soort.value if zaak_obj.soort else None,
        'kabinetsappreciatie': zaak_obj.kabinetsappreciatie.value if zaak_obj.kabinetsappreciatie else None,
        'data_source': 'tk_api_fresh',  # Mark as fresh from API
        'last_updated': str(datetime.now())
    }
    session.execute_write(merge_node, 'Zaak', 'nummer', zaak_props)
    
    # Create related Agendapunten with relationships (API authoritative)
    try:
        agendapunten = zaak_obj.agendapunten
        print(f"        📋 Creating {len(agendapunten)} agendapunten...")
        
        for agendapunt in agendapunten:
            if agendapunt and agendapunt.id:
                agendapunt_props = {
                    'id': agendapunt.id,
                    'onderwerp': getattr(agendapunt, 'onderwerp', ''),
                    'nummer': getattr(agendapunt, 'nummer', None),
                    'volgorde': getattr(agendapunt, 'volgorde', None),
                    'data_source': 'tk_api_fresh',
                    'last_updated': str(datetime.now())
                }
                session.execute_write(merge_node, 'Agendapunt', 'id', agendapunt_props)
                session.execute_write(merge_rel, 'Zaak', 'nummer', zaak_obj.nummer,
                                      'Agendapunt', 'id', agendapunt.id, 'HAS_AGENDAPUNT')
    except Exception as e:
        print(f"        ⚠️ Error creating agendapunten: {e}")
    
    # Create related Activiteiten with relationships (API authoritative)
    try:
        activiteiten = zaak_obj.activiteiten
        print(f"        🎯 Creating {len(activiteiten)} activiteiten...")
        
        for activiteit in activiteiten:
            if activiteit and activiteit.id:
                activiteit_props = {
                    'id': activiteit.id,
                    'onderwerp': getattr(activiteit, 'onderwerp', ''),
                    'nummer': getattr(activiteit, 'nummer', None),
                    'soort': str(getattr(activiteit, 'soort', '')),
                    'begin': str(getattr(activiteit, 'begin', None)) if getattr(activiteit, 'begin', None) else None,
                    'einde': str(getattr(activiteit, 'einde', None)) if getattr(activiteit, 'einde', None) else None,
                    'data_source': 'tk_api_fresh',
                    'last_updated': str(datetime.now())
                }
                session.execute_write(merge_node, 'Activiteit', 'id', activiteit_props)
                session.execute_write(merge_rel, 'Zaak', 'nummer', zaak_obj.nummer,
                                      'Activiteit', 'id', activiteit.id, 'HAS_ACTIVITEIT')
    except Exception as e:
        print(f"        ⚠️ Error creating activiteiten: {e}")
    
    print(f"      ✅ Successfully created/updated Zaak {zaak_obj.nummer} with all relationships")
    return zaak_obj.nummer


def find_best_zaak_or_fallback(session, topics: List[str], fallback_dossier_ids: List[str] = None) -> Optional[Tuple[str, str, bool]]:
    """Find best matching zaak or fallback to dossier with enhanced logic"""
    
    for topic in topics:
        if not topic:
            continue
            
        topic_normalized = normalize_topic(topic)
        
        # Strategy 1: Exact zaak nummer match
        zaak_match = session.run("""
            MATCH (z:Zaak)
            WHERE (z.onderwerp IS NOT NULL AND toLower(toString(z.onderwerp)) CONTAINS toLower($topic))
            OR (z.nummer IS NOT NULL AND toString(z.nummer) CONTAINS $topic)
            RETURN z.nummer as id, z.nummer as nummer, z.onderwerp as onderwerp
            ORDER BY size(toString(coalesce(z.onderwerp, ''))) ASC
            LIMIT 1
        """, topic=topic_normalized).single()
        
        if zaak_match:
            return zaak_match['id'], zaak_match['nummer'], False
        
        # Strategy 2: Keyword-based zaak search
        if len(topic_normalized) > 10:  # Only for substantial topics
            keywords = [word for word in topic_normalized.split() if len(word) > 3]
            if keywords:
                keyword_query = " AND ".join([f"toLower(z.onderwerp) CONTAINS toLower('{word}')" 
                                            for word in keywords[:3]])  # Max 3 keywords
                
                zaak_keyword_match = session.run(f"""
                    MATCH (z:Zaak)
                    WHERE z.onderwerp IS NOT NULL AND ({keyword_query})
                    RETURN z.nummer as id, z.nummer as nummer, z.onderwerp as onderwerp
                    ORDER BY size(toString(coalesce(z.onderwerp, ''))) ASC
                    LIMIT 1
                """).single()
                
                if zaak_keyword_match:
                    return zaak_keyword_match['id'], zaak_keyword_match['nummer'], False
    
    # Strategy 3: Dossier fallback
    if fallback_dossier_ids:
        for dossier_id in fallback_dossier_ids:
            dossier_match = session.run("""
                MATCH (d:Dossier {id: $dossier_id})
                RETURN d.id as id, d.nummer as nummer
            """, dossier_id=dossier_id).single()
            
            if dossier_match:
                return dossier_match['id'], dossier_match['nummer'], True
    
    return None


def match_vlos_speakers_to_personen(session) -> int:
    """Match VLOS speakers to Persoon nodes with enhanced logic"""
    
    # Get all unmatched VLOS speakers - using correct field names from VlosSpeaker
    vlos_speakers = session.run("""
        MATCH (vs:VlosSpeaker)
        WHERE NOT EXISTS((vs)-[:MATCHED_TO_PERSOON]->(:Persoon))
        RETURN vs.id as vlos_id, vs.naam as naam, vs.voornaam as voornaam, 
               vs.tussenvoegsel as tussenvoegsel, vs.achternaam as achternaam,
               vs.verslagnaam as verslagnaam
    """).data()
    
    matched_count = 0
    
    for speaker in vlos_speakers:
        vlos_id = speaker['vlos_id']
        naam = speaker.get('naam', '')
        voornaam = speaker.get('voornaam', '')
        tussenvoegsel = speaker.get('tussenvoegsel', '')
        achternaam = speaker.get('achternaam', '')
        verslagnaam = speaker.get('verslagnaam', '')
        
        # Use verslagnaam as primary last name, fallback to achternaam
        effective_achternaam = verslagnaam if verslagnaam else achternaam
        
        print(f"🔍 DEBUG: Matching speaker - naam: '{naam}', voornaam: '{voornaam}', achternaam: '{effective_achternaam}'")
        
        # Strategy 1: Exact full name match
        if naam:
            persoon_match = session.run("""
                MATCH (p:Persoon)
                WHERE toLower(p.roepnaam + ' ' + coalesce(p.tussenvoegsel, '') + ' ' + p.achternaam) = toLower($naam)
                OR toLower(p.voornaam + ' ' + coalesce(p.tussenvoegsel, '') + ' ' + p.achternaam) = toLower($naam)
                RETURN p.id as persoon_id, p.roepnaam, p.achternaam
                LIMIT 1
            """, naam=naam.strip()).single()
            
            if persoon_match:
                roepnaam = persoon_match.get('roepnaam', '') or ''
                achternaam = persoon_match.get('achternaam', '') or ''
                print(f"✅ Matched by full name: {naam} → {roepnaam} {achternaam}")
                session.run("""
                    MATCH (vs:VlosSpeaker {id: $vlos_id})
                    MATCH (p:Persoon {id: $persoon_id})
                    MERGE (vs)-[:MATCHED_TO_PERSOON]->(p)
                """, vlos_id=vlos_id, persoon_id=persoon_match['persoon_id'])
                matched_count += 1
                continue
        
        # Strategy 2: Component-based matching using voornaam (not roepnaam)
        if voornaam and effective_achternaam:
            # Try matching with voornaam first
            query_parts = ["(toLower(p.roepnaam) = toLower($voornaam) OR toLower(p.voornaam) = toLower($voornaam))"]
            query_params = {"voornaam": voornaam, "achternaam": effective_achternaam}
            
            if tussenvoegsel:
                query_parts.append("toLower(coalesce(p.tussenvoegsel, '')) = toLower($tussenvoegsel)")
                query_params["tussenvoegsel"] = tussenvoegsel
            
            query_parts.append("toLower(p.achternaam) = toLower($achternaam)")
            
            persoon_match = session.run(f"""
                MATCH (p:Persoon)
                WHERE {' AND '.join(query_parts)}
                RETURN p.id as persoon_id, p.roepnaam, p.voornaam, p.achternaam
                LIMIT 1
            """, **query_params).single()
            
            if persoon_match:
                roepnaam = persoon_match.get('roepnaam', '') or ''
                achternaam = persoon_match.get('achternaam', '') or ''
                print(f"✅ Matched by components: {voornaam} {effective_achternaam} → {roepnaam} {achternaam}")
                session.run("""
                    MATCH (vs:VlosSpeaker {id: $vlos_id})
                    MATCH (p:Persoon {id: $persoon_id})
                    MERGE (vs)-[:MATCHED_TO_PERSOON]->(p)
                """, vlos_id=vlos_id, persoon_id=persoon_match['persoon_id'])
                matched_count += 1
                continue
        
        # Strategy 3: Fuzzy matching for common cases
        if effective_achternaam:
            # Try matching just by last name and see if there's a unique match
            persoon_matches = session.run("""
                MATCH (p:Persoon)
                WHERE toLower(p.achternaam) = toLower($achternaam)
                RETURN p.id as persoon_id, p.roepnaam, p.voornaam, p.achternaam
                LIMIT 3
            """, achternaam=effective_achternaam).data()
            
            if len(persoon_matches) == 1:
                # Unique match by last name
                persoon_match = persoon_matches[0]
                roepnaam = persoon_match.get('roepnaam', '') or ''
                achternaam = persoon_match.get('achternaam', '') or ''
                print(f"✅ Matched by unique last name: {effective_achternaam} → {roepnaam} {achternaam}")
                session.run("""
                    MATCH (vs:VlosSpeaker {id: $vlos_id})
                    MATCH (p:Persoon {id: $persoon_id})
                    MERGE (vs)-[:MATCHED_TO_PERSOON]->(p)
                """, vlos_id=vlos_id, persoon_id=persoon_match['persoon_id'])
                matched_count += 1
                continue
        
        print(f"❌ No match found for speaker: {naam}")
    
    return matched_count


def detect_interruptions_in_activity(activity_elem: ET.Element) -> List[Dict[str, Any]]:
    """Detect interruption patterns within a VLOS activity"""
    
    interruptions = []
    
    # Find all draadboekfragments (speech sections)
    fragments = activity_elem.findall('.//vlos:draadboekfragment', NS_VLOS)
    
    for fragment in fragments:
        speakers_in_fragment = []
        
        # Collect all speakers in this fragment
        for spreker_elem in fragment.findall('.//vlos:spreker', NS_VLOS):
            # Use proper XML text elements, not attributes
            v_first = spreker_elem.findtext("vlos:voornaam", default="", namespaces=NS_VLOS)
            v_last = (
                spreker_elem.findtext("vlos:verslagnaam", default="", namespaces=NS_VLOS)
                or spreker_elem.findtext("vlos:achternaam", default="", namespaces=NS_VLOS)
            )
            speaker_name = f"{v_first} {v_last}".strip()
            if speaker_name:
                speakers_in_fragment.append({
                    'naam': speaker_name,
                    'element': spreker_elem
                })
        
        # Check for interruption patterns
        if len(speakers_in_fragment) > 1:
            # Fragment interruption: Multiple speakers in same fragment
            interruptions.append({
                'type': 'fragment_interruption',
                'speakers': speakers_in_fragment,
                'fragment': fragment
            })
    
    # Check for sequential interruptions across fragments
    all_speakers = []
    for fragment in fragments:
        for spreker_elem in fragment.findall('.//vlos:spreker', NS_VLOS):
            # Use proper XML text elements, not attributes
            v_first = spreker_elem.findtext("vlos:voornaam", default="", namespaces=NS_VLOS)
            v_last = (
                spreker_elem.findtext("vlos:verslagnaam", default="", namespaces=NS_VLOS)
                or spreker_elem.findtext("vlos:achternaam", default="", namespaces=NS_VLOS)
            )
            speaker_name = f"{v_first} {v_last}".strip()
            if speaker_name:
                all_speakers.append({
                    'naam': speaker_name,
                    'fragment': fragment,
                    'element': spreker_elem
                })
    
    # Detect simple interruptions (A → B)
    for i in range(len(all_speakers) - 1):
        current = all_speakers[i]
        next_speaker = all_speakers[i + 1]
        
        if current['naam'] != next_speaker['naam']:
            interruptions.append({
                'type': 'simple_interruption',
                'original_speaker': current,
                'interrupter': next_speaker
            })
            
            # Check for response (A → B → A)
            if i + 2 < len(all_speakers):
                after_next = all_speakers[i + 2]
                if after_next['naam'] == current['naam']:
                    interruptions.append({
                        'type': 'interruption_with_response',
                        'original_speaker': current,
                        'interrupter': next_speaker,
                        'response': after_next
                    })
    
    return interruptions


def analyze_voting_in_activity(activity_elem: ET.Element, related_zaak_ids: List[str] = None) -> List[Dict[str, Any]]:
    """Analyze voting patterns in VLOS activity element and link to API Stemming records"""
    
    voting_events = []
    
    # Find besluit items with voting data
    besluit_items = activity_elem.findall('.//vlos:activiteititem[@soort="Besluit"]', NS_VLOS)
    
    for besluit in besluit_items:
        # Get besluit metadata
        besluit_titel = besluit.findtext('.//vlos:titel', default='', namespaces=NS_VLOS)
        besluit_uitslag = besluit.findtext('.//vlos:uitslag', default='', namespaces=NS_VLOS)
        
        # Look for voting sections
        stemmingen_elem = besluit.find('.//vlos:stemmingen', NS_VLOS)
        
        if stemmingen_elem is not None:
            fractie_votes = []
            
            # Extract individual fractie votes
            for stemming in stemmingen_elem.findall('.//vlos:stemming', NS_VLOS):
                fractie_naam = stemming.get('fractie', 'Unknown')
                stem_waarde = stemming.get('stemming', 'Unknown')
                
                fractie_votes.append({
                    'fractie': fractie_naam,
                    'vote': stem_waarde,
                    'vote_normalized': stem_waarde.lower()
                })
            
            if fractie_votes:
                # Calculate consensus level
                total_votes = len(fractie_votes)
                voor_votes = len([v for v in fractie_votes if v['vote_normalized'] == 'voor'])
                tegen_votes = len([v for v in fractie_votes if v['vote_normalized'] == 'tegen'])
                
                consensus_percentage = (voor_votes / total_votes * 100) if total_votes > 0 else 0
                
                voting_event = {
                    'type': 'fractie_voting',
                    'titel': besluit_titel,
                    'uitslag': besluit_uitslag,
                    'besluit_element': besluit,
                    'fractie_votes': fractie_votes,
                    'total_votes': total_votes,
                    'voor_votes': voor_votes,
                    'tegen_votes': tegen_votes,
                    'consensus_percentage': consensus_percentage,
                    'is_unanimous': consensus_percentage >= 95,
                    'is_controversial': consensus_percentage < 80,
                    'vote_breakdown': {}
                }
                
                # Calculate vote breakdown
                for vote in fractie_votes:
                    vote_type = vote['vote_normalized']
                    if vote_type not in voting_event['vote_breakdown']:
                        voting_event['vote_breakdown'][vote_type] = []
                    voting_event['vote_breakdown'][vote_type].append(vote['fractie'])
                
                # 🚀 NEW: Try to find matching API Stemming records
                try:
                    print(f"    🔍 Looking for API Stemming records for voting event: {besluit_titel}")
                    matched_stemmingen = find_stemmingen_for_voting_event(voting_event, related_zaak_ids or [])
                    voting_event['matched_stemmingen'] = matched_stemmingen
                    voting_event['api_matches'] = len(matched_stemmingen)
                    
                    if matched_stemmingen:
                        print(f"    ✅ Found {len(matched_stemmingen)} matching API Stemming records")
                    else:
                        print(f"    ❌ No matching API Stemming records found")
                        
                except Exception as e:
                    print(f"    ⚠️ Error looking for API Stemming records: {e}")
                    voting_event['matched_stemmingen'] = []
                    voting_event['api_matches'] = 0
                
                voting_events.append(voting_event)
    
    return voting_events


def process_enhanced_vlos_activity(session, activity_elem: ET.Element, api_activities: List[Dict[str, Any]], 
                                  canonical_vergadering_id: str, activity_speakers: Dict[str, List[str]], 
                                  activity_zaken: Dict[str, List[str]], interruption_events: List[Dict[str, Any]], 
                                  voting_events: List[Dict[str, Any]]) -> Optional[str]:
    """Process a single VLOS activity with comprehensive analysis"""
    
    # Extract activity metadata
    activity_objectid = activity_elem.get('objectid', f"vlos_activity_{hash(ET.tostring(activity_elem))}")
    activity_soort = activity_elem.get('soort', 'Unknown')
    activity_startdate = activity_elem.get('startdate')
    activity_enddate = activity_elem.get('enddate')
    
    # Get activity title
    activity_title = activity_elem.findtext('.//vlos:titel', default='', namespaces=NS_VLOS)
    
    # Skip procedural activities that don't have meaningful API counterparts
    if (activity_soort.lower() in ['opening', 'sluiting'] or 
        'opening' in activity_title.lower() or 'sluiting' in activity_title.lower()):
        print(f"  ⏭️ Skipping procedural activity: '{activity_soort}' - '{activity_title}'")
        return None
    
    print(f"🔄 Processing VLOS activity: {activity_objectid}")
    print(f"  📋 Soort: {activity_soort}, Title: {activity_title}")
    
    # Find best matching API activity
    best_match_score = 0.0
    best_api_activity = None
    
    # Use EXACT SAME MATCHING LOGIC as working test file
    for api_activity in api_activities:
        score = 0.0
        reasons = []
        
        # Parse activity times
        xml_start = parse_xml_datetime(activity_startdate)
        xml_end = parse_xml_datetime(activity_enddate)
        
        # Time matching - parse API activity times
        api_start = parse_xml_datetime(api_activity.get('begin')) if api_activity.get('begin') else None
        api_end = parse_xml_datetime(api_activity.get('einde')) if api_activity.get('einde') else None
        
        time_score, time_reason = evaluate_time_match(xml_start, xml_end, api_start, api_end)
        score += time_score
        if time_score:
            reasons.append(time_reason)
        
        # Soort matching - EXACT logic from test file
        xml_s = (activity_soort or '').lower()
        api_s = (api_activity.get('soort') or '').lower()
        if xml_s and api_s:
            if xml_s == api_s:
                score += SCORE_SOORT_EXACT
                reasons.append("Soort exact match")
            elif xml_s in api_s:
                score += SCORE_SOORT_PARTIAL_XML_IN_API
                reasons.append("Soort partial XML in API")
            elif api_s in xml_s:
                score += SCORE_SOORT_PARTIAL_API_IN_XML
                reasons.append("Soort partial API in XML")
            else:
                # Alias check from test file
                for alias in SOORT_ALIAS.get(xml_s, []):
                    if alias in api_s:
                        score += SCORE_SOORT_PARTIAL_XML_IN_API
                        reasons.append(f"Soort alias match ('{alias}')")
                        break
        
        # Onderwerp/title matching - EXACT logic from test file  
        api_ond = (api_activity.get('onderwerp') or '').lower()
        xml_tit = (activity_title or '').lower()
        
        # Normalized versions for fuzzy comparison
        norm_api_ond = normalize_topic(api_ond)
        norm_xml_tit = normalize_topic(xml_tit)
        
        if xml_tit and api_ond:
            if norm_xml_tit == norm_api_ond:
                score += SCORE_ONDERWERP_EXACT
                reasons.append("Onderwerp exact")
            else:
                ratio = fuzz.ratio(norm_xml_tit, norm_api_ond)
                if ratio >= FUZZY_SIMILARITY_THRESHOLD_HIGH:
                    score += SCORE_ONDERWERP_FUZZY_HIGH
                    reasons.append(f"Onderwerp fuzzy high ({ratio}%)")
                elif ratio >= FUZZY_SIMILARITY_THRESHOLD_MEDIUM:
                    score += SCORE_ONDERWERP_FUZZY_MEDIUM
                    reasons.append(f"Onderwerp fuzzy medium ({ratio}%)")
        
        if score > best_match_score:
            best_match_score = score
            best_api_activity = api_activity
    
    # Use EXACT SAME THRESHOLD as working test file
    if best_api_activity and best_match_score >= MIN_MATCH_SCORE_FOR_ACTIVITEIT:
        api_activity_id = best_api_activity['id']
        print(f"  ✅ Matched to API activity: {api_activity_id} (score: {best_match_score:.2f})")
        
        # Create/update VLOS activity node
        vlos_activity_props = {
            'id': activity_objectid,
            'api_activity_id': api_activity_id,  # Link to API activity
            'soort': activity_soort,
            'title': activity_title,
            'startdate': activity_startdate,
            'enddate': activity_enddate,
            'match_score': best_match_score,
            'source': 'enhanced_vlos_xml'
        }
        session.execute_write(merge_node, 'EnhancedVlosActivity', 'id', vlos_activity_props)

        # Link to canonical vergadering
        session.execute_write(merge_rel, 'Vergadering', 'id', canonical_vergadering_id,
                              'EnhancedVlosActivity', 'id', activity_objectid, 'HAS_ENHANCED_VLOS_ACTIVITY')

        # Link to matched API activity
        session.execute_write(merge_node, 'Activiteit', 'id', {'id': api_activity_id})
        session.execute_write(merge_rel, 'EnhancedVlosActivity', 'id', activity_objectid,
                              'Activiteit', 'id', api_activity_id, 'MATCHES_API_ACTIVITY')
    else:
        print(f"  ❌ No matching API activity found (best score: {best_match_score:.2f})")
        # Use fallback key for unmatched activities but still process speakers
        api_activity_id = f"unmatched_{activity_objectid}"

    # Process speakers in this activity (ALWAYS process, regardless of API match)
    # Use the SAME logic as the working test file
    speakers = []
    draadboek_fragments = activity_elem.findall('.//vlos:draadboekfragment', NS_VLOS)
    print(f"  🔍 DEBUG: Found {len(draadboek_fragments)} draadboekfragments in this activity")
    
    for frag in draadboek_fragments:
        tekst_el = frag.find('vlos:tekst', NS_VLOS)
        if tekst_el is None:
            continue
        speech_text = collapse_text(tekst_el)
        
        spreker_elements = frag.findall('vlos:sprekers/vlos:spreker', NS_VLOS)
        print(f"    🔍 DEBUG: Found {len(spreker_elements)} speakers in this fragment")
        
        for spreker_elem in spreker_elements:
            # Process speaker similar to test file
            v_first = spreker_elem.findtext("vlos:voornaam", default="", namespaces=NS_VLOS)
            v_last = (
                spreker_elem.findtext("vlos:verslagnaam", default="", namespaces=NS_VLOS)
                or spreker_elem.findtext("vlos:achternaam", default="", namespaces=NS_VLOS)
            )
            
            if v_last:  # Must have at least a last name
                # Try to match to Persoon - EXACT SAME LOGIC as working test file
                matched = None
                if best_api_activity:
                    # Try to get full API object to access actors - EXACT SAME as test file
                    try:
                        api = TKApi(verbose=False)
                        # Use proper TKApi method like in test file
                        act_filter = Activiteit.create_filter()
                        act_filter.add_filter_str(f"Id eq '{best_api_activity['id']}'")
                        candidates = api.get_items(Activiteit, filter=act_filter, max_items=1)
                        if candidates:
                            full_api_activity = candidates[0]
                            actor_persons = full_api_activity.actors if hasattr(full_api_activity, 'actors') else []
                            matched = best_persoon_from_actors(v_first, v_last, actor_persons)
                    except Exception as e:
                        print(f"    ⚠️ Could not get full API activity for actor lookup: {e}")
                
                # Fallback to general search - EXACT SAME as test file
                if not matched:
                    matched = find_best_persoon(v_first, v_last)
                
                speaker_data = process_vlos_speaker(session, spreker_elem, activity_objectid, matched)
                if speaker_data:
                    speakers.append(speaker_data)
                    print(f"      • Found speaker: {speaker_data['naam']}")

    print(f"  📊 Total speakers found in activity: {len(speakers)}")
    
    # Track speakers for this activity (using tracking ID)
    if api_activity_id not in activity_speakers:
        activity_speakers[api_activity_id] = []
    activity_speakers[api_activity_id].extend(speakers)
    
    # Process zaken/topics mentioned in this activity
    print(f"  🔍 DEBUG: Extracting topics from activity...")
    topics = extract_activity_topics(activity_elem)
    print(f"  📋 Found {len(topics)} topics: {topics[:3]}...")  # Show first 3 topics
    
    # 🚀 Find related zaken using enhanced API-FIRST approach with dossier/document processing
    for topic in topics:
        if topic:
            try:
                print(f"  🔍 DEBUG: Looking for zaak matching topic: '{topic[:50]}...'")
                
                # Try API-first approach
                api_zaak_result = find_best_zaak_from_api([topic])
                if api_zaak_result:
                    zaak_id, zaak_nummer, is_dossier, zaak_obj = api_zaak_result
                    print(f"    ✅ Found zaak via API: {zaak_nummer} ({'dossier' if is_dossier else 'zaak'})")
                    
                    # 🏗️ Create/update the zaak and its relationships in Neo4j from fresh API data
                    try:
                        created_zaak_nummer = create_or_update_zaak_from_api(session, zaak_obj)
                        print(f"    🏗️ Created/updated zaak {created_zaak_nummer} with fresh API data")
                    except Exception as e:
                        print(f"    ⚠️ Error creating zaak from API: {e}")
                    
                    # Track zaak for this API activity (using API ID)
                    if api_activity_id not in activity_zaken:
                        activity_zaken[api_activity_id] = []
                    activity_zaken[api_activity_id].append({
                        'id': zaak_nummer,  # Use nummer as ID since that's our primary key
                        'nummer': zaak_nummer,
                        'is_dossier': is_dossier,
                        'topic': topic,
                        'source': 'tk_api_fresh'  # Mark as fresh from API
                    })
                else:
                    # Fallback to database search if API fails
                    print(f"    🔄 No API result, trying database fallback...")
                    db_zaak_result = find_best_zaak_or_fallback(session, [topic])
                    if db_zaak_result:
                        zaak_id, zaak_nummer, is_dossier = db_zaak_result
                        print(f"    ✅ Found zaak via database: {zaak_nummer} ({'dossier' if is_dossier else 'zaak'})")
                        
                        # Track zaak for this API activity (using API ID)
                        if api_activity_id not in activity_zaken:
                            activity_zaken[api_activity_id] = []
                        activity_zaken[api_activity_id].append({
                            'id': zaak_id,
                            'nummer': zaak_nummer,
                            'is_dossier': is_dossier,
                            'topic': topic,
                            'source': 'database_fallback'
                        })
                    else:
                        print(f"    ❌ No zaak found for topic: '{topic[:50]}...'")
            except Exception as e:
                print(f"    ❌ Error looking for zaak with topic '{topic[:50]}...': {e}")
                # Continue processing even if one topic fails
    
    # Process any XML zaak elements with enhanced dossier/document processing
    xml_zaak_elements = activity_elem.findall('.//vlos:zaak', NS_VLOS)
    if xml_zaak_elements:
        print(f"  🔍 Processing {len(xml_zaak_elements)} explicit XML zaak elements...")
        api = TKApi(verbose=False)
        
        for xml_zaak in xml_zaak_elements:
            dossiernr = xml_zaak.findtext("vlos:dossiernummer", default="", namespaces=NS_VLOS).strip()
            stuknr = xml_zaak.findtext("vlos:stuknummer", default="", namespaces=NS_VLOS).strip()
            zaak_titel = xml_zaak.findtext("vlos:titel", default="", namespaces=NS_VLOS).strip()
            
            if dossiernr or stuknr:
                print(f"    🔍 Processing XML zaak: dossier={dossiernr}, stuk={stuknr}, titel='{zaak_titel[:50]}...'")
                
                # Use enhanced zaak fallback logic from test file
                match_result = find_best_zaak_or_fallback_enhanced(api, dossiernr, stuknr)
                
                if match_result['success']:
                    if match_result['match_type'] == 'zaak':
                        zaak_obj = match_result['zaak']
                        print(f"      ✅ Found specific zaak: {zaak_obj.nummer}")
                        
                        # Create/update zaak in Neo4j
                        try:
                            created_zaak_nummer = create_or_update_zaak_from_api(session, zaak_obj)
                            print(f"      🏗️ Created/updated zaak {created_zaak_nummer}")
                        except Exception as e:
                            print(f"      ⚠️ Error creating zaak: {e}")
                        
                        # Track zaak
                        if api_activity_id not in activity_zaken:
                            activity_zaken[api_activity_id] = []
                        activity_zaken[api_activity_id].append({
                            'id': zaak_obj.nummer,
                            'nummer': zaak_obj.nummer,
                            'is_dossier': False,
                            'topic': zaak_titel or f"dossier {dossiernr}",
                            'source': 'xml_zaak_element'
                        })
                        
                    elif match_result['match_type'] == 'dossier_fallback':
                        dossier_obj = match_result['dossier']
                        document_obj = match_result.get('document')
                        print(f"      ✅ Found dossier fallback: {dossier_obj.nummer}{(' '+dossier_obj.toevoeging) if dossier_obj.toevoeging else ''}")
                        
                        # Create dossier node in Neo4j
                        dossier_props = {
                            'id': dossier_obj.id,
                            'nummer': dossier_obj.nummer,
                            'toevoeging': getattr(dossier_obj, 'toevoeging', ''),
                            'titel': getattr(dossier_obj, 'titel', ''),
                            'data_source': 'tk_api_fresh',
                            'last_updated': str(datetime.now())
                        }
                        session.execute_write(merge_node, 'Dossier', 'id', dossier_props)
                        
                        # Create document node if found
                        if document_obj:
                            print(f"        📄 Also found document: {document_obj.volgnummer}")
                            document_props = {
                                'id': document_obj.id,
                                'nummer': getattr(document_obj, 'nummer', ''),
                                'volgnummer': document_obj.volgnummer,
                                'titel': getattr(document_obj, 'titel', ''),
                                'data_source': 'tk_api_fresh',
                                'last_updated': str(datetime.now())
                            }
                            session.execute_write(merge_node, 'Document', 'id', document_props)
                            
                            # Link document to dossier
                            session.execute_write(merge_rel, 'Dossier', 'id', dossier_obj.id,
                                                  'Document', 'id', document_obj.id, 'HAS_DOCUMENT')
                        
                        # Track dossier
                        if api_activity_id not in activity_zaken:
                            activity_zaken[api_activity_id] = []
                        activity_zaken[api_activity_id].append({
                            'id': dossier_obj.id,
                            'nummer': f"{dossier_obj.nummer}{(' '+dossier_obj.toevoeging) if dossier_obj.toevoeging else ''}",
                            'is_dossier': True,
                            'topic': zaak_titel or f"dossier {dossiernr}",
                            'source': 'xml_dossier_fallback'
                        })
                else:
                    print(f"      ❌ No match found for dossier={dossiernr}, stuk={stuknr}")
            
            # Process speakers directly linked to this zaak element
            zaak_speakers = xml_zaak.findall('.//vlos:spreker', NS_VLOS)
            if zaak_speakers:
                print(f"      👥 Processing {len(zaak_speakers)} speakers linked to this zaak")
                for spreker_elem in zaak_speakers:
                    # Process speaker similar to fragment speakers
                    v_first = spreker_elem.findtext("vlos:voornaam", default="", namespaces=NS_VLOS)
                    v_last = (
                        spreker_elem.findtext("vlos:verslagnaam", default="", namespaces=NS_VLOS)
                        or spreker_elem.findtext("vlos:achternaam", default="", namespaces=NS_VLOS)
                    )
                    
                    if v_last:
                        matched = find_best_persoon(v_first, v_last)
                        speaker_data = process_vlos_speaker(session, spreker_elem, activity_objectid, matched)
                        if speaker_data:
                            speakers.append(speaker_data)
                            print(f"        • Zaak-linked speaker: {speaker_data['naam']}")
    
    # Detect interruptions in this activity
    activity_interruptions = detect_interruptions_in_activity(activity_elem)
    interruption_events.extend(activity_interruptions)
    
    # Analyze voting in this activity - pass related zaak IDs for Stemming matching
    related_zaak_ids = [zaak['nummer'] for zaak in activity_zaken.get(api_activity_id, [])]
    activity_voting = analyze_voting_in_activity(activity_elem, related_zaak_ids)
    voting_events.extend(activity_voting)
    
    return api_activity_id


def process_vlos_speaker(session, spreker_elem: ET.Element, activity_id: str, matched_persoon: Optional[Persoon] = None) -> Optional[Dict[str, Any]]:
    """Process a VLOS speaker element - using EXACT same logic as working test file"""
    
    # Extract speaker info EXACTLY like the working test file
    v_first = spreker_elem.findtext("vlos:voornaam", default="", namespaces=NS_VLOS)
    v_last = (
        spreker_elem.findtext("vlos:verslagnaam", default="", namespaces=NS_VLOS)
        or spreker_elem.findtext("vlos:achternaam", default="", namespaces=NS_VLOS)
    )
    
    # Additional fields for completeness
    fractie = spreker_elem.findtext('vlos:fractie', '', NS_VLOS)
    aanhef = spreker_elem.findtext('vlos:aanhef', '', NS_VLOS)
    tussenvoegsel = spreker_elem.findtext('vlos:tussenvoegsel', '', NS_VLOS)
    
    print(f"🔍 DEBUG: Speaker data - voornaam: '{v_first}', verslagnaam/achternaam: '{v_last}', fractie: '{fractie}'")
    
    if not v_last:  # Must have at least a last name
        return None
    
    # Create speaker identifier using the same pattern as test file
    full_name = f"{v_first} {v_last}".strip()
    full_name = re.sub(r'\s+', ' ', full_name)
    
    speaker_id = f"vlos_speaker_{hash(full_name)}_{activity_id}"
    
    # Create VLOS speaker node with proper field mapping
    speaker_props = {
        'id': speaker_id,
        'naam': full_name,
        'voornaam': v_first,
        'verslagnaam': spreker_elem.findtext("vlos:verslagnaam", default="", namespaces=NS_VLOS),
        'achternaam': spreker_elem.findtext("vlos:achternaam", default="", namespaces=NS_VLOS),
        'tussenvoegsel': tussenvoegsel,
        'fractie': fractie,
        'aanhef': aanhef,
        'activity_id': activity_id,
        'source': 'enhanced_vlos_xml'
    }
    session.execute_write(merge_node, 'VlosSpeaker', 'id', speaker_props)
    
    # Link to activity (try both possible activity types)
    try:
        session.execute_write(merge_rel, 'EnhancedVlosActivity', 'id', activity_id,
                              'VlosSpeaker', 'id', speaker_id, 'HAS_SPEAKER')
    except:
        # Fallback - link to any activity type
        session.execute_write(merge_rel, 'VlosActivity', 'id', activity_id,
                              'VlosSpeaker', 'id', speaker_id, 'HAS_SPEAKER')
    
    # If we have a matched persoon, create the relationship
    if matched_persoon:
        # Create/update the Persoon node
        persoon_props = {
            'id': matched_persoon.id,
            'roepnaam': getattr(matched_persoon, 'roepnaam', ''),
            'voornaam': getattr(matched_persoon, 'voornaam', ''),
            'achternaam': getattr(matched_persoon, 'achternaam', ''),
            'tussenvoegsel': getattr(matched_persoon, 'tussenvoegsel', ''),
        }
        session.execute_write(merge_node, 'Persoon', 'id', persoon_props)
        
        # Create relationship between VlosSpeaker and Persoon
        session.execute_write(merge_rel, 'VlosSpeaker', 'id', speaker_id,
                              'Persoon', 'id', matched_persoon.id, 'MATCHED_TO_PERSOON')
    
    return {
        'id': speaker_id,
        'naam': full_name,
        'voornaam': v_first,
        'achternaam': v_last,
        'matched_persoon': matched_persoon
    }


def extract_activity_topics(activity_elem: ET.Element) -> List[str]:
    """Extract meaningful Zaak topics from VLOS activity element, filtering out motion titles and speaker labels"""
    topics = []
    
    # Get main activity title (but filter out procedural/speaker content)
    main_title = activity_elem.findtext('.//vlos:titel', default='', namespaces=NS_VLOS)
    if main_title and _is_valid_zaak_topic(main_title):
        topics.append(main_title)
    
    # Get onderwerp if different and valid
    onderwerp = activity_elem.findtext('.//vlos:onderwerp', default='', namespaces=NS_VLOS)
    if onderwerp and onderwerp != main_title and _is_valid_zaak_topic(onderwerp):
        topics.append(onderwerp)
    
    # Extract zaak titles (these are usually valid)
    for zaak_elem in activity_elem.findall('.//vlos:zaak', NS_VLOS):
        zaak_title = zaak_elem.findtext('vlos:titel', default='', namespaces=NS_VLOS)
        if zaak_title:
            topics.append(zaak_title)
    
    # ONLY extract activiteititem titles that look like actual policy topics, not motions or speaker labels
    for item_elem in activity_elem.findall('.//vlos:activiteititem', NS_VLOS):
        item_title = item_elem.findtext('vlos:titel', default='', namespaces=NS_VLOS)
        if item_title and _is_valid_zaak_topic(item_title):
            topics.append(item_title)
    
    return topics


def _is_valid_zaak_topic(topic: str) -> bool:
    """Filter out motion titles, speaker labels, and other non-Zaak content"""
    if not topic or len(topic.strip()) < 3:
        return False
    
    topic_lower = topic.lower().strip()
    
    # Filter out motion titles
    if any(prefix in topic_lower for prefix in [
        'de motie-', 'het amendement-', 'de vraag van', 'woordvoerder -', 
        'spreker:', 'de voorzitter', 'de heer ', 'mevrouw ', 'minister '
    ]):
        return False
    
    # Filter out procedural content
    if any(prefix in topic_lower for prefix in [
        'opening', 'sluiting', 'stemming over', 'stemmingen', 'aanvang', 
        'einde vergadering', 'regeling van werkzaamheden'
    ]):
        return False
    
    # Filter out very short topics that are likely not meaningful
    if len(topic_lower) < 10:
        return False
    
    return True





def calculate_activity_match_score(vlos_activity: ET.Element, api_activity: Dict[str, Any]) -> float:
    """Calculate sophisticated matching score between VLOS and API activities"""
    
    score = 0.0
    max_score = 5.0  # Maximum possible score
    
    # 1. Time overlap scoring (40% weight)
    vlos_begin = parse_xml_datetime(vlos_activity.get('startdate'))
    vlos_end = parse_xml_datetime(vlos_activity.get('enddate'))
    
    if vlos_begin and api_activity.get('begin'):
        try:
            api_begin = datetime.fromisoformat(api_activity['begin'].replace('Z', '+00:00'))
            api_end = None
            if api_activity.get('einde'):
                api_end = datetime.fromisoformat(api_activity['einde'].replace('Z', '+00:00'))
            
            # Check for time overlap
            if api_end and vlos_end:
                # Full overlap check
                overlap_start = max(vlos_begin, api_begin)
                overlap_end = min(vlos_end, api_end)
                if overlap_start < overlap_end:
                    overlap_duration = (overlap_end - overlap_start).total_seconds()
                    total_duration = max((vlos_end - vlos_begin).total_seconds(), 
                                       (api_end - api_begin).total_seconds())
                    overlap_ratio = overlap_duration / total_duration if total_duration > 0 else 0
                    score += 2.0 * overlap_ratio
                else:
                    # Check proximity (within 2 hours)
                    time_diff = abs((vlos_begin - api_begin).total_seconds())
                    if time_diff <= 7200:  # 2 hours
                        score += 1.0 * (1 - time_diff / 7200)
            else:
                # Single time point comparison
                time_diff = abs((vlos_begin - api_begin).total_seconds())
                if time_diff <= 7200:  # 2 hours
                    score += 1.5 * (1 - time_diff / 7200)
        except Exception as e:
            print(f"⚠️ Time comparison error: {e}")
    
    # 2. Activity type/soort matching (30% weight)
    vlos_soort = vlos_activity.get('soort', '').lower()
    api_soort = api_activity.get('soort', '').lower()
    
    if vlos_soort and api_soort:
        if vlos_soort == api_soort:
            score += 1.5
        elif vlos_soort in api_soort or api_soort in vlos_soort:
            score += 1.0
        elif any(keyword in vlos_soort and keyword in api_soort 
                for keyword in ['besluit', 'stemming', 'motie', 'debat']):
            score += 0.5
    
    # 3. Topic/onderwerp similarity (30% weight)
    vlos_topic = normalize_topic(vlos_activity.findtext('.//vlos:titel', '', NS_VLOS))
    api_topic = normalize_topic(api_activity.get('onderwerp', ''))
    
    if vlos_topic and api_topic:
        # Exact match
        if vlos_topic == api_topic:
            score += 1.5
        # Substring match
        elif vlos_topic in api_topic or api_topic in vlos_topic:
            score += 1.0
        # Keyword overlap
        else:
            vlos_words = set(vlos_topic.split())
            api_words = set(api_topic.split())
            if vlos_words and api_words:
                overlap = len(vlos_words & api_words)
                total = len(vlos_words | api_words)
                if overlap > 0:
                    score += 0.5 * (overlap / total)
    
    return min(score, max_score)


def find_best_zaak_from_api(topics: List[str]) -> Optional[Tuple[str, str, bool, object]]:
    """🚀 API-FIRST: Find best matching Zaak from TK API directly, bypassing database issues"""
    
    if not topics:
        return None
    
    # Create API instance outside session context to avoid conflicts
    api = TKApi(verbose=False)
    
    for topic in topics:
        topic_normalized = normalize_topic(topic)
        if len(topic_normalized) < 3:
            continue
        
        print(f"🔍 DEBUG: Searching TK API for topic: '{topic_normalized[:50]}...'")
        
        try:
            # Strategy 1: Direct onderwerp filtering using built-in filter method
            try:
                filter_obj = Zaak.create_filter()
                filter_obj.filter_onderwerp(topic_normalized)
                zaken = api.get_zaken(filter=filter_obj)
                
                if zaken:
                    best_zaak = zaken[0]  # Take first match
                    print(f"    ✅ Found zaak via exact onderwerp: {best_zaak.nummer} - {best_zaak.onderwerp[:50]}...")
                    return (best_zaak.nummer, best_zaak.nummer, False, best_zaak)
            except Exception as e:
                print(f"    ⚠️ Exact onderwerp search failed: {e}")
            
            # Strategy 2: Search by nummer if topic looks like a zaak number
            if re.match(r'^\d{4}Z\d{5}$', topic.strip()):
                try:
                    filter_obj = Zaak.create_filter()
                    filter_obj.filter_nummer(topic.strip())
                    zaken = api.get_zaken(filter=filter_obj)
                    
                    if zaken:
                        best_zaak = zaken[0]
                        print(f"    ✅ Found zaak via nummer: {best_zaak.nummer}")
                        return (best_zaak.nummer, best_zaak.nummer, False, best_zaak)
                except Exception as e:
                    print(f"    ⚠️ Nummer search failed: {e}")
            
            # Strategy 3: Try partial onderwerp matches with keywords
            keywords = [word for word in topic_normalized.split() if len(word) >= 4]
            for keyword in keywords[:3]:  # Try up to 3 keywords
                try:
                    filter_obj = Zaak.create_filter()
                    filter_obj.filter_onderwerp(keyword)  # Use built-in method
                    zaken = api.get_zaken(filter=filter_obj)
                    
                    if zaken:
                        best_zaak = zaken[0]
                        print(f"    ✅ Found zaak via keyword '{keyword}': {best_zaak.nummer}")
                        return (best_zaak.nummer, best_zaak.nummer, False, best_zaak)
                except Exception as e:
                    print(f"    ⚠️ Keyword search failed for '{keyword}': {e}")
                    continue
                        
        except Exception as e:
            print(f"    ⚠️ API search error for topic '{topic_normalized[:30]}...': {e}")
            continue
    
    print(f"    ❌ No zaak found in API for any topics")
    return None





def find_best_zaak_or_fallback(session, topics: List[str], fallback_dossier_ids: List[str] = None) -> Optional[Tuple[str, str, bool]]:
    """Find best matching zaak or fallback to dossier with enhanced logic"""
    
    for topic in topics:
        if not topic:
            continue
            
        topic_normalized = normalize_topic(topic)
        
        # Strategy 1: Exact zaak nummer match
        zaak_match = session.run("""
            MATCH (z:Zaak)
            WHERE (z.onderwerp IS NOT NULL AND toLower(toString(z.onderwerp)) CONTAINS toLower($topic))
            OR (z.nummer IS NOT NULL AND toString(z.nummer) CONTAINS $topic)
            RETURN z.nummer as id, z.nummer as nummer, z.onderwerp as onderwerp
            ORDER BY size(toString(coalesce(z.onderwerp, ''))) ASC
            LIMIT 1
        """, topic=topic_normalized).single()
        
        if zaak_match:
            return zaak_match['id'], zaak_match['nummer'], False
        
        # Strategy 2: Keyword-based zaak search
        if len(topic_normalized) > 10:  # Only for substantial topics
            keywords = [word for word in topic_normalized.split() if len(word) > 3]
            if keywords:
                keyword_query = " AND ".join([f"toLower(z.onderwerp) CONTAINS toLower('{word}')" 
                                            for word in keywords[:3]])  # Max 3 keywords
                
                zaak_keyword_match = session.run(f"""
                    MATCH (z:Zaak)
                    WHERE z.onderwerp IS NOT NULL AND ({keyword_query})
                    RETURN z.nummer as id, z.nummer as nummer, z.onderwerp as onderwerp
                    ORDER BY size(toString(coalesce(z.onderwerp, ''))) ASC
                    LIMIT 1
                """).single()
                
                if zaak_keyword_match:
                    return zaak_keyword_match['id'], zaak_keyword_match['nummer'], False
    
    # Strategy 3: Dossier fallback
    if fallback_dossier_ids:
        for dossier_id in fallback_dossier_ids:
            dossier_match = session.run("""
                MATCH (d:Dossier {id: $dossier_id})
                RETURN d.id as id, d.nummer as nummer
            """, dossier_id=dossier_id).single()
            
            if dossier_match:
                return dossier_match['id'], dossier_match['nummer'], True
    
    return None


def match_vlos_speakers_to_personen(session) -> int:
    """Match VLOS speakers to Persoon nodes with enhanced logic"""
    
    # Get all unmatched VLOS speakers - using correct field names from VlosSpeaker
    vlos_speakers = session.run("""
        MATCH (vs:VlosSpeaker)
        WHERE NOT EXISTS((vs)-[:MATCHED_TO_PERSOON]->(:Persoon))
        RETURN vs.id as vlos_id, vs.naam as naam, vs.voornaam as voornaam, 
               vs.tussenvoegsel as tussenvoegsel, vs.achternaam as achternaam,
               vs.verslagnaam as verslagnaam
    """).data()
    
    matched_count = 0
    
    for speaker in vlos_speakers:
        vlos_id = speaker['vlos_id']
        naam = speaker.get('naam', '')
        voornaam = speaker.get('voornaam', '')
        tussenvoegsel = speaker.get('tussenvoegsel', '')
        achternaam = speaker.get('achternaam', '')
        verslagnaam = speaker.get('verslagnaam', '')
        
        # Use verslagnaam as primary last name, fallback to achternaam
        effective_achternaam = verslagnaam if verslagnaam else achternaam
        
        print(f"🔍 DEBUG: Matching speaker - naam: '{naam}', voornaam: '{voornaam}', achternaam: '{effective_achternaam}'")
        
        # Strategy 1: Exact full name match
        if naam:
            persoon_match = session.run("""
                MATCH (p:Persoon)
                WHERE toLower(p.roepnaam + ' ' + coalesce(p.tussenvoegsel, '') + ' ' + p.achternaam) = toLower($naam)
                OR toLower(p.voornaam + ' ' + coalesce(p.tussenvoegsel, '') + ' ' + p.achternaam) = toLower($naam)
                RETURN p.id as persoon_id, p.roepnaam, p.achternaam
                LIMIT 1
            """, naam=naam.strip()).single()
            
            if persoon_match:
                roepnaam = persoon_match.get('roepnaam', '') or ''
                achternaam = persoon_match.get('achternaam', '') or ''
                print(f"✅ Matched by full name: {naam} → {roepnaam} {achternaam}")
                session.run("""
                    MATCH (vs:VlosSpeaker {id: $vlos_id})
                    MATCH (p:Persoon {id: $persoon_id})
                    MERGE (vs)-[:MATCHED_TO_PERSOON]->(p)
                """, vlos_id=vlos_id, persoon_id=persoon_match['persoon_id'])
                matched_count += 1
                continue
        
        # Strategy 2: Component-based matching using voornaam (not roepnaam)
        if voornaam and effective_achternaam:
            # Try matching with voornaam first
            query_parts = ["(toLower(p.roepnaam) = toLower($voornaam) OR toLower(p.voornaam) = toLower($voornaam))"]
            query_params = {"voornaam": voornaam, "achternaam": effective_achternaam}
            
            if tussenvoegsel:
                query_parts.append("toLower(coalesce(p.tussenvoegsel, '')) = toLower($tussenvoegsel)")
                query_params["tussenvoegsel"] = tussenvoegsel
            
            query_parts.append("toLower(p.achternaam) = toLower($achternaam)")
            
            persoon_match = session.run(f"""
                MATCH (p:Persoon)
                WHERE {' AND '.join(query_parts)}
                RETURN p.id as persoon_id, p.roepnaam, p.voornaam, p.achternaam
                LIMIT 1
            """, **query_params).single()
            
            if persoon_match:
                roepnaam = persoon_match.get('roepnaam', '') or ''
                achternaam = persoon_match.get('achternaam', '') or ''
                print(f"✅ Matched by components: {voornaam} {effective_achternaam} → {roepnaam} {achternaam}")
                session.run("""
                    MATCH (vs:VlosSpeaker {id: $vlos_id})
                    MATCH (p:Persoon {id: $persoon_id})
                    MERGE (vs)-[:MATCHED_TO_PERSOON]->(p)
                """, vlos_id=vlos_id, persoon_id=persoon_match['persoon_id'])
                matched_count += 1
                continue
        
        # Strategy 3: Fuzzy matching for common cases
        if effective_achternaam:
            # Try matching just by last name and see if there's a unique match
            persoon_matches = session.run("""
                MATCH (p:Persoon)
                WHERE toLower(p.achternaam) = toLower($achternaam)
                RETURN p.id as persoon_id, p.roepnaam, p.voornaam, p.achternaam
                LIMIT 3
            """, achternaam=effective_achternaam).data()
            
            if len(persoon_matches) == 1:
                # Unique match by last name
                persoon_match = persoon_matches[0]
                roepnaam = persoon_match.get('roepnaam', '') or ''
                achternaam = persoon_match.get('achternaam', '') or ''
                print(f"✅ Matched by unique last name: {effective_achternaam} → {roepnaam} {achternaam}")
                session.run("""
                    MATCH (vs:VlosSpeaker {id: $vlos_id})
                    MATCH (p:Persoon {id: $persoon_id})
                    MERGE (vs)-[:MATCHED_TO_PERSOON]->(p)
                """, vlos_id=vlos_id, persoon_id=persoon_match['persoon_id'])
                matched_count += 1
                continue
        
        print(f"❌ No match found for speaker: {naam}")
    
    return matched_count


def detect_interruptions_in_activity(activity_elem: ET.Element) -> List[Dict[str, Any]]:
    """Detect interruption patterns within a VLOS activity"""
    
    interruptions = []
    
    # Find all draadboekfragments (speech sections)
    fragments = activity_elem.findall('.//vlos:draadboekfragment', NS_VLOS)
    
    for fragment in fragments:
        speakers_in_fragment = []
        
        # Collect all speakers in this fragment
        for spreker_elem in fragment.findall('.//vlos:spreker', NS_VLOS):
            # Use proper XML text elements, not attributes
            v_first = spreker_elem.findtext("vlos:voornaam", default="", namespaces=NS_VLOS)
            v_last = (
                spreker_elem.findtext("vlos:verslagnaam", default="", namespaces=NS_VLOS)
                or spreker_elem.findtext("vlos:achternaam", default="", namespaces=NS_VLOS)
            )
            speaker_name = f"{v_first} {v_last}".strip()
            if speaker_name:
                speakers_in_fragment.append({
                    'naam': speaker_name,
                    'element': spreker_elem
                })
        
        # Check for interruption patterns
        if len(speakers_in_fragment) > 1:
            # Fragment interruption: Multiple speakers in same fragment
            interruptions.append({
                'type': 'fragment_interruption',
                'speakers': speakers_in_fragment,
                'fragment': fragment
            })
    
    # Check for sequential interruptions across fragments
    all_speakers = []
    for fragment in fragments:
        for spreker_elem in fragment.findall('.//vlos:spreker', NS_VLOS):
            # Use proper XML text elements, not attributes
            v_first = spreker_elem.findtext("vlos:voornaam", default="", namespaces=NS_VLOS)
            v_last = (
                spreker_elem.findtext("vlos:verslagnaam", default="", namespaces=NS_VLOS)
                or spreker_elem.findtext("vlos:achternaam", default="", namespaces=NS_VLOS)
            )
            speaker_name = f"{v_first} {v_last}".strip()
            if speaker_name:
                all_speakers.append({
                    'naam': speaker_name,
                    'fragment': fragment,
                    'element': spreker_elem
                })
    
    # Detect simple interruptions (A → B)
    for i in range(len(all_speakers) - 1):
        current = all_speakers[i]
        next_speaker = all_speakers[i + 1]
        
        if current['naam'] != next_speaker['naam']:
            interruptions.append({
                'type': 'simple_interruption',
                'original_speaker': current,
                'interrupter': next_speaker
            })
            
            # Check for response (A → B → A)
            if i + 2 < len(all_speakers):
                after_next = all_speakers[i + 2]
                if after_next['naam'] == current['naam']:
                    interruptions.append({
                        'type': 'interruption_with_response',
                        'original_speaker': current,
                        'interrupter': next_speaker,
                        'response': after_next
                    })
    
    return interruptions


def analyze_voting_in_activity(activity_elem: ET.Element, related_zaak_ids: List[str] = None) -> List[Dict[str, Any]]:
    """Analyze voting patterns in VLOS activity element and link to API Stemming records"""
    
    voting_events = []
    
    # Find besluit items with voting data
    besluit_items = activity_elem.findall('.//vlos:activiteititem[@soort="Besluit"]', NS_VLOS)
    
    for besluit in besluit_items:
        # Get besluit metadata
        besluit_titel = besluit.findtext('.//vlos:titel', default='', namespaces=NS_VLOS)
        besluit_uitslag = besluit.findtext('.//vlos:uitslag', default='', namespaces=NS_VLOS)
        
        # Look for voting sections
        stemmingen_elem = besluit.find('.//vlos:stemmingen', NS_VLOS)
        
        if stemmingen_elem is not None:
            fractie_votes = []
            
            # Extract individual fractie votes
            for stemming in stemmingen_elem.findall('.//vlos:stemming', NS_VLOS):
                fractie_naam = stemming.get('fractie', 'Unknown')
                stem_waarde = stemming.get('stemming', 'Unknown')
                
                fractie_votes.append({
                    'fractie': fractie_naam,
                    'vote': stem_waarde,
                    'vote_normalized': stem_waarde.lower()
                })
            
            if fractie_votes:
                # Calculate consensus level
                total_votes = len(fractie_votes)
                voor_votes = len([v for v in fractie_votes if v['vote_normalized'] == 'voor'])
                tegen_votes = len([v for v in fractie_votes if v['vote_normalized'] == 'tegen'])
                
                consensus_percentage = (voor_votes / total_votes * 100) if total_votes > 0 else 0
                
                voting_event = {
                    'type': 'fractie_voting',
                    'titel': besluit_titel,
                    'uitslag': besluit_uitslag,
                    'besluit_element': besluit,
                    'fractie_votes': fractie_votes,
                    'total_votes': total_votes,
                    'voor_votes': voor_votes,
                    'tegen_votes': tegen_votes,
                    'consensus_percentage': consensus_percentage,
                    'is_unanimous': consensus_percentage >= 95,
                    'is_controversial': consensus_percentage < 80,
                    'vote_breakdown': {}
                }
                
                # Calculate vote breakdown
                for vote in fractie_votes:
                    vote_type = vote['vote_normalized']
                    if vote_type not in voting_event['vote_breakdown']:
                        voting_event['vote_breakdown'][vote_type] = []
                    voting_event['vote_breakdown'][vote_type].append(vote['fractie'])
                
                # 🚀 NEW: Try to find matching API Stemming records
                try:
                    print(f"    🔍 Looking for API Stemming records for voting event: {besluit_titel}")
                    matched_stemmingen = find_stemmingen_for_voting_event(voting_event, related_zaak_ids or [])
                    voting_event['matched_stemmingen'] = matched_stemmingen
                    voting_event['api_matches'] = len(matched_stemmingen)
                    
                    if matched_stemmingen:
                        print(f"    ✅ Found {len(matched_stemmingen)} matching API Stemming records")
                    else:
                        print(f"    ❌ No matching API Stemming records found")
                        
                except Exception as e:
                    print(f"    ⚠️ Error looking for API Stemming records: {e}")
                    voting_event['matched_stemmingen'] = []
                    voting_event['api_matches'] = 0
                
                voting_events.append(voting_event)
    
    return voting_events


def create_speaker_zaak_connections(session, activity_speakers: Dict[str, List[str]], 
                                   activity_zaken: Dict[str, List[str]]) -> int:
    """Create comprehensive speaker-zaak relationship network"""
    
    connection_count = 0
    
    print("🔗 Creating speaker-zaak connections...")
    
    for api_activity_id, speakers in activity_speakers.items():
        zaken = activity_zaken.get(api_activity_id, [])
        
        if not speakers or not zaken:
            continue
        
        print(f"  📊 Processing {len(speakers)} speakers × {len(zaken)} zaken for activity {api_activity_id}")
        
        for speaker in speakers:
            speaker_id = speaker['id']
            
            for zaak_info in zaken:
                zaak_id = zaak_info['id']
                is_dossier = zaak_info['is_dossier']
                topic = zaak_info['topic']
                
                # Create connection from VlosSpeaker to Zaak/Dossier
                target_label = 'Dossier' if is_dossier else 'Zaak'
                rel_type = 'SPOKE_ABOUT'
                
                # Only proceed if zaak_id is not None
                if zaak_id:
                    session.execute_write(merge_node, target_label, 'id', {'id': zaak_id})
                    session.execute_write(merge_rel, 'VlosSpeaker', 'id', speaker_id,
                                          target_label, 'id', zaak_id, rel_type)
                else:
                    print(f"⚠️ Skipping connection for speaker {speaker_id} - zaak_id is None")
                
                # Also create connection from matched Persoon to Zaak/Dossier (if matched)
                persoon_match = session.run("""
                    MATCH (vs:VlosSpeaker {id: $speaker_id})-[:MATCHED_TO_PERSOON]->(p:Persoon)
                    RETURN p.id as persoon_id
                """, speaker_id=speaker_id).single()
                
                if persoon_match and zaak_id:
                    persoon_id = persoon_match['persoon_id']
                    session.execute_write(merge_rel, 'Persoon', 'id', persoon_id,
                                          target_label, 'id', zaak_id, 'DISCUSSED')
                
                connection_count += 1
    
    print(f"✅ Created {connection_count} speaker-zaak connections")
    return connection_count


def create_voting_stemming_connections(session, voting_events: List[Dict[str, Any]], doc_id: str) -> int:
    """Create connections between VLOS voting events and API Stemming records"""
    
    connection_count = 0
    
    print("🗳️ Creating VLOS voting → API Stemming connections...")
    
    for i, voting_event in enumerate(voting_events):
        if not voting_event.get('matched_stemmingen'):
            continue
        
        # Create VLOS voting event node
        voting_event_id = f"vlos_voting_{doc_id}_{i}"
        voting_props = {
            'id': voting_event_id,
            'titel': voting_event.get('titel', ''),
            'uitslag': voting_event.get('uitslag', ''),
            'total_votes': voting_event['total_votes'],
            'voor_votes': voting_event['voor_votes'],
            'tegen_votes': voting_event['tegen_votes'],
            'consensus_percentage': voting_event['consensus_percentage'],
            'is_unanimous': voting_event['is_unanimous'],
            'is_controversial': voting_event['is_controversial'],
            'api_matches': voting_event['api_matches'],
            'document_id': doc_id,
            'source': 'enhanced_vlos_xml'
        }
        session.execute_write(merge_node, 'VlosVotingEvent', 'id', voting_props)
        
        # Create connections to matched API Stemming records
        for match in voting_event['matched_stemmingen']:
            stemming = match['stemming']
            vlos_vote = match['vlos_vote']
            confidence = match['match_confidence']
            
            # Create/update API Stemming node
            stemming_props = {
                'id': stemming.id,
                'soort': getattr(stemming, 'soort', ''),
                'actor_naam': getattr(stemming, 'actor_naam', ''),
                'actor_fractie': getattr(stemming, 'actor_fractie', ''),
                'fractie_id': getattr(stemming, 'fractie_id', ''),
                'persoon_id': getattr(stemming, 'persoon_id', ''),
                'data_source': 'tk_api_fresh'
            }
            session.execute_write(merge_node, 'Stemming', 'id', stemming_props)
            
            # Create relationship with match details
            session.execute_write(merge_rel, 'VlosVotingEvent', 'id', voting_event_id,
                                  'Stemming', 'id', stemming.id, 'MATCHES_API_STEMMING',
                                  {'match_confidence': confidence, 
                                   'vlos_fractie': vlos_vote['fractie'],
                                   'vlos_vote': vlos_vote['vote']})
            
            # Also create Fractie and Persoon nodes if available
            if stemming.fractie:
                fractie_props = {
                    'id': stemming.fractie.id,
                    'naam': getattr(stemming.fractie, 'naam', ''),
                    'afkorting': getattr(stemming.fractie, 'afkorting', ''),
                    'data_source': 'tk_api_fresh'
                }
                session.execute_write(merge_node, 'Fractie', 'id', fractie_props)
                session.execute_write(merge_rel, 'Stemming', 'id', stemming.id,
                                      'Fractie', 'id', stemming.fractie.id, 'VOTED_BY_FRACTIE')
            
            if stemming.persoon:
                persoon_props = {
                    'id': stemming.persoon.id,
                    'roepnaam': getattr(stemming.persoon, 'roepnaam', ''),
                    'voornaam': getattr(stemming.persoon, 'voornaam', ''),
                    'achternaam': getattr(stemming.persoon, 'achternaam', ''),
                    'data_source': 'tk_api_fresh'
                }
                session.execute_write(merge_node, 'Persoon', 'id', persoon_props)
                session.execute_write(merge_rel, 'Stemming', 'id', stemming.id,
                                      'Persoon', 'id', stemming.persoon.id, 'VOTED_BY_PERSOON')
            
            connection_count += 1
            
        print(f"  ✅ Created {len(voting_event['matched_stemmingen'])} connections for voting event: {voting_event.get('titel', 'Unknown')}")
    
    print(f"✅ Created {connection_count} VLOS voting → API Stemming connections")
    return connection_count


def create_enriched_zaak_activity_connections(session) -> int:
    """Create rich connections from Zaak nodes to their related Agendapunten and Activiteiten using TK API data"""
    
    connection_count = 0
    
    print("🔗 Creating enriched Zaak→Agendapunt/Activiteit connections...")
    
    # Get all Zaak nodes that we've successfully matched
    zaken_with_speakers = session.run("""
        MATCH (vs:VlosSpeaker)-[:SPOKE_ABOUT]->(z:Zaak)
        RETURN DISTINCT z.nummer as zaak_nummer
    """).data()
    
    print(f"  📊 Found {len(zaken_with_speakers)} zaken with speaker connections")
    
    for zaak_data in zaken_with_speakers:
        zaak_nummer = zaak_data['zaak_nummer']
        
        try:
            # Use TK API to get full Zaak object with related items
            api = TKApi()
            
            # Find the zaak by nummer using proper filter
            print(f"    🔍 Looking up Zaak with nummer: {zaak_nummer}")
            try:
                # Create proper filter for TK API
                filter_obj = Zaak.create_filter()
                filter_obj.add_filter_str(f"Nummer eq '{zaak_nummer}'")
                zaak_list = api.get_zaken(filter=filter_obj)
                
                if not zaak_list:
                    print(f"    ❌ No Zaak found with nummer: {zaak_nummer}")
                    continue
            except Exception as e:
                print(f"    ⚠️ Error looking up Zaak {zaak_nummer}: {e}")
                continue
                
            zaak_obj = zaak_list[0]
            print(f"    ✅ Found Zaak: {zaak_obj.nummer} - {getattr(zaak_obj, 'onderwerp', 'No subject')[:50]}...")
            
            # Get related agendapunten
            agendapunten = zaak_obj.agendapunten
            print(f"    📋 Zaak {zaak_nummer} has {len(agendapunten)} related agendapunten")
            
            for agendapunt in agendapunten:
                if agendapunt and hasattr(agendapunt, 'id') and agendapunt.id:
                    print(f"      📋 Processing Agendapunt: {agendapunt.id} - {getattr(agendapunt, 'onderwerp', 'No subject')[:30]}...")
                    
                    # Create/ensure Agendapunt node exists
                    agendapunt_props = {
                        'id': agendapunt.id,
                        'onderwerp': getattr(agendapunt, 'onderwerp', ''),
                        'nummer': getattr(agendapunt, 'nummer', None),
                        'volgorde': getattr(agendapunt, 'volgorde', None)
                    }
                    session.execute_write(merge_node, 'Agendapunt', 'id', agendapunt_props)
                    
                    # Create Zaak → Agendapunt relationship
                    session.execute_write(merge_rel, 'Zaak', 'nummer', zaak_nummer,
                                          'Agendapunt', 'id', agendapunt.id, 'HAS_AGENDAPUNT')
                    connection_count += 1
            
            # Get related activiteiten  
            activiteiten = zaak_obj.activiteiten
            print(f"    🎯 Zaak {zaak_nummer} has {len(activiteiten)} related activiteiten")
            
            for activiteit in activiteiten:
                if activiteit and hasattr(activiteit, 'id') and activiteit.id:
                    print(f"      🎯 Processing Activiteit: {activiteit.id} - {getattr(activiteit, 'onderwerp', 'No subject')[:30]}...")
                    
                    # Create/ensure Activiteit node exists
                    activiteit_props = {
                        'id': activiteit.id,
                        'onderwerp': getattr(activiteit, 'onderwerp', ''),
                        'nummer': getattr(activiteit, 'nummer', None),
                        'soort': getattr(activiteit, 'soort', {}).get('name', '') if hasattr(getattr(activiteit, 'soort', {}), 'name') else str(getattr(activiteit, 'soort', '')),
                        'begin': str(getattr(activiteit, 'begin', None)) if getattr(activiteit, 'begin', None) else None,
                        'einde': str(getattr(activiteit, 'einde', None)) if getattr(activiteit, 'einde', None) else None
                    }
                    session.execute_write(merge_node, 'Activiteit', 'id', activiteit_props)
                    
                    # Create Zaak → Activiteit relationship
                    session.execute_write(merge_rel, 'Zaak', 'nummer', zaak_nummer,
                                          'Activiteit', 'id', activiteit.id, 'HAS_ACTIVITEIT')
                    connection_count += 1
                    
        except Exception as e:
            print(f"    ⚠️ Could not enrich connections for Zaak {zaak_nummer}: {e}")
            continue
    
    print(f"✅ Created {connection_count} enriched Zaak→Agendapunt/Activiteit connections")
    return connection_count


def generate_comprehensive_vlos_analysis_report(session, all_interruptions: List[Dict[str, Any]], 
                                                all_voting_events: List[Dict[str, Any]], 
                                                processing_stats: Dict[str, Any]) -> Dict[str, Any]:
    """Generate comprehensive analysis report similar to test file output - FOR PRODUCTION USE."""
    
    print("\n" + "="*80)
    print("📊 GENERATING COMPREHENSIVE VLOS ANALYSIS REPORT")
    print("="*80)
    
    # Get overall statistics from Neo4j
    stats_query = """
    MATCH (eva:EnhancedVlosActivity) 
    WITH count(eva) as total_activities
    MATCH (eva:EnhancedVlosActivity)-[:MATCHES_API_ACTIVITY]->(a:Activiteit)
    WITH total_activities, count(eva) as matched_activities
    MATCH (vs:VlosSpeaker)
    WITH total_activities, matched_activities, count(vs) as total_speakers
    MATCH (vs:VlosSpeaker)-[:MATCHED_TO_PERSOON]->(p:Persoon)
    WITH total_activities, matched_activities, total_speakers, count(vs) as matched_speakers
    MATCH (z:Zaak)
    WHERE z.data_source = 'tk_api_fresh'
    WITH total_activities, matched_activities, total_speakers, matched_speakers, count(z) as matched_zaken
    MATCH (d:Dossier)
    WHERE d.data_source = 'tk_api_fresh'
    WITH total_activities, matched_activities, total_speakers, matched_speakers, matched_zaken, count(d) as matched_dossiers
    MATCH (doc:Document)
    WHERE doc.data_source = 'tk_api_fresh'
    RETURN total_activities, matched_activities, total_speakers, matched_speakers, 
           matched_zaken, matched_dossiers, count(doc) as matched_documents
    """
    
    stats_result = session.run(stats_query).single()
    
    if stats_result:
        total_activities = stats_result['total_activities'] or 0
        matched_activities = stats_result['matched_activities'] or 0
        total_speakers = stats_result['total_speakers'] or 0  
        matched_speakers = stats_result['matched_speakers'] or 0
        matched_zaken = stats_result['matched_zaken'] or 0
        matched_dossiers = stats_result['matched_dossiers'] or 0
        matched_documents = stats_result['matched_documents'] or 0
    else:
        total_activities = matched_activities = total_speakers = matched_speakers = 0
        matched_zaken = matched_dossiers = matched_documents = 0

    # Calculate percentages
    activity_match_pct = (matched_activities / total_activities * 100.0) if total_activities else 0.0
    speaker_match_pct = (matched_speakers / total_speakers * 100.0) if total_speakers else 0.0
    
    print(f"\n=== OVERALL MATCH RATES ===")
    print(f"🎯 Activities: {matched_activities}/{total_activities} ({activity_match_pct:.1f}%)")
    print(f"👥 Speakers: {matched_speakers}/{total_speakers} ({speaker_match_pct:.1f}%)")
    print(f"📋 Zaken: {matched_zaken} (fresh from API)")
    print(f"📁 Dossiers: {matched_dossiers} (fresh from API)")
    print(f"📄 Documents: {matched_documents} (fresh from API)")
    
    # Speaker-Zaak Connection Analysis
    speaker_zaak_query = """
    MATCH (vs:VlosSpeaker)-[:SPOKE_ABOUT]->(target)
    WHERE target:Zaak OR target:Dossier
    WITH vs, target, labels(target)[0] as target_type
    MATCH (vs)-[:MATCHED_TO_PERSOON]->(p:Persoon)
    RETURN p.roepnaam + ' ' + p.achternaam as speaker_name, 
           CASE WHEN target_type = 'Zaak' THEN target.nummer ELSE target.nummer + ' (Dossier)' END as target_label,
           target_type,
           count(*) as connection_count
    ORDER BY connection_count DESC
    LIMIT 20
    """
    
    speaker_connections = session.run(speaker_zaak_query).data()
    connection_count = len(speaker_connections)
    
    print(f"\n=== SPEAKER-ZAAK CONNECTION ANALYSIS ===")
    print(f"🔗 Total connections: {connection_count}")
    if speaker_connections:
        print(f"\n--- TOP SPEAKERS BY LEGISLATIVE ITEMS DISCUSSED ---")
        speaker_totals = {}
        for conn in speaker_connections:
            speaker = conn['speaker_name']
            speaker_totals[speaker] = speaker_totals.get(speaker, 0) + conn['connection_count']
        
        for i, (speaker, count) in enumerate(sorted(speaker_totals.items(), key=lambda x: x[1], reverse=True)[:10], 1):
            print(f"  {i:2d}. {speaker}: {count} connections")
    
    # Interruption Analysis
    interruption_analysis = {}
    if all_interruptions:
        interruption_analysis = analyze_interruption_patterns(all_interruptions)
        print(f"\n=== PARLIAMENTARY INTERRUPTION ANALYSIS ===")
        print(f"🗣️ Total interruptions: {interruption_analysis['total_interruptions']}")
        
        if interruption_analysis['most_frequent_interrupters']:
            print(f"\n--- TOP INTERRUPTERS ---")
            for i, (interrupter, count) in enumerate(list(interruption_analysis['most_frequent_interrupters'].items())[:5], 1):
                print(f"  {i}. {interrupter}: {count} interruptions")
        
        if interruption_analysis['topics_causing_interruptions']:
            print(f"\n--- TOPICS GENERATING MOST INTERRUPTIONS ---")
            for i, (topic, data) in enumerate(list(interruption_analysis['topics_causing_interruptions'].items())[:5], 1):
                topic_clean = topic.replace('[FALLBACK]', '').strip()
                print(f"  {i}. {topic_clean[:60]}...: {data['count']} interruptions")
    
    # Voting Analysis  
    voting_analysis = {}
    if all_voting_events:
        voting_analysis = analyze_voting_patterns(all_voting_events)
        print(f"\n=== PARLIAMENTARY VOTING ANALYSIS ===")
        print(f"🗳️ Total voting events: {voting_analysis['total_voting_events']}")
        print(f"📈 Total individual fractie votes: {voting_analysis['total_individual_votes']}")
        
        if voting_analysis['fractie_alignment']:
            print(f"\n--- MOST SUPPORTIVE FRACTIES (by Voor percentage) ---")
            for i, (fractie, data) in enumerate(list(voting_analysis['fractie_alignment'].items())[:10], 1):
                print(f"  {i:2d}. {fractie}: {data['voor_percentage']:.1f}% Voor ({data['total_votes']} total votes)")
        
        if voting_analysis['most_controversial_topics']:
            print(f"\n--- MOST CONTROVERSIAL TOPICS ---")
            for i, (topic, data) in enumerate(list(voting_analysis['most_controversial_topics'].items())[:5], 1):
                topic_clean = topic.replace('[FALLBACK]', '').strip()
                print(f"  {i}. {topic_clean[:60]}...: {data['consensus_level']:.1f}% consensus")
    
    # Store analysis in Neo4j for future reference
    analysis_summary = {
        'generated_at': str(datetime.now()),
        'total_activities': total_activities,
        'matched_activities': matched_activities,
        'activity_match_percentage': round(activity_match_pct, 1),
        'total_speakers': total_speakers,
        'matched_speakers': matched_speakers,
        'speaker_match_percentage': round(speaker_match_pct, 1),
        'matched_zaken': matched_zaken,
        'matched_dossiers': matched_dossiers,
        'matched_documents': matched_documents,
        'speaker_zaak_connections': connection_count,
        'total_interruptions': len(all_interruptions),
        'total_voting_events': len(all_voting_events),
        'processing_stats': processing_stats
    }
    
    # Create analysis summary node
    session.execute_write(merge_node, 'VlosAnalysisSummary', 'generated_at', analysis_summary)
    
    print(f"\n✅ Comprehensive analysis complete!")
    print(f"📊 Analysis summary stored in Neo4j as VlosAnalysisSummary node")
    print("="*80)
    
    return {
        'summary': analysis_summary,
        'interruption_analysis': interruption_analysis,
        'voting_analysis': voting_analysis,
        'speaker_connections': speaker_connections
    }