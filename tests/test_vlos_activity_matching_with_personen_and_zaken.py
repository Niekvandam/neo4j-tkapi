import glob
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
import re
from thefuzz import fuzz
from tkapi import TKApi
from tkapi.vergadering import Vergadering, VergaderingSoort
from tkapi.activiteit import Activiteit
from tkapi.zaak import Zaak  # Zaak linking
from tkapi.dossier import Dossier  # NEW – link <dossiernummer>
from tkapi.document import Document  # NEW – link <stuknummer> (volgnummer)
# ---------------- Person-matching helpers ----------------------------------
from typing import Optional, List
from tkapi.persoon import Persoon

# ---------------------------------------------------------------------------
# Override calc_name_similarity to consider tussenvoegsel+achternaam together
# ---------------------------------------------------------------------------


def _build_full_surname(p: Persoon) -> str:
    """Return full surname including tussenvoegsel (if any)."""
    full = f"{p.tussenvoegsel} {p.achternaam}".strip()
    return re.sub(r"\s+", " ", full).lower()


# Shadow/replace the imported helper with an enhanced version
def calc_name_similarity(v_first: str, v_last: str, p: Persoon) -> int:  # type: ignore
    """Similarity score combining full surname (tv + achternaam) and optional first name."""

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

# ---------------------------------------------------------------------------
# Zaak matching helpers
# ---------------------------------------------------------------------------


def _safe_int(val: str):
    """Return int(val) if val represents an integer, else None."""
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


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


def find_best_zaak_or_fallback(api: TKApi, dossiernummer: str, stuknummer: str) -> dict:
    """Enhanced zaak finding with dossier fallback.
    
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

# ---------------------------------------------------------------------------
# Dossier & Document helpers
# ---------------------------------------------------------------------------


_DOSSIER_REGEX = re.compile(r"^(\d+)(?:[-\s]?([A-Za-z0-9]+))?$")


def _split_dossier_code(code: str):
    """Return (nummer:int|None, toevoeging:str|None) for a dossier code like '36725-VI'."""
    m = _DOSSIER_REGEX.match(code.strip()) if code else None
    if not m:
        return None, None
    nummer = _safe_int(m.group(1))
    toevoeg = m.group(2) or None
    return nummer, toevoeg


def find_best_dossier(api: TKApi, dossier_code: str) -> Optional[Dossier]:
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

# ---------------------------------------------------------------------------
# Re-use fuzzy-name utilities from the dedicated speaker-matching test to keep
# behaviour identical and avoid code drift.
try:
    from test_vlos_speaker_quote_matching import (
        FUZZY_FIRSTNAME_THRESHOLD,
        FUZZY_SURNAME_THRESHOLD,
        calc_name_similarity as _orig_calc_name_similarity,  # ignored (overridden)
        find_best_persoon as _generic_find_best_persoon,
        collapse_text,
    )
except ModuleNotFoundError:
    from tests.test_vlos_speaker_quote_matching import (
        FUZZY_FIRSTNAME_THRESHOLD,
        FUZZY_SURNAME_THRESHOLD,
        calc_name_similarity as _orig_calc_name_similarity,  # ignored (overridden)
        find_best_persoon as _generic_find_best_persoon,
        collapse_text,
    )


def find_best_persoon(api: TKApi, first: str, last: str) -> Optional[Persoon]:
    """Thin wrapper so we can call the shared helper with local name."""
    # First try the generic exact-achternaam search (fast cache-friendly)
    res = _generic_find_best_persoon(api, first, last)
    if res:
        return res

    # Fallback: search by *contains* main surname token (last word of v_last)
    if not last:
        return None

    main_last_token = last.strip().split()[-1]
    pf = Persoon.create_filter()
    safe_last = main_last_token.replace("'", "''")
    pf.add_filter_str(f"contains(tolower(Achternaam), '{safe_last.lower()}')")

    candidates = api.get_items(Persoon, filter=pf, max_items=100)
    if not candidates:
        return None

    best_p = None
    best_sc = 0
    for p in candidates:
        s = calc_name_similarity(first, last, p)
        if s > best_sc:
            best_sc = s
            best_p = p

    return best_p if best_sc >= 60 else None


def best_persoon_from_actors(first: str, last: str, actors) -> Optional[Persoon]:
    """Pick the actor.persoon with highest similarity ≥60; None if no good hit."""
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

# ---------------------------------------------------------------------------
# Configuration (copied / aligned with tests/test.py)
# ---------------------------------------------------------------------------
LOCAL_TIMEZONE_OFFSET_HOURS = 2  # CEST for summer samples

SCORE_TIME_START_PROXIMITY = 3.0
SCORE_TIME_OVERLAP_ONLY = 1.5
SCORE_SOORT_EXACT = 2.0
SCORE_SOORT_PARTIAL_XML_IN_API = 2.0  # was 1.5 – reward stronger for partial soort hit
SCORE_SOORT_PARTIAL_API_IN_XML = 1.5  # was 1.0 – likewise
SCORE_ONDERWERP_EXACT          = 4.0   # big reward
SCORE_ONDERWERP_FUZZY_HIGH     = 2.5
SCORE_ONDERWERP_FUZZY_MEDIUM   = 2.0
MIN_MATCH_SCORE_FOR_AGENDAPUNT = 4.0
SCORE_TITEL_EXACT_VS_API_ONDERWERP = 1.5
SCORE_TITEL_FUZZY_HIGH_VS_API_ONDERWERP = 1.25
SCORE_TITEL_FUZZY_MEDIUM_VS_API_ONDERWERP = 0.5

MIN_MATCH_SCORE_FOR_ACTIVITEIT = 3.0  # lower threshold so time + soort alone can suffice
TIME_START_PROXIMITY_TOLERANCE_SECONDS = 300  # 5 minutes
TIME_GENERAL_OVERLAP_BUFFER_SECONDS = 600     # 10 minutes

FUZZY_SIMILARITY_THRESHOLD_HIGH = 85  # was 90 – slightly looser
FUZZY_SIMILARITY_THRESHOLD_MEDIUM = 70  # slightly looser medium fuzzy cut-off

# ---------------------------------------------------------------------------
# A) Topic normalisation helpers (strip generic prefixes)
# ---------------------------------------------------------------------------

COMMON_TOPIC_PREFIXES = [
    'tweeminutendebat',
    'procedurevergadering',
    'wetgevingsoverleg',
    'plenaire afronding',
    'plenaire afronding in 1 termijn',
    'plenaire afronding in één termijn',
    'plenaire afronding in één termijn',
    'plenaire afronding in één termijn',
    'plenaire afronding in een termijn',
    'plenaire afronding',
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

_PREFIX_REGEX = re.compile(r'^(' + '|'.join(re.escape(p) for p in COMMON_TOPIC_PREFIXES) + r')[\s:,-]+', re.IGNORECASE)


def normalize_topic(text: str) -> str:
    """Lower-case, strip, and remove common boilerplate prefixes for fair fuzzy matching."""
    if not text:
        return ''
    text = text.strip().lower()
    # remove prefix once
    text = _PREFIX_REGEX.sub('', text, count=1)
    # collapse whitespace
    text = re.sub(r'\s+', ' ', text)
    return text

# Slightly boost high-fuzzy rewards
SCORE_ONDERWERP_FUZZY_HIGH     = 2.5  # was 3.0? adjust for new scale (if earlier 3 stays fine, keep)
SCORE_TITEL_FUZZY_HIGH_VS_API_ONDERWERP = 1.25  # was 1.0

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

NS = {'vlos': 'http://www.tweedekamer.nl/ggm/vergaderverslag/v1.0'}

# ---------------------------------------------------------------------------
# Helper functions (trimmed version of tests/test.py helpers)
# ---------------------------------------------------------------------------

def parse_xml_datetime(datetime_val):
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
    if not dt_obj:
        return None
    if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
        return (dt_obj - timedelta(hours=local_offset_hours)).replace(tzinfo=timezone.utc)
    return dt_obj.astimezone(timezone.utc)


def evaluate_time_match(xml_start, xml_end, api_start, api_end):
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

# ---------------------------------------------------------------------------
# Test routine
# ---------------------------------------------------------------------------

def test_sample_vlos_files_agendapunt_matching():
    """Iterate over sample_vlos_*.xml files and attempt to find best API Activiteit matches.
    
    Enhanced version with Dossier fallback logic for Zaak matching.
    """
    api = TKApi(verbose=False)

    xml_files = glob.glob('sample_vlos_*.xml')
    assert xml_files, 'No sample_vlos_*.xml files found in repository root.'
    # Global counters – activiteiten and speakers
    total_xml_acts = 0
    total_matched_acts = 0
    total_speakers = 0
    total_matched_speakers = 0

    # Zaak counters (init before first use)
    total_xml_zaken = 0
    total_matched_zaken = 0

    unmatched_acts = []  # collect details of unmatched activiteiten

    matched_speaker_labels = []  # store unique labels for summary
    unmatched_speaker_labels = []
    matched_zaak_labels = []
    unmatched_zaak_labels = []

    # Dossier / Document tracking
    total_xml_dossiers = 0
    total_matched_dossiers = 0
    matched_dossier_labels = []
    unmatched_dossier_labels = []

    total_xml_docs = 0
    total_matched_docs = 0
    matched_doc_labels = []
    unmatched_doc_labels = []

    # NEW: Speaker-Zaak connection tracking
    speaker_zaak_connections = []  # List of (persoon, zaak/dossier, context) tuples
    speaker_activity_map = {}      # persoon_id -> list of activities they spoke in
    zaak_activity_map = {}         # zaak_id -> list of activities it was discussed in
    activity_speakers = {}         # activity_id -> list of speakers
    activity_zaken = {}            # activity_id -> list of zaken/dossiers

    for xml_path in xml_files:
        print('\n' + '=' * 80)
        print(f'Processing XML file: {xml_path}')

        with open(xml_path, 'r', encoding='utf-8') as fh:
            xml_data = fh.read()
        root = ET.fromstring(xml_data)
        vergadering_el = root.find('vlos:vergadering', NS)
        assert vergadering_el is not None, 'XML lacks <vergadering> element.'

        # Extract basic vergadering info
        xml_soort = vergadering_el.get('soort', '')
        xml_titel = vergadering_el.findtext('vlos:titel', default='', namespaces=NS)
        xml_nummer = vergadering_el.findtext('vlos:vergaderingnummer', default='', namespaces=NS)
        xml_date_str = vergadering_el.findtext('vlos:datum', default='', namespaces=NS)
        assert xml_date_str, 'XML vergadering missing <datum>'

        target_date = datetime.strptime(xml_date_str.split('T')[0], '%Y-%m-%d')
        utc_start = target_date - timedelta(hours=LOCAL_TIMEZONE_OFFSET_HOURS)
        utc_end = target_date + timedelta(days=1) - timedelta(hours=LOCAL_TIMEZONE_OFFSET_HOURS)

        v_filter = Vergadering.create_filter()
        v_filter.filter_date_range(begin_datetime=utc_start, end_datetime=utc_end)
        if xml_soort:
            if xml_soort.lower() == 'plenair':
                v_filter.filter_soort(VergaderingSoort.PLENAIR)
            elif xml_soort.lower() == 'commissie':
                v_filter.filter_soort(VergaderingSoort.COMMISSIE)
            else:
                v_filter.filter_soort(xml_soort)
        if xml_nummer:
            try:
                v_filter.add_filter_str(f'VergaderingNummer eq {int(xml_nummer)}')
            except ValueError:
                pass

        Vergadering.expand_params = ['Verslag']
        vergaderingen = api.get_items(Vergadering, filter=v_filter, max_items=5)
        Vergadering.expand_params = None
        assert vergaderingen, 'No TKApi Vergadering found for XML file.'
        canonical_verg = vergaderingen[0]
        print(f'Canonical Vergadering chosen: {canonical_verg.id} ({canonical_verg.titel})')

        # Fetch activiteiten in timeframe (reuse earlier logic)
        act_filter = Activiteit.create_filter()
        time_buffer = timedelta(minutes=60)  # wider buffer (±1 hour)

        # Convert to UTC before sending to TK-API – avoids paging out morning items
        start_utc = (canonical_verg.begin - time_buffer).astimezone(timezone.utc)
        end_utc = (canonical_verg.einde + time_buffer).astimezone(timezone.utc)

        act_filter.filter_date_range(
            begin_datetime=start_utc,
            end_datetime=end_utc,
        )
        candidate_acts = api.get_items(Activiteit, filter=act_filter, max_items=200)

        # Drop Agendapunt import – we focus on Activiteit matching

        print(f'Fetched {len(candidate_acts)} candidate API activiteiten')

        # ------------------------------------------------------------------
        # Match each top-level XML <activiteit> directly to API Activiteit
        # ------------------------------------------------------------------
        file_xml_count = 0
        file_match_count = 0

        for xml_act in vergadering_el.findall('vlos:activiteit', NS):
            total_xml_acts += 1
            file_xml_count += 1
            xml_id = xml_act.get('objectid')
            xml_soort = xml_act.get('soort')
            xml_titel = xml_act.findtext('vlos:titel', default='', namespaces=NS)
            xml_onderwerp = xml_act.findtext('vlos:onderwerp', default='', namespaces=NS)

            # Initialize tracking for this activity
            activity_speakers[xml_id] = []
            activity_zaken[xml_id] = []

            xml_start = parse_xml_datetime(
                xml_act.findtext('vlos:aanvangstijd', default=None, namespaces=NS)
                or xml_act.findtext('vlos:markeertijdbegin', default=None, namespaces=NS)
            )
            xml_end = parse_xml_datetime(
                xml_act.findtext('vlos:eindtijd', default=None, namespaces=NS)
                or xml_act.findtext('vlos:markeertijdeind', default=None, namespaces=NS)
            )

            # C) Fallback to canonical vergadering timeframe when XML lacks explicit times
            if not xml_start:
                xml_start = canonical_verg.begin
            if not xml_end:
                xml_end = canonical_verg.einde

            best_match = None
            best_score = 0.0
            potential_matches = []  # collect (score, reasons, api_act)

            for api_act in candidate_acts:
                score = 0.0
                reasons = []

                # ------------------------ Time proximity ------------------
                time_score, time_reason = evaluate_time_match(
                    xml_start,
                    xml_end,
                    api_act.begin,
                    api_act.einde,
                )
                score += time_score
                if time_score:
                    reasons.append(time_reason)

                # ------------------------ Soort comparison ---------------
                xml_s = (xml_soort or '').lower()
                api_s = (
                    api_act.soort.value.lower()
                    if api_act.soort and hasattr(api_act.soort, 'value')
                    else str(api_act.soort).lower()
                )
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
                        # Alias check: if any alias of xml_s appears in api_s
                        for alias in SOORT_ALIAS.get(xml_s, []):
                            if alias in api_s:
                                score += SCORE_SOORT_PARTIAL_XML_IN_API
                                reasons.append(f"Soort alias match ('{alias}')")
                                break

                # ------------------------ Onderwerp / titel fuzz ---------
                api_ond = (api_act.onderwerp or '').lower()
                # Normalised versions for fuzzy comparison (A)
                xml_ond = (xml_onderwerp or '').lower()
                xml_tit = (xml_titel or '').lower()

                norm_api_ond = normalize_topic(api_ond)
                norm_xml_ond = normalize_topic(xml_ond)
                norm_xml_tit = normalize_topic(xml_tit)

                if xml_ond and api_ond:
                    if norm_xml_ond == norm_api_ond:
                        score += SCORE_ONDERWERP_EXACT
                        reasons.append("Onderwerp exact")
                    else:
                        ratio = fuzz.ratio(norm_xml_ond, norm_api_ond)
                        if ratio >= FUZZY_SIMILARITY_THRESHOLD_HIGH:
                            score += SCORE_ONDERWERP_FUZZY_HIGH
                            reasons.append(f"Onderwerp fuzzy high ({ratio}%)")
                        elif ratio >= FUZZY_SIMILARITY_THRESHOLD_MEDIUM:
                            score += SCORE_ONDERWERP_FUZZY_MEDIUM
                            reasons.append(f"Onderwerp fuzzy medium ({ratio}%)")

                if xml_tit and api_ond:
                    if norm_xml_tit == norm_api_ond:
                        score += SCORE_TITEL_EXACT_VS_API_ONDERWERP
                        reasons.append("Titel exact vs API onderwerp")
                    else:
                        ratio = fuzz.ratio(norm_xml_tit, norm_api_ond)
                        if ratio >= FUZZY_SIMILARITY_THRESHOLD_HIGH:
                            score += SCORE_TITEL_FUZZY_HIGH_VS_API_ONDERWERP
                            reasons.append(f"Titel fuzzy high ({ratio}%)")
                        elif ratio >= FUZZY_SIMILARITY_THRESHOLD_MEDIUM:
                            score += SCORE_TITEL_FUZZY_MEDIUM_VS_API_ONDERWERP
                            reasons.append(f"Titel fuzzy medium ({ratio}%)")

                potential_matches.append({
                    'score': score,
                    'reasons': reasons,
                    'api_act': api_act,
                })

                if score > best_score:
                    best_score = score
                    best_match = api_act

            # Sort potentials by score desc
            potential_matches.sort(key=lambda d: d['score'], reverse=True)

            # ------------------------ Reporting ---------------------------
            print(f'  XML activiteit {xml_id} ("{xml_titel}") best score: {best_score:.2f}')

            # Detailed listing of all potentials for debugging
            for pot in potential_matches:
                act = pot['api_act']
                api_soort_display = (
                    act.soort.value if act.soort and hasattr(act.soort, 'value') else str(act.soort)
                )
                print(
                    f"    -> API_ID={act.id}, Score={pot['score']:.2f}, Soort='{api_soort_display}', "
                    f"Onderwerp='{act.onderwerp}', Reasons={'; '.join(pot['reasons'])}"
                )

            # Determine acceptance based on threshold or relative lead (D)
            accept_match = False
            if best_score >= MIN_MATCH_SCORE_FOR_ACTIVITEIT:
                accept_match = True
            else:
                runner_up_score = potential_matches[1]['score'] if len(potential_matches) > 1 else 0.0
                if best_score - runner_up_score >= 1.0 and best_score >= 1.0:
                    accept_match = True

            if accept_match and best_match:
                print(
                    f'    ✅ BEST MATCH: API activiteit {best_match.id} '
                    f'(onderwerp="{best_match.onderwerp}")'
                )
                total_matched_acts += 1
                file_match_count += 1
            else:
                print('    ❌ No strong match found')
                unmatched_acts.append({
                    'file': xml_path,
                    'xml_id': xml_id,
                    'titel': xml_titel,
                    'best_score': best_score,
                })

            # ------------------------------------------------------------------
            # SPEAKER PROCESSING – map <spreker> elements to TK-API Personen
            # ------------------------------------------------------------------
            selected_act = best_match if accept_match and best_match else None
            actor_persons = selected_act.actors if selected_act else []

            for frag in xml_act.findall(".//vlos:draadboekfragment", NS):
                tekst_el = frag.find("vlos:tekst", NS)
                if tekst_el is None:
                    continue
                speech_text = collapse_text(tekst_el)
                if not speech_text:
                    continue

                for sprek_el in frag.findall("vlos:sprekers/vlos:spreker", NS):
                    v_first = sprek_el.findtext("vlos:voornaam", default="", namespaces=NS)
                    v_last = (
                        sprek_el.findtext("vlos:verslagnaam", default="", namespaces=NS)
                        or sprek_el.findtext("vlos:achternaam", default="", namespaces=NS)
                    )

                    total_speakers += 1

                    # 1) Prefer someone already registered as actor in this activiteit
                    matched = best_persoon_from_actors(v_first, v_last, actor_persons)

                    # 2) Fallback to surname search across all TK-API Personen
                    if not matched:
                        matched = find_best_persoon(api, v_first, v_last)

                    if matched:
                        total_matched_speakers += 1
                        person_label = f"{matched.roepnaam or matched.voornaam} {matched.achternaam} (id {matched.id})"
                        matched_speaker_labels.append(person_label)
                        
                        # Track this speaker in this activity
                        speaker_info = {
                            'persoon': matched,
                            'name': f"{matched.roepnaam or matched.voornaam} {matched.achternaam}",
                            'speech_text': speech_text[:200],  # Keep some context
                        }
                        activity_speakers[xml_id].append(speaker_info)
                        
                        # Update speaker->activity mapping
                        if matched.id not in speaker_activity_map:
                            speaker_activity_map[matched.id] = []
                        speaker_activity_map[matched.id].append({
                            'activity_id': xml_id,
                            'activity_title': xml_titel,
                            'speech_preview': speech_text[:100]
                        })
                    else:
                        person_label = f"{v_first} {v_last} [NO MATCH]"
                        unmatched_speaker_labels.append(person_label)

                    print(f"        • {person_label} — \"{speech_text[:120]}...\"")

            # ------------------------------------------------------------------
            # ZAAK PROCESSING – link XML <zaak> elements to TK-API Zaken + speakers
            # ------------------------------------------------------------------

            for xml_zaak in xml_act.findall(".//vlos:zaak", NS):
                total_xml_zaken += 1

                dossiernr = xml_zaak.findtext("vlos:dossiernummer", default="", namespaces=NS).strip()
                stuknr = xml_zaak.findtext("vlos:stuknummer", default="", namespaces=NS).strip()
                zaak_titel = xml_zaak.findtext("vlos:titel", default="", namespaces=NS).strip()

                # Use enhanced matching with fallback logic
                match_result = find_best_zaak_or_fallback(api, dossiernr, stuknr)
                
                # Initialize variables for this zaak
                zaak_obj = None
                zaak_type = None
                zaak_label = None

                if match_result['success']:
                    total_matched_zaken += 1
                    
                    if match_result['match_type'] == 'zaak':
                        zaak = match_result['zaak']
                        zaak_label = f"{zaak.soort.value if zaak.soort else ''} {zaak.nummer} (id {zaak.id})"
                        zaak_obj = zaak
                        zaak_type = 'zaak'
                    elif match_result['match_type'] == 'dossier_fallback':
                        dossier = match_result['dossier']
                        zaak_label = f"Dossier {dossier.nummer}{(' '+dossier.toevoeging) if dossier.toevoeging else ''} (id {dossier.id}) [FALLBACK]"
                        zaak_obj = dossier
                        zaak_type = 'dossier'
                    
                    matched_zaak_labels.append(zaak_label)
                    
                    # Track this zaak in this activity
                    zaak_info = {
                        'object': zaak_obj,
                        'type': zaak_type,
                        'label': zaak_label,
                        'dossiernr': dossiernr,
                        'stuknr': stuknr,
                        'titel': zaak_titel
                    }
                    activity_zaken[xml_id].append(zaak_info)
                    
                    # Update zaak->activity mapping
                    if zaak_obj.id not in zaak_activity_map:
                        zaak_activity_map[zaak_obj.id] = []
                    zaak_activity_map[zaak_obj.id].append({
                        'activity_id': xml_id,
                        'activity_title': xml_titel,
                        'zaak_title': zaak_titel
                    })
                    
                    # Create connections between speakers and this zaak within this activity
                    for speaker_info in activity_speakers[xml_id]:
                        connection = {
                            'persoon': speaker_info['persoon'],
                            'persoon_name': speaker_info['name'],
                            'zaak_object': zaak_obj,
                            'zaak_type': zaak_type,
                            'zaak_label': zaak_label,
                            'activity_id': xml_id,
                            'activity_title': xml_titel,
                            'context': f"Spoke in activity about {zaak_titel or dossiernr}",
                            'speech_preview': speaker_info['speech_text']
                        }
                        speaker_zaak_connections.append(connection)
                else:
                    zaak_label = f"[NO MATCH] dossier={dossiernr} stuk={stuknr}"
                    unmatched_zaak_labels.append(zaak_label)

                print(f"        ↳ Zaak: {zaak_titel or dossiernr} → {zaak_label}")

                # Attempt to link speakers inside this zaak element
                for sprek_el in xml_zaak.findall("vlos:sprekers/vlos:spreker", NS):
                    v_first = sprek_el.findtext("vlos:voornaam", default="", namespaces=NS)
                    v_last = (
                        sprek_el.findtext("vlos:verslagnaam", default="", namespaces=NS)
                        or sprek_el.findtext("vlos:achternaam", default="", namespaces=NS)
                    )

                    persoon = find_best_persoon(api, v_first, v_last)
                    person_display = (
                        f"{persoon.roepnaam or persoon.voornaam} {persoon.achternaam} (id {persoon.id})"
                        if persoon
                        else f"{v_first} {v_last} [NO MATCH]"
                    )

                    print(f"            • Speaker link: {person_display}")
                    
                    # Create direct speaker-zaak connection if both matched
                    if persoon and zaak_obj is not None:
                        connection = {
                            'persoon': persoon,
                            'persoon_name': f"{persoon.roepnaam or persoon.voornaam} {persoon.achternaam}",
                            'zaak_object': zaak_obj,
                            'zaak_type': zaak_type,
                            'zaak_label': zaak_label,
                            'activity_id': xml_id,
                            'activity_title': xml_titel,
                            'context': f"Directly linked to {zaak_titel or dossiernr}",
                            'speech_preview': f"[Direct zaak speaker link - no speech text]"
                        }
                        speaker_zaak_connections.append(connection)

                # ------------------------------------------------------------------
                # DOSSIER / DOCUMENT PROCESSING – derive from same dossier/stuk pair
                # ------------------------------------------------------------------

                if dossiernr:
                    total_xml_dossiers += 1
                    dossier_obj = find_best_dossier(api, dossiernr)
                    if dossier_obj:
                        total_matched_dossiers += 1
                        dossier_label = f"{dossier_obj.nummer}{(' '+dossier_obj.toevoeging) if dossier_obj.toevoeging else ''} (id {dossier_obj.id})"
                        matched_dossier_labels.append(dossier_label)
                    else:
                        dossier_label = f"[NO MATCH] {dossiernr}"
                        unmatched_dossier_labels.append(dossier_label)

                    print(f"            ↳ Dossier: {dossiernr} → {dossier_label}")

                    if stuknr:
                        total_xml_docs += 1
                        num, toevoeg = _split_dossier_code(dossiernr)
                        doc_obj = find_best_document(api, num, toevoeg, stuknr)
                        if doc_obj:
                            total_matched_docs += 1
                            doc_label = f"Doc {doc_obj.nummer or ''}/{doc_obj.volgnummer} (id {doc_obj.id})"
                            matched_doc_labels.append(doc_label)
                        else:
                            doc_label = f"[NO MATCH] stuk={stuknr}"
                            unmatched_doc_labels.append(f"{dossiernr}:{stuknr}")

                        print(f"                • Document: {stuknr} → {doc_label}")

        print(f'File summary: matched {file_match_count}/{file_xml_count} activiteiten')
        print('-' * 80)

    # Overall summary – activiteit matches
    match_pct = (total_matched_acts / total_xml_acts * 100.0) if total_xml_acts else 0.0
    print(f"\n=== OVERALL MATCH RATE: {total_matched_acts}/{total_xml_acts} "
          f"({match_pct:.1f}%) ===")

    # Speaker summary
    if total_speakers:
        speaker_pct = total_matched_speakers / total_speakers * 100.0
        print(f"=== SPEAKER MATCH RATE: {total_matched_speakers}/{total_speakers} ({speaker_pct:.1f}%) ===")
    else:
        print("No speaker fragments processed.")

    # Detailed speaker lists
    if matched_speaker_labels:
        print("\n--- MATCHED SPEAKERS ---")
        for lbl in sorted(set(matched_speaker_labels)):
            print(f"  • {lbl}")
    if unmatched_speaker_labels:
        print("\n--- UNMATCHED SPEAKERS ---")
        for lbl in sorted(set(unmatched_speaker_labels)):
            print(f"  • {lbl}")

    # Zaak summary (with fallback logic)
    zaak_pct = (total_matched_zaken / total_xml_zaken * 100.0) if total_xml_zaken else 0.0
    print(f"\n=== ZAAK MATCH RATE (with Dossier fallback): {total_matched_zaken}/{total_xml_zaken} ({zaak_pct:.1f}%) ===")

    if matched_zaak_labels:
        print("\n--- MATCHED ZAKEN (including Dossier fallbacks) ---")
        direct_zaken = [lbl for lbl in matched_zaak_labels if '[FALLBACK]' not in lbl]
        fallback_zaken = [lbl for lbl in matched_zaak_labels if '[FALLBACK]' in lbl]
        
        if direct_zaken:
            print(f"  Direct Zaak matches ({len(direct_zaken)}):")
            for lbl in sorted(set(direct_zaken)):
                print(f"    • {lbl}")
        
        if fallback_zaken:
            print(f"  Dossier fallback matches ({len(fallback_zaken)}):")
            for lbl in sorted(set(fallback_zaken)):
                print(f"    • {lbl}")

    if unmatched_zaak_labels:
        print("\n--- UNMATCHED ZAKEN (no Zaak or Dossier found) ---")
        for lbl in sorted(set(unmatched_zaak_labels)):
            print(f"  • {lbl}")

    # ------------------------------------------------------------
    # Dossier summary
    dossier_pct = (total_matched_dossiers / total_xml_dossiers * 100.0) if total_xml_dossiers else 0.0
    print(f"\n=== DOSSIER MATCH RATE: {total_matched_dossiers}/{total_xml_dossiers} ({dossier_pct:.1f}%) ===")

    if matched_dossier_labels:
        print("\n--- MATCHED DOSSIERS ---")
        for lbl in sorted(set(matched_dossier_labels)):
            print(f"  • {lbl}")

    if unmatched_dossier_labels:
        print("\n--- UNMATCHED DOSSIERS ---")
        for lbl in sorted(set(unmatched_dossier_labels)):
            print(f"  • {lbl}")

    # ------------------------------------------------------------
    # Document summary
    doc_pct = (total_matched_docs / total_xml_docs * 100.0) if total_xml_docs else 0.0
    print(f"\n=== DOCUMENT MATCH RATE: {total_matched_docs}/{total_xml_docs} ({doc_pct:.1f}%) ===")

    if matched_doc_labels:
        print("\n--- MATCHED DOCUMENTS ---")
        for lbl in sorted(set(matched_doc_labels)):
            print(f"  • {lbl}")

    if unmatched_doc_labels:
        print("\n--- UNMATCHED DOCUMENTS ---")
        for lbl in sorted(set(unmatched_doc_labels)):
            print(f"  • {lbl}")

    # ============================================================================
    # NEW: Speaker-Zaak Connection Analysis
    # ============================================================================
    
    print(f"\n{'='*80}")
    print(f"🔗 SPEAKER-ZAAK CONNECTION ANALYSIS")
    print(f"{'='*80}")
    
    connection_count = len(speaker_zaak_connections)
    unique_speakers_with_connections = len(set(conn['persoon'].id for conn in speaker_zaak_connections))
    unique_zaken_discussed = len(set(conn['zaak_object'].id for conn in speaker_zaak_connections))
    
    print(f"📊 Total speaker-zaak connections: {connection_count}")
    print(f"👥 Unique speakers with connections: {unique_speakers_with_connections}")
    print(f"📋 Unique zaken/dossiers discussed: {unique_zaken_discussed}")
    
    if speaker_zaak_connections:
        # Group connections by speaker
        speaker_connections = {}
        for conn in speaker_zaak_connections:
            speaker_id = conn['persoon'].id
            if speaker_id not in speaker_connections:
                speaker_connections[speaker_id] = {
                    'name': conn['persoon_name'],
                    'connections': []
                }
            speaker_connections[speaker_id]['connections'].append(conn)
        
        print(f"\n--- TOP SPEAKERS BY LEGISLATIVE ITEMS DISCUSSED ---")
        speaker_counts = [(sid, len(data['connections']), data['name']) 
                         for sid, data in speaker_connections.items()]
        speaker_counts.sort(key=lambda x: x[1], reverse=True)
        
        for i, (speaker_id, count, name) in enumerate(speaker_counts[:10], 1):
            print(f"  {i:2d}. {name}: {count} items")
        
        # Group connections by zaak/dossier
        zaak_connections = {}
        for conn in speaker_zaak_connections:
            zaak_id = conn['zaak_object'].id
            if zaak_id not in zaak_connections:
                zaak_connections[zaak_id] = {
                    'label': conn['zaak_label'],
                    'type': conn['zaak_type'],
                    'speakers': []
                }
            zaak_connections[zaak_id]['speakers'].append(conn)
        
        print(f"\n--- TOP LEGISLATIVE ITEMS BY NUMBER OF SPEAKERS ---")
        zaak_counts = [(zid, len(data['speakers']), data['label']) 
                      for zid, data in zaak_connections.items()]
        zaak_counts.sort(key=lambda x: x[1], reverse=True)
        
        for i, (zaak_id, count, label) in enumerate(zaak_counts[:10], 1):
            print(f"  {i:2d}. {label}: {count} speakers")
        
        # Show some detailed examples
        print(f"\n--- DETAILED EXAMPLES: WHO SAID WHAT ABOUT WHAT ---")
        for i, conn in enumerate(speaker_zaak_connections[:5], 1):
            print(f"\n  Example {i}:")
            print(f"    👤 Speaker: {conn['persoon_name']}")
            print(f"    📋 About: {conn['zaak_label']}")
            print(f"    🎯 Activity: {conn['activity_title']}")
            print(f"    💬 Speech preview: \"{conn['speech_preview']}...\"")
        
        if len(speaker_zaak_connections) > 5:
            print(f"\n    ... and {len(speaker_zaak_connections) - 5} more connections")
    
    # List any unmatched activiteiten
    if unmatched_acts:
        print("\n--- UNMATCHED XML ACTIVITEITEN ---")
        for item in unmatched_acts:
            print(f"{item['file']} :: {item['xml_id']} — \"{item['titel']}\" (best score {item['best_score']:.2f})")
    else:
        print("\nAll activiteiten matched ✅") 