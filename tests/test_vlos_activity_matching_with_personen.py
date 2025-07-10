import glob
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
import re
from thefuzz import fuzz
from tkapi import TKApi
from tkapi.vergadering import Vergadering, VergaderingSoort
from tkapi.activiteit import Activiteit
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
    """Iterate over sample_vlos_*.xml files and attempt to find best API Activiteit matches."""
    api = TKApi(verbose=False)

    xml_files = glob.glob('sample_vlos_*.xml')
    assert xml_files, 'No sample_vlos_*.xml files found in repository root.'
    # Global counters – activiteiten and speakers
    total_xml_acts = 0
    total_matched_acts = 0
    total_speakers = 0
    total_matched_speakers = 0
    unmatched_acts = []  # collect details of unmatched activiteiten

    matched_speaker_labels = []  # store unique labels for summary
    unmatched_speaker_labels = []

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
                    else:
                        person_label = f"{v_first} {v_last} [NO MATCH]"
                        unmatched_speaker_labels.append(person_label)

                    print(f"        • {person_label} — \"{speech_text[:120]}...\"")

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

    # List any unmatched activiteiten
    if unmatched_acts:
        print("\n--- UNMATCHED XML ACTIVITEITEN ---")
        for item in unmatched_acts:
            print(f"{item['file']} :: {item['xml_id']} — \"{item['titel']}\" (best score {item['best_score']:.2f})")
    else:
        print("\nAll activiteiten matched ✅") 