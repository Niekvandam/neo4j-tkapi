import glob
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

# Reuse identical matching logic from the activity-matching test
from thefuzz import fuzz
try:
    from test_vlos_activity_matching import (
        SCORE_TIME_START_PROXIMITY,
        SCORE_TIME_OVERLAP_ONLY,
        SCORE_SOORT_EXACT,
        SCORE_SOORT_PARTIAL_XML_IN_API,
        SCORE_SOORT_PARTIAL_API_IN_XML,
        SCORE_ONDERWERP_EXACT,
        SCORE_ONDERWERP_FUZZY_HIGH,
        SCORE_ONDERWERP_FUZZY_MEDIUM,
        SCORE_TITEL_EXACT_VS_API_ONDERWERP,
        SCORE_TITEL_FUZZY_HIGH_VS_API_ONDERWERP,
        SCORE_TITEL_FUZZY_MEDIUM_VS_API_ONDERWERP,
        MIN_MATCH_SCORE_FOR_ACTIVITEIT,
        TIME_START_PROXIMITY_TOLERANCE_SECONDS,
        TIME_GENERAL_OVERLAP_BUFFER_SECONDS,
        FUZZY_SIMILARITY_THRESHOLD_HIGH,
        FUZZY_SIMILARITY_THRESHOLD_MEDIUM,
        SOORT_ALIAS,
        normalize_topic,
        evaluate_time_match as original_evaluate_time_match,
        parse_xml_datetime as ref_parse_xml_datetime,
    )
except ModuleNotFoundError:
    from tests.test_vlos_activity_matching import (
        SCORE_TIME_START_PROXIMITY,
        SCORE_TIME_OVERLAP_ONLY,
        SCORE_SOORT_EXACT,
        SCORE_SOORT_PARTIAL_XML_IN_API,
        SCORE_SOORT_PARTIAL_API_IN_XML,
        SCORE_ONDERWERP_EXACT,
        SCORE_ONDERWERP_FUZZY_HIGH,
        SCORE_ONDERWERP_FUZZY_MEDIUM,
        SCORE_TITEL_EXACT_VS_API_ONDERWERP,
        SCORE_TITEL_FUZZY_HIGH_VS_API_ONDERWERP,
        SCORE_TITEL_FUZZY_MEDIUM_VS_API_ONDERWERP,
        MIN_MATCH_SCORE_FOR_ACTIVITEIT,
        TIME_START_PROXIMITY_TOLERANCE_SECONDS,
        TIME_GENERAL_OVERLAP_BUFFER_SECONDS,
        FUZZY_SIMILARITY_THRESHOLD_HIGH,
        FUZZY_SIMILARITY_THRESHOLD_MEDIUM,
        SOORT_ALIAS,
        normalize_topic,
        evaluate_time_match as original_evaluate_time_match,
        parse_xml_datetime as ref_parse_xml_datetime,
    )

# Make sure alias exists regardless of which import branch triggered
parse_xml_datetime = ref_parse_xml_datetime  # type: ignore

from tkapi import TKApi
from tkapi.activiteit import Activiteit
from tkapi.persoon import Persoon
from tkapi.vergadering import Vergadering, VergaderingSoort

# ---------------------------------------------------------------------------
# Namespace & basic constants (shared with tests/test_vlos_activity_matching)
# ---------------------------------------------------------------------------
NS = {"vlos": "http://www.tweedekamer.nl/ggm/vergaderverslag/v1.0"}
LOCAL_TIMEZONE_OFFSET_HOURS = 2  # CEST for summer samples

FUZZY_FIRSTNAME_THRESHOLD = 80  # percentage
FUZZY_SURNAME_THRESHOLD = 85

# Activity-matching helpers now come straight from the original test to avoid divergence
# ---------------------------------------------------------------------------

# We alias the imported evaluate_time_match so we can keep the name unchanged below.
def evaluate_time_match(xml_start, xml_end, api_start, api_end):
    return original_evaluate_time_match(xml_start, xml_end, api_start, api_end)


def collapse_text(elem: ET.Element) -> str:
    """Return all inner text of an XML element, collapsed to single-spaced string."""
    texts: List[str] = []
    for t in elem.itertext():
        t = t.strip()
        if t:
            texts.append(t)
    return " ".join(texts)


# ---------------------------------------------------------------------------
# Fuzzy matching helpers for Persoon selection
# ---------------------------------------------------------------------------

def calc_name_similarity(v_first: str, v_last: str, p: Persoon) -> int:
    """Very simple similarity score combining surname (mandatory) and optional firstname/roepnaam."""
    score = 0
    p_achternaam = getattr(p, 'achternaam', None)
    if not v_last or not p_achternaam:
        return score

    # Surname exact / fuzzy
    if v_last.lower() == p_achternaam.lower():
        score += 60
    else:
        score += max(fuzz.ratio(v_last.lower(), p_achternaam.lower()) - 20, 0)  # dampen partials

    # Firstname / roepnaam boost
    v_first_lower = (v_first or "").lower()
    if v_first_lower:
        first_candidates = [c for c in [getattr(p, 'roepnaam', None), getattr(p, 'voornaam', None)] if c]
        best = max((fuzz.ratio(v_first_lower, fc.lower()) for fc in first_candidates), default=0)
        if best >= FUZZY_FIRSTNAME_THRESHOLD:
            score += 40
        elif best >= 60:
            score += 20
    return score


def find_best_persoon(api: TKApi, v_first: str, v_last: str) -> Optional[Persoon]:
    """Query TK-API for Personen whose surname matches (exact) and pick best fuzzy-scored candidate."""
    if not v_last:
        return None

    # Search by exact achternaam to limit results
    pf = Persoon.create_filter()
    # Double single-quotes per OData escaping rules
    safe_last = v_last.replace("'", "''")
    pf.add_filter_str(f"Achternaam eq '{safe_last}'")

    candidates = api.get_items(Persoon, filter=pf, max_items=100)
    if not candidates:
        return None

    best_p = None
    best_score = 0
    for p in candidates:
        s = calc_name_similarity(v_first, v_last, p)
        if s > best_score:
            best_score = s
            best_p = p
    # Require a reasonable similarity threshold
    return best_p if best_score >= 60 else None


# ---------------------------------------------------------------------------
# Main routine: iterate sample_vlos_*.xml and print quotes mapped to Persoon & Activiteit
# ---------------------------------------------------------------------------

def test_vlos_speaker_quote_matching():
    """End-to-end smoke test: print which Persoon (TK-API) spoke during which Activity."""
    api = TKApi(verbose=False)

    xml_files = glob.glob("sample_vlos_*.xml")
    assert xml_files, "No sample_vlos_*.xml files found in repository root."

    # --------------------
    # Counters for quality metrics
    # --------------------
    total_xml_acts = 0
    total_linked_acts = 0
    total_speakers = 0
    total_matched_speakers = 0

    for xml_path in xml_files:
        print("\n" + "=" * 100)
        print(f"Processing XML file: {xml_path}")

        # Parse XML
        with open(xml_path, "r", encoding="utf-8") as fh:
            root = ET.fromstring(fh.read())

        vergadering_el = root.find("vlos:vergadering", NS)
        assert vergadering_el is not None, "XML lacks <vergadering> element."

        xml_date_str = vergadering_el.findtext("vlos:datum", default="", namespaces=NS)
        target_date = datetime.strptime(xml_date_str.split("T")[0], "%Y-%m-%d")
        utc_start = target_date - timedelta(hours=LOCAL_TIMEZONE_OFFSET_HOURS)
        utc_end = target_date + timedelta(days=1) - timedelta(hours=LOCAL_TIMEZONE_OFFSET_HOURS)

        # Select canonical Vergadering via TK-API (reuse logic from activity matching)
        v_filter = Vergadering.create_filter()
        v_filter.filter_date_range(begin_datetime=utc_start, end_datetime=utc_end)
        Vergadering.expand_params = ["Verslag"]
        vergaderingen = api.get_items(Vergadering, filter=v_filter, max_items=5)
        Vergadering.expand_params = None
        assert vergaderingen, "No TKApi Vergadering found for XML file."
        canonical_verg = vergaderingen[0]
        print(f"  Canonical Vergadering: {canonical_verg.id} — {canonical_verg.titel}")

        # Pre-fetch Kandidaten activiteiten in (begin-1h, einde+1h) timeframe
        act_filter = Activiteit.create_filter()
        buffer = timedelta(minutes=60)
        act_filter.filter_date_range(
            begin_datetime=(canonical_verg.begin - buffer).astimezone(timezone.utc),
            end_datetime=(canonical_verg.einde + buffer).astimezone(timezone.utc),
        )
        candidate_acts = api.get_items(Activiteit, filter=act_filter, max_items=200)
        print(f"  Retrieved {len(candidate_acts)} candidate activiteiten from TK-API")

        # Index activities by rough start time (rounded minute) to aid quick lookup
        def key_for_api_act(a: Activiteit) -> Tuple[str, str]:
            ts = a.begin.replace(second=0, microsecond=0).isoformat()
            soort = a.soort.value if a.soort and hasattr(a.soort, "value") else str(a.soort)
            return ts, soort.lower()

        api_index: Dict[Tuple[str, str], Activiteit] = {key_for_api_act(a): a for a in candidate_acts}

        # Iterate XML activiteiten and map to API using simple key (time+soort) first; fallback by fuzzy titel
        for xml_act in vergadering_el.findall("vlos:activiteit", NS):
            xml_soort = (xml_act.get("soort") or "").lower()
            xml_title = xml_act.findtext("vlos:titel", default="", namespaces=NS)
            xml_onderwerp = xml_act.findtext("vlos:onderwerp", default="", namespaces=NS)

            def ensure_aware(dt: Optional[datetime]) -> Optional[datetime]:
                """Return tz-aware datetime (UTC) even when source is naive."""
                if dt is None:
                    return None
                if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                    return dt.replace(tzinfo=timezone.utc)
                return dt

            xml_start_raw = parse_xml_datetime(
                xml_act.findtext("vlos:aanvangstijd", default=None, namespaces=NS)
                or xml_act.findtext("vlos:markeertijdbegin", default=None, namespaces=NS)
            )
            xml_start = ensure_aware(xml_start_raw) or ensure_aware(canonical_verg.begin)

            # --------------------------------------------------
            # Improved matching: score all candidate_acts and pick best
            # --------------------------------------------------

            xml_end_raw = parse_xml_datetime(
                xml_act.findtext("vlos:eindtijd", default=None, namespaces=NS)
                or xml_act.findtext("vlos:markeertijdeind", default=None, namespaces=NS)
            )
            xml_end = ensure_aware(xml_end_raw) or ensure_aware(canonical_verg.einde)

            best_match = None
            best_score = 0.0
            potential_matches = []  # collect (score, reasons, api_act)

            for cand in candidate_acts:
                score = 0.0
                reasons = []

                # ------------------------ Time proximity ------------------
                time_score, time_reason = evaluate_time_match(
                    xml_start,
                    xml_end,
                    cand.begin,
                    cand.einde,
                )
                score += time_score
                if time_score:
                    reasons.append(time_reason)

                # ------------------------ Soort comparison ---------------
                cand_soort = (
                    cand.soort.value.lower() if cand.soort and hasattr(cand.soort, "value") else str(cand.soort).lower()
                )
                if xml_soort and cand_soort:
                    if xml_soort == cand_soort:
                        score += SCORE_SOORT_EXACT
                        reasons.append("Soort exact match")
                    elif xml_soort in cand_soort:
                        score += SCORE_SOORT_PARTIAL_XML_IN_API
                        reasons.append("Soort partial XML in API")
                    elif cand_soort in xml_soort:
                        score += SCORE_SOORT_PARTIAL_API_IN_XML
                        reasons.append("Soort partial API in XML")
                    else:
                        for alias in SOORT_ALIAS.get(xml_soort, []):
                            if alias in cand_soort:
                                score += SCORE_SOORT_PARTIAL_XML_IN_API
                                reasons.append(f"Soort alias match ('{alias}')")
                                break

                # ------------------------ Onderwerp / titel fuzz ---------
                cand_ond = (cand.onderwerp or "").lower()

                norm_api_ond = normalize_topic(cand_ond)
                norm_xml_ond = normalize_topic(xml_onderwerp.lower())
                norm_xml_tit = normalize_topic(xml_title.lower())

                if xml_onderwerp and cand_ond:
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

                if xml_title and cand_ond:
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
                    'api_act': cand,
                })

                if score > best_score:
                    best_score = score
                    best_match = cand

            # Determine acceptance based on threshold or relative lead (same as activity match)
            # Sort potentials by score desc
            potential_matches.sort(key=lambda d: d['score'], reverse=True)

            api_act = None
            if best_match:
                if best_score >= MIN_MATCH_SCORE_FOR_ACTIVITEIT:
                    api_act = best_match
                else:
                    runner_up_score = potential_matches[1]['score'] if len(potential_matches) > 1 else 0.0
                    if best_score - runner_up_score >= 1.0 and best_score >= 1.0:
                        api_act = best_match

            # Optionally print reasons for best match (debugging)
            if api_act:
                print(f"    ✅ BEST MATCH: API activiteit {api_act.id} (onderwerp='{api_act.onderwerp}') – score {best_score:.2f}")
            else:
                print(f"    ❌ No strong match found (best score {best_score:.2f})")

            # --------------------------------------------------
            # Track activiteit linkage success
            # --------------------------------------------------
            total_xml_acts += 1
            if api_act:
                total_linked_acts += 1

            api_act_id = api_act.id if api_act else "UNKNOWN"
            print("\n" + "-" * 80)
            print(f"XML activiteit '{xml_title}' (soort={xml_soort}) mapped to API {api_act_id}")

            # ------------------------------------------------------------------
            # Extract all draadboekfragmenten (these carry speeches)
            # ------------------------------------------------------------------
            for frag in xml_act.findall(".//vlos:draadboekfragment", NS):
                # Compile one clean text string
                tekst_el = frag.find("vlos:tekst", NS)
                if tekst_el is None:
                    continue
                speech_text = collapse_text(tekst_el)
                if not speech_text:
                    continue

                # Gather speakers listed for this fragment
                for sprek_el in frag.findall("vlos:sprekers/vlos:spreker", NS):
                    v_first = sprek_el.findtext("vlos:voornaam", default="", namespaces=NS)
                    v_last = sprek_el.findtext("vlos:achternaam", default="", namespaces=NS)
                    fractie = sprek_el.findtext("vlos:fractie", default="", namespaces=NS)

                    matched_persoon = find_best_persoon(api, v_first, v_last)
                    # --------------------------------------------------
                    # Track speaker resolution success
                    # --------------------------------------------------
                    total_speakers += 1
                    if matched_persoon:
                        total_matched_speakers += 1
                    if matched_persoon:
                        person_label = f"{matched_persoon.roepnaam or matched_persoon.voornaam} {matched_persoon.achternaam} (TK-API id {matched_persoon.id})"
                    else:
                        person_label = f"{v_first} {v_last} [NO MATCH]"

                    print(f"  • {person_label} [{fractie}] —\n    \"{speech_text[:200]}...\"") 

    # --------------------
    # Summary output
    # --------------------
    if total_xml_acts:
        act_ratio = total_linked_acts / total_xml_acts
    else:
        act_ratio = 0.0
    if total_speakers:
        speaker_ratio = total_matched_speakers / total_speakers
    else:
        speaker_ratio = 0.0

    print("\n=== MATCH SUMMARY ===")
    print(f"Activiteiten linked : {total_linked_acts}/{total_xml_acts} ({act_ratio:.1%})")
    print(f"Sprekers resolved   : {total_matched_speakers}/{total_speakers} ({speaker_ratio:.1%})") 