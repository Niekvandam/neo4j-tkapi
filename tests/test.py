import xml.etree.ElementTree as ET
from tkapi import TKApi
from tkapi.vergadering import Vergadering, VergaderingFilter, VergaderingSoort
from tkapi.activiteit import Activiteit, ActiviteitFilter, ActiviteitSoort as TKActiviteitSoortEnum
# Ensure Zaak, Persoon, Verslag are imported if you uncomment their sections later
from tkapi.zaak import Zaak, ZaakFilter
from tkapi.persoon import Persoon
from tkapi.verslag import Verslag
from tkapi.util import util as tkapi_util
import requests
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Any # For type hinting
from thefuzz import fuzz # For fuzzy string matching

# --- Configuration ---
XML_FILE_PATH = "/Users/niek/Downloads/Open Data Portaal Verslag.xml" # Path to your full XML file
LOCAL_TIMEZONE_OFFSET_HOURS = 2 # CEST for May 2019 in NL

# Scoring weights
SCORE_TIME_START_PROXIMITY = 3.0
SCORE_TIME_OVERLAP_ONLY = 1.5 # If only overlap, but starts are not close
SCORE_SOORT_EXACT = 2.0
SCORE_SOORT_PARTIAL_XML_IN_API = 1.5 # e.g., XML "Plenair debat" in API "Plenair debat (tweeminutendebat)"
SCORE_SOORT_PARTIAL_API_IN_XML = 1.0
SCORE_ONDERWERP_EXACT = 2.5
SCORE_ONDERWERP_FUZZY_HIGH = 2.0 # For fuzzy > 90%
SCORE_ONDERWERP_FUZZY_MEDIUM = 1.0 # For fuzzy > 75%
SCORE_TITEL_EXACT_VS_API_ONDERWERP = 1.5
SCORE_TITEL_FUZZY_HIGH_VS_API_ONDERWERP = 1.0 # For fuzzy > 90%
SCORE_TITEL_FUZZY_MEDIUM_VS_API_ONDERWERP = 0.5 # For fuzzy > 75%


MIN_MATCH_SCORE_FOR_ACTIVITEIT = 4.5 # Example: TimeProx (3) + PartialSoort (1.5) = 4.5 OR TimeProx(3) + MedFuzzyOnderwerp(1) = 4
TIME_START_PROXIMITY_TOLERANCE_SECONDS = 300 # +/- 5 minutes for "close start"
TIME_GENERAL_OVERLAP_BUFFER_SECONDS = 600 # +/- 10 minutes for considering general overlap

FUZZY_SIMILARITY_THRESHOLD_HIGH = 90
FUZZY_SIMILARITY_THRESHOLD_MEDIUM = 75


# --- Helper Functions ---
def parse_xml_datetime(datetime_val: Optional[str]) -> Optional[datetime]:
    if not datetime_val: return None
    if not isinstance(datetime_val, str):
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
        dt_obj_naive = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S")
        return dt_obj_naive
    except ValueError:
        try:
            return tkapi_util.odatedatetime_to_datetime(datetime_str)
        except Exception:
            return None

def get_utc_datetime(dt_obj: Optional[datetime], local_offset_hours: int) -> Optional[datetime]:
    """Converts a naive or aware datetime to UTC. Assumes naive is local."""
    if not dt_obj:
        return None
    if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None: # Naive
        return (dt_obj - timedelta(hours=local_offset_hours)).replace(tzinfo=timezone.utc)
    else: # Aware
        return dt_obj.astimezone(timezone.utc)

def evaluate_time_match(
    xml_start: Optional[datetime], xml_end: Optional[datetime],
    api_start: Optional[datetime], api_end: Optional[datetime],
    start_proximity_tolerance_sec: int, general_overlap_buffer_sec: int
) -> tuple[float, str]:
    score = 0.0
    reason = "No significant time match"

    if not (xml_start and api_start and api_end): # xml_end can be missing for point activities
        return score, reason

    xml_start_utc = get_utc_datetime(xml_start, LOCAL_TIMEZONE_OFFSET_HOURS)
    api_start_utc = get_utc_datetime(api_start, LOCAL_TIMEZONE_OFFSET_HOURS) # API times are already aware
    api_end_utc = get_utc_datetime(api_end, LOCAL_TIMEZONE_OFFSET_HOURS)

    xml_end_for_check = xml_end
    if not xml_end_for_check and xml_start: # If XML has no end, assume it's a point or short event
        xml_end_for_check = xml_start + timedelta(minutes=1) # Give it a small duration for overlap check
    
    xml_end_utc = get_utc_datetime(xml_end_for_check, LOCAL_TIMEZONE_OFFSET_HOURS) if xml_end_for_check else None

    if not (xml_start_utc and api_start_utc and api_end_utc and xml_end_utc): # Need all for robust overlap
         return score, "Missing converted UTC time data"

    # Check 1: XML start time is very close to API start time
    start_proximity_ok = abs((xml_start_utc - api_start_utc).total_seconds()) <= start_proximity_tolerance_sec
    
    # Check 2: XML activity timeframe overlaps with API activity timeframe (with buffer)
    # Overlap: max(start1, start2) < min(end1, end2)
    overlap_exists = (max(xml_start_utc, api_start_utc - timedelta(seconds=general_overlap_buffer_sec)) <
                      min(xml_end_utc, api_end_utc + timedelta(seconds=general_overlap_buffer_sec)))

    if start_proximity_ok:
        score = SCORE_TIME_START_PROXIMITY
        reason = f"XML_start ({xml_start_utc.time()}) PROXIMATE to API_start ({api_start_utc.time()})"
        if overlap_exists:
            reason += " & timeframes overlap"
    elif overlap_exists:
        score = SCORE_TIME_OVERLAP_ONLY
        reason = f"Timeframes OVERLAP (XML: {xml_start_utc.time()}-{xml_end_utc.time()}, API: {api_start_utc.time()}-{api_end_utc.time()})"
    
    return score, reason

# --- Main Script ---
# ... (NS, api initialization as before) ...
NS = {'vlos': 'http://www.tweedekamer.nl/ggm/vergaderverslag/v1.0'}
api = TKApi(verbose=False) 
print("TKApi initialized.\n")

try:
    with open(XML_FILE_PATH, 'r', encoding='utf-8') as file:
        xml_data = file.read()
    root = ET.fromstring(xml_data)
    xml_vergadering_element = root.find('vlos:vergadering', NS)
    
    canonical_tk_vergadering = None

    if xml_vergadering_element is not None:
        xml_v_objectid_root = xml_vergadering_element.get('objectid')
        xml_v_soort_str_from_tag = xml_vergadering_element.get('soort')
        xml_v_titel = xml_vergadering_element.findtext('vlos:titel', default='', namespaces=NS)
        xml_v_nummer_str_from_tag = xml_vergadering_element.findtext('vlos:vergaderingnummer', default='', namespaces=NS)
        xml_v_datum_str_from_tag = xml_vergadering_element.findtext('vlos:datum', default='', namespaces=NS)

        if not xml_v_datum_str_from_tag:
            print("❌ Critical error: XML <vergadering> is missing <datum> tag. Cannot proceed.")
            exit()
        
        target_date_from_xml = xml_v_datum_str_from_tag.split('T')[0]

        print(f"--- Attempting to find TKApi.Vergadering for XML Vergadering ---")
        print(f"    XML Details: Titel='{xml_v_titel}', Soort='{xml_v_soort_str_from_tag}', Nummer='{xml_v_nummer_str_from_tag}', Datum='{target_date_from_xml}'")

        local_target_dt_obj = datetime.strptime(target_date_from_xml, "%Y-%m-%d")
        local_timezone_delta = timedelta(hours=LOCAL_TIMEZONE_OFFSET_HOURS)
        utc_filter_start = local_target_dt_obj - local_timezone_delta
        utc_filter_end = (local_target_dt_obj + timedelta(days=1)) - local_timezone_delta

        v_filter = Vergadering.create_filter()
        v_filter.filter_date_range(begin_datetime=utc_filter_start, end_datetime=utc_filter_end)
        
        if xml_v_soort_str_from_tag:
            if xml_v_soort_str_from_tag.lower() == "plenair": v_filter.filter_soort(VergaderingSoort.PLENAIR)
            elif xml_v_soort_str_from_tag.lower() == "commissie": v_filter.filter_soort(VergaderingSoort.COMMISSIE)
            else: v_filter.filter_soort(xml_v_soort_str_from_tag)
        
        if xml_v_nummer_str_from_tag:
            try:
                v_filter.add_filter_str(f"VergaderingNummer eq {int(xml_v_nummer_str_from_tag)}")
            except ValueError:
                print(f"    [Warning] Could not parse VergaderingNummer from XML: '{xml_v_nummer_str_from_tag}'")

        print(f"    Fetching canonical Vergadering from API. Filter: {v_filter.filter_str}")
        Vergadering.expand_params = ['Verslag'] 
        tk_vergaderingen_results = api.get_items(Vergadering, filter=v_filter, max_items=5)
        Vergadering.expand_params = None 

        if tk_vergaderingen_results:
            if len(tk_vergaderingen_results) == 1:
                canonical_tk_vergadering = tk_vergaderingen_results[0]
                print(f"✅ Found Canonical TKApi.Vergadering: ID={canonical_tk_vergadering.id}, Titel='{canonical_tk_vergadering.titel}'")
                print(f"    API Vergadering Start: {canonical_tk_vergadering.begin}, End: {canonical_tk_vergadering.einde}")
                if canonical_tk_vergadering.verslag:
                    print(f"    Associated TKApi Verslag ID: {canonical_tk_vergadering.verslag.id}")
                    if canonical_tk_vergadering.verslag.id == xml_v_objectid_root:
                        print(f"       🎉 XML root objectid ({xml_v_objectid_root}) MATCHES this Verslag.Id.")
                    else:
                        print(f"       ℹ️  Note: XML root objectid ({xml_v_objectid_root}) does NOT match this Verslag.Id ({canonical_tk_vergadering.verslag.id}).")
                else:
                     print(f"       No Verslag associated in API for this Vergadering.")
            else:
                print(f"⚠️ Found {len(tk_vergaderingen_results)} TKApi.Vergaderingen matching criteria. This might indicate the filter is not specific enough or there are multiple API entries for the same conceptual meeting.")
                if tk_vergaderingen_results: 
                    canonical_tk_vergadering = tk_vergaderingen_results[0] 
                    print(f"    Using the first match: ID={canonical_tk_vergadering.id}, Titel='{canonical_tk_vergadering.titel}' for further processing. REVIEW THIS!")
                else:
                    print(f"❌ Could not identify canonical TKApi.Vergadering. Exiting.")
                    exit()
        else:
            print(f"❌ Could not find canonical TKApi.Vergadering for XML. Exiting.")
            exit()
        print("-" * 50 + "\n")

        api_candidate_activities = []
        if canonical_tk_vergadering and canonical_tk_vergadering.begin and canonical_tk_vergadering.einde:
            act_filter = Activiteit.create_filter()
            time_buffer = timedelta(minutes=30) # Wider buffer for fetching candidates
            api_verg_begin_utc = get_utc_datetime(canonical_tk_vergadering.begin, LOCAL_TIMEZONE_OFFSET_HOURS)
            api_verg_einde_utc = get_utc_datetime(canonical_tk_vergadering.einde, LOCAL_TIMEZONE_OFFSET_HOURS)
            
            if api_verg_begin_utc and api_verg_einde_utc: # Ensure times are valid
                act_filter.filter_date_range(
                    begin_datetime=api_verg_begin_utc - time_buffer,
                    end_datetime=api_verg_einde_utc + time_buffer
                )
                print(f"Fetching candidate API Activiteiten for Vergadering ID {canonical_tk_vergadering.id}.")
                print(f"  Timeframe (UTC with buffer): {api_verg_begin_utc - time_buffer} to {api_verg_einde_utc + time_buffer}")
                print(f"  Filter: {act_filter.filter_str}")
                
                api_candidate_activities = api.get_items(Activiteit, filter=act_filter, max_items=150)
                print(f"    Found {len(api_candidate_activities)} candidate API Activiteiten in the timeframe.\n")

                if api_candidate_activities:
                    print(f"--- Details of {len(api_candidate_activities)} Candidate API Activiteiten ---")
                    for idx, api_act_cand in enumerate(api_candidate_activities):
                        print(f"  Candidate {idx+1}: ID={api_act_cand.id}, Soort='{api_act_cand.soort.value if api_act_cand.soort and hasattr(api_act_cand.soort, 'value') else api_act_cand.soort}', "
                              f"Onderwerp='{api_act_cand.onderwerp}', Begin={api_act_cand.begin}, End={api_act_cand.einde}")
                    print("-" * 50 + "\n")
            else:
                print("Canonical Vergadering has invalid begin/end times. Cannot fetch API activities.")
        else:
            print("Could not get canonical Vergadering details or times to filter API activities.")

        print("--- Matching Top-Level XML <activiteit> elements to API Activiteiten ---")
        for xml_top_act_el in xml_vergadering_element.findall('vlos:activiteit', NS):
            xml_act_id = xml_top_act_el.get('objectid')
            xml_act_soort_str = xml_top_act_el.get('soort')
            xml_act_titel = xml_top_act_el.findtext('vlos:titel', default='', namespaces=NS)
            xml_act_onderwerp = xml_top_act_el.findtext('vlos:onderwerp', default='', namespaces=NS)
            val_aanvangstijd = xml_top_act_el.findtext('vlos:aanvangstijd', default=None, namespaces=NS)
            val_markeertijdbegin = xml_top_act_el.findtext('vlos:markeertijdbegin', default=None, namespaces=NS)
            xml_start_str = val_aanvangstijd or val_markeertijdbegin
            val_eindtijd = xml_top_act_el.findtext('vlos:eindtijd', default=None, namespaces=NS)
            val_markeertijdeind = xml_top_act_el.findtext('vlos:markeertijdeind', default=None, namespaces=NS)
            xml_end_str = val_eindtijd or val_markeertijdeind
            xml_act_start_dt = parse_xml_datetime(xml_start_str)
            xml_act_end_dt = parse_xml_datetime(xml_end_str)

            print(f"\n  Processing TOP-LEVEL XML <activiteit>: ID={xml_act_id}, Soort='{xml_act_soort_str}', Titel='{xml_act_titel}'")
            print(f"    XML Onderwerp: '{xml_act_onderwerp}'")
            print(f"    XML Start: {xml_act_start_dt}, XML End: {xml_act_end_dt}")

            best_match_api_activity = None
            highest_score = 0.0
            all_potential_matches = []

            for api_act in api_candidate_activities:
                current_score = 0.0
                reasons = []
                
                time_score, time_reason = evaluate_time_match(
                    xml_act_start_dt, xml_act_end_dt, 
                    api_act.begin, api_act.einde, 
                    start_proximity_tolerance_sec=TIME_START_PROXIMITY_TOLERANCE_SECONDS,
                    general_overlap_buffer_sec=TIME_GENERAL_OVERLAP_BUFFER_SECONDS # Pass this too
                )
                current_score += time_score
                if time_score > 0: reasons.append(time_reason)
                
                api_act_soort_val = api_act.soort.value if api_act.soort and hasattr(api_act.soort, 'value') else str(api_act.soort)
                xml_s_lower = (xml_act_soort_str or "").lower()
                api_s_lower = (api_act_soort_val or "").lower()

                if xml_s_lower and api_s_lower:
                    if xml_s_lower == api_s_lower:
                        current_score += SCORE_SOORT_EXACT
                        reasons.append(f"Soort exact match ('{xml_s_lower}')")
                    elif xml_s_lower in api_s_lower:
                        current_score += SCORE_SOORT_PARTIAL_XML_IN_API
                        reasons.append(f"Soort partial (XML '{xml_s_lower}' in API '{api_s_lower}')")
                    elif api_s_lower in xml_s_lower:
                        current_score += SCORE_SOORT_PARTIAL_API_IN_XML
                        reasons.append(f"Soort partial (API '{api_s_lower}' in XML '{xml_s_lower}')")
                
                api_onderwerp_lower = (api_act.onderwerp or "").strip().lower()
                xml_onderwerp_from_xml_lower = (xml_act_onderwerp or "").strip().lower()
                xml_titel_from_xml_lower = (xml_act_titel or "").strip().lower()

                if xml_onderwerp_from_xml_lower and api_onderwerp_lower:
                    if xml_onderwerp_from_xml_lower == api_onderwerp_lower:
                        current_score += SCORE_ONDERWERP_EXACT
                        reasons.append(f"Onderwerp exact match")
                    else:
                        ratio = fuzz.ratio(xml_onderwerp_from_xml_lower, api_onderwerp_lower)
                        if ratio >= FUZZY_SIMILARITY_THRESHOLD_HIGH:
                            current_score += SCORE_ONDERWERP_FUZZY_HIGH
                            reasons.append(f"Onderwerp fuzzy high ({ratio}%)")
                        elif ratio >= FUZZY_SIMILARITY_THRESHOLD_MEDIUM:
                            current_score += SCORE_ONDERWERP_FUZZY_MEDIUM
                            reasons.append(f"Onderwerp fuzzy medium ({ratio}%)")
                
                if xml_titel_from_xml_lower and api_onderwerp_lower: # Match XML Titel against API Onderwerp
                    if xml_titel_from_xml_lower == api_onderwerp_lower:
                        current_score += SCORE_TITEL_EXACT_VS_API_ONDERWERP
                        reasons.append(f"Titel exact match to API onderwerp")
                    else:
                        ratio = fuzz.ratio(xml_titel_from_xml_lower, api_onderwerp_lower)
                        if ratio >= FUZZY_SIMILARITY_THRESHOLD_HIGH:
                            current_score += SCORE_TITEL_FUZZY_HIGH_VS_API_ONDERWERP
                            reasons.append(f"Titel fuzzy high to API onderwerp ({ratio}%)")
                        elif ratio >= FUZZY_SIMILARITY_THRESHOLD_MEDIUM:
                            current_score += SCORE_TITEL_FUZZY_MEDIUM_VS_API_ONDERWERP
                            reasons.append(f"Titel fuzzy medium to API onderwerp ({ratio}%)")

                if current_score > 0:
                    all_potential_matches.append({
                        'score': current_score, 
                        'reasons': list(reasons), # Ensure reasons are copied for this specific match
                        'api_act': api_act
                    })
                if current_score > highest_score:
                    highest_score = current_score
                    best_match_api_activity = api_act
            
            all_potential_matches.sort(key=lambda x: x['score'], reverse=True)

            if best_match_api_activity and highest_score >= MIN_MATCH_SCORE_FOR_ACTIVITEIT:
                best_match_details = next(m for m in all_potential_matches if m['api_act'].id == best_match_api_activity.id)
                api_act_soort_val_match = best_match_api_activity.soort.value if best_match_api_activity.soort and hasattr(best_match_api_activity.soort, 'value') else str(best_match_api_activity.soort)
                print(f"    ✅ BEST MATCH for XML ID {xml_act_id} is API Activiteit ID {best_match_api_activity.id} (Score: {highest_score:.2f})")
                print(f"       Reasons: {', '.join(best_match_details['reasons'])}")
                print(f"       API Soort: '{api_act_soort_val_match}', API Onderwerp: '{best_match_api_activity.onderwerp}'")
                print(f"       API Start: {best_match_api_activity.begin}, API End: {best_match_api_activity.einde}")

                sub_elements_to_process = xml_top_act_el.findall('.//vlos:activiteithoofd', NS) + \
                                          xml_top_act_el.findall('.//vlos:activiteitdeel', NS)
                
                for xml_sub_act_el in sub_elements_to_process:
                    # ... (sub-element processing logic as before) ...
                    sub_act_id_xml = xml_sub_act_el.get('objectid')
                    sub_act_soort_xml = xml_sub_act_el.get('soort')
                    sub_act_titel_xml = xml_sub_act_el.findtext('vlos:titel', default='', namespaces=NS)
                    sub_val_aanvangstijd = xml_sub_act_el.findtext('vlos:aanvangstijd', default=None, namespaces=NS)
                    sub_val_markeertijdbegin = xml_sub_act_el.findtext('vlos:markeertijdbegin', default=None, namespaces=NS)
                    sub_start_str = sub_val_aanvangstijd or sub_val_markeertijdbegin
                    sub_act_start_dt = parse_xml_datetime(sub_start_str)
                    print(f"      ↳ Sub-XML {xml_sub_act_el.tag.split('}')[-1]}: XML_ID={sub_act_id_xml}, Soort='{sub_act_soort_xml}', Titel='{sub_act_titel_xml}', Start='{sub_act_start_dt}'")
                    print(f"         (This XML sub-activity is content within API Activiteit: {best_match_api_activity.id} - '{best_match_api_activity.onderwerp}')")

            else:
                print(f"    ❌ No strong API match found for top-level XML Activiteit ID {xml_act_id} (Highest score: {highest_score:.2f}).")
                if all_potential_matches:
                    print("      Top considered (but below threshold or no match met MIN_MATCH_SCORE):")
                    for pot_match in all_potential_matches[:3]: # Show top 3 potential
                         print(f"        - API_ID={pot_match['api_act'].id}, Score={pot_match['score']:.2f}, Reasons=\"{', '.join(pot_match['reasons'])}\", API_Onderwerp='{pot_match['api_act'].onderwerp}'")
            print("    ----")
    else:
        print("No <vergadering> tag found in XML.")

except ET.ParseError as e:
    print(f"XML Parsing Error: {e}")
except Exception as e:
    print(f"An unexpected error occurred in the main script: {e}")
    import traceback
    traceback.print_exc()

print("\n--- Test Finished ---")