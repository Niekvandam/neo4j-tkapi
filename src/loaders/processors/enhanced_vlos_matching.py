"""
Enhanced VLOS Matching Processor - Comprehensive parliamentary discourse analysis
Migrated from test_vlos_activity_matching_with_personen_and_zaken.py
"""

import xml.etree.ElementTree as ET
import time
from typing import Optional, Dict, List, Any, Tuple, Set
from datetime import datetime, timedelta
from collections import defaultdict
import re

from utils.helpers import merge_node, merge_rel

# XML namespaces
NS_VLOS = {'vlos': 'http://www.vlosstichting.nl'}

# Local timezone offset (CEST = UTC+2)
LOCAL_TIMEZONE_OFFSET_HOURS = 2

def parse_xml_datetime(datetime_str: str) -> Optional[datetime]:
    """Parse XML datetime string to datetime object"""
    if not datetime_str:
        return None
    
    try:
        # Remove timezone info for parsing, then add local offset
        if datetime_str.endswith('Z'):
            dt = datetime.fromisoformat(datetime_str[:-1])
        elif '+' in datetime_str or datetime_str.count('-') > 2:
            dt = datetime.fromisoformat(datetime_str.split('+')[0].split('T')[0] + 'T' + datetime_str.split('T')[1].split('+')[0])
        else:
            dt = datetime.fromisoformat(datetime_str)
        
        # Add local timezone offset
        dt += timedelta(hours=LOCAL_TIMEZONE_OFFSET_HOURS)
        return dt
    except Exception as e:
        print(f"⚠️ Failed to parse datetime '{datetime_str}': {e}")
        return None


def get_candidate_api_activities(session, canonical_vergadering_node: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Get candidate API activities for matching with sophisticated traversal"""
    
    api_activities = []
    vergadering_id = canonical_vergadering_node['id']
    
    print(f"🔍 Finding API activities for Vergadering {vergadering_id}")
    
    # Strategy 1: Direct HAS_ACTIVITEIT relationships
    direct_activities = session.run("""
        MATCH (v:Vergadering {id: $vergadering_id})-[:HAS_ACTIVITEIT]->(a:Activiteit)
        RETURN a.id as id, a.nummer as nummer, a.onderwerp as onderwerp, 
               a.begin as begin, a.einde as einde, a.soort as soort
    """, vergadering_id=vergadering_id).data()
    
    api_activities.extend(direct_activities)
    print(f"  📊 Found {len(direct_activities)} direct activities")
    
    # Strategy 2: Via Agendapunten
    agenda_activities = session.run("""
        MATCH (v:Vergadering {id: $vergadering_id})-[:HAS_AGENDAPUNT]->(ap:Agendapunt)-[:HAS_ACTIVITEIT]->(a:Activiteit)
        WHERE NOT a.id IN [act.id | (v)-[:HAS_ACTIVITEIT]->(act)]
        RETURN a.id as id, a.nummer as nummer, a.onderwerp as onderwerp,
               a.begin as begin, a.einde as einde, a.soort as soort
    """, vergadering_id=vergadering_id).data()
    
    api_activities.extend(agenda_activities)
    print(f"  📊 Found {len(agenda_activities)} agenda-linked activities")
    
    # Strategy 3: Same-day activities (fallback)
    vergadering_date = canonical_vergadering_node.get('datum')
    if vergadering_date:
        same_day_activities = session.run("""
            MATCH (a:Activiteit)
            WHERE date(a.begin) = date($vergadering_date)
            AND NOT a.id IN $existing_ids
            RETURN a.id as id, a.nummer as nummer, a.onderwerp as onderwerp,
                   a.begin as begin, a.einde as einde, a.soort as soort
        """, vergadering_date=vergadering_date, 
             existing_ids=[act['id'] for act in api_activities]).data()
        
        api_activities.extend(same_day_activities)
        print(f"  📊 Found {len(same_day_activities)} same-day activities")
    
    print(f"📊 Total candidate API activities: {len(api_activities)}")
    return api_activities


def normalize_topic(text: str) -> str:
    """Normalize topic text for matching"""
    if not text:
        return ""
    
    # Remove common prefixes and normalize
    normalized = text.lower().strip()
    normalized = re.sub(r'^(motie|amendement|wetgeving|stemming|besluit)\s*', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized)
    return normalized


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


def find_best_zaak_or_fallback(session, topics: List[str], fallback_dossier_ids: List[str] = None) -> Optional[Tuple[str, str, bool]]:
    """Find best matching zaak or fallback to dossier with enhanced logic"""
    
    for topic in topics:
        if not topic:
            continue
            
        topic_normalized = normalize_topic(topic)
        
        # Strategy 1: Exact zaak nummer match
        zaak_match = session.run("""
            MATCH (z:Zaak)
            WHERE toLower(z.onderwerp) CONTAINS toLower($topic)
            OR z.nummer CONTAINS $topic
            RETURN z.id as id, z.nummer as nummer, z.onderwerp as onderwerp
            ORDER BY length(z.onderwerp) ASC
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
                    WHERE {keyword_query}
                    RETURN z.id as id, z.nummer as nummer, z.onderwerp as onderwerp
                    ORDER BY length(z.onderwerp) ASC
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
    
    # Get all unmatched VLOS speakers
    vlos_speakers = session.run("""
        MATCH (vs:VlosSpeaker)
        WHERE NOT EXISTS((vs)-[:MATCHED_TO_PERSOON]->(:Persoon))
        RETURN vs.id as vlos_id, vs.naam as naam, vs.roepnaam as roepnaam, 
               vs.tussenvoegsel as tussenvoegsel, vs.achternaam as achternaam
    """).data()
    
    matched_count = 0
    
    for speaker in vlos_speakers:
        vlos_id = speaker['vlos_id']
        naam = speaker.get('naam', '')
        roepnaam = speaker.get('roepnaam', '')
        tussenvoegsel = speaker.get('tussenvoegsel', '')
        achternaam = speaker.get('achternaam', '')
        
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
                session.run("""
                    MATCH (vs:VlosSpeaker {id: $vlos_id})
                    MATCH (p:Persoon {id: $persoon_id})
                    MERGE (vs)-[:MATCHED_TO_PERSOON]->(p)
                """, vlos_id=vlos_id, persoon_id=persoon_match['persoon_id'])
                matched_count += 1
                continue
        
        # Strategy 2: Component-based matching
        if roepnaam and achternaam:
            query_parts = ["toLower(p.roepnaam) = toLower($roepnaam)"]
            query_params = {"roepnaam": roepnaam, "achternaam": achternaam}
            
            if tussenvoegsel:
                query_parts.append("toLower(p.tussenvoegsel) = toLower($tussenvoegsel)")
                query_params["tussenvoegsel"] = tussenvoegsel
            
            query_parts.append("toLower(p.achternaam) = toLower($achternaam)")
            
            persoon_match = session.run(f"""
                MATCH (p:Persoon)
                WHERE {' AND '.join(query_parts)}
                RETURN p.id as persoon_id, p.roepnaam, p.achternaam
                LIMIT 1
            """, **query_params).single()
            
            if persoon_match:
                session.run("""
                    MATCH (vs:VlosSpeaker {id: $vlos_id})
                    MATCH (p:Persoon {id: $persoon_id})
                    MERGE (vs)-[:MATCHED_TO_PERSOON]->(p)
                """, vlos_id=vlos_id, persoon_id=persoon_match['persoon_id'])
                matched_count += 1
                continue
    
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
            speaker_name = (spreker_elem.get('roepnaam', '') + ' ' + 
                          spreker_elem.get('achternaam', '')).strip()
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
            speaker_name = (spreker_elem.get('roepnaam', '') + ' ' + 
                          spreker_elem.get('achternaam', '')).strip()
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


def analyze_voting_in_activity(activity_elem: ET.Element) -> List[Dict[str, Any]]:
    """Analyze voting patterns in VLOS activity element"""
    
    voting_events = []
    
    # Find besluit items with voting data
    besluit_items = activity_elem.findall('.//vlos:activiteititem[@soort="Besluit"]', NS_VLOS)
    
    for besluit in besluit_items:
        # Look for voting sections
        stemmingen_elem = besluit.find('.//vlos:stemmingen', NS_VLOS)
        
        if stemmingen_elem is not None:
            votes = []
            
            # Extract individual votes
            for stemming in stemmingen_elem.findall('.//vlos:stemming', NS_VLOS):
                fractie_naam = stemming.get('fractie', 'Unknown')
                stem_waarde = stemming.get('stemming', 'Unknown')
                
                votes.append({
                    'fractie': fractie_naam,
                    'stemming': stem_waarde
                })
            
            if votes:
                # Calculate consensus level
                total_votes = len(votes)
                voor_votes = len([v for v in votes if v['stemming'] == 'Voor'])
                tegen_votes = len([v for v in votes if v['stemming'] == 'Tegen'])
                
                consensus_percentage = (voor_votes / total_votes * 100) if total_votes > 0 else 0
                
                voting_events.append({
                    'besluit_element': besluit,
                    'votes': votes,
                    'total_votes': total_votes,
                    'voor_votes': voor_votes,
                    'tegen_votes': tegen_votes,
                    'consensus_percentage': consensus_percentage,
                    'is_unanimous': consensus_percentage >= 95,
                    'is_controversial': consensus_percentage < 80
                })
    
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
    
    print(f"🔄 Processing VLOS activity: {activity_objectid}")
    print(f"  📋 Soort: {activity_soort}, Title: {activity_title}")
    
    # Find best matching API activity
    best_match_score = 0.0
    best_api_activity = None
    
    for api_activity in api_activities:
        score = calculate_activity_match_score(activity_elem, api_activity)
        if score > best_match_score and score >= 2.0:  # Minimum threshold
            best_match_score = score
            best_api_activity = api_activity
    
    if not best_api_activity:
        print(f"  ❌ No matching API activity found (best score: {best_match_score:.2f})")
        return None
    
    # Use API activity ID for tracking (NOT VLOS objectid)
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
    
    # Process speakers in this activity
    speakers = []
    for spreker_elem in activity_elem.findall('.//vlos:spreker', NS_VLOS):
        speaker_data = process_vlos_speaker(session, spreker_elem, activity_objectid)
        if speaker_data:
            speakers.append(speaker_data)
    
    # Track speakers for this API activity (using API ID, not VLOS objectid)
    if api_activity_id not in activity_speakers:
        activity_speakers[api_activity_id] = []
    activity_speakers[api_activity_id].extend(speakers)
    
    # Process zaken/topics mentioned in this activity
    topics = extract_activity_topics(activity_elem)
    
    # Find related zaken
    for topic in topics:
        if topic:
            zaak_result = find_best_zaak_or_fallback(session, [topic])
            if zaak_result:
                zaak_id, zaak_nummer, is_dossier = zaak_result
                
                # Track zaak for this API activity (using API ID)
                if api_activity_id not in activity_zaken:
                    activity_zaken[api_activity_id] = []
                activity_zaken[api_activity_id].append({
                    'id': zaak_id,
                    'nummer': zaak_nummer,
                    'is_dossier': is_dossier,
                    'topic': topic
                })
    
    # Detect interruptions in this activity
    activity_interruptions = detect_interruptions_in_activity(activity_elem)
    interruption_events.extend(activity_interruptions)
    
    # Analyze voting in this activity
    activity_voting = analyze_voting_in_activity(activity_elem)
    voting_events.extend(activity_voting)
    
    return api_activity_id


def process_vlos_speaker(session, spreker_elem: ET.Element, activity_id: str) -> Optional[Dict[str, Any]]:
    """Process a VLOS speaker element"""
    
    roepnaam = spreker_elem.get('roepnaam', '')
    achternaam = spreker_elem.get('achternaam', '')
    tussenvoegsel = spreker_elem.get('tussenvoegsel', '')
    voornaam = spreker_elem.get('voornaam', '')
    
    if not (roepnaam or voornaam) or not achternaam:
        return None
    
    # Create speaker identifier
    full_name = f"{roepnaam or voornaam} {tussenvoegsel} {achternaam}".strip()
    full_name = re.sub(r'\s+', ' ', full_name)
    
    speaker_id = f"vlos_speaker_{hash(full_name)}_{activity_id}"
    
    # Create VLOS speaker node
    speaker_props = {
        'id': speaker_id,
        'naam': full_name,
        'roepnaam': roepnaam,
        'voornaam': voornaam,
        'achternaam': achternaam,
        'tussenvoegsel': tussenvoegsel,
        'activity_id': activity_id,
        'source': 'enhanced_vlos_xml'
    }
    session.execute_write(merge_node, 'VlosSpeaker', 'id', speaker_props)
    
    # Link to activity
    session.execute_write(merge_rel, 'EnhancedVlosActivity', 'id', activity_id,
                          'VlosSpeaker', 'id', speaker_id, 'HAS_SPEAKER')
    
    return {
        'id': speaker_id,
        'naam': full_name,
        'roepnaam': roepnaam,
        'achternaam': achternaam
    }


def extract_activity_topics(activity_elem: ET.Element) -> List[str]:
    """Extract topics/subjects from VLOS activity element"""
    
    topics = []
    
    # Extract from title
    title = activity_elem.findtext('.//vlos:titel', default='', namespaces=NS_VLOS)
    if title and len(title.strip()) > 5:
        topics.append(title.strip())
    
    # Extract from onderwerp elements
    for onderwerp in activity_elem.findall('.//vlos:onderwerp', NS_VLOS):
        if onderwerp.text and len(onderwerp.text.strip()) > 5:
            topics.append(onderwerp.text.strip())
    
    # Extract from activiteititem elements
    for item in activity_elem.findall('.//vlos:activiteititem', NS_VLOS):
        item_onderwerp = item.findtext('.//vlos:onderwerp', default='', namespaces=NS_VLOS)
        if item_onderwerp and len(item_onderwerp.strip()) > 5:
            topics.append(item_onderwerp.strip())
    
    return list(set(topics))  # Remove duplicates


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
                
                session.execute_write(merge_node, target_label, 'id', {'id': zaak_id})
                session.execute_write(merge_rel, 'VlosSpeaker', 'id', speaker_id,
                                      target_label, 'id', zaak_id, rel_type, {'topic': topic})
                
                # Also create connection from matched Persoon to Zaak/Dossier (if matched)
                persoon_match = session.run("""
                    MATCH (vs:VlosSpeaker {id: $speaker_id})-[:MATCHED_TO_PERSOON]->(p:Persoon)
                    RETURN p.id as persoon_id
                """, speaker_id=speaker_id).single()
                
                if persoon_match:
                    persoon_id = persoon_match['persoon_id']
                    session.execute_write(merge_rel, 'Persoon', 'id', persoon_id,
                                          target_label, 'id', zaak_id, 'DISCUSSED', {'topic': topic})
                
                connection_count += 1
    
    print(f"✅ Created {connection_count} speaker-zaak connections")
    return connection_count 