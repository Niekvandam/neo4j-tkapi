#!/usr/bin/env python3
"""
Comprehensive test script for Enhanced VLOS Loader including speaker-zaak connections
"""

import sys
import os
import glob
import xml.etree.ElementTree as ET
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.core.connection.neo4j_connection import Neo4jConnection
from src.loaders.enhanced_vlos_verslag_loader import load_enhanced_vlos_verslag
from src.core.interfaces import LoaderConfig
from src.loaders.enhanced_vlos_verslag_loader import EnhancedVlosVerslagLoader

NS_VLOS = {'vlos': 'http://www.tweedekamer.nl/ggm/vergaderverslag/v1.0'}


def test_enhanced_vlos_loader_comprehensive():
    """Comprehensive test of the enhanced VLOS loader including speaker-zaak connections."""
    print("🧪 Comprehensive Enhanced VLOS Loader Test")
    
    # Find sample XML files
    xml_files = glob.glob('sample_vlos_*.xml')
    if not xml_files:
        xml_files = glob.glob('_sample_vlos_*.xml')
    
    if not xml_files:
        print("❌ No sample_vlos_*.xml files found in current directory")
        return False
    
    print(f"📁 Found {len(xml_files)} sample XML files")
    
    try:
        # Connect to Neo4j
        conn = Neo4jConnection()
        print("✅ Connected to Neo4j")
        
        total_stats = {
            'files_processed': 0,
            'total_activities': 0,
            'matched_activities': 0,
            'total_speakers': 0,
            'matched_speakers': 0,
            'total_zaken': 0,
            'direct_zaak_matches': 0,
            'dossier_fallback_matches': 0,
            'total_zaak_successes': 0,
            'speaker_zaak_connections': 0,
            'personen_with_connections': 0
        }
        
        # Process each XML file
        for xml_file in xml_files[:2]:  # Test with first 2 files for demo
            print(f"\n{'='*80}")
            print(f"🔄 Processing {xml_file}")
            print(f"{'='*80}")
            
            # Read and parse XML to get vergadering info
            with open(xml_file, 'r', encoding='utf-8') as f:
                xml_content = f.read()
            
            root = ET.fromstring(xml_content)
            vergadering_el = root.find('vlos:vergadering', NS_VLOS)
            
            if vergadering_el is None:
                print(f"❌ No vergadering element found in {xml_file}")
                continue
            
            xml_soort = vergadering_el.get('soort', '')
            xml_date_str = vergadering_el.findtext('vlos:datum', default='', namespaces=NS_VLOS)
            xml_nummer = vergadering_el.findtext('vlos:vergaderingnummer', default='', namespaces=NS_VLOS)
            
            print(f"📋 XML Vergadering - Soort: {xml_soort}, Datum: {xml_date_str}, Nummer: {xml_nummer}")
            
            # Find matching vergadering in Neo4j
            target_date = datetime.strptime(xml_date_str.split('T')[0], '%Y-%m-%d')
            
            with conn.driver.session() as session:
                vergadering_query = """
                MATCH (v:Vergadering)
                WHERE date(v.begin) = date($target_date)
                AND v.begin IS NOT NULL AND v.einde IS NOT NULL
                RETURN v.id as id, v.titel as titel, v.begin as begin, v.soort as soort
                ORDER BY v.begin DESC
                LIMIT 3
                """
                
                result = session.run(vergadering_query, target_date=target_date)
                vergaderingen = list(result)
                
                if not vergaderingen:
                    print(f"❌ No vergadering found for date {xml_date_str}")
                    continue
                
                # Use first matching vergadering
                test_vergadering = vergaderingen[0]
                test_vergadering_id = test_vergadering['id']
                
                print(f"🎯 Using vergadering: {test_vergadering_id}")
                print(f"   📋 Titel: {test_vergadering['titel']}")
                print(f"   📅 Begin: {test_vergadering['begin']}")
                print(f"   🏛️ Soort: {test_vergadering['soort']}")
                
                # Test enhanced VLOS loading
                print(f"\n🚀 Loading enhanced VLOS data...")
                counts = load_enhanced_vlos_verslag(
                    conn.driver,
                    xml_content,
                    test_vergadering_id,
                    f"test_verslag_{test_vergadering_id}_{xml_file.replace('.xml', '')}"
                )
                
                total_stats['files_processed'] += 1
                
                # Detailed verification and analysis
                print(f"\n🔍 Detailed Analysis for {xml_file}")
                
                # Get comprehensive statistics
                enhanced_stats_query = """
                MATCH (v:Vergadering {id: $vergadering_id})-[:HAS_ENHANCED_VLOS_DOCUMENT]->(edoc:EnhancedVlosDocument)
                -[:HAS_SUMMARY]->(s:VlosProcessingSummary)
                RETURN s.total_activities as total_activities,
                       s.matched_activities as matched_activities,
                       s.total_speakers as total_speakers,
                       s.matched_speakers as matched_speakers,
                       s.total_zaken as total_zaken,
                       s.direct_zaak_matches as direct_zaak_matches,
                       s.dossier_fallback_matches as dossier_fallback_matches,
                       s.total_zaak_successes as total_zaak_successes,
                       s.speakers_with_zaak_connections as speakers_with_zaak_connections,
                       s.personen_with_zaak_connections as personen_with_zaak_connections,
                       s.unique_zaken_discussed_by_personen as unique_zaken_discussed,
                       s.activity_match_rate as activity_match_rate,
                       s.speaker_match_rate as speaker_match_rate,
                       s.zaak_match_rate as zaak_match_rate,
                       s.persoon_zaak_connection_rate as persoon_zaak_connection_rate
                """
                
                stats_result = session.run(enhanced_stats_query, vergadering_id=test_vergadering_id).single()
                
                if stats_result:
                    print(f"📊 Enhanced Statistics:")
                    print(f"   🎯 Activities: {stats_result['matched_activities']}/{stats_result['total_activities']} "
                          f"({stats_result['activity_match_rate']*100:.1f}%)")
                    print(f"   👥 Speakers: {stats_result['matched_speakers']}/{stats_result['total_speakers']} "
                          f"({stats_result['speaker_match_rate']*100:.1f}%)")
                    print(f"   📋 Zaken: {stats_result['total_zaak_successes']}/{stats_result['total_zaken']} "
                          f"({stats_result['zaak_match_rate']*100:.1f}%)")
                    print(f"      ├─ Direct Zaak matches: {stats_result['direct_zaak_matches']}")
                    print(f"      └─ Dossier fallback matches: {stats_result['dossier_fallback_matches']}")
                    print(f"   🔗 Speaker-Zaak Connections:")
                    print(f"      ├─ Speakers with connections: {stats_result['speakers_with_zaak_connections']}")
                    print(f"      ├─ Personen with connections: {stats_result['personen_with_zaak_connections']}")
                    print(f"      ├─ Unique zaken discussed: {stats_result['unique_zaken_discussed']}")
                    print(f"      └─ Connection rate: {stats_result['persoon_zaak_connection_rate']*100:.1f}%")
                    
                    # Update totals
                    total_stats['total_activities'] += stats_result['total_activities'] or 0
                    total_stats['matched_activities'] += stats_result['matched_activities'] or 0
                    total_stats['total_speakers'] += stats_result['total_speakers'] or 0
                    total_stats['matched_speakers'] += stats_result['matched_speakers'] or 0
                    total_stats['total_zaken'] += stats_result['total_zaken'] or 0
                    total_stats['direct_zaak_matches'] += stats_result['direct_zaak_matches'] or 0
                    total_stats['dossier_fallback_matches'] += stats_result['dossier_fallback_matches'] or 0
                    total_stats['total_zaak_successes'] += stats_result['total_zaak_successes'] or 0
                    total_stats['speaker_zaak_connections'] += stats_result['speakers_with_zaak_connections'] or 0
                    total_stats['personen_with_connections'] += stats_result['personen_with_zaak_connections'] or 0
                
                # Show sample speaker-zaak connections
                print(f"\n🔗 Sample Speaker-Zaak Connections:")
                connections_query = """
                MATCH (v:Vergadering {id: $vergadering_id})-[:HAS_ENHANCED_VLOS_DOCUMENT]->(edoc:EnhancedVlosDocument)
                -[:HAS_ACTIVITY]->(va:VlosActivity)-[:HAS_SPEAKER]->(vs:VlosSpeaker)
                -[:MATCHES_PERSOON]->(p:Persoon)
                WHERE EXISTS((p)-[:DISCUSSED|DISCUSSED_DIRECTLY]->())
                WITH p, va
                MATCH (p)-[r:DISCUSSED|DISCUSSED_DIRECTLY]->(target)
                RETURN p.roepnaam as roepnaam, p.achternaam as achternaam, 
                       labels(target) as target_type, 
                       CASE 
                         WHEN 'Zaak' IN labels(target) THEN target.nummer
                         WHEN 'Dossier' IN labels(target) THEN target.nummer
                         ELSE 'Unknown'
                       END as target_nummer,
                       type(r) as relationship_type,
                       va.titel as activity_title
                LIMIT 10
                """
                
                connections = session.run(connections_query, vergadering_id=test_vergadering_id)
                
                for i, conn in enumerate(connections, 1):
                    speaker_name = f"{conn['roepnaam'] or ''} {conn['achternaam']}"
                    target_type = conn['target_type'][0] if conn['target_type'] else 'Unknown'
                    target_nummer = conn['target_nummer']
                    rel_type = conn['relationship_type']
                    activity = conn['activity_title'][:50] + "..." if len(conn['activity_title']) > 50 else conn['activity_title']
                    
                    print(f"   {i:2d}. {speaker_name} --[{rel_type}]--> {target_type} {target_nummer}")
                    print(f"       Activity: {activity}")
                
                print(f"\n✅ {xml_file} processed successfully!")
        
        # Overall summary
        print(f"\n{'='*80}")
        print(f"🎉 COMPREHENSIVE TEST SUMMARY")
        print(f"{'='*80}")
        print(f"📁 Files processed: {total_stats['files_processed']}")
        print(f"📊 Total activities: {total_stats['matched_activities']}/{total_stats['total_activities']} matched")
        if total_stats['total_activities'] > 0:
            print(f"   Activity match rate: {total_stats['matched_activities']/total_stats['total_activities']*100:.1f}%")
        
        print(f"👥 Total speakers: {total_stats['matched_speakers']}/{total_stats['total_speakers']} matched")
        if total_stats['total_speakers'] > 0:
            print(f"   Speaker match rate: {total_stats['matched_speakers']/total_stats['total_speakers']*100:.1f}%")
        
        print(f"📋 Total zaken: {total_stats['total_zaak_successes']}/{total_stats['total_zaken']} matched")
        if total_stats['total_zaken'] > 0:
            print(f"   Zaak match rate: {total_stats['total_zaak_successes']/total_stats['total_zaken']*100:.1f}%")
        print(f"   ├─ Direct Zaak matches: {total_stats['direct_zaak_matches']}")
        print(f"   └─ Dossier fallback matches: {total_stats['dossier_fallback_matches']}")
        
        print(f"🔗 Speaker-Zaak network:")
        print(f"   ├─ Speakers with connections: {total_stats['speaker_zaak_connections']}")
        print(f"   └─ Personen with connections: {total_stats['personen_with_connections']}")
        
        if total_stats['matched_speakers'] > 0:
            connection_rate = total_stats['personen_with_connections']/total_stats['matched_speakers']*100
            print(f"   Connection rate: {connection_rate:.1f}%")
        
        print(f"\n🎉 All tests completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        conn.close()


if __name__ == "__main__":
    success = test_enhanced_vlos_loader_comprehensive()
    sys.exit(0 if success else 1) 