#!/usr/bin/env python3
"""
Test Comprehensive Parliamentary Analysis System
Demonstrates the enhanced VLOS processing with full discourse analysis
"""

import sys
import os
sys.path.append('src')

from core.connection.neo4j_connection import Neo4jConnection
from core.config.tkapi_config import get_tkapi_config
from loaders.enhanced_vlos_verslag_loader import load_enhanced_vlos_verslag

def test_comprehensive_analysis():
    """Test the comprehensive parliamentary analysis system"""
    
    print("=" * 80)
    print("🚀 COMPREHENSIVE PARLIAMENTARY ANALYSIS SYSTEM TEST")
    print("=" * 80)
    
    # Setup connection
    config = get_tkapi_config()
    conn = Neo4jConnection(
        config['neo4j']['host'],
        config['neo4j']['port'],
        config['neo4j']['username'],
        config['neo4j']['password'],
        config['neo4j']['database']
    )
    
    # Test with existing sample XML files
    sample_files = [
        'sample_vlos_3ad601d2-ac2d-4fec-9a9c-2dda28a6ca38_d35df127-e439-4538-8954-0205f56e7636.xml',
        'sample_vlos_986eef87-cb77-4fe9-9b8a-c843ea38f21f_0f7db5f3-0fe6-499d-a5f3-0f83a8320ac1.xml',
        'sample_vlos_ce0428cf-375f-41e3-baef-e15cd953172a_604b6f07-0077-4e6c-bd09-03509b9b8849.xml'
    ]
    
    total_stats = {
        'files_processed': 0,
        'total_activities': 0,
        'matched_activities': 0,
        'total_speakers': 0,
        'matched_speakers': 0,
        'total_zaken': 0,
        'matched_zaken': 0,
        'speaker_zaak_connections': 0,
        'interruptions': 0,
        'voting_events': 0
    }
    
    for i, xml_file in enumerate(sample_files, 1):
        if not os.path.exists(xml_file):
            print(f"⚠️ Sample file {xml_file} not found, skipping...")
            continue
        
        print(f"\n📄 Processing sample file {i}/{len(sample_files)}: {xml_file}")
        print("-" * 60)
        
        try:
            # Read XML content
            with open(xml_file, 'r', encoding='utf-8') as f:
                xml_content = f.read()
            
            # Use a test vergadering ID (in practice this would be from the API)
            test_vergadering_id = f"test_vergadering_{i}"
            
            # Create a minimal test vergadering node
            with conn.driver.session() as session:
                session.run("""
                    MERGE (v:Vergadering {id: $id})
                    SET v.titel = $title, v.datum = date(), v.source = 'test'
                """, id=test_vergadering_id, title=f"Test Vergadering {i}")
            
            # Process with comprehensive analysis
            counts = load_enhanced_vlos_verslag(
                conn.driver, 
                xml_content, 
                test_vergadering_id, 
                f"test_verslag_{i}"
            )
            
            # Accumulate statistics
            total_stats['files_processed'] += 1
            for key in ['total_activities', 'matched_activities', 'total_speakers', 'matched_speakers',
                       'total_zaken', 'matched_zaken', 'speaker_zaak_connections', 'interruptions', 'voting_events']:
                if key in ['total_activities', 'total_speakers', 'total_zaken']:
                    total_stats[key] += counts.get(key.replace('total_', ''), 0)
                else:
                    total_stats[key] += counts.get(key, 0)
            
            print(f"✅ File {i} processed successfully!")
            
        except Exception as e:
            print(f"❌ Error processing {xml_file}: {e}")
    
    # Display comprehensive results
    print("\n" + "=" * 80)
    print("🎯 COMPREHENSIVE ANALYSIS RESULTS")
    print("=" * 80)
    print(f"📁 Files processed: {total_stats['files_processed']}")
    print(f"📊 Overall statistics:")
    print(f"  🎯 Activities: {total_stats['matched_activities']}/{total_stats['total_activities']} matched "
          f"({100 * total_stats['matched_activities'] / max(total_stats['total_activities'], 1):.1f}%)")
    print(f"  👥 Speakers: {total_stats['matched_speakers']}/{total_stats['total_speakers']} matched "
          f"({100 * total_stats['matched_speakers'] / max(total_stats['total_speakers'], 1):.1f}%)")
    print(f"  📋 Zaken: {total_stats['matched_zaken']}/{total_stats['total_zaken']} matched "
          f"({100 * total_stats['matched_zaken'] / max(total_stats['total_zaken'], 1):.1f}%)")
    print(f"🔗 Speaker-Zaak Connections: {total_stats['speaker_zaak_connections']}")
    print(f"🗣️ Interruption Events: {total_stats['interruptions']}")
    print(f"🗳️ Voting Events: {total_stats['voting_events']}")
    
    # Show some sample queries users can run
    print("\n" + "=" * 80)
    print("🔍 SAMPLE QUERIES TO EXPLORE THE DATA")
    print("=" * 80)
    print("Try these Cypher queries in Neo4j Browser:")
    print()
    print("1. View all speaker-zaak connections:")
    print("   MATCH (p:Persoon)-[r:DISCUSSED]->(z) RETURN p.roepnaam, p.achternaam, type(z), z.nummer LIMIT 10")
    print()
    print("2. Find interruption events:")
    print("   MATCH (ie:InterruptionEvent) RETURN ie.type, ie.original_speaker, ie.interrupter LIMIT 5")
    print()
    print("3. Analyze voting patterns:")
    print("   MATCH (ve:VotingEvent)-[:HAS_VOTE]->(v:IndividualVote)")
    print("   RETURN ve.consensus_percentage, COUNT(v) as votes ORDER BY ve.consensus_percentage")
    print()
    print("4. Most active speakers:")
    print("   MATCH (p:Persoon)-[:DISCUSSED]->(z)")
    print("   RETURN p.roepnaam, p.achternaam, COUNT(z) as topics_discussed")
    print("   ORDER BY topics_discussed DESC LIMIT 10")
    print()
    print("5. Enhanced VLOS document overview:")
    print("   MATCH (evd:EnhancedVlosDocument)-[:HAS_ANALYSIS_SUMMARY]->(pas:ParliamentaryAnalysisSummary)")
    print("   RETURN evd.vergadering_id, pas.total_activities, pas.speaker_zaak_connections, pas.total_interruptions")
    
    print("\n✅ Comprehensive Parliamentary Analysis System test complete!")
    print("=" * 80)


if __name__ == "__main__":
    test_comprehensive_analysis() 