#!/usr/bin/env python3
"""
Test script for Enhanced VLOS Loader
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.core.connection.neo4j_connection import Neo4jConnection
from src.loaders.enhanced_vlos_verslag_loader import load_enhanced_vlos_verslag
from src.core.interfaces import LoaderConfig
from src.loaders.enhanced_vlos_verslag_loader import EnhancedVlosVerslagLoader


def test_enhanced_vlos_loader():
    """Test the enhanced VLOS loader with a sample XML file."""
    print("🧪 Testing Enhanced VLOS Loader")
    
    # Find sample XML files
    sample_files = [f for f in os.listdir('.') if f.startswith('sample_vlos_') and f.endswith('.xml')]
    
    if not sample_files:
        print("❌ No sample_vlos_*.xml files found in current directory")
        return False
    
    print(f"📁 Found {len(sample_files)} sample XML files")
    
    # Use the first sample file
    sample_file = sample_files[0]
    print(f"🔍 Testing with {sample_file}")
    
    try:
        # Read XML content
        with open(sample_file, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        print(f"📄 Read {len(xml_content)} characters from {sample_file}")
        
        # Connect to Neo4j
        conn = Neo4jConnection()
        print("✅ Connected to Neo4j")
        
        # Find a suitable vergadering to test with
        with conn.driver.session() as session:
            result = session.run("""
                MATCH (v:Vergadering)
                WHERE v.begin IS NOT NULL AND v.einde IS NOT NULL
                RETURN v.id as id, v.titel as titel, v.begin as begin
                ORDER BY v.begin DESC
                LIMIT 1
            """)
            
            vergadering_record = result.single()
            
            if not vergadering_record:
                print("❌ No suitable vergadering found in Neo4j")
                return False
            
            test_vergadering_id = vergadering_record['id']
            print(f"🎯 Using test vergadering: {test_vergadering_id}")
            print(f"   📋 Titel: {vergadering_record['titel']}")
            print(f"   📅 Begin: {vergadering_record['begin']}")
        
        # Test direct function call
        print("\n🚀 Testing direct function call...")
        counts = load_enhanced_vlos_verslag(
            conn.driver, 
            xml_content, 
            test_vergadering_id, 
            f"test_verslag_{test_vergadering_id}"
        )
        
        print(f"✅ Direct function call successful!")
        print(f"   📊 Processed {counts['activities']} activities")
        print(f"   👥 Processed {counts['speakers']} speakers")
        print(f"   📋 Processed {counts['zaken']} zaken")
        
        # Test via loader interface
        print("\n🔧 Testing via loader interface...")
        loader = EnhancedVlosVerslagLoader()
        
        config = LoaderConfig(
            batch_size=100,
            skip_count=0,
            start_date="2024-01-01",
            custom_params={
                'xml_content': xml_content,
                'canonical_api_vergadering_id': test_vergadering_id,
                'api_verslag_id': f"test_verslag_interface_{test_vergadering_id}"
            }
        )
        
        result = loader.load(conn, config)
        
        if result.success:
            print("✅ Loader interface test successful!")
            print(f"   📊 Processed {result.processed_count} items")
            print(f"   ⏱️ Execution time: {result.execution_time_seconds:.2f} seconds")
            if result.warnings:
                print(f"   ⚠️  Warnings: {result.warnings}")
        else:
            print("❌ Loader interface test failed!")
            print(f"   🚨 Errors: {result.error_messages}")
            return False
        
        # Verify data in Neo4j
        print("\n🔍 Verifying data in Neo4j...")
        with conn.driver.session() as session:
            # Check enhanced VLOS document
            doc_count = session.run("""
                MATCH (edoc:EnhancedVlosDocument)
                WHERE edoc.vergadering_id = $vergadering_id
                RETURN COUNT(*) as count
            """, vergadering_id=test_vergadering_id).single()
            
            print(f"   📄 Enhanced VLOS documents: {doc_count['count']}")
            
            # Check VLOS activities
            activity_count = session.run("""
                MATCH (v:Vergadering {id: $vergadering_id})-[:HAS_VLOS_ACTIVITY]->(va:VlosActivity)
                RETURN COUNT(*) as count
            """, vergadering_id=test_vergadering_id).single()
            
            print(f"   🎯 VLOS activities: {activity_count['count']}")
            
            # Check matched activities
            matched_activity_count = session.run("""
                MATCH (v:Vergadering {id: $vergadering_id})-[:HAS_VLOS_ACTIVITY]->(va:VlosActivity)
                -[:MATCHES_API_ACTIVITY]->(a:Activiteit)
                RETURN COUNT(*) as count
            """, vergadering_id=test_vergadering_id).single()
            
            print(f"   🔗 Matched activities: {matched_activity_count['count']}")
            
            # Check speakers
            speaker_count = session.run("""
                MATCH (v:Vergadering {id: $vergadering_id})-[:HAS_VLOS_ACTIVITY]->(va:VlosActivity)
                -[:HAS_SPEAKER]->(vs:VlosSpeaker)
                RETURN COUNT(*) as count
            """, vergadering_id=test_vergadering_id).single()
            
            print(f"   👥 VLOS speakers: {speaker_count['count']}")
            
            # Check matched speakers
            matched_speaker_count = session.run("""
                MATCH (v:Vergadering {id: $vergadering_id})-[:HAS_VLOS_ACTIVITY]->(va:VlosActivity)
                -[:HAS_SPEAKER]->(vs:VlosSpeaker)-[:MATCHES_PERSOON]->(p:Persoon)
                RETURN COUNT(*) as count
            """, vergadering_id=test_vergadering_id).single()
            
            print(f"   🔗 Matched speakers: {matched_speaker_count['count']}")
            
            # Check processing summary
            summary_count = session.run("""
                MATCH (v:Vergadering {id: $vergadering_id})-[:HAS_ENHANCED_VLOS_DOCUMENT]->(edoc:EnhancedVlosDocument)
                -[:HAS_SUMMARY]->(s:VlosProcessingSummary)
                RETURN COUNT(*) as count
            """, vergadering_id=test_vergadering_id).single()
            
            print(f"   📊 Processing summaries: {summary_count['count']}")
        
        print("\n🎉 All tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        conn.close()


if __name__ == "__main__":
    success = test_enhanced_vlos_loader()
    sys.exit(0 if success else 1) 