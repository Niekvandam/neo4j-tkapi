"""
Enhanced VLOS Verslag Loader - Uses sophisticated matching algorithms from test file
"""

import xml.etree.ElementTree as ET
import time
from typing import Optional, Dict, List, Any
from datetime import datetime

from utils.helpers import merge_node, merge_rel
from core.connection.neo4j_connection import Neo4jConnection

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry

# Import enhanced matching logic
from .processors.enhanced_vlos_matching import (
    NS_VLOS,
    parse_xml_datetime,
    get_candidate_api_activities,
    process_enhanced_vlos_activity,
    normalize_topic,
    LOCAL_TIMEZONE_OFFSET_HOURS
)


class EnhancedVlosVerslagLoader(BaseLoader):
    """Enhanced VLOS Verslag Loader with sophisticated matching algorithms"""
    
    def __init__(self):
        super().__init__(
            name="enhanced_vlos_verslag_loader",
            description="Enhanced VLOS XML processor with sophisticated activity, speaker, and zaak matching"
        )
        self._capabilities = [
            LoaderCapability.BATCH_PROCESSING,
            LoaderCapability.RELATIONSHIP_PROCESSING
        ]
    
    def validate_config(self, config: LoaderConfig) -> list[str]:
        """Validate configuration specific to Enhanced VLOS loader"""
        errors = super().validate_config(config)
        
        if config.custom_params:
            if 'xml_content' in config.custom_params and not isinstance(config.custom_params['xml_content'], str):
                errors.append("custom_params.xml_content must be a string")
            if 'canonical_api_vergadering_id' in config.custom_params and not isinstance(config.custom_params['canonical_api_vergadering_id'], str):
                errors.append("custom_params.canonical_api_vergadering_id must be a string")
        else:
            errors.append("custom_params required with xml_content and canonical_api_vergadering_id")
        
        return errors
    
    def load(self, conn: Neo4jConnection, config: LoaderConfig, 
             checkpoint_manager=None) -> LoaderResult:
        """Main loading method implementing the interface"""
        start_time = time.time()
        result = LoaderResult(
            success=False,
            processed_count=0,
            failed_count=0,
            skipped_count=0,
            total_items=0,
            execution_time_seconds=0.0,
            error_messages=[],
            warnings=[]
        )
        
        try:
            # Validate configuration
            validation_errors = self.validate_config(config)
            if validation_errors:
                result.error_messages.extend(validation_errors)
                return result
            
            # Extract required parameters
            xml_content = config.custom_params['xml_content']
            canonical_api_vergadering_id = config.custom_params['canonical_api_vergadering_id']
            api_verslag_id = config.custom_params.get('api_verslag_id')
            
            # Use the enhanced loading function
            counts = load_enhanced_vlos_verslag(
                conn.driver, 
                xml_content, 
                canonical_api_vergadering_id, 
                api_verslag_id
            )
            
            result.success = True
            result.processed_count = counts.get('activities', 0)
            result.total_items = counts.get('total_items', 0)
            result.execution_time_seconds = time.time() - start_time
            
            # Add summary to warnings for visibility
            result.warnings.append(f"Processed {counts.get('activities', 0)} activities, "
                                 f"{counts.get('speakers', 0)} speakers, "
                                 f"{counts.get('zaken', 0)} zaken")
            
        except Exception as e:
            result.error_messages.append(f"Enhanced VLOS loading failed: {str(e)}")
            result.execution_time_seconds = time.time() - start_time
        
        return result


# Register the loader
enhanced_vlos_verslag_loader_instance = EnhancedVlosVerslagLoader()
loader_registry.register(enhanced_vlos_verslag_loader_instance)


def load_enhanced_vlos_verslag(driver, xml_content: str, canonical_api_vergadering_id: str, 
                              api_verslag_id: str = None) -> Dict[str, int]:
    """
    Enhanced VLOS loading with sophisticated matching algorithms.
    
    Args:
        driver: Neo4j driver instance
        xml_content: Raw XML content from VLOS
        canonical_api_vergadering_id: ID of the Vergadering from TK API
        api_verslag_id: Optional ID of the API Verslag
        
    Returns:
        Dict with counts of processed items
    """
    print(f"🚀 Processing VLOS XML with Enhanced Matching for Vergadering {canonical_api_vergadering_id}")
    
    counts = {
        'activities': 0,
        'speakers': 0,
        'zaken': 0,
        'matched_activities': 0,
        'matched_speakers': 0,
        'matched_zaken': 0,
        'total_items': 0
    }
    
    try:
        # Parse XML
        root = ET.fromstring(xml_content)
        
        # Debug XML structure
        print(f"🔍 XML Root: {root.tag}")
        print(f"🔍 XML Attributes: {root.attrib}")
        
        with driver.session() as session:
            # Get canonical vergadering node
            canonical_vergadering_node = session.run(
                "MATCH (v:Vergadering {id: $id}) RETURN v",
                id=canonical_api_vergadering_id
            ).single()
            
            if not canonical_vergadering_node:
                print(f"❌ Canonical Vergadering {canonical_api_vergadering_id} not found")
                return counts
            
            canonical_vergadering_node = canonical_vergadering_node['v']
            
            # Get API activities for matching
            print("📊 Fetching candidate API activities...")
            api_activities = get_candidate_api_activities(session, canonical_vergadering_node)
            print(f"📊 Found {len(api_activities)} candidate API activities")
            
            # Create enhanced VLOS document node
            doc_id = f"enhanced_vlos_doc_{canonical_api_vergadering_id}"
            doc_props = {
                'id': doc_id,
                'vergadering_id': canonical_api_vergadering_id,
                'source': 'enhanced_vlos_xml',
                'processed_at': str(time.time()),
                'matching_version': '2.0'
            }
            session.execute_write(merge_node, 'EnhancedVlosDocument', 'id', doc_props)
            
            # Link to vergadering
            session.execute_write(merge_rel, 'Vergadering', 'id', canonical_api_vergadering_id,
                                  'EnhancedVlosDocument', 'id', doc_id, 'HAS_ENHANCED_VLOS_DOCUMENT')
            
            # Link to API verslag if provided
            if api_verslag_id:
                session.execute_write(merge_node, 'Verslag', 'id', {'id': api_verslag_id})
                session.execute_write(merge_rel, 'Verslag', 'id', api_verslag_id,
                                      'EnhancedVlosDocument', 'id', doc_id, 'IS_ENHANCED_VLOS_FOR')
            
            # Process all vergadering elements
            for vergadering_elem in root.findall('.//vlos:vergadering', NS_VLOS):
                print(f"🔄 Processing vergadering: {vergadering_elem.get('objectid', 'Unknown')}")
                
                # Extract vergadering metadata
                vergadering_soort = vergadering_elem.get('soort', '')
                vergadering_titel = vergadering_elem.findtext('vlos:titel', default='', namespaces=NS_VLOS)
                vergadering_nummer = vergadering_elem.findtext('vlos:vergaderingnummer', default='', namespaces=NS_VLOS)
                
                print(f"  📋 Soort: {vergadering_soort}")
                print(f"  📋 Titel: {vergadering_titel}")
                print(f"  📋 Nummer: {vergadering_nummer}")
                
                # Process all activities within this vergadering
                activities = vergadering_elem.findall('.//vlos:activiteit', NS_VLOS)
                print(f"  📊 Found {len(activities)} XML activities to process")
                
                for xml_act in activities:
                    counts['activities'] += 1
                    counts['total_items'] += 1
                    
                    # Use enhanced activity processing
                    activity_id = process_enhanced_vlos_activity(
                        session, 
                        xml_act, 
                        canonical_vergadering_node, 
                        api_activities
                    )
                    
                    if activity_id:
                        # Check if activity was matched (has relationship to API activity)
                        match_check = session.run(
                            "MATCH (va:VlosActivity {id: $id})-[:MATCHES_API_ACTIVITY]->(a:Activiteit) RETURN COUNT(*) as count",
                            id=activity_id
                        ).single()
                        
                        if match_check and match_check['count'] > 0:
                            counts['matched_activities'] += 1
                        
                        # Count speakers and zaken processed for this activity
                        speaker_count = session.run(
                            "MATCH (va:VlosActivity {id: $id})-[:HAS_SPEAKER]->(vs:VlosSpeaker) RETURN COUNT(*) as count",
                            id=activity_id
                        ).single()
                        
                        if speaker_count:
                            counts['speakers'] += speaker_count['count']
                        
                        matched_speaker_count = session.run(
                            "MATCH (va:VlosActivity {id: $id})-[:HAS_SPEAKER]->(vs:VlosSpeaker)-[:MATCHES_PERSOON]->(p:Persoon) RETURN COUNT(*) as count",
                            id=activity_id
                        ).single()
                        
                        if matched_speaker_count:
                            counts['matched_speakers'] += matched_speaker_count['count']
                        
                        zaak_count = session.run(
                            "MATCH (va:VlosActivity {id: $id})-[:HAS_ZAAK]->(vz:VlosZaak) RETURN COUNT(*) as count",
                            id=activity_id
                        ).single()
                        
                        if zaak_count:
                            counts['zaken'] += zaak_count['count']
                        
                        # Count both direct Zaak matches and Dossier fallback matches
                        matched_zaak_count = session.run("""
                            MATCH (va:VlosActivity {id: $id})-[:HAS_ZAAK]->(vz:VlosZaak)
                            WHERE EXISTS((vz)-[:MATCHES_API_ZAAK]->()) OR 
                                  (NOT EXISTS((vz)-[:MATCHES_API_ZAAK]->()) AND EXISTS((vz)-[:RELATED_TO_DOSSIER]->()))
                            RETURN COUNT(*) as count
                        """, id=activity_id).single()
                        
                        if matched_zaak_count:
                            counts['matched_zaken'] += matched_zaak_count['count']
            
            # Post-processing: Create comprehensive summary statistics with speaker-zaak connections
            from .processors.enhanced_vlos_matching import calculate_enhanced_vlos_statistics
            
            enhanced_stats = calculate_enhanced_vlos_statistics(session, doc_id)
            
            # Create comprehensive summary
            summary_props = {
                'id': f"summary_{doc_id}",
                'document_id': doc_id,
                'total_activities': enhanced_stats.get('total_activities', 0),
                'matched_activities': enhanced_stats.get('matched_activities', 0),
                'total_speakers': enhanced_stats.get('total_speakers', 0),
                'matched_speakers': enhanced_stats.get('matched_speakers', 0),
                'total_zaken': enhanced_stats.get('total_zaken', 0),
                'direct_zaak_matches': enhanced_stats.get('direct_zaak_matches', 0),
                'dossier_fallback_matches': enhanced_stats.get('dossier_fallback_matches', 0),
                'total_zaak_successes': enhanced_stats.get('total_zaak_successes', 0),
                'document_matches': enhanced_stats.get('document_matches', 0),
                'activity_match_rate': enhanced_stats.get('activity_match_rate', 0.0),
                'speaker_match_rate': enhanced_stats.get('speaker_match_rate', 0.0),
                'zaak_match_rate': enhanced_stats.get('zaak_match_rate', 0.0),
                'speakers_with_zaak_connections': enhanced_stats.get('speakers_with_zaak_connections', 0),
                'unique_zaken_discussed_by_speakers': enhanced_stats.get('unique_zaken_discussed_by_speakers', 0),
                'personen_with_zaak_connections': enhanced_stats.get('personen_with_zaak_connections', 0),
                'unique_zaken_discussed_by_personen': enhanced_stats.get('unique_zaken_discussed_by_personen', 0),
                'speaker_zaak_connection_rate': enhanced_stats.get('speaker_zaak_connection_rate', 0.0),
                'persoon_zaak_connection_rate': enhanced_stats.get('persoon_zaak_connection_rate', 0.0),
                'source': 'enhanced_vlos_summary_v2'
            }
            
            session.execute_write(merge_node, 'VlosProcessingSummary', 'id', summary_props)
            session.execute_write(merge_rel, 'EnhancedVlosDocument', 'id', doc_id,
                                  'VlosProcessingSummary', 'id', summary_props['id'], 'HAS_SUMMARY')
            
        # Final summary with enhanced statistics
        print(f"\n🎯 Enhanced VLOS Processing Complete!")
        print(f"📊 Activities: {enhanced_stats.get('matched_activities', 0)}/{enhanced_stats.get('total_activities', 0)} matched "
              f"({enhanced_stats.get('activity_match_rate', 0)*100:.1f}%)")
        print(f"👥 Speakers: {enhanced_stats.get('matched_speakers', 0)}/{enhanced_stats.get('total_speakers', 0)} matched "
              f"({enhanced_stats.get('speaker_match_rate', 0)*100:.1f}%)")
        
        # Enhanced Zaak statistics with fallback breakdown
        total_zaken = enhanced_stats.get('total_zaken', 0)
        direct_matches = enhanced_stats.get('direct_zaak_matches', 0)
        fallback_matches = enhanced_stats.get('dossier_fallback_matches', 0)
        total_successes = enhanced_stats.get('total_zaak_successes', 0)
        
        print(f"📋 Zaken: {total_successes}/{total_zaken} matched ({enhanced_stats.get('zaak_match_rate', 0)*100:.1f}%)")
        print(f"   ├─ Direct Zaak matches: {direct_matches}")
        print(f"   └─ Dossier fallback matches: {fallback_matches}")
        
        # Speaker-Zaak connection statistics
        speakers_connected = enhanced_stats.get('speakers_with_zaak_connections', 0)
        personen_connected = enhanced_stats.get('personen_with_zaak_connections', 0)
        unique_zaken_discussed = enhanced_stats.get('unique_zaken_discussed_by_personen', 0)
        
        print(f"🔗 Speaker-Zaak Connections:")
        print(f"   ├─ Speakers with zaak connections: {speakers_connected}")
        print(f"   ├─ Personen with zaak connections: {personen_connected}")
        print(f"   ├─ Unique zaken/dossiers discussed: {unique_zaken_discussed}")
        print(f"   └─ Persoon-zaak connection rate: {enhanced_stats.get('persoon_zaak_connection_rate', 0)*100:.1f}%")
        
        return counts
        
    except ET.ParseError as e:
        print(f"❌ XML parsing error: {e}")
        raise
    except Exception as e:
        print(f"❌ Error in enhanced VLOS processing: {e}")
        raise


# Utility function to run enhanced VLOS processing on existing data
def reprocess_existing_vlos_with_enhanced_matching(driver, limit: int = 10):
    """
    Reprocess existing VLOS data with enhanced matching.
    
    Args:
        driver: Neo4j driver instance
        limit: Maximum number of vergaderingen to reprocess
    """
    print(f"🔄 Reprocessing existing VLOS data with enhanced matching (limit: {limit})")
    
    with driver.session() as session:
        # Find vergaderingen with existing VLOS data but no enhanced processing
        query = """
        MATCH (v:Vergadering)-[:HAS_VLOS_DOCUMENT]->(vd:VlosDocument)
        WHERE NOT EXISTS((v)-[:HAS_ENHANCED_VLOS_DOCUMENT]->())
        RETURN v.id as vergadering_id, vd.id as vlos_doc_id
        LIMIT $limit
        """
        
        results = session.run(query, limit=limit)
        candidates = list(results)
        
        print(f"📊 Found {len(candidates)} vergaderingen to reprocess")
        
        for record in candidates:
            vergadering_id = record['vergadering_id']
            vlos_doc_id = record['vlos_doc_id']
            
            print(f"\n🔄 Reprocessing vergadering {vergadering_id}")
            
            # Find associated verslag with XML content
            verslag_query = """
            MATCH (v:Vergadering {id: $vergadering_id})-[:HAS_API_VERSLAG]->(vs:Verslag)
            WHERE vs.vlos_xml_processed = true
            RETURN vs.id as verslag_id
            LIMIT 1
            """
            
            verslag_result = session.run(verslag_query, vergadering_id=vergadering_id).single()
            
            if verslag_result:
                verslag_id = verslag_result['verslag_id']
                
                # For reprocessing, we would need to download the XML again
                # This is a placeholder - in practice you'd call download_verslag_xml
                print(f"  📥 Would reprocess verslag {verslag_id} with enhanced matching")
                print(f"  💡 Implementation needed: download XML and call load_enhanced_vlos_verslag")
            else:
                print(f"  ❌ No verslag found for vergadering {vergadering_id}")


if __name__ == "__main__":
    # Test the enhanced loader
    from core.connection.neo4j_connection import Neo4jConnection
    
    print("🧪 Testing Enhanced VLOS Loader")
    
    # This would be used for standalone testing
    conn = Neo4jConnection()
    
    # Example usage would go here
    print("💡 Use this loader via the interface system or import the functions directly") 