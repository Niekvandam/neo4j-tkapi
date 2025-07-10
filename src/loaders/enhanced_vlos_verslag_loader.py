"""
Enhanced VLOS Verslag Loader - Comprehensive parliamentary discourse analysis
Migrated from test_vlos_activity_matching_with_personen_and_zaken.py
"""

import xml.etree.ElementTree as ET
import time
from typing import Optional, Dict, List, Any
from datetime import datetime
from collections import defaultdict

from utils.helpers import merge_node, merge_rel
from core.connection.neo4j_connection import Neo4jConnection

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry

# Import comprehensive enhanced matching logic
from .processors.enhanced_vlos_matching import (
    NS_VLOS,
    parse_xml_datetime,
    get_candidate_api_activities,
    process_enhanced_vlos_activity,
    normalize_topic,
    LOCAL_TIMEZONE_OFFSET_HOURS,
    match_vlos_speakers_to_personen,
    create_speaker_zaak_connections,
    detect_interruptions_in_activity,
    analyze_voting_in_activity
)


class EnhancedVlosVerslagLoader(BaseLoader):
    """Enhanced VLOS Verslag Loader with comprehensive parliamentary discourse analysis"""
    
    def __init__(self):
        super().__init__(
            name="enhanced_vlos_verslag_loader",
            description="Comprehensive VLOS XML processor with sophisticated activity, speaker, zaak matching, interruption analysis, and voting analysis"
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
            
            # Use the comprehensive enhanced loading function
            counts = load_enhanced_vlos_verslag(
                conn.driver, 
                xml_content, 
                canonical_api_vergadering_id, 
                api_verslag_id
            )
            
            result.success = True
            result.processed_count = counts.get('matched_activities', 0)
            result.total_items = counts.get('activities', 0)
            result.execution_time_seconds = time.time() - start_time
            
            # Add comprehensive summary to warnings for visibility
            result.warnings.append(
                f"🎯 Match rates - Activities: {counts.get('matched_activities', 0)}/{counts.get('activities', 0)}, "
                f"Speakers: {counts.get('matched_speakers', 0)}/{counts.get('speakers', 0)}, "
                f"Zaken: {counts.get('matched_zaken', 0)}/{counts.get('zaken', 0)}"
            )
            result.warnings.append(
                f"🔗 Connections: {counts.get('speaker_zaak_connections', 0)} speaker-zaak, "
                f"🗣️ Interruptions: {counts.get('interruptions', 0)}, "
                f"🗳️ Voting events: {counts.get('voting_events', 0)}"
            )
            
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
    Comprehensive enhanced VLOS loading with full parliamentary discourse analysis.
    
    This function implements the complete sophisticated matching system including:
    - Enhanced activity matching with scoring algorithms
    - Speaker identification and linking to Personen
    - Zaak/Dossier linking with fallback logic
    - Speaker-zaak relationship network creation
    - Parliamentary interruption analysis
    - Parliamentary voting analysis
    - Proper use of TK-API IDs (not VLOS objectids)
    
    Args:
        driver: Neo4j driver instance
        xml_content: Raw XML content from VLOS
        canonical_api_vergadering_id: ID of the Vergadering from TK API
        api_verslag_id: Optional ID of the API Verslag
        
    Returns:
        Dict with comprehensive counts and statistics
    """
    print(f"🚀 Processing VLOS XML with Comprehensive Parliamentary Analysis for Vergadering {canonical_api_vergadering_id}")
    
    counts = {
        'activities': 0,
        'speakers': 0,
        'zaken': 0,
        'matched_activities': 0,
        'matched_speakers': 0,
        'matched_zaken': 0,
        'speaker_zaak_connections': 0,
        'interruptions': 0,
        'voting_events': 0,
        'total_items': 0
    }
    
    # Track relationships using TK-API IDs (not VLOS objectids)
    activity_speakers = defaultdict(list)  # api_activity_id -> list of speakers
    activity_zaken = defaultdict(list)     # api_activity_id -> list of zaken
    interruption_events = []
    voting_events = []
    
    try:
        # Parse XML
        root = ET.fromstring(xml_content)
        
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
            print("📊 Fetching candidate API activities for sophisticated matching...")
            api_activities = get_candidate_api_activities(session, canonical_vergadering_node)
            print(f"📊 Found {len(api_activities)} candidate API activities")
            
            # Create enhanced VLOS document node
            doc_id = f"enhanced_vlos_doc_{canonical_api_vergadering_id}"
            doc_props = {
                'id': doc_id,
                'vergadering_id': canonical_api_vergadering_id,
                'source': 'enhanced_vlos_xml',
                'processed_at': str(time.time()),
                'matching_version': '3.0',  # Updated version
                'features': 'activity_matching,speaker_matching,zaak_matching,interruption_analysis,voting_analysis'
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
                vergadering_objectid = vergadering_elem.get('objectid', 'Unknown')
                print(f"🔄 Processing vergadering: {vergadering_objectid}")
                
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
                counts['activities'] += len(activities)
                
                for activity_elem in activities:
                    # Process with comprehensive analysis
                    api_activity_id = process_enhanced_vlos_activity(
                        session, activity_elem, api_activities, canonical_api_vergadering_id,
                        activity_speakers, activity_zaken, interruption_events, voting_events
                    )
                    
                    if api_activity_id:
                        counts['matched_activities'] += 1
                        print(f"    ✅ Successfully processed activity → API ID: {api_activity_id}")
                    else:
                        print(f"    ❌ Failed to match activity")
            
            # Post-processing: Match VLOS speakers to Personen
            print("🔗 Phase 2: Matching VLOS speakers to Persoon nodes...")
            matched_speakers = match_vlos_speakers_to_personen(session)
            counts['matched_speakers'] = matched_speakers
            print(f"✅ Matched {matched_speakers} speakers to Persoon nodes")
            
            # Post-processing: Create speaker-zaak relationship network
            print("🔗 Phase 3: Creating comprehensive speaker-zaak relationship network...")
            speaker_connections = create_speaker_zaak_connections(session, activity_speakers, activity_zaken)
            counts['speaker_zaak_connections'] = speaker_connections
            print(f"✅ Created {speaker_connections} speaker-zaak connections")
            
            # Count total speakers and zaken
            all_speakers = set()
            all_zaken = set()
            for speakers_list in activity_speakers.values():
                all_speakers.update(speaker['id'] for speaker in speakers_list)
            for zaken_list in activity_zaken.values():
                all_zaken.update(zaak['id'] for zaak in zaken_list)
            
            counts['speakers'] = len(all_speakers)
            counts['zaken'] = len(all_zaken)
            counts['matched_zaken'] = len(all_zaken)  # All found zaken are considered matched
            
            # Analyze interruptions
            counts['interruptions'] = len(interruption_events)
            if interruption_events:
                print(f"🗣️ Detected {len(interruption_events)} interruption events")
                _create_interruption_analysis_nodes(session, interruption_events, doc_id)
            
            # Analyze voting
            counts['voting_events'] = len(voting_events)
            if voting_events:
                print(f"🗳️ Detected {len(voting_events)} voting events")
                _create_voting_analysis_nodes(session, voting_events, doc_id)
            
            counts['total_items'] = counts['activities']
            
            # Create summary statistics node
            _create_analysis_summary(session, doc_id, counts, activity_speakers, activity_zaken, 
                                   interruption_events, voting_events)
            
        print("=" * 80)
        print("🎯 COMPREHENSIVE PARLIAMENTARY DISCOURSE ANALYSIS COMPLETE")
        print("=" * 80)
        print(f"📊 Overall Match Rate: {counts['matched_activities']}/{counts['activities']} "
              f"({100 * counts['matched_activities'] / max(counts['activities'], 1):.1f}%)")
        print(f"👥 Speaker Match Rate: {counts['matched_speakers']}/{counts['speakers']} "
              f"({100 * counts['matched_speakers'] / max(counts['speakers'], 1):.1f}%)")
        print(f"📋 Zaak Match Rate: {counts['matched_zaken']}/{counts['zaken']} "
              f"({100 * counts['matched_zaken'] / max(counts['zaken'], 1):.1f}%)")
        print(f"🔗 Speaker-Zaak Connections: {counts['speaker_zaak_connections']}")
        print(f"🗣️ Interruption Events: {counts['interruptions']}")
        print(f"🗳️ Voting Events: {counts['voting_events']}")
        print("=" * 80)
        
        return counts
        
    except ET.ParseError as e:
        print(f"❌ XML parsing error: {e}")
        raise
    except Exception as e:
        print(f"❌ Error in comprehensive VLOS processing: {e}")
        raise


def _create_interruption_analysis_nodes(session, interruption_events: List[Dict[str, Any]], doc_id: str):
    """Create Neo4j nodes for interruption analysis"""
    
    for i, event in enumerate(interruption_events):
        event_id = f"interruption_{doc_id}_{i}"
        
        event_props = {
            'id': event_id,
            'type': event['type'],
            'document_id': doc_id,
            'source': 'enhanced_vlos_analysis'
        }
        
        if event['type'] == 'simple_interruption':
            event_props['original_speaker'] = event['original_speaker']['naam']
            event_props['interrupter'] = event['interrupter']['naam']
        elif event['type'] == 'interruption_with_response':
            event_props['original_speaker'] = event['original_speaker']['naam']
            event_props['interrupter'] = event['interrupter']['naam']
            event_props['response_speaker'] = event['response']['naam']
        
        session.execute_write(merge_node, 'InterruptionEvent', 'id', event_props)
        session.execute_write(merge_rel, 'EnhancedVlosDocument', 'id', doc_id,
                              'InterruptionEvent', 'id', event_id, 'HAS_INTERRUPTION_EVENT')


def _create_voting_analysis_nodes(session, voting_events: List[Dict[str, Any]], doc_id: str):
    """Create Neo4j nodes for voting analysis"""
    
    for i, event in enumerate(voting_events):
        event_id = f"voting_{doc_id}_{i}"
        
        event_props = {
            'id': event_id,
            'total_votes': event['total_votes'],
            'voor_votes': event['voor_votes'],
            'tegen_votes': event['tegen_votes'],
            'consensus_percentage': event['consensus_percentage'],
            'is_unanimous': event['is_unanimous'],
            'is_controversial': event['is_controversial'],
            'document_id': doc_id,
            'source': 'enhanced_vlos_analysis'
        }
        
        session.execute_write(merge_node, 'VotingEvent', 'id', event_props)
        session.execute_write(merge_rel, 'EnhancedVlosDocument', 'id', doc_id,
                              'VotingEvent', 'id', event_id, 'HAS_VOTING_EVENT')
        
        # Create individual vote nodes
        for j, vote in enumerate(event['votes']):
            vote_id = f"vote_{event_id}_{j}"
            vote_props = {
                'id': vote_id,
                'fractie': vote['fractie'],
                'stemming': vote['stemming'],
                'voting_event_id': event_id,
                'source': 'enhanced_vlos_analysis'
            }
            
            session.execute_write(merge_node, 'IndividualVote', 'id', vote_props)
            session.execute_write(merge_rel, 'VotingEvent', 'id', event_id,
                                  'IndividualVote', 'id', vote_id, 'HAS_VOTE')


def _create_analysis_summary(session, doc_id: str, counts: Dict[str, int], 
                           activity_speakers: Dict[str, List[str]], activity_zaken: Dict[str, List[str]],
                           interruption_events: List[Dict[str, Any]], voting_events: List[Dict[str, Any]]):
    """Create comprehensive analysis summary node"""
    
    # Calculate additional statistics
    unique_speakers = set()
    unique_zaken = set()
    
    for speakers_list in activity_speakers.values():
        unique_speakers.update(speaker['id'] for speaker in speakers_list)
    
    for zaken_list in activity_zaken.values():
        unique_zaken.update(zaak['id'] for zaak in zaken_list)
    
    # Interruption statistics
    interruption_types = defaultdict(int)
    for event in interruption_events:
        interruption_types[event['type']] += 1
    
    # Voting statistics
    unanimous_votes = sum(1 for event in voting_events if event['is_unanimous'])
    controversial_votes = sum(1 for event in voting_events if event['is_controversial'])
    
    summary_props = {
        'id': f"summary_{doc_id}",
        'document_id': doc_id,
        'total_activities': counts['activities'],
        'matched_activities': counts['matched_activities'],
        'total_speakers': counts['speakers'],
        'matched_speakers': counts['matched_speakers'],
        'total_zaken': counts['zaken'],
        'speaker_zaak_connections': counts['speaker_zaak_connections'],
        'unique_speakers_with_connections': len(unique_speakers),
        'unique_zaken_discussed': len(unique_zaken),
        'total_interruptions': counts['interruptions'],
        'simple_interruptions': interruption_types.get('simple_interruption', 0),
        'interruptions_with_response': interruption_types.get('interruption_with_response', 0),
        'fragment_interruptions': interruption_types.get('fragment_interruption', 0),
        'total_voting_events': counts['voting_events'],
        'unanimous_votes': unanimous_votes,
        'controversial_votes': controversial_votes,
        'analysis_timestamp': str(time.time()),
        'source': 'enhanced_vlos_analysis'
    }
    
    session.execute_write(merge_node, 'ParliamentaryAnalysisSummary', 'id', summary_props)
    session.execute_write(merge_rel, 'EnhancedVlosDocument', 'id', doc_id,
                          'ParliamentaryAnalysisSummary', 'id', summary_props['id'], 'HAS_ANALYSIS_SUMMARY')


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