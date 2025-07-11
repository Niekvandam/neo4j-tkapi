"""
VLOS Loader V2 - Clean Implementation

A clean, maintainable VLOS loader using the new modular pipeline system.
This replaces all the deprecated VLOS loaders with a single, comprehensive solution.
"""

import time
import requests
from typing import Optional, Dict, Any
from datetime import datetime

from tkapi import TKApi
from tkapi.verslag import Verslag
from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node, merge_rel

# Import the new VLOS system
from vlos import VlosPipeline, VlosConfig
from vlos.models import VlosProcessingResult

# Import interface system for compatibility
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry


class VlosLoaderV2(BaseLoader):
    """Clean VLOS loader using the new modular pipeline system"""
    
    def __init__(self):
        super().__init__(
            name="vlos_loader_v2",
            description="Modular VLOS processing with comprehensive parliamentary analysis"
        )
        self._capabilities = [
            LoaderCapability.BATCH_PROCESSING,
            LoaderCapability.RELATIONSHIP_PROCESSING
        ]
    
    def validate_config(self, config: LoaderConfig) -> list[str]:
        """Validate configuration for VLOS loader"""
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
        """Main loading method using the new pipeline"""
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
            
            # Extract parameters
            xml_content = config.custom_params['xml_content']
            canonical_api_vergadering_id = config.custom_params['canonical_api_vergadering_id']
            api_verslag_id = config.custom_params.get('api_verslag_id')
            
            # Use the new modular pipeline
            vlos_config = VlosConfig.for_production()
            api = TKApi(verbose=False)
            pipeline = VlosPipeline(vlos_config, api)
            
            # Process the VLOS XML
            processing_result = pipeline.process_vlos_xml(xml_content, api_verslag_id)
            
            if processing_result.success:
                # Persist to Neo4j
                persisted_count = self._persist_to_neo4j(conn, processing_result, canonical_api_vergadering_id)
                
                result.success = True
                result.processed_count = persisted_count
                result.total_items = processing_result.statistics.xml_activities_total
                
                # Add detailed statistics to warnings for visibility
                stats = processing_result.statistics
                result.warnings.append(
                    f"🎯 Processing complete: {stats.xml_activities_matched}/{stats.xml_activities_total} activities, "
                    f"{stats.xml_speakers_matched}/{stats.xml_speakers_total} speakers, "
                    f"{stats.xml_zaken_matched}/{stats.xml_zaken_total} zaken matched"
                )
                result.warnings.append(
                    f"🔗 Created {stats.speaker_zaak_connections} speaker-zaak connections, "
                    f"{stats.interruption_events} interruption events, {stats.voting_events} voting events"
                )
            else:
                result.error_messages.extend(processing_result.error_messages)
                result.failed_count = 1
            
            result.execution_time_seconds = time.time() - start_time
            
        except Exception as e:
            result.error_messages.append(f"VLOS loading failed: {str(e)}")
            result.failed_count = 1
            result.execution_time_seconds = time.time() - start_time
        
        return result
    
    def _persist_to_neo4j(self, conn: Neo4jConnection, processing_result: VlosProcessingResult, 
                         canonical_api_vergadering_id: str) -> int:
        """Persist VLOS processing results to Neo4j"""
        
        persisted_count = 0
        
        with conn.driver.session() as session:
            # Create main VLOS document node
            doc_id = f"vlos_v2_doc_{canonical_api_vergadering_id}"
            doc_props = {
                'id': doc_id,
                'vergadering_id': canonical_api_vergadering_id,
                'source': 'vlos_v2_modular',
                'processed_at': processing_result.processing_timestamp.isoformat(),
                'processing_time_seconds': processing_result.statistics.processing_time_seconds,
                'success': processing_result.success
            }
            session.execute_write(merge_node, 'VlosDocumentV2', 'id', doc_props)
            
            # Link to Vergadering
            session.execute_write(merge_rel, 'Vergadering', 'id', canonical_api_vergadering_id,
                                  'VlosDocumentV2', 'id', doc_id, 'HAS_VLOS_V2_DOCUMENT')
            persisted_count += 1
            
            # Persist activity matches
            for activity_match in processing_result.activity_matches:
                if activity_match.match_result.success:
                    activity_props = {
                        'id': f"vlos_activity_{activity_match.xml_activity.object_id}",
                        'xml_object_id': activity_match.xml_activity.object_id,
                        'api_activity_id': activity_match.api_activity_id,
                        'soort': activity_match.xml_activity.soort,
                        'titel': activity_match.xml_activity.titel,
                        'onderwerp': activity_match.xml_activity.onderwerp,
                        'match_score': activity_match.match_result.score,
                        'match_type': activity_match.match_result.match_type.value,
                        'source': 'vlos_v2'
                    }
                    session.execute_write(merge_node, 'VlosActivity', 'id', activity_props)
                    
                    # Link to document
                    session.execute_write(merge_rel, 'VlosDocumentV2', 'id', doc_id,
                                          'VlosActivity', 'id', activity_props['id'], 'CONTAINS_ACTIVITY')
                    
                    # Link to API activity if matched
                    if activity_match.api_activity_id:
                        session.execute_write(merge_rel, 'VlosActivity', 'id', activity_props['id'],
                                              'Activiteit', 'id', activity_match.api_activity_id, 'MATCHES_API_ACTIVITY')
                    
                    persisted_count += 1
            
            # Persist speaker matches
            for speaker_match in processing_result.speaker_matches:
                speaker_props = {
                    'id': f"vlos_speaker_{hash(speaker_match.xml_speaker.fragment_id + speaker_match.xml_speaker.achternaam)}",
                    'voornaam': speaker_match.xml_speaker.voornaam,
                    'achternaam': speaker_match.xml_speaker.achternaam,
                    'verslagnaam': speaker_match.xml_speaker.verslagnaam,
                    'fractie': speaker_match.xml_speaker.fractie,
                    'fragment_id': speaker_match.xml_speaker.fragment_id,
                    'speech_preview': speaker_match.xml_speaker.speech_text[:200],
                    'match_success': speaker_match.match_result.success,
                    'match_score': speaker_match.match_result.score,
                    'source': 'vlos_v2'
                }
                session.execute_write(merge_node, 'VlosSpeaker', 'id', speaker_props)
                
                # Link to document
                session.execute_write(merge_rel, 'VlosDocumentV2', 'id', doc_id,
                                      'VlosSpeaker', 'id', speaker_props['id'], 'CONTAINS_SPEAKER')
                
                # Link to Persoon if matched
                if speaker_match.match_result.success and speaker_match.persoon_id:
                    session.execute_write(merge_rel, 'VlosSpeaker', 'id', speaker_props['id'],
                                          'Persoon', 'id', speaker_match.persoon_id, 'MATCHES_PERSOON')
                
                persisted_count += 1
            
            # Persist zaak matches
            for zaak_match in processing_result.zaak_matches:
                zaak_props = {
                    'id': f"vlos_zaak_{hash(zaak_match.xml_zaak.dossiernummer + zaak_match.xml_zaak.stuknummer)}",
                    'dossiernummer': zaak_match.xml_zaak.dossiernummer,
                    'stuknummer': zaak_match.xml_zaak.stuknummer,
                    'titel': zaak_match.xml_zaak.titel,
                    'match_success': zaak_match.match_result.success,
                    'match_type': zaak_match.match_result.match_type.value,
                    'zaak_type': zaak_match.zaak_type,
                    'source': 'vlos_v2'
                }
                session.execute_write(merge_node, 'VlosZaak', 'id', zaak_props)
                
                # Link to document
                session.execute_write(merge_rel, 'VlosDocumentV2', 'id', doc_id,
                                      'VlosZaak', 'id', zaak_props['id'], 'CONTAINS_ZAAK')
                
                # Link to API entities if matched
                if zaak_match.match_result.success:
                    if zaak_match.zaak_id:
                        session.execute_write(merge_rel, 'VlosZaak', 'id', zaak_props['id'],
                                              'Zaak', 'id', zaak_match.zaak_id, 'MATCHES_ZAAK')
                    elif zaak_match.dossier_id:
                        session.execute_write(merge_rel, 'VlosZaak', 'id', zaak_props['id'],
                                              'Dossier', 'id', zaak_match.dossier_id, 'MATCHES_DOSSIER')
                
                persisted_count += 1
            
            # Persist speaker-zaak connections
            for connection in processing_result.speaker_zaak_connections:
                connection_props = {
                    'id': f"connection_{hash(str(connection.speaker_match.persoon_id) + str(connection.zaak_match.zaak_id or connection.zaak_match.dossier_id) + connection.activity_id)}",
                    'activity_id': connection.activity_id,
                    'activity_title': connection.activity_title,
                    'context': connection.context,
                    'connection_type': connection.connection_type,
                    'speech_preview': connection.speech_preview,
                    'source': 'vlos_v2'
                }
                session.execute_write(merge_node, 'SpeakerZaakConnection', 'id', connection_props)
                
                # Link to document
                session.execute_write(merge_rel, 'VlosDocumentV2', 'id', doc_id,
                                      'SpeakerZaakConnection', 'id', connection_props['id'], 'CONTAINS_CONNECTION')
                
                persisted_count += 1
            
            # Persist analysis results as summary nodes
            if processing_result.statistics:
                stats_props = {
                    'id': f"stats_{doc_id}",
                    'activities_total': processing_result.statistics.xml_activities_total,
                    'activities_matched': processing_result.statistics.xml_activities_matched,
                    'speakers_total': processing_result.statistics.xml_speakers_total,
                    'speakers_matched': processing_result.statistics.xml_speakers_matched,
                    'zaken_total': processing_result.statistics.xml_zaken_total,
                    'zaken_matched': processing_result.statistics.xml_zaken_matched,
                    'connections_created': processing_result.statistics.speaker_zaak_connections,
                    'interruption_events': processing_result.statistics.interruption_events,
                    'voting_events': processing_result.statistics.voting_events,
                    'activity_match_rate': processing_result.statistics.activity_match_rate,
                    'speaker_match_rate': processing_result.statistics.speaker_match_rate,
                    'zaak_match_rate': processing_result.statistics.zaak_match_rate,
                    'source': 'vlos_v2'
                }
                session.execute_write(merge_node, 'VlosStatistics', 'id', stats_props)
                
                # Link to document
                session.execute_write(merge_rel, 'VlosDocumentV2', 'id', doc_id,
                                      'VlosStatistics', 'id', stats_props['id'], 'HAS_STATISTICS')
                
                persisted_count += 1
        
        return persisted_count


# Register the loader
vlos_loader_v2_instance = VlosLoaderV2()
loader_registry.register(vlos_loader_v2_instance)


def load_vlos_with_pipeline(xml_content: str, canonical_api_vergadering_id: str, 
                           api_verslag_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Convenience function to process VLOS XML using the new pipeline.
    
    Args:
        xml_content: Raw XML content from VLOS
        canonical_api_vergadering_id: ID of the Vergadering from TK API
        api_verslag_id: Optional ID of the API Verslag
        
    Returns:
        Dict with processing results and statistics
    """
    print(f"🚀 Processing VLOS XML with New Modular Pipeline for Vergadering {canonical_api_vergadering_id}")
    
    # Create pipeline with production configuration
    vlos_config = VlosConfig.for_production()
    api = TKApi(verbose=False)
    pipeline = VlosPipeline(vlos_config, api)
    
    # Process XML
    result = pipeline.process_vlos_xml(xml_content, api_verslag_id)
    
    if result.success:
        return {
            'success': True,
            'statistics': {
                'activities_matched': result.statistics.xml_activities_matched,
                'activities_total': result.statistics.xml_activities_total,
                'speakers_matched': result.statistics.xml_speakers_matched,
                'speakers_total': result.statistics.xml_speakers_total,
                'zaken_matched': result.statistics.xml_zaken_matched,
                'zaken_total': result.statistics.xml_zaken_total,
                'speaker_zaak_connections': result.statistics.speaker_zaak_connections,
                'interruption_events': result.statistics.interruption_events,
                'voting_events': result.statistics.voting_events,
                'processing_time': result.statistics.processing_time_seconds
            },
            'match_rates': {
                'activities': result.statistics.activity_match_rate,
                'speakers': result.statistics.speaker_match_rate,
                'zaken': result.statistics.zaak_match_rate
            },
            'result': result
        }
    else:
        return {
            'success': False,
            'error_messages': result.error_messages,
            'result': result
        } 