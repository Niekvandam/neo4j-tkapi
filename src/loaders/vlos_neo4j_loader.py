"""
VLOS Neo4j Loader - Persists VLOS analysis results to Neo4j

Focuses on the key relationships:
- Speakers and what they spoke about
- Activities and who participated
- Dossiers and who mentioned them
- Interruption patterns
- Voting patterns
"""

import glob
import os
from datetime import datetime
from typing import List, Dict, Any

from core.connection.neo4j_connection import Neo4jConnection
from core.checkpoint.checkpoint_decorator import checkpoint_loader, with_checkpoint
from utils.helpers import merge_node, merge_rel
from vlos import VlosPipeline, VlosConfig
from vlos.models import VlosProcessingResult


class VlosNeo4jLoader:
    """Loads VLOS analysis results into Neo4j with focus on parliamentary relationships"""
    
    def __init__(self):
        self.config = VlosConfig.for_production()
        self.pipeline = VlosPipeline(self.config)
    
    def fetch_vlos_from_api(self, start_date_str: str):
        """
        Fetch VLOS documents from the API using date filtering.
        
        Since Verslag doesn't have a Datum field, we filter Vergaderingen by date
        and then get their related Verslagen.
        
        Args:
            start_date_str: Start date for filtering (YYYY-MM-DD)
            
        Returns:
            List of Verslag objects with VLOS data
        """
        from tkapi import TKApi
        from tkapi.vergadering import Vergadering
        from tkapi.util import util as tkapi_util
        from datetime import datetime, timezone
        
        print(f"🔍 Fetching Vergaderingen (for VLOS sources) from API since {start_date_str}")
        
        # Create API instance
        api = TKApi(verbose=False)
        
        # Parse start date
        start_datetime = datetime.strptime(start_date_str, "%Y-%m-%d")
        start_utc = start_datetime.replace(tzinfo=timezone.utc)
        odata_start_date = tkapi_util.datetime_to_odata(start_utc)
        
        # Create filter for Vergaderingen (which have Datum field)
        filter_obj = Vergadering.create_filter()
        filter_obj.add_filter_str(f"Datum ge {odata_start_date}")
        
        # Expand to get related Verslagen
        original_expand = Vergadering.expand_params
        Vergadering.expand_params = ['Verslag']
        
        try:
            # Get vergaderingen from API
            vergaderingen = api.get_items(Vergadering, filter=filter_obj)
            print(f"→ Found {len(vergaderingen)} Vergaderingen since {start_date_str}")
            
            # Extract verslagen from vergaderingen
            vlos_items = []
            for vergadering in vergaderingen:
                if hasattr(vergadering, 'verslag') and vergadering.verslag:
                    # Check if this verslag has VLOS XML content
                    if self._has_vlos_content(vergadering.verslag):
                        vlos_items.append(vergadering.verslag)
            
            print(f"→ Found {len(vlos_items)} Verslagen with VLOS XML content")
            return vlos_items
            
        finally:
            # Restore original expand params
            Vergadering.expand_params = original_expand
    
    def _has_vlos_content(self, verslag) -> bool:
        """Check if a verslag has VLOS XML content available"""
        # Check if the verslag has an ID to construct the resource URL
        if not hasattr(verslag, 'id') or not verslag.id:
            return False
        
        # Additional checks could be added here in the future
        # For example, checking the status or soort of the verslag
        
        return True
    
    def process_single_vlos_from_api(self, conn: Neo4jConnection, verslag) -> Dict[str, Any]:
        """
        Process a single VLOS verslag from the API.
        
        Args:
            conn: Neo4j connection
            verslag: Verslag object from API
            
        Returns:
            Processing result dictionary
        """
        try:
            print(f"🔍 Processing VLOS from API Verslag: {verslag.id}")
            
            # Download XML content
            xml_content = self._download_vlos_xml(verslag)
            if not xml_content:
                print(f"❌ Could not download VLOS XML for verslag {verslag.id}")
                return {'success': False, 'error': 'Could not download XML'}
            
            # Process with VLOS pipeline
            result = self.pipeline.process_vlos_xml(xml_content)
            
            if not result.success:
                print(f"❌ VLOS processing failed for verslag {verslag.id}")
                return {'success': False, 'error': 'VLOS processing failed'}
            
            # Persist results to Neo4j
            with conn.driver.session(database=conn.database) as session:
                # Create VlosDocument node
                doc_id = self._create_vlos_document_node(session, f"api_verslag_{verslag.id}", result)
                
                # Persist all extracted entities
                self._persist_vlos_results(session, result, doc_id)
                
                # Additional metadata for API-sourced VLOS
                session.execute_write(merge_node, 'VlosDocument', 'id', {
                    'id': doc_id,
                    'api_verslag_id': verslag.id,
                    'source_type': 'api'
                })
            
            print(f"✅ Processed VLOS from API verslag {verslag.id}")
            return {'success': True, 'doc_id': doc_id}
            
        except Exception as e:
            print(f"❌ Error processing VLOS from API verslag {verslag.id}: {e}")
            return {'success': False, 'error': str(e)}
    
    def _download_vlos_xml(self, verslag) -> str:
        """Download VLOS XML content from a verslag"""
        import requests
        
        try:
            if not hasattr(verslag, 'id') or not verslag.id:
                return None
                
            # Build the correct URL to get the XML resource content
            xml_url = f"https://gegevensmagazijn.tweedekamer.nl/OData/v4/2.0/Verslag({verslag.id})/resource"
            
            # Download XML content
            response = requests.get(xml_url, timeout=30)
            response.raise_for_status()
            
            # Handle BOM properly - it might be double-encoded
            xml_content = response.text
            
            # Remove various forms of BOM that might appear
            if xml_content.startswith('\ufeff'):  # Unicode BOM
                xml_content = xml_content[1:]
            elif xml_content.startswith('ï»¿'):  # UTF-8 BOM as characters
                xml_content = xml_content[3:]
            elif xml_content.startswith('\xef\xbb\xbf'):  # UTF-8 BOM as bytes
                xml_content = xml_content[3:]
            
            return xml_content
            
        except Exception as e:
            print(f"❌ Error downloading VLOS XML: {e}")
            return None
        
    def _create_vlos_document_node(self, session, file_path: str, result: VlosProcessingResult) -> str:
        """Create a VlosDocument node representing the processed XML file"""
        doc_id = f"vlos_doc_{os.path.basename(file_path).replace('.xml', '')}"
        
        props = {
            'id': doc_id,
            'file_path': file_path,
            'file_name': os.path.basename(file_path),
            'processed_at': result.processing_timestamp.isoformat(),
            'processing_time_seconds': result.statistics.processing_time_seconds,
            'success': result.success,
            'activities_total': result.statistics.xml_activities_total,
            'activities_matched': result.statistics.xml_activities_matched,
            'speakers_total': result.statistics.xml_speakers_total,
            'speakers_matched': result.statistics.xml_speakers_matched,
            'zaken_total': result.statistics.xml_zaken_total,
            'zaken_matched': result.statistics.xml_zaken_matched,
            'connections_created': result.statistics.speaker_zaak_connections,
            'interruption_events': result.statistics.interruption_events,
            'voting_events': result.statistics.voting_events,
            'source': 'vlos_v2_analysis'
        }
        
        session.execute_write(merge_node, 'VlosDocument', 'id', props)
        
        # Link to canonical vergadering if found
        if result.canonical_api_vergadering_id:
            session.execute_write(merge_rel, 
                                'VlosDocument', 'id', doc_id,
                                'Vergadering', 'id', result.canonical_api_vergadering_id,
                                'ANALYZES_VERGADERING')
        
        return doc_id
    
    def _persist_vlos_results(self, session, result: VlosProcessingResult, doc_id: str):
        """Persist all VLOS analysis results to Neo4j"""
        # Persist each type of analysis
        speaker_count = self._persist_speaker_analysis(session, doc_id, result)
        activity_count = self._persist_activity_analysis(session, doc_id, result)
        zaak_count = self._persist_zaak_analysis(session, doc_id, result)
        connection_count = self._persist_speaker_zaak_connections(session, doc_id, result)
        interruption_count = self._persist_interruption_analysis(session, doc_id, result)
        voting_count = self._persist_voting_analysis(session, doc_id, result)
        
        # Return summary
        return {
            'speakers': speaker_count,
            'activities': activity_count,
            'zaken': zaak_count,
            'connections': connection_count,
            'interruptions': interruption_count,
            'votes': voting_count
        }
    
    def _persist_speaker_analysis(self, session, doc_id: str, result: VlosProcessingResult):
        """Persist speaker analysis and their connections to activities and zaken"""
        speaker_count = 0
        
        for speaker_match in result.speaker_matches:
            # Create VlosSpeaker node
            speaker_id = f"vlos_speaker_{hash(speaker_match.xml_speaker.fragment_id + speaker_match.xml_speaker.achternaam)}"
            
            speaker_props = {
                'id': speaker_id,
                'voornaam': speaker_match.xml_speaker.voornaam,
                'achternaam': speaker_match.xml_speaker.achternaam,
                'verslagnaam': speaker_match.xml_speaker.verslagnaam,
                'fractie': speaker_match.xml_speaker.fractie,
                'fragment_id': speaker_match.xml_speaker.fragment_id,
                'speech_preview': speaker_match.xml_speaker.speech_text[:200],
                'speech_length': len(speaker_match.xml_speaker.speech_text),
                'match_success': speaker_match.match_result.success,
                'match_score': speaker_match.match_result.score,
                'source': 'vlos_v2_analysis'
            }
            
            session.execute_write(merge_node, 'VlosSpeaker', 'id', speaker_props)
            
            # Link to document
            session.execute_write(merge_rel, 
                                'VlosDocument', 'id', doc_id,
                                'VlosSpeaker', 'id', speaker_id,
                                'CONTAINS_SPEAKER')
            
            # Link to matched Persoon if successful
            if speaker_match.match_result.success and speaker_match.persoon_id:
                session.execute_write(merge_rel, 
                                    'VlosSpeaker', 'id', speaker_id,
                                    'Persoon', 'id', speaker_match.persoon_id,
                                    'MATCHES_PERSOON')
            
            speaker_count += 1
        
        return speaker_count
    
    def _persist_activity_analysis(self, session, doc_id: str, result: VlosProcessingResult):
        """Persist activity analysis and their connections to API activities"""
        activity_count = 0
        
        for activity_match in result.activity_matches:
            # Create VlosActivity node
            activity_id = f"vlos_activity_{activity_match.xml_activity.object_id}"
            
            activity_props = {
                'id': activity_id,
                'xml_object_id': activity_match.xml_activity.object_id,
                'soort': activity_match.xml_activity.soort,
                'titel': activity_match.xml_activity.titel,
                'onderwerp': activity_match.xml_activity.onderwerp,
                'match_success': activity_match.match_result.success,
                'match_score': activity_match.match_result.score,
                'match_type': activity_match.match_result.match_type.value,
                'api_activity_id': activity_match.api_activity_id,
                'source': 'vlos_v2_analysis'
            }
            
            session.execute_write(merge_node, 'VlosActivity', 'id', activity_props)
            
            # Link to document
            session.execute_write(merge_rel, 
                                'VlosDocument', 'id', doc_id,
                                'VlosActivity', 'id', activity_id,
                                'CONTAINS_ACTIVITY')
            
            # Link to matched API activity if successful
            if activity_match.match_result.success and activity_match.api_activity_id:
                session.execute_write(merge_rel, 
                                    'VlosActivity', 'id', activity_id,
                                    'Activiteit', 'id', activity_match.api_activity_id,
                                    'MATCHES_API_ACTIVITY')
            
            activity_count += 1
        
        return activity_count
    
    def _persist_zaak_analysis(self, session, doc_id: str, result: VlosProcessingResult):
        """Persist zaak/dossier analysis and their connections to API entities"""
        zaak_count = 0
        
        for zaak_match in result.zaak_matches:
            # Create VlosZaak node
            zaak_id = f"vlos_zaak_{hash(zaak_match.xml_zaak.dossiernummer + zaak_match.xml_zaak.stuknummer)}"
            
            zaak_props = {
                'id': zaak_id,
                'dossiernummer': zaak_match.xml_zaak.dossiernummer,
                'stuknummer': zaak_match.xml_zaak.stuknummer,
                'titel': zaak_match.xml_zaak.titel,
                'match_success': zaak_match.match_result.success,
                'match_score': zaak_match.match_result.score,
                'match_type': zaak_match.match_result.match_type.value,
                'zaak_type': zaak_match.zaak_type,
                'api_zaak_id': zaak_match.zaak_id,
                'api_dossier_id': zaak_match.dossier_id,
                'source': 'vlos_v2_analysis'
            }
            
            session.execute_write(merge_node, 'VlosZaak', 'id', zaak_props)
            
            # Link to document
            session.execute_write(merge_rel, 
                                'VlosDocument', 'id', doc_id,
                                'VlosZaak', 'id', zaak_id,
                                'CONTAINS_ZAAK')
            
            # Link to matched API entities if successful
            if zaak_match.match_result.success:
                if zaak_match.zaak_id:
                    session.execute_write(merge_rel, 
                                        'VlosZaak', 'id', zaak_id,
                                        'Zaak', 'id', zaak_match.zaak_id,
                                        'MATCHES_API_ZAAK')
                elif zaak_match.dossier_id:
                    session.execute_write(merge_rel, 
                                        'VlosZaak', 'id', zaak_id,
                                        'Dossier', 'id', zaak_match.dossier_id,
                                        'MATCHES_API_DOSSIER')
            
            zaak_count += 1
        
        return zaak_count
    
    def _persist_speaker_zaak_connections(self, session, doc_id: str, result: VlosProcessingResult):
        """Persist the key speaker-zaak connections showing who spoke about what"""
        connection_count = 0
        
        for connection in result.speaker_zaak_connections:
            # Create unique connection ID
            connection_id = f"connection_{hash(str(connection.speaker_match.persoon_id) + str(connection.zaak_match.zaak_id or connection.zaak_match.dossier_id) + connection.activity_id)}"
            
            connection_props = {
                'id': connection_id,
                'activity_id': connection.activity_id,
                'activity_title': connection.activity_title,
                'context': connection.context,
                'connection_type': connection.connection_type,
                'speech_preview': connection.speech_preview,
                'speaker_name': connection.speaker_match.persoon_name,
                'zaak_title': connection.zaak_match.xml_zaak.titel,
                'zaak_dossier_nummer': connection.zaak_match.xml_zaak.dossiernummer,
                'zaak_stuk_nummer': connection.zaak_match.xml_zaak.stuknummer,
                'source': 'vlos_v2_analysis'
            }
            
            session.execute_write(merge_node, 'SpeakerZaakConnection', 'id', connection_props)
            
            # Link to document
            session.execute_write(merge_rel, 
                                'VlosDocument', 'id', doc_id,
                                'SpeakerZaakConnection', 'id', connection_id,
                                'CONTAINS_CONNECTION')
            
            # Link to the speaker (via Persoon)
            if connection.speaker_match.persoon_id:
                session.execute_write(merge_rel, 
                                    'Persoon', 'id', connection.speaker_match.persoon_id,
                                    'SpeakerZaakConnection', 'id', connection_id,
                                    'SPOKE_IN_CONNECTION')
            
            # Link to the zaak/dossier
            if connection.zaak_match.zaak_id:
                session.execute_write(merge_rel, 
                                    'SpeakerZaakConnection', 'id', connection_id,
                                    'Zaak', 'id', connection.zaak_match.zaak_id,
                                    'DISCUSSES_ZAAK')
            elif connection.zaak_match.dossier_id:
                session.execute_write(merge_rel, 
                                    'SpeakerZaakConnection', 'id', connection_id,
                                    'Dossier', 'id', connection.zaak_match.dossier_id,
                                    'DISCUSSES_DOSSIER')
            
            # Link to the activity
            session.execute_write(merge_rel, 
                                'SpeakerZaakConnection', 'id', connection_id,
                                'Activiteit', 'id', connection.activity_id,
                                'OCCURRED_DURING_ACTIVITY')
            
            connection_count += 1
        
        return connection_count
    
    def _persist_interruption_analysis(self, session, doc_id: str, result: VlosProcessingResult):
        """Persist interruption events showing parliamentary dynamics"""
        interruption_count = 0
        
        for interruption in result.interruption_events:
            # Create interruption ID
            interruption_id = f"interruption_{hash(interruption.fragment_id + interruption.original_speaker.persoon_name + interruption.interrupting_speaker.persoon_name)}"
            
            interruption_props = {
                'id': interruption_id,
                'type': interruption.type.value,
                'fragment_id': interruption.fragment_id,
                'activity_id': interruption.activity_id,
                'context': interruption.context,
                'speech_context': interruption.speech_context,
                'interruption_length': interruption.interruption_length,
                'original_speaker_name': interruption.original_speaker.persoon_name,
                'interrupting_speaker_name': interruption.interrupting_speaker.persoon_name,
                'responding_speaker_name': interruption.responding_speaker.persoon_name if interruption.responding_speaker else None,
                'topics_discussed': ', '.join(interruption.topics_discussed),
                'source': 'vlos_v2_analysis'
            }
            
            session.execute_write(merge_node, 'InterruptionEvent', 'id', interruption_props)
            
            # Link to document
            session.execute_write(merge_rel, 
                                'VlosDocument', 'id', doc_id,
                                'InterruptionEvent', 'id', interruption_id,
                                'CONTAINS_INTERRUPTION')
            
            # Link to the speakers involved
            if interruption.original_speaker.persoon_id:
                session.execute_write(merge_rel, 
                                    'Persoon', 'id', interruption.original_speaker.persoon_id,
                                    'InterruptionEvent', 'id', interruption_id,
                                    'WAS_INTERRUPTED_IN')
            
            if interruption.interrupting_speaker.persoon_id:
                session.execute_write(merge_rel, 
                                    'Persoon', 'id', interruption.interrupting_speaker.persoon_id,
                                    'InterruptionEvent', 'id', interruption_id,
                                    'INTERRUPTED_IN')
            
            if interruption.responding_speaker and interruption.responding_speaker.persoon_id:
                session.execute_write(merge_rel, 
                                    'Persoon', 'id', interruption.responding_speaker.persoon_id,
                                    'InterruptionEvent', 'id', interruption_id,
                                    'RESPONDED_IN')
            
            # Link to the activity
            session.execute_write(merge_rel, 
                                'InterruptionEvent', 'id', interruption_id,
                                'Activiteit', 'id', interruption.activity_id,
                                'OCCURRED_DURING_ACTIVITY')
            
            interruption_count += 1
        
        return interruption_count
    
    def _persist_voting_analysis(self, session, doc_id: str, result: VlosProcessingResult):
        """Persist voting events showing fractie positions"""
        voting_count = 0
        
        for voting in result.voting_analyses:
            # Create voting event ID
            voting_id = f"voting_{hash(voting.activity_id + voting.voting_event.titel + str(voting.voting_event.uitslag))}"
            
            voting_props = {
                'id': voting_id,
                'activity_id': voting.activity_id,
                'besluit_titel': voting.voting_event.titel,
                'besluit_vorm': voting.voting_event.besluitvorm,
                'uitslag': voting.voting_event.uitslag,
                'fractie_votes_voor': len([v for v in voting.voting_event.fractie_votes if v.get('vote_normalized') == 'voor']),
                'fractie_votes_tegen': len([v for v in voting.voting_event.fractie_votes if v.get('vote_normalized') == 'tegen']),
                'fractie_votes_onthouden': len([v for v in voting.voting_event.fractie_votes if v.get('vote_normalized') == 'onthouden']),
                'total_votes': len(voting.voting_event.fractie_votes),
                'is_controversial': voting.consensus_level < 75,  # Less than 75% consensus is controversial
                'consensus_score': voting.consensus_level,
                'source': 'vlos_v2_analysis'
            }
            
            session.execute_write(merge_node, 'VotingEvent', 'id', voting_props)
            
            # Link to document
            session.execute_write(merge_rel, 
                                'VlosDocument', 'id', doc_id,
                                'VotingEvent', 'id', voting_id,
                                'CONTAINS_VOTING')
            
            # Link to the activity
            session.execute_write(merge_rel, 
                                'VotingEvent', 'id', voting_id,
                                'Activiteit', 'id', voting.activity_id,
                                'OCCURRED_DURING_ACTIVITY')
            
            # Create individual fractie vote nodes
            for fractie_vote in voting.voting_event.fractie_votes:
                vote_id = f"vote_{voting_id}_{fractie_vote.get('fractie', 'unknown')}"
                
                vote_props = {
                    'id': vote_id,
                    'fractie_naam': fractie_vote.get('fractie', 'unknown'),
                    'stem': fractie_vote.get('vote', 'unknown'),
                    'source': 'vlos_v2_analysis'
                }
                
                session.execute_write(merge_node, 'FractieVote', 'id', vote_props)
                
                # Link to voting event
                session.execute_write(merge_rel, 
                                    'VotingEvent', 'id', voting_id,
                                    'FractieVote', 'id', vote_id,
                                    'HAS_FRACTIE_VOTE')
                
                # Link to fractie if we can find it
                session.execute_write(merge_rel, 
                                    'FractieVote', 'id', vote_id,
                                    'Fractie', 'naam', fractie_vote.get('fractie', 'unknown'),
                                    'CAST_BY_FRACTIE')
            
            voting_count += 1
        
        return voting_count
    
    def process_single_vlos_file(self, conn: Neo4jConnection, file_path: str) -> Dict[str, Any]:
        """Process a single VLOS XML file and persist analysis results"""
        try:
            print(f"🔍 Processing VLOS file: {os.path.basename(file_path)}")
            
            # Read XML content
            with open(file_path, 'r', encoding='utf-8') as f:
                xml_content = f.read()
            
            # Process with VLOS pipeline
            result = self.pipeline.process_vlos_xml(xml_content)
            
            if not result.success:
                print(f"❌ VLOS processing failed for {file_path}")
                return {'success': False, 'error': 'VLOS processing failed'}
            
            # Persist results to Neo4j
            with conn.driver.session(database=conn.database) as session:
                # Create main document node
                doc_id = self._create_vlos_document_node(session, file_path, result)
                
                # Persist all analysis components
                speaker_count = self._persist_speaker_analysis(session, doc_id, result)
                activity_count = self._persist_activity_analysis(session, doc_id, result)
                zaak_count = self._persist_zaak_analysis(session, doc_id, result)
                connection_count = self._persist_speaker_zaak_connections(session, doc_id, result)
                interruption_count = self._persist_interruption_analysis(session, doc_id, result)
                voting_count = self._persist_voting_analysis(session, doc_id, result)
                
                total_persisted = speaker_count + activity_count + zaak_count + connection_count + interruption_count + voting_count
                
                print(f"✅ Persisted {total_persisted} total relationships:")
                print(f"   📊 Speakers: {speaker_count}")
                print(f"   🎭 Activities: {activity_count}")
                print(f"   📋 Zaken: {zaak_count}")
                print(f"   🔗 Connections: {connection_count}")
                print(f"   ⚡ Interruptions: {interruption_count}")
                print(f"   🗳️ Voting Events: {voting_count}")
                
                return {
                    'success': True,
                    'doc_id': doc_id,
                    'total_persisted': total_persisted,
                    'speakers': speaker_count,
                    'activities': activity_count,
                    'zaken': zaak_count,
                    'connections': connection_count,
                    'interruptions': interruption_count,
                    'voting': voting_count
                }
        
        except Exception as e:
            print(f"❌ Error processing {file_path}: {str(e)}")
            return {'success': False, 'error': str(e)}


# Main loader function with checkpoint support
@with_checkpoint(checkpoint_interval=1, get_item_id=lambda item: getattr(item, 'id', str(item)))  # Checkpoint after each file
def load_vlos_analysis(conn: Neo4jConnection, xml_files_pattern: str = "sample_vlos_*.xml", 
                       start_date_str: str = "2024-01-01", use_api: bool = True,
                       _checkpoint_context=None) -> None:
    """
    Load VLOS analysis results from XML files into Neo4j.
    
    Args:
        conn: Neo4j connection
        xml_files_pattern: Glob pattern for XML files to process (used only if use_api=False)
        start_date_str: Start date for API filtering (YYYY-MM-DD)
        use_api: Whether to fetch from API (True) or use local files (False)
        _checkpoint_context: Checkpoint context (handled by decorator)
    """
    
    # Initialize loader
    loader = VlosNeo4jLoader()
    
    if use_api:
        # Fetch VLOS documents from API
        print(f"🔍 Fetching VLOS documents from API since {start_date_str}")
        vlos_items = loader.fetch_vlos_from_api(start_date_str)
        if not vlos_items:
            print(f"❌ No VLOS documents found from API since {start_date_str}")
            return
        
        print(f"🔍 Found {len(vlos_items)} VLOS documents from API")
        
        # Process each API item
        def process_api_item(vlos_item):
            return loader.process_single_vlos_from_api(conn, vlos_item)
        
        # Use checkpoint context to process items
        if _checkpoint_context:
            _checkpoint_context.process_items(vlos_items, process_api_item)
        else:
            # Fallback for when decorator is not used
            for vlos_item in vlos_items:
                process_api_item(vlos_item)
    else:
        # Find all VLOS XML files (legacy mode)
        xml_files = glob.glob(xml_files_pattern)
        if not xml_files:
            print(f"❌ No VLOS XML files found matching pattern: {xml_files_pattern}")
            return
        
        print(f"🔍 Found {len(xml_files)} VLOS XML files to process")
        
        # Process each file
        def process_file(file_path):
            return loader.process_single_vlos_file(conn, file_path)
        
        # Use checkpoint context to process items
        if _checkpoint_context:
            _checkpoint_context.process_items(xml_files, process_file)
        else:
            # Fallback for when decorator is not used
            for file_path in xml_files:
                process_file(file_path)
    
    print("✅ VLOS analysis loading completed!")


# Alternative function for loading from a specific directory
def load_vlos_analysis_from_directory(conn: Neo4jConnection, directory_path: str = ".") -> None:
    """Load VLOS analysis from all XML files in a directory"""
    xml_pattern = os.path.join(directory_path, "sample_vlos_*.xml")
    load_vlos_analysis(conn, xml_pattern) 