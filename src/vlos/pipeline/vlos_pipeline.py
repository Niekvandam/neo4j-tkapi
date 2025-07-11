"""
VLOS Processing Pipeline

Main orchestration pipeline that coordinates all VLOS processing components
to analyze parliamentary session data and create comprehensive reports.
"""

import time
from datetime import datetime
from typing import List, Optional, Dict, Any
from collections import defaultdict

from tkapi import TKApi

from ..config import VlosConfig
from ..models import (
    VlosProcessingResult, ProcessingStatistics, XmlVergadering,
    ActivityMatch, SpeakerMatch, ZaakMatch, SpeakerZaakConnection
)
from ..extractors import XmlExtractor, ApiExtractor
from ..matchers import NameMatcher, ActivityMatcher
from ..analyzers import InterruptionAnalyzer, VotingAnalyzer


class VlosPipeline:
    """Main VLOS processing pipeline"""
    
    def __init__(self, config: VlosConfig = None, api: TKApi = None):
        self.config = config or VlosConfig.default()
        self.api = api or TKApi()
        
        # Initialize components
        self.xml_extractor = XmlExtractor(self.config)
        self.api_extractor = ApiExtractor(self.config, self.api)
        self.activity_matcher = ActivityMatcher(self.config)
        self.interruption_analyzer = InterruptionAnalyzer(self.config)
        self.voting_analyzer = VotingAnalyzer(self.config)
    
    def process_vlos_xml(self, xml_content: str, api_verslag_id: Optional[str] = None) -> VlosProcessingResult:
        """Process VLOS XML content and return comprehensive analysis results"""
        
        start_time = time.time()
        
        try:
            # Step 1: Extract XML data
            print(f"🔍 STEP 1: Extracting XML data...")
            xml_vergadering = self.xml_extractor.extract_vergadering(xml_content)
            xml_activities = self.xml_extractor.extract_activities(xml_content)
            
            print(f"🔍 Extracted {len(xml_activities)} activities from XML")
            for i, activity in enumerate(xml_activities, 1):
                print(f"   Activity {i}: {activity.soort} - {activity.titel}")
            
            # Step 2: Find canonical Vergadering from API
            print(f"\n🎯 STEP 2: Finding canonical Vergadering...")
            canonical_vergadering = self.api_extractor.find_canonical_vergadering(xml_vergadering)
            if not canonical_vergadering:
                return self._create_error_result(
                    xml_vergadering, "No matching Vergadering found in TK API"
                )
            
            print(f"🎯 Found canonical Vergadering: {canonical_vergadering.id}")
            
            # Step 3: Get candidate API activities
            print(f"\n📊 STEP 3: Getting candidate API activities...")
            api_activities = self.api_extractor.get_candidate_activities(canonical_vergadering)
            print(f"📊 Retrieved {len(api_activities)} candidate API activities")
            
            # Step 4: Process each XML activity
            print(f"\n🔄 STEP 4: Processing each XML activity...")
            activity_matches = []
            all_speaker_matches = []
            all_zaak_matches = []
            speaker_zaak_connections = []
            interruption_events = []
            voting_analyses = []
            
            # Track connections across activities
            activity_speakers_map = defaultdict(list)  # activity_id -> speakers
            activity_zaken_map = defaultdict(list)
            
            for i, xml_activity in enumerate(xml_activities, 1):
                print(f"\n   Processing XML Activity {i}/{len(xml_activities)}: {xml_activity.soort} - {xml_activity.titel[:50]}...")
                
                # Match activity
                activity_match = self.activity_matcher.match_activity(
                    xml_activity, api_activities, canonical_vergadering
                )
                activity_matches.append(activity_match)
                
                api_activity_id = activity_match.api_activity_id or f"unmatched_{xml_activity.object_id}"
                
                # Extract and match speakers
                print(f"      → Extracting speakers...")
                xml_speakers = self.xml_extractor.extract_speakers_from_activity(xml_activity)
                speaker_matches = self._match_speakers(
                    xml_speakers, 
                    activity_match.match_result.matched_entity.actors if activity_match.match_result.success else []
                )
                all_speaker_matches.extend(speaker_matches)
                activity_speakers_map[api_activity_id] = speaker_matches
                
                # Extract and match zaken (both from XML and via Agendapunten)
                print(f"      → Extracting zaken...")
                xml_zaken = self.xml_extractor.extract_zaken_from_activity(xml_activity)
                xml_zaak_matches = self._match_zaken(xml_zaken)
                
                # Get additional zaken via Agendapunten if activity matched
                agendapunt_zaak_matches = []
                if activity_match.match_result.success and activity_match.api_activity_id:
                    print(f"      → Getting Agendapunten for matched activity...")
                    agendapunten = self.api_extractor.get_agendapunten_for_activity(activity_match.api_activity_id)
                    print(f"      → Found {len(agendapunten)} Agendapunten")
                    
                    for agendapunt in agendapunten:
                        # Create ZaakMatch objects from Agendapunt connections
                        agendapunt_zaak_matches_from_ap = self._create_zaak_match_from_agendapunt(agendapunt)
                        if agendapunt_zaak_matches_from_ap:
                            agendapunt_zaak_matches.extend(agendapunt_zaak_matches_from_ap)
                
                # Combine XML and Agendapunt zaken
                zaak_matches = xml_zaak_matches + agendapunt_zaak_matches
                all_zaak_matches.extend(zaak_matches)
                activity_zaken_map[api_activity_id] = zaak_matches
                
                # Create speaker-zaak connections
                connections = self._create_speaker_zaak_connections(
                    speaker_matches, zaak_matches, api_activity_id, xml_activity.titel
                )
                speaker_zaak_connections.extend(connections)
                
                # Extract speakers directly from XML zaak elements (not agendapunt-derived ones)
                for zaak_match in xml_zaak_matches:
                    if zaak_match.match_result.success:
                        # Extract speakers directly from this zaak element
                        direct_zaak_speakers = self.xml_extractor.extract_speakers_from_zaak(zaak_match.xml_zaak)
                        direct_speaker_matches = self._match_speakers(direct_zaak_speakers, [])
                        
                        # Create direct speaker-zaak connections
                        for direct_speaker_match in direct_speaker_matches:
                            if direct_speaker_match.match_result.success:
                                direct_connection = SpeakerZaakConnection(
                                    speaker_match=direct_speaker_match,
                                    zaak_match=zaak_match,
                                    activity_id=api_activity_id,
                                    activity_title=xml_activity.titel,
                                    context=f"Directly linked to {zaak_match.xml_zaak.titel or zaak_match.xml_zaak.dossiernummer}",
                                    speech_preview=direct_speaker_match.xml_speaker.speech_text[:100],
                                    connection_type="direct_zaak_link"
                                )
                                speaker_zaak_connections.append(direct_connection)
                
                print(f"  📋 Activity: {xml_activity.titel[:30]}... → Speakers: {len(speaker_matches)}, Zaken: {len(zaak_matches)} (XML: {len(xml_zaak_matches)}, Agendapunt: {len(agendapunt_zaak_matches)})")
                
                # Extract voting events and analyze
                xml_voting_events = self.xml_extractor.extract_voting_from_activity(xml_activity)
                if xml_voting_events:
                    activity_voting_analyses = self.voting_analyzer.analyze_voting_in_activity(
                        xml_voting_events, zaak_matches, api_activity_id
                    )
                    voting_analyses.extend(activity_voting_analyses)
                
                # Detect interruptions
                if self.config.analysis.detect_fragment_interruptions or self.config.analysis.detect_sequential_interruptions:
                    activity_interruptions = self.interruption_analyzer.detect_interruptions_in_activity(
                        xml_activity, speaker_matches, zaak_matches, api_activity_id
                    )
                    interruption_events.extend(activity_interruptions)
                
            # Step 5: Comprehensive analysis
            interruption_analysis = None
            if interruption_events and self.config.analysis.detect_fragment_interruptions:
                interruption_analysis = self.interruption_analyzer.analyze_interruption_patterns(interruption_events)
            
            voting_pattern_analysis = None
            if voting_analyses and self.config.analysis.analyze_fractie_voting:
                voting_pattern_analysis = self.voting_analyzer.analyze_voting_patterns(voting_analyses)
            
            # Step 6: Calculate statistics
            processing_time = time.time() - start_time
            statistics = ProcessingStatistics(
                xml_activities_total=len(xml_activities),
                xml_activities_matched=sum(1 for m in activity_matches if m.match_result.success),
                xml_speakers_total=len(all_speaker_matches),
                xml_speakers_matched=sum(1 for m in all_speaker_matches if m.match_result.success),
                xml_zaken_total=len(all_zaak_matches),
                xml_zaken_matched=sum(1 for m in all_zaak_matches if m.match_result.success),
                speaker_zaak_connections=len(speaker_zaak_connections),
                interruption_events=len(interruption_events),
                voting_events=len(voting_analyses),
                processing_time_seconds=processing_time
            )
            
            # Create result
            result = VlosProcessingResult(
                xml_vergadering=xml_vergadering,
                canonical_api_vergadering_id=canonical_vergadering.id,
                activity_matches=activity_matches,
                speaker_matches=all_speaker_matches,
                zaak_matches=all_zaak_matches,
                speaker_zaak_connections=speaker_zaak_connections,
                interruption_events=interruption_events,
                voting_analyses=voting_analyses,
                interruption_analysis=interruption_analysis,
                voting_pattern_analysis=voting_pattern_analysis,
                statistics=statistics,
                success=True
            )
            
            self._print_processing_summary(result)
            return result
            
        except Exception as e:
            processing_time = time.time() - start_time
            print(f"❌ Error in VLOS processing: {e}")
            
            # Try to extract basic vergadering info for error result
            try:
                xml_vergadering = self.xml_extractor.extract_vergadering(xml_content)
            except:
                xml_vergadering = None
            
            return VlosProcessingResult(
                xml_vergadering=xml_vergadering,
                canonical_api_vergadering_id="",
                success=False,
                error_messages=[str(e)],
                statistics=ProcessingStatistics(
                    xml_activities_total=0, xml_activities_matched=0,
                    xml_speakers_total=0, xml_speakers_matched=0,
                    xml_zaken_total=0, xml_zaken_matched=0,
                    speaker_zaak_connections=0, interruption_events=0,
                    voting_events=0, processing_time_seconds=processing_time
                )
            )
    
    def _match_speakers(self, xml_speakers: List, actor_persons: List = None) -> List[SpeakerMatch]:
        """Match XML speakers to API Persoon entities"""
        speaker_matches = []
        
        for xml_speaker in xml_speakers:
            # Get candidate personen
            persoon = self.api_extractor.find_persoon_by_name(
                xml_speaker.voornaam, xml_speaker.achternaam, actor_persons
            )
            
            if persoon:
                # Create match using name matcher
                candidate_personen = [persoon]
                speaker_match = NameMatcher.match_speaker(xml_speaker, candidate_personen, self.config)
            else:
                # No match found
                from ..models import MatchResult, MatchType
                match_result = MatchResult(
                    success=False,
                    match_type=MatchType.NO_MATCH,
                    score=0.0,
                    matched_entity=None,
                    reasons=["No Persoon found"]
                )
                speaker_match = SpeakerMatch(xml_speaker=xml_speaker, match_result=match_result)
            
            speaker_matches.append(speaker_match)
        
        return speaker_matches
    
    def _match_zaken(self, xml_zaken: List) -> List[ZaakMatch]:
        """Match XML zaken to API entities with fallback logic"""
        zaak_matches = []
        
        for xml_zaak in xml_zaken:
            # Use API extractor's enhanced matching
            match_result_dict = self.api_extractor.find_zaak_with_fallback(
                xml_zaak.dossiernummer, xml_zaak.stuknummer
            )
            
            # Convert to our match result format
            from ..models import MatchResult, MatchType
            
            if match_result_dict['success']:
                if match_result_dict['match_type'] == 'zaak':
                    match_type = MatchType.EXACT
                    matched_entity = match_result_dict['zaak']
                    zaak_id = matched_entity.id
                    zaak_type = 'zaak'
                else:
                    match_type = MatchType.FALLBACK
                    matched_entity = match_result_dict['dossier']
                    zaak_id = None
                    zaak_type = 'dossier'
                
                match_result = MatchResult(
                    success=True,
                    match_type=match_type,
                    score=100.0 if match_type == MatchType.EXACT else 75.0,
                    matched_entity=matched_entity,
                    fallback_entity=match_result_dict.get('document'),
                    reasons=[f"Found {match_result_dict['match_type']}"]
                )
                
                zaak_match = ZaakMatch(
                    xml_zaak=xml_zaak,
                    match_result=match_result,
                    zaak_id=zaak_id,
                    dossier_id=matched_entity.id if zaak_type == 'dossier' else None,
                    document_id=match_result_dict.get('document').id if match_result_dict.get('document') else None,
                    zaak_type=zaak_type
                )
            else:
                match_result = MatchResult(
                    success=False,
                    match_type=MatchType.NO_MATCH,
                    score=0.0,
                    matched_entity=None,
                    reasons=["No matching Zaak or Dossier found"]
                )
                
                zaak_match = ZaakMatch(xml_zaak=xml_zaak, match_result=match_result)
            
            zaak_matches.append(zaak_match)
        
        return zaak_matches
    
    def _create_speaker_zaak_connections(self, speaker_matches: List[SpeakerMatch], 
                                       zaak_matches: List[ZaakMatch],
                                       activity_id: str, activity_title: str) -> List[SpeakerZaakConnection]:
        """Create connections between speakers and zaken within an activity"""
        
        if not self.config.analysis.build_speaker_zaak_networks:
            return []
        
        connections = []
        
        for speaker_match in speaker_matches:
            if not speaker_match.match_result.success:
                continue
            
            for zaak_match in zaak_matches:
                if not zaak_match.match_result.success:
                    continue
                
                connection = SpeakerZaakConnection(
                    speaker_match=speaker_match,
                    zaak_match=zaak_match,
                    activity_id=activity_id,
                    activity_title=activity_title,
                    context=f"Spoke in activity about {zaak_match.xml_zaak.titel or zaak_match.xml_zaak.dossiernummer}",
                    speech_preview=speaker_match.xml_speaker.speech_text[:100],
                    connection_type="activity_based"
                )
                connections.append(connection)
        
        return connections

    def _create_zaak_match_from_agendapunt(self, agendapunt):
        """Create a ZaakMatch from an Agendapunt's connected Zaak"""
        if not hasattr(agendapunt, 'zaken') or not agendapunt.zaken:
            return None
            
        from ..models import ZaakMatch, XmlZaak, MatchResult, MatchType
        
        # Create ZaakMatch objects for each connected zaak
        zaak_matches = []
        for api_zaak in agendapunt.zaken:
            try:
                # Use the correct property names from the Zaak API
                dossiernummer = ''
                stuknummer = ''
                titel = ''
                
                # Check various possible attribute names
                if hasattr(api_zaak, 'nummer'):
                    dossiernummer = str(api_zaak.nummer)
                elif hasattr(api_zaak, 'dossiernummer'):
                    dossiernummer = str(api_zaak.dossiernummer)
                
                if hasattr(api_zaak, 'volgnummer'):
                    stuknummer = str(api_zaak.volgnummer)
                elif hasattr(api_zaak, 'stuknummer'):
                    stuknummer = str(api_zaak.stuknummer)
                
                if hasattr(api_zaak, 'onderwerp'):
                    titel = str(api_zaak.onderwerp)
                elif hasattr(api_zaak, 'titel'):
                    titel = str(api_zaak.titel)
                
                # Create a synthetic XmlZaak from the API Zaak
                xml_zaak = XmlZaak(
                    dossiernummer=dossiernummer,
                    stuknummer=stuknummer,
                    titel=titel,
                    raw_xml=None  # No XML element for API-derived zaken
                )
                
                match_result = MatchResult(
                    success=True,
                    match_type=MatchType.EXACT,
                    score=100.0,
                    matched_entity=api_zaak,
                    reasons=["Found via Agendapunt connection"]
                )
                
                zaak_match = ZaakMatch(
                    xml_zaak=xml_zaak,
                    match_result=match_result,
                    zaak_id=api_zaak.id,
                    zaak_type='zaak'
                )
                zaak_matches.append(zaak_match)
                
            except Exception as e:
                print(f"      ❌ Error creating ZaakMatch from Agendapunt: {e}")
                continue
        
        return zaak_matches
    
    def _create_error_result(self, xml_vergadering: Optional[XmlVergadering], error_message: str) -> VlosProcessingResult:
        """Create an error result"""
        return VlosProcessingResult(
            xml_vergadering=xml_vergadering,
            canonical_api_vergadering_id="",
            success=False,
            error_messages=[error_message],
            statistics=ProcessingStatistics(
                xml_activities_total=0, xml_activities_matched=0,
                xml_speakers_total=0, xml_speakers_matched=0,
                xml_zaken_total=0, xml_zaken_matched=0,
                speaker_zaak_connections=0, interruption_events=0,
                voting_events=0, processing_time_seconds=0.0
            )
        )
    
    def _print_processing_summary(self, result: VlosProcessingResult):
        """Print a comprehensive processing summary"""
        stats = result.statistics
        
        print("\n" + "="*80)
        print("🎯 VLOS PROCESSING COMPLETE")
        print("="*80)
        
        print(f"📊 Match Rates:")
        print(f"  🎯 Activities: {stats.xml_activities_matched}/{stats.xml_activities_total} ({stats.activity_match_rate:.1f}%)")
        print(f"  👥 Speakers: {stats.xml_speakers_matched}/{stats.xml_speakers_total} ({stats.speaker_match_rate:.1f}%)")
        print(f"  📋 Zaken: {stats.xml_zaken_matched}/{stats.xml_zaken_total} ({stats.zaak_match_rate:.1f}%)")
        
        print(f"\n🔗 Connections & Analysis:")
        print(f"  🤝 Speaker-Zaak connections: {stats.speaker_zaak_connections}")
        print(f"  🗣️ Interruption events: {stats.interruption_events}")
        print(f"  🗳️ Voting events: {stats.voting_events}")
        
        print(f"\n⏱️ Processing time: {stats.processing_time_seconds:.2f} seconds")
        
        if result.interruption_analysis:
            print(f"\n📈 Interruption Analysis:")
            print(f"  Most frequent interrupters: {len(result.interruption_analysis.most_frequent_interrupters)}")
            print(f"  Topics causing interruptions: {len(result.interruption_analysis.topics_causing_interruptions)}")
        
        if result.voting_pattern_analysis:
            print(f"\n🗳️ Voting Analysis:")
            print(f"  Total voting events: {result.voting_pattern_analysis.total_voting_events}")
            print(f"  Controversial topics: {len(result.voting_pattern_analysis.most_controversial_topics)}")
            print(f"  Unanimous topics: {len(result.voting_pattern_analysis.unanimous_topics)}")
        
        print("="*80) 