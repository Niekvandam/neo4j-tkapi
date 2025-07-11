"""
VLOS Data Models

Comprehensive data models representing parliamentary session data and analysis results.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Union
from datetime import datetime
from enum import Enum


class MatchType(Enum):
    """Types of entity matches"""
    EXACT = "exact"
    FUZZY = "fuzzy"
    FALLBACK = "fallback"
    NO_MATCH = "no_match"


class InterruptionType(Enum):
    """Types of parliamentary interruptions"""
    FRAGMENT_INTERRUPTION = "fragment_interruption"
    SIMPLE_INTERRUPTION = "simple_interruption"
    INTERRUPTION_WITH_RESPONSE = "interruption_with_response"


@dataclass
class XmlVergadering:
    """Extracted vergadering data from XML"""
    object_id: str
    soort: str
    titel: str
    nummer: Optional[str]
    datum: datetime
    raw_xml: Any  # ET.Element


@dataclass
class XmlActivity:
    """Extracted activity data from XML"""
    object_id: str
    soort: str
    titel: str
    onderwerp: str
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    raw_xml: Any  # ET.Element


@dataclass
class XmlSpeaker:
    """Extracted speaker data from XML"""
    voornaam: str
    achternaam: str
    verslagnaam: Optional[str]
    fractie: Optional[str]
    speech_text: str
    fragment_id: str
    raw_xml: Any  # ET.Element


@dataclass
class XmlZaak:
    """Extracted zaak data from XML"""
    dossiernummer: str
    stuknummer: str
    titel: str
    raw_xml: Any  # ET.Element


@dataclass
class XmlVotingEvent:
    """Extracted voting event data from XML"""
    titel: str
    besluitvorm: str
    uitslag: str
    fractie_votes: List[Dict[str, str]]
    raw_xml: Any  # ET.Element


@dataclass
class MatchResult:
    """Result of an entity matching operation"""
    success: bool
    match_type: MatchType
    score: float
    matched_entity: Optional[Any]  # TK API entity
    fallback_entity: Optional[Any] = None  # Fallback entity if applicable
    reasons: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ActivityMatch:
    """Result of activity matching with comprehensive details"""
    xml_activity: XmlActivity
    match_result: MatchResult
    api_activity_id: Optional[str] = None
    potential_matches: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class SpeakerMatch:
    """Result of speaker matching"""
    xml_speaker: XmlSpeaker
    match_result: MatchResult
    persoon_id: Optional[str] = None
    persoon_name: Optional[str] = None


@dataclass
class ZaakMatch:
    """Result of zaak matching with fallback logic"""
    xml_zaak: XmlZaak
    match_result: MatchResult
    zaak_id: Optional[str] = None
    dossier_id: Optional[str] = None
    document_id: Optional[str] = None
    zaak_type: Optional[str] = None  # 'zaak', 'dossier', 'document'


@dataclass
class SpeakerZaakConnection:
    """Connection between a speaker and a zaak/topic"""
    speaker_match: SpeakerMatch
    zaak_match: ZaakMatch
    activity_id: str
    activity_title: str
    context: str
    speech_preview: str
    connection_type: str  # 'fragment_based', 'direct_link', 'activity_based'


@dataclass
class InterruptionEvent:
    """Parliamentary interruption event"""
    type: InterruptionType
    original_speaker: SpeakerMatch
    interrupting_speaker: SpeakerMatch
    activity_id: str
    fragment_id: str
    context: str
    speech_context: str
    responding_speaker: Optional[SpeakerMatch] = None
    topics_discussed: List[str] = field(default_factory=list)
    interruption_length: Optional[int] = None


@dataclass
class VotingAnalysis:
    """Analysis of voting patterns"""
    voting_event: XmlVotingEvent
    activity_id: str
    topics_discussed: List[str]
    vote_breakdown: Dict[str, List[str]]  # vote_type -> list of fracties
    consensus_level: float
    total_votes: int


@dataclass
class InterruptionAnalysis:
    """Comprehensive interruption pattern analysis"""
    total_interruptions: int
    interruption_types: Dict[str, int]
    most_frequent_interrupters: Dict[str, int]
    most_interrupted_speakers: Dict[str, int]
    interruption_pairs: Dict[str, Dict[str, Any]]
    topics_causing_interruptions: Dict[str, Dict[str, Any]]
    response_patterns: Dict[str, Dict[str, Any]]


@dataclass
class VotingPatternAnalysis:
    """Comprehensive voting pattern analysis"""
    total_voting_events: int
    total_individual_votes: int
    fractie_vote_counts: Dict[str, Dict[str, int]]
    fractie_alignment: Dict[str, Dict[str, float]]
    topic_vote_patterns: Dict[str, Dict[str, Any]]
    vote_type_distribution: Dict[str, int]
    most_controversial_topics: Dict[str, Dict[str, Any]]
    unanimous_topics: Dict[str, Dict[str, Any]]


@dataclass
class ProcessingStatistics:
    """Statistics from VLOS processing"""
    xml_activities_total: int
    xml_activities_matched: int
    xml_speakers_total: int
    xml_speakers_matched: int
    xml_zaken_total: int
    xml_zaken_matched: int
    speaker_zaak_connections: int
    interruption_events: int
    voting_events: int
    processing_time_seconds: float
    
    @property
    def activity_match_rate(self) -> float:
        return (self.xml_activities_matched / max(self.xml_activities_total, 1)) * 100
    
    @property
    def speaker_match_rate(self) -> float:
        return (self.xml_speakers_matched / max(self.xml_speakers_total, 1)) * 100
    
    @property
    def zaak_match_rate(self) -> float:
        return (self.xml_zaken_matched / max(self.xml_zaken_total, 1)) * 100


@dataclass
class VlosProcessingResult:
    """Complete result of VLOS processing pipeline"""
    
    # Input data
    xml_vergadering: XmlVergadering
    canonical_api_vergadering_id: str
    
    # Matching results
    activity_matches: List[ActivityMatch] = field(default_factory=list)
    speaker_matches: List[SpeakerMatch] = field(default_factory=list)
    zaak_matches: List[ZaakMatch] = field(default_factory=list)
    
    # Connection networks
    speaker_zaak_connections: List[SpeakerZaakConnection] = field(default_factory=list)
    
    # Analysis results
    interruption_events: List[InterruptionEvent] = field(default_factory=list)
    voting_analyses: List[VotingAnalysis] = field(default_factory=list)
    interruption_analysis: Optional[InterruptionAnalysis] = None
    voting_pattern_analysis: Optional[VotingPatternAnalysis] = None
    
    # Statistics
    statistics: Optional[ProcessingStatistics] = None
    
    # Metadata
    processing_timestamp: datetime = field(default_factory=datetime.now)
    success: bool = True
    error_messages: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list) 