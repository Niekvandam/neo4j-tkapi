"""
VLOS Configuration System

Centralized configuration for all VLOS processing components including
scoring parameters, thresholds, and processing options.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import timedelta


@dataclass
class MatchingConfig:
    """Configuration for entity matching algorithms"""
    
    # Activity Matching Scores
    score_time_start_proximity: float = 3.0
    score_time_overlap_only: float = 1.5
    score_soort_exact: float = 2.0
    score_soort_partial_xml_in_api: float = 2.0
    score_soort_partial_api_in_xml: float = 1.5
    score_onderwerp_exact: float = 4.0
    score_onderwerp_fuzzy_high: float = 2.5
    score_onderwerp_fuzzy_medium: float = 2.0
    score_titel_exact_vs_api_onderwerp: float = 1.5
    score_titel_fuzzy_high_vs_api_onderwerp: float = 1.25
    score_titel_fuzzy_medium_vs_api_onderwerp: float = 0.5
    
    # Matching Thresholds
    min_match_score_for_activiteit: float = 3.0
    time_start_proximity_tolerance_seconds: int = 300  # 5 minutes
    time_general_overlap_buffer_seconds: int = 600     # 10 minutes
    
    # Fuzzy Matching Thresholds
    fuzzy_similarity_threshold_high: int = 85
    fuzzy_similarity_threshold_medium: int = 70
    fuzzy_firstname_threshold: int = 75
    fuzzy_surname_threshold: int = 80
    
    # Speaker Matching
    min_speaker_similarity_score: int = 60
    
    # Topic Normalization Prefixes
    common_topic_prefixes: List[str] = field(default_factory=lambda: [
        'tweeminutendebat', 'procedurevergadering', 'wetgevingsoverleg',
        'plenaire afronding', 'plenaire debat', 'debate over', 'debate',
        'aanvang', 'einde vergadering', 'regeling van werkzaamheden',
        'stemmingen', 'aanbieding', 'technische briefing'
    ])


@dataclass
class TimeConfig:
    """Configuration for time handling and timezone conversion"""
    
    local_timezone_offset_hours: int = 2  # CEST for summer samples
    api_time_buffer: timedelta = field(default_factory=lambda: timedelta(hours=1))
    vergadering_lookup_buffer: timedelta = field(default_factory=lambda: timedelta(days=1))


@dataclass
class ProcessingConfig:
    """Configuration for processing behavior and limits"""
    
    max_candidate_activities: int = 200
    max_candidate_vergaderingen: int = 5
    max_persoon_candidates: int = 100
    max_zaak_candidates: int = 10
    
    # Processing flags
    skip_procedural_activities: bool = True
    enable_interruption_analysis: bool = True
    enable_voting_analysis: bool = True
    enable_speaker_zaak_connections: bool = True
    
    # Procedural activities to skip
    procedural_activity_types: List[str] = field(default_factory=lambda: [
        'opening', 'sluiting', 'aanvang', 'einde vergadering'
    ])


@dataclass
class AnalysisConfig:
    """Configuration for parliamentary analysis features"""
    
    # Interruption Analysis
    detect_fragment_interruptions: bool = True
    detect_sequential_interruptions: bool = True
    detect_response_patterns: bool = True
    
    # Voting Analysis
    analyze_fractie_voting: bool = True
    analyze_consensus_patterns: bool = True
    analyze_controversial_topics: bool = True
    
    # Connection Analysis
    build_speaker_zaak_networks: bool = True
    track_topic_discussions: bool = True


@dataclass
class VlosConfig:
    """Main VLOS configuration containing all sub-configurations"""
    
    matching: MatchingConfig = field(default_factory=MatchingConfig)
    time: TimeConfig = field(default_factory=TimeConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    analysis: AnalysisConfig = field(default_factory=AnalysisConfig)
    
    # XML Namespace
    xml_namespace: Dict[str, str] = field(default_factory=lambda: {
        'vlos': 'http://www.tweedekamer.nl/ggm/vergaderverslag/v1.0'
    })
    
    @classmethod
    def default(cls) -> 'VlosConfig':
        """Create a default configuration"""
        return cls()
    
    @classmethod
    def for_testing(cls) -> 'VlosConfig':
        """Create a configuration optimized for testing"""
        config = cls()
        config.processing.max_candidate_activities = 50
        config.processing.max_persoon_candidates = 20
        return config
    
    @classmethod
    def for_production(cls) -> 'VlosConfig':
        """Create a configuration optimized for production"""
        config = cls()
        config.processing.max_candidate_activities = 500
        config.processing.max_persoon_candidates = 200
        return config 