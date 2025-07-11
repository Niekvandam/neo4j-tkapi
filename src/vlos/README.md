# VLOS Processing System V2

A modular, scalable system for processing parliamentary session data from VLOS XML files with comprehensive analysis capabilities.

## Overview

The new VLOS system replaces the deprecated monolithic loaders with a clean, modular architecture that provides:

- **Comprehensive Entity Matching**: Activities, speakers, zaken with sophisticated scoring
- **Advanced Parliamentary Analysis**: Interruption detection, voting patterns, consensus analysis  
- **Speaker-Topic Networks**: Rich connection analysis between speakers and legislative items
- **Robust Fallback Logic**: Multi-tier matching with graceful degradation
- **Configurable Processing**: YAML-based configuration with different deployment modes
- **Extensive Validation**: Quality metrics and transparent matching decisions

## Architecture

```
src/vlos/
├── config/           # Configuration system
├── extractors/       # Data extraction (XML, API)
├── transformers/     # Data normalization
├── matchers/         # Entity matching logic  
├── analyzers/        # Parliamentary analysis
├── loaders/          # Data persistence (future)
├── pipeline/         # Main orchestration
└── models.py         # Data models
```

## Quick Start

### Basic Usage

```python
from vlos import VlosPipeline, VlosConfig
from tkapi import TKApi

# Create pipeline with default config
config = VlosConfig.default()
api = TKApi()
pipeline = VlosPipeline(config, api)

# Process VLOS XML
with open('sample_vlos.xml', 'r') as f:
    xml_content = f.read()

result = pipeline.process_vlos_xml(xml_content)

# Check results
if result.success:
    print(f"✅ Processed {result.statistics.xml_activities_total} activities")
    print(f"📊 Match rates: {result.statistics.activity_match_rate:.1f}% activities")
    print(f"🔗 Created {result.statistics.speaker_zaak_connections} connections")
else:
    print(f"❌ Processing failed: {result.error_messages}")
```

### Using the New Loader

```python
from loaders.vlos_loader_v2 import load_vlos_with_pipeline

# Simple convenience function
result = load_vlos_with_pipeline(
    xml_content=xml_string,
    canonical_api_vergadering_id="some-vergadering-id"
)

if result['success']:
    print(f"🎯 Processing complete!")
    print(f"📊 Statistics: {result['statistics']}")
    print(f"📈 Match rates: {result['match_rates']}")
```

## Configuration

### Pre-defined Configurations

```python
from vlos import VlosConfig

# Default configuration (balanced)
config = VlosConfig.default()

# Testing configuration (faster, fewer candidates)
config = VlosConfig.for_testing()

# Production configuration (comprehensive, more candidates)  
config = VlosConfig.for_production()
```

### Custom Configuration

```python
from vlos.config import VlosConfig, MatchingConfig, ProcessingConfig

# Create custom matching config
matching = MatchingConfig()
matching.min_match_score_for_activiteit = 4.0  # Stricter matching
matching.fuzzy_similarity_threshold_high = 90   # Higher fuzzy threshold

# Create custom processing config
processing = ProcessingConfig()
processing.enable_interruption_analysis = True
processing.enable_voting_analysis = True
processing.max_candidate_activities = 300

# Combine into main config
config = VlosConfig(
    matching=matching,
    processing=processing
)
```

## Key Features

### 1. Sophisticated Activity Matching

The system uses a comprehensive scoring algorithm that considers:

- **Time Proximity** (40%): Start time alignment and temporal overlap
- **Type Matching** (30%): Exact and partial soort matching with aliases
- **Topic Similarity** (30%): Fuzzy matching of onderwerp and titel

```python
# Scoring is transparent and configurable
activity_match = pipeline.activity_matcher.match_activity(xml_activity, api_activities, vergadering)
print(f"Match score: {activity_match.match_result.score}")
print(f"Reasons: {activity_match.match_result.reasons}")
```

### 2. Enhanced Speaker Matching

Speaker matching includes:

- **Priority Matching**: Activity actors first, then general search
- **Enhanced Surnames**: Handles tussenvoegsel correctly
- **Fuzzy Names**: Configurable thresholds for first/last names
- **Quality Scores**: Transparent similarity calculations

### 3. Multi-tier Zaak Matching

Robust zaak matching with fallback logic:

1. **Tier 1**: Exact Zaak match using dossier + stuk numbers
2. **Tier 2**: Dossier fallback if no specific Zaak found  
3. **Tier 3**: Document matching within dossier

### 4. Parliamentary Analysis

#### Interruption Detection
```python
# Configure interruption analysis
config.analysis.detect_fragment_interruptions = True
config.analysis.detect_sequential_interruptions = True
config.analysis.detect_response_patterns = True

# Results include comprehensive analysis
if result.interruption_analysis:
    print(f"Most frequent interrupters: {result.interruption_analysis.most_frequent_interrupters}")
    print(f"Topics causing interruptions: {result.interruption_analysis.topics_causing_interruptions}")
```

#### Voting Pattern Analysis
```python
# Enable voting analysis
config.analysis.analyze_fractie_voting = True
config.analysis.analyze_consensus_patterns = True

# Results include political dynamics
if result.voting_pattern_analysis:
    print(f"Controversial topics: {result.voting_pattern_analysis.most_controversial_topics}")
    print(f"Unanimous decisions: {result.voting_pattern_analysis.unanimous_topics}")
```

### 5. Connection Networks

The system builds rich networks showing:

- **Speaker-Zaak connections**: Who spoke about what topics
- **Activity relationships**: Cross-activity speaker patterns
- **Topic discussions**: Which topics generate most debate

## Data Models

### Processing Result
```python
@dataclass
class VlosProcessingResult:
    xml_vergadering: XmlVergadering
    canonical_api_vergadering_id: str
    activity_matches: List[ActivityMatch]
    speaker_matches: List[SpeakerMatch]  
    zaak_matches: List[ZaakMatch]
    speaker_zaak_connections: List[SpeakerZaakConnection]
    interruption_events: List[InterruptionEvent]
    voting_analyses: List[VotingAnalysis]
    statistics: ProcessingStatistics
    success: bool
```

### Match Results
All matching operations return structured results with:
- Success/failure status
- Match type (exact, fuzzy, fallback, no_match)
- Confidence scores
- Detailed reasons
- Metadata for debugging

## Performance

### Benchmarks
- **Small XML** (5 activities): ~2-3 seconds
- **Medium XML** (20 activities): ~8-12 seconds  
- **Large XML** (50+ activities): ~20-30 seconds

### Optimization
- Configurable candidate limits
- Efficient API queries with filtering
- Parallel processing potential (future)
- Caching support (future)

## Migration from Deprecated System

### Old System
```python
# Deprecated approach
from loaders.enhanced_vlos_verslag_loader import load_enhanced_vlos_verslag
counts = load_enhanced_vlos_verslag(driver, xml_str, vergadering_id, verslag_id)
```

### New System  
```python
# New modular approach
from vlos import VlosPipeline, VlosConfig
pipeline = VlosPipeline(VlosConfig.for_production())
result = pipeline.process_vlos_xml(xml_content)
```

### Benefits of Migration
1. **Better Separation of Concerns**: Clear module boundaries
2. **Easier Testing**: Independent, testable components
3. **More Configurable**: Flexible configuration system
4. **Better Error Handling**: Graceful degradation and detailed reporting
5. **Richer Analysis**: Enhanced parliamentary behavior analysis
6. **Future-Proof**: Easy to extend with new features

## Extending the System

### Adding New Matchers
```python
from vlos.matchers import BaseMatcher

class CustomMatcher(BaseMatcher):
    def match(self, xml_entity, api_entities, config):
        # Custom matching logic
        return MatchResult(...)
```

### Adding New Analyzers
```python
from vlos.analyzers import BaseAnalyzer

class SentimentAnalyzer(BaseAnalyzer):
    def analyze(self, speaker_matches, config):
        # Sentiment analysis logic
        return SentimentAnalysis(...)
```

### Custom Pipeline Steps
```python
# Extend the pipeline
class CustomVlosPipeline(VlosPipeline):
    def process_vlos_xml(self, xml_content, api_verslag_id=None):
        result = super().process_vlos_xml(xml_content, api_verslag_id)
        
        # Add custom processing
        result.custom_analysis = self.custom_analyzer.analyze(result)
        
        return result
```

## Troubleshooting

### Common Issues

1. **Low Match Rates**: Adjust scoring thresholds in config
2. **Performance Issues**: Reduce candidate limits or enable caching
3. **Missing Analysis**: Check analysis configuration flags
4. **API Errors**: Verify TK API connectivity and rate limits

### Debug Mode
```python
# Enable detailed logging
config = VlosConfig.default()
config.debug_mode = True  # Future feature

# Examine match details
for activity_match in result.activity_matches:
    print(f"Activity: {activity_match.xml_activity.titel}")
    print(f"Score: {activity_match.match_result.score}")
    print(f"Reasons: {activity_match.match_result.reasons}")
    print(f"Potential matches: {len(activity_match.potential_matches)}")
```

## Future Enhancements

- **Caching System**: Redis-based caching for API results
- **Parallel Processing**: Multi-threading for large XML files
- **Real-time Processing**: Streaming XML processing
- **ML Enhancement**: Machine learning for improved matching
- **Visualization**: Interactive network analysis tools
- **API Integration**: REST API for external systems 