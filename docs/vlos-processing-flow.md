# VLOS Processing Flow - Parliamentary Session Analysis

## Overview

This document outlines the comprehensive step-by-step process for analyzing parliamentary sessions from VLOS XML files and linking them to TK API entities (Activiteiten, Personen, Zaken, Dossiers, etc.).

## System Architecture

```
VLOS XML Files → Processing Pipeline → TK API Matching → Neo4j Knowledge Graph
     ↓                    ↓                   ↓                    ↓
Parliamentary      Speaker & Topic      Entity Resolution    Relationship
Session Data       Extraction           & Validation        Networks
```

## Step-by-Step Processing Flow

### 1. Setup and Configuration

**Purpose**: Initialize the processing environment and set matching parameters

**Key Components**:
- Initialize TK API connection
- Set up scoring constants for matching algorithms
- Define fuzzy matching thresholds
- Configure time zone handling (LOCAL_TIMEZONE_OFFSET_HOURS = 2 for CEST)

**Configuration Constants**:
```python
# Scoring weights
SCORE_TIME_START_PROXIMITY = 3.0
SCORE_SOORT_EXACT = 2.0
SCORE_ONDERWERP_EXACT = 4.0
MIN_MATCH_SCORE_FOR_ACTIVITEIT = 3.0

# Fuzzy matching thresholds
FUZZY_SIMILARITY_THRESHOLD_HIGH = 85
FUZZY_SIMILARITY_THRESHOLD_MEDIUM = 70
```

### 2. File Discovery and Processing Loop

**Purpose**: Find and process all VLOS XML files in the repository

**Process**:
1. Scan for `sample_vlos_*.xml` files
2. Initialize global counters for tracking match rates
3. Set up data structures for connection tracking

**Tracking Data Structures**:
- `speaker_zaak_connections[]` - Speaker-topic relationships
- `activity_speakers{}` - Activity → speakers mapping
- `activity_zaken{}` - Activity → zaken/dossiers mapping
- `all_interruptions[]` - Parliamentary interruption events
- `all_voting_events[]` - Voting pattern data

### 3. XML Vergadering Extraction

**Purpose**: Parse XML file and extract basic parliamentary session information

**Process**:
1. Parse XML using ElementTree
2. Extract vergadering metadata:
   - `soort` (type: plenair/commissie)
   - `titel` (session title)
   - `vergaderingnummer` (session number)
   - `datum` (session date)

**Example XML Structure**:
```xml
<vlos:vergadering soort="plenair">
    <vlos:titel>Vergadering van de Tweede Kamer</vlos:titel>
    <vlos:vergaderingnummer>95</vlos:vergaderingnummer>
    <vlos:datum>2024-01-15T09:00:00</vlos:datum>
    ...
</vlos:vergadering>
```

### 4. TK API Vergadering Matching

**Purpose**: Find the corresponding Vergadering in the TK API

**Process**:
1. Convert XML date to UTC timezone
2. Create date range filter (±1 day buffer)
3. Apply additional filters:
   - Vergadering soort (if specified)
   - Vergadering nummer (if specified)
4. Retrieve candidate Vergadering objects
5. Select canonical Vergadering (first match)

**API Query Example**:
```python
v_filter = Vergadering.create_filter()
v_filter.filter_date_range(begin_datetime=utc_start, end_datetime=utc_end)
v_filter.filter_soort(VergaderingSoort.PLENAIR)
```

### 5. Candidate API Activiteiten Retrieval

**Purpose**: Get all potential Activiteit matches from TK API

**Process**:
1. Create time-based Activiteit filter
2. Use canonical Vergadering timeframe ± 1 hour buffer
3. Convert to UTC before API call
4. Retrieve up to 200 candidate Activiteit objects

**Time Buffer Logic**:
- Accounts for scheduling variations
- Prevents missing activities due to slight time differences
- Ensures comprehensive coverage of the session period

### 6. XML Activiteit Processing Loop

**Purpose**: Process each activity within the parliamentary session

**Process**:
1. Skip procedural activities (opening, sluiting)
2. Extract activity metadata:
   - `objectid` (unique identifier)
   - `soort` (activity type)
   - `titel` (activity title)
   - `onderwerp` (subject matter)
   - Start/end times
3. Initialize tracking structures for this activity

**Filtering Logic**:
```python
# Skip procedural activities
if (xml_soort in ['opening', 'sluiting'] or 
    'opening' in xml_titel or 'sluiting' in xml_titel):
    continue
```

### 7. Activity Matching Algorithm

**Purpose**: Match XML activities to TK API Activiteit objects using sophisticated scoring

**Scoring Components**:

#### A. Time Proximity Scoring (Weight: 40%)
- **Start time proximity**: ±5 minutes tolerance
- **Time overlap**: Check for temporal overlap with buffer
- **Fallback to vergadering times**: If XML lacks explicit times

#### B. Soort (Type) Matching (Weight: 30%)
- **Exact match**: Full score for identical types
- **Partial match**: Substring matching in both directions
- **Alias matching**: Handle synonyms (e.g., 'opening' → 'aanvang')

#### C. Onderwerp/Titel Similarity (Weight: 30%)
- **Topic normalization**: Strip common prefixes
- **Exact match**: Highest score for identical topics
- **Fuzzy matching**: High/medium thresholds for similar content

**Matching Decision**:
```python
# Accept match if score exceeds threshold
if best_score >= MIN_MATCH_SCORE_FOR_ACTIVITEIT:
    accept_match = True
# Or if significantly better than runner-up
elif best_score - runner_up_score >= 1.0 and best_score >= 1.0:
    accept_match = True
```

### 8. Speaker Processing and Matching

**Purpose**: Extract speakers from XML and match to TK API Persoon objects

**Process**:
1. **Fragment Processing**:
   - Find all `draadboekfragment` elements
   - Extract speech text using `collapse_text()`
   - Process each `spreker` element within fragments

2. **Speaker Data Extraction**:
   - `voornaam` (first name)
   - `verslagnaam` or `achternaam` (last name)
   - `fractie` (political party)

3. **Matching Strategy**:
   - **Priority 1**: Match against activity actors (if API activity matched)
   - **Priority 2**: General Persoon search across all TK API data
   - **Enhanced surname matching**: Include tussenvoegsel handling

4. **Similarity Calculation**:
   - Surname matching (exact or fuzzy)
   - First name boost (roepnaam/voornaam)
   - Minimum similarity threshold: 60%

**Speaker Connection Tracking**:
```python
speaker_info = {
    'persoon': matched_persoon,
    'name': full_name,
    'speech_text': speech_text[:200],  # Context
}
activity_speakers[api_activity_id].append(speaker_info)
```

### 9. Zaak/Dossier Processing with Fallback Logic

**Purpose**: Link parliamentary discussions to legislative items

**Enhanced Matching Strategy**:

#### A. XML Zaak Element Processing
1. Extract zaak metadata:
   - `dossiernummer` (dossier number)
   - `stuknummer` (document number)
   - `titel` (zaak title)

#### B. Multi-tier Matching Logic
1. **Tier 1 - Specific Zaak Match**:
   - Try to find exact Zaak using dossier + stuk numbers
   - Use TK API filters: `filter_kamerstukdossier()`, `filter_document()`

2. **Tier 2 - Dossier Fallback**:
   - If no specific Zaak found, look for parent Dossier
   - Parse dossier code (e.g., "36725-VI" → nummer: 36725, toevoeging: "VI")
   - Also attempt to find related Document within dossier

3. **Tier 3 - Document Matching**:
   - Match individual documents by volgnummer
   - Link documents to their parent dossiers

**Fallback Result Structure**:
```python
{
    'zaak': zaak_object,          # If specific zaak found
    'dossier': dossier_object,    # If fallback to dossier
    'document': document_object,  # If document also found
    'match_type': 'zaak|dossier_fallback|no_match',
    'success': boolean
}
```

### 10. Speaker-Zaak Connection Network Creation

**Purpose**: Build relationship network between speakers and legislative topics

**Connection Types**:
1. **Fragment-based connections**: Speaker mentioned zaak in speech
2. **Direct zaak-speaker links**: Explicit speaker-zaak relationships in XML
3. **Activity-based connections**: Speakers and zaken within same activity

**Connection Data Structure**:
```python
{
    'persoon': persoon_object,
    'persoon_name': full_name,
    'zaak_object': zaak_or_dossier,
    'zaak_type': 'zaak|dossier',
    'activity_id': api_activity_id,
    'context': description,
    'speech_preview': text_excerpt
}
```

### 11. Advanced Parliamentary Analysis

#### A. Interruption Detection and Analysis

**Purpose**: Identify and analyze speaker interruption patterns

**Detection Methods**:
1. **Fragment interruptions**: Multiple speakers in same fragment
2. **Sequential interruptions**: A→B→A speaker patterns
3. **Response tracking**: Interruption followed by response

**Analysis Outputs**:
- Most frequent interrupters
- Most interrupted speakers
- Topics causing interruptions
- Interruption pair relationships

#### B. Voting Pattern Analysis

**Purpose**: Analyze fractie voting behavior and consensus patterns

**Process**:
1. **Vote Extraction**:
   - Find `activiteititem[@soort="Besluit"]` elements
   - Extract individual fractie votes
   - Normalize vote values (voor/tegen/onthouding)

2. **Pattern Analysis**:
   - Fractie voting behavior across topics
   - Consensus levels for different topics
   - Controversial vs. unanimous topics
   - Vote distribution patterns

**Voting Event Structure**:
```python
{
    'type': 'fractie_voting',
    'titel': besluit_title,
    'fractie_votes': [{'fractie': name, 'vote': value}],
    'consensus_percentage': percentage,
    'topics_discussed': topic_list
}
```

### 12. Comprehensive Reporting and Analysis

**Purpose**: Generate detailed reports and statistics

**Report Components**:

#### A. Match Rate Analysis
- Activity matching: XML activities → TK API Activiteit
- Speaker matching: VLOS speakers → TK API Persoon
- Zaak matching: XML zaken → TK API Zaak/Dossier
- Document matching: stuknummers → TK API Document

#### B. Connection Network Analysis
- Speaker-zaak relationships
- Activity-speaker mappings
- Zaak-activity relationships
- Cross-entity connection counts

#### C. Parliamentary Behavior Analysis
- Interruption patterns and key players
- Voting behavior and political alignment
- Topic-based interaction patterns
- Consensus vs. controversy analysis

#### D. Detailed Examples
- Speaker-zaak connection examples
- Interruption event details
- Voting pattern examples
- Match quality demonstrations

### 13. Data Quality and Validation

**Purpose**: Ensure high-quality matches and identify potential issues

**Quality Measures**:
1. **Scoring transparency**: Show match scores and reasoning
2. **Fallback tracking**: Distinguish exact matches from fallbacks
3. **Unmatched item reporting**: List items that couldn't be matched
4. **Match confidence levels**: Provide confidence scores for matches

**Error Handling**:
- Graceful degradation when API calls fail
- Fallback strategies for missing data
- Detailed logging of matching decisions
- Validation of extracted data quality

## Key Benefits of This Approach

1. **Comprehensive Entity Linking**: Connects parliamentary discourse to legislative items, speakers, and procedural elements
2. **Advanced Behavioral Analysis**: Identifies interruption patterns, voting behavior, and political dynamics
3. **Robust Matching**: Uses multi-tier fallback strategies to maximize successful matches
4. **Quality Assurance**: Provides transparency in matching decisions and quality metrics
5. **Scalable Architecture**: Can process multiple sessions and build comprehensive knowledge graphs
6. **Rich Context Preservation**: Maintains speech context and interaction patterns for deeper analysis

## Output Data Structure

The system produces a rich knowledge graph with the following entity types and relationships:

### Entities
- **Vergadering** (Parliamentary Session)
- **Activiteit** (Activity within session)
- **Persoon** (Speaker/Politician)
- **Zaak** (Legislative case)
- **Dossier** (Legislative dossier)
- **Document** (Legislative document)
- **VlosSpeaker** (Speaker mention in VLOS)
- **VotingEvent** (Voting occurrence)
- **InterruptionEvent** (Interruption occurrence)

### Relationships
- **SPOKE_IN** (Persoon → Activiteit)
- **DISCUSSED** (Persoon → Zaak/Dossier)
- **HAS_ACTIVITEIT** (Vergadering → Activiteit)
- **MATCHES_TO** (VlosSpeaker → Persoon)
- **INTERRUPTED** (Persoon → Persoon)
- **VOTED_ON** (Persoon → Zaak)
- **BELONGS_TO** (Document → Dossier)

This comprehensive approach enables sophisticated analysis of parliamentary behavior, legislative processes, and political dynamics through structured data and relationship networks. 