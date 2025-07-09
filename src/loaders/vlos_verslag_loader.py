"""
VLOS Verslag Loader - Processes VLOS XML verslag content with interface support
"""
import xml.etree.ElementTree as ET
import time
from typing import Optional, Dict, List, Any
from datetime import datetime

from utils.helpers import merge_node, merge_rel
from core.connection.neo4j_connection import Neo4jConnection

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry

# Import VLOS processors
from .processors.vlos_processor import (
    NS_VLOS, process_vlos_activity_element,
    process_vlos_speakers, process_vlos_zaken
)
from .processors.vlos_matching import get_candidate_api_activities
from .processors.vlos_speaker_matching import match_vlos_speakers_to_personen


class VlosVerslagLoader(BaseLoader):
    """Loader for VLOS Verslag XML processing with full interface support"""
    
    def __init__(self):
        super().__init__(
            name="vlos_verslag_loader",
            description="Processes VLOS XML verslag content and creates detailed report structure"
        )
        self._capabilities = [
            LoaderCapability.BATCH_PROCESSING,
            LoaderCapability.RELATIONSHIP_PROCESSING
        ]
    
    def validate_config(self, config: LoaderConfig) -> list[str]:
        """Validate configuration specific to VlosVerslagLoader"""
        errors = super().validate_config(config)
        
        # Add specific validation for VLOS loader
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
            
            # Use the existing function for actual loading
            load_vlos_verslag(conn.driver, xml_content, canonical_api_vergadering_id)
            
            # For now, we'll mark as successful if no exceptions occurred
            result.success = True
            result.execution_time_seconds = time.time() - start_time
            
        except Exception as e:
            result.error_messages.append(f"Loading failed: {str(e)}")
            result.execution_time_seconds = time.time() - start_time
        
        return result


# Register the loader
vlos_verslag_loader_instance = VlosVerslagLoader()
loader_registry.register(vlos_verslag_loader_instance)


def load_vlos_verslag(driver, xml_content: str, canonical_api_vergadering_id: str):
    """
    Main function to process VLOS XML content and create Neo4j structure.
    
    Args:
        driver: Neo4j driver instance
        xml_content: Raw XML content from VLOS
        canonical_api_vergadering_id: ID of the Vergadering from TK API
    """
    print(f"🔄 Processing VLOS XML for Vergadering {canonical_api_vergadering_id}")
    
    try:
        # Parse XML
        root = ET.fromstring(xml_content)
        
        # DEBUG: Print XML structure info
        print(f"🔍 DEBUG: Root element tag: {root.tag}")
        print(f"🔍 DEBUG: Root element attributes: {root.attrib}")
        print(f"🔍 DEBUG: Root element namespace: {root.tag.split('}')[0] if '}' in root.tag else 'No namespace'}")
        
        # Find all child elements to understand structure
        print(f"🔍 DEBUG: Direct children of root:")
        for child in root:
            print(f"  - {child.tag} (attrib: {child.attrib})")
        
        # Check for sections with different approaches
        sections_with_ns = root.findall('.//vlos:section', NS_VLOS)
        sections_without_ns = root.findall('.//section')
        
        print(f"🔍 DEBUG: Found {len(sections_with_ns)} sections with namespace")
        print(f"🔍 DEBUG: Found {len(sections_without_ns)} sections without namespace")
        
        with driver.session() as session:
            # Get the canonical Vergadering node
            canonical_vergadering_node = session.run(
                "MATCH (v:Vergadering {id: $id}) RETURN v",
                id=canonical_api_vergadering_id
            ).single()
            
            if not canonical_vergadering_node:
                print(f"❌ Canonical Vergadering {canonical_api_vergadering_id} not found in Neo4j")
                return
            
            canonical_vergadering_node = canonical_vergadering_node['v']
            
            # Get API activities for matching
            print(f"🔍 DEBUG: About to call get_candidate_api_activities...")
            try:
                api_activities = get_candidate_api_activities(session, canonical_vergadering_node)
                print(f"📊 Found {len(api_activities)} API activities for matching")
            except Exception as e:
                print(f"❌ ERROR in get_candidate_api_activities: {e}")
                api_activities = []
            
            # Process main document structure
            _process_vlos_document_structure(session, root, canonical_vergadering_node, api_activities)
            
            # Match VLOS speakers to Persoon nodes
            print("🔗 Matching VLOS speakers to Persoon nodes...")
            matched_count = match_vlos_speakers_to_personen(session)
            print(f"✅ Matched {matched_count} VLOS speakers to Persoon nodes")
            
        print("✅ VLOS XML processing completed successfully")
        
    except ET.ParseError as e:
        print(f"❌ XML parsing error: {e}")
        raise
    except Exception as e:
        print(f"❌ Error processing VLOS XML: {e}")
        raise


def _analyze_xml_structure(element: ET.Element, max_depth: int = 2, current_depth: int = 0, prefix: str = ""):
    """Analyze and print XML structure for debugging"""
    if current_depth >= max_depth:
        return
    
    # Count children by tag
    child_counts = {}
    for child in element:
        tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
        child_counts[tag] = child_counts.get(tag, 0) + 1
    
    # Print structure
    if child_counts:
        print(f"{prefix}├─ {len(element)} children:")
        for tag, count in sorted(child_counts.items()):
            print(f"{prefix}│  └─ {tag}: {count}")
            
            # Show first child of each type for deeper analysis
            if current_depth < max_depth - 1:
                first_child = element.find(f".//*[local-name()='{tag}']") or element.find(f".//vlos:{tag}", NS_VLOS)
                if first_child is not None:
                    _analyze_xml_structure(first_child, max_depth, current_depth + 1, prefix + "│     ")
    else:
        text_content = (element.text or "").strip()
        if text_content:
            print(f"{prefix}└─ Text content: {text_content[:50]}{'...' if len(text_content) > 50 else ''}")


def _process_vlos_document_structure(session, root: ET.Element, canonical_vergadering_node, api_activities):
    """Process the main VLOS document structure"""
    
    # Create main VLOS document node
    doc_id = f"vlos_doc_{canonical_vergadering_node['id']}"
    doc_props = {
        'id': doc_id,
        'vergadering_id': canonical_vergadering_node['id'],
        'source': 'vlos_xml',
        'processed_at': str(time.time())
    }
    session.execute_write(merge_node, 'VlosDocument', 'id', doc_props)
    
    # Link to Vergadering
    session.execute_write(merge_rel, 'Vergadering', 'id', canonical_vergadering_node['id'],
                          'VlosDocument', 'id', doc_id, 'HAS_VLOS_DOCUMENT')
    
    # Process the actual XML structure - look for vergadering elements (the real structure)
    sections_processed = 0
    activities_processed = 0
    speakers_processed = 0
    
    # Look for vergadering elements (the actual structure)
    for vergadering_elem in root.findall('.//vlos:vergadering', NS_VLOS):
        print(f"🔄 Processing vergadering element: {vergadering_elem.get('objectid', 'Unknown')}")
        
        # Create vergadering section
        section_id = _create_vlos_vergadering_section(session, vergadering_elem, doc_id, canonical_vergadering_node)
        if section_id:
            sections_processed += 1
            
            # Process all activities within this vergadering
            activity_count = _process_vlos_activities(session, vergadering_elem, section_id, canonical_vergadering_node, api_activities)
            activities_processed += activity_count
            
            # Process all speakers within this vergadering
            speaker_count = _process_vlos_speakers_detailed(session, vergadering_elem, section_id)
            speakers_processed += speaker_count
            
            # Process any agendapunten
            _process_vlos_agendapunten(session, vergadering_elem, section_id, canonical_vergadering_node)
    
    print(f"📊 Processed {sections_processed} VLOS sections, {activities_processed} activities, {speakers_processed} speakers")


def _create_vlos_vergadering_section(session, vergadering_elem: ET.Element, parent_id: str, 
                                    canonical_vergadering_node) -> Optional[str]:
    """Create a VLOS section for a vergadering element"""
    
    vergadering_id = vergadering_elem.get('objectid', f"vlos_vergadering_{hash(ET.tostring(vergadering_elem))}")
    vergadering_soort = vergadering_elem.get('soort', 'Unknown')
    vergadering_kamer = vergadering_elem.get('kamer', 'Unknown')
    
    # Get the title from the titel element
    titel_elem = vergadering_elem.find('.//vlos:titel', NS_VLOS)
    title = titel_elem.text if titel_elem is not None else f"{vergadering_soort} vergadering"
    
    # Create vergadering section node
    section_props = {
        'id': vergadering_id,
        'title': title,
        'type': 'vergadering',
        'soort': vergadering_soort,
        'kamer': vergadering_kamer,
        'source': 'vlos_xml'
    }
    session.execute_write(merge_node, 'VlosSection', 'id', section_props)
    
    # Link to parent document
    session.execute_write(merge_rel, 'VlosDocument', 'id', parent_id,
                          'VlosSection', 'id', vergadering_id, 'CONTAINS_SECTION')
    
    return vergadering_id


def _process_vlos_activities(session, vergadering_elem: ET.Element, section_id: str, 
                           canonical_vergadering_node, api_activities) -> int:
    """Process all activities within a vergadering element"""
    activities_processed = 0
    
    # Find all activiteit elements (this is the actual structure)
    for activiteit_elem in vergadering_elem.findall('.//vlos:activiteit', NS_VLOS):
        activity_id = _process_single_vlos_activiteit(session, activiteit_elem, section_id, canonical_vergadering_node, api_activities)
        if activity_id:
            activities_processed += 1
    
    return activities_processed


def _process_single_vlos_activiteit(session, activiteit_elem: ET.Element, section_id: str,
                                   canonical_vergadering_node, api_activities) -> Optional[str]:
    """Process a single activiteit element"""
    
    activity_id = activiteit_elem.get('objectid', f"vlos_activiteit_{hash(ET.tostring(activiteit_elem))}")
    activity_soort = activiteit_elem.get('soort', 'Unknown')
    
    # Get timing information
    aanvangstijd_elem = activiteit_elem.find('.//vlos:aanvangstijd', NS_VLOS)
    eindtijd_elem = activiteit_elem.find('.//vlos:eindtijd', NS_VLOS)
    
    start_time = parse_vlos_xml_datetime(aanvangstijd_elem.text) if aanvangstijd_elem is not None else None
    end_time = parse_vlos_xml_datetime(eindtijd_elem.text) if eindtijd_elem is not None else None
    
    # Get title/onderwerp
    titel_elem = activiteit_elem.find('.//vlos:titel', NS_VLOS)
    title = titel_elem.text if titel_elem is not None else f"{activity_soort} activiteit"
    
    # Create VLOS activity node
    activity_props = {
        'id': activity_id,
        'title': title,
        'soort': activity_soort,
        'start_time': str(start_time) if start_time else None,
        'end_time': str(end_time) if end_time else None,
        'source': 'vlos_xml'
    }
    session.execute_write(merge_node, 'VlosActivity', 'id', activity_props)
    
    # Link to section
    session.execute_write(merge_rel, 'VlosSection', 'id', section_id,
                          'VlosActivity', 'id', activity_id, 'CONTAINS_ACTIVITY')
    
    # Link to Vergadering
    session.execute_write(merge_rel, 'Vergadering', 'id', canonical_vergadering_node['id'],
                          'VlosActivity', 'id', activity_id, 'HAS_VLOS_ACTIVITY')
    
    # Try to match with API activities using the sophisticated matching logic
    best_match, best_score = _find_best_api_activity_match(activity_props, api_activities)
    
    if best_match and best_score >= 4.0:  # Use the threshold from vlos_matching
        session.execute_write(merge_rel, 'VlosActivity', 'id', activity_id,
                              'Activiteit', 'id', best_match['id'], 'MATCHES_API_ACTIVITY')
        print(f"    🔗 Matched VLOS activity '{title}' to API activity {best_match['id']} (score: {best_score:.1f})")
    
    # Process sub-elements within this activity
    _process_activity_speakers(session, activiteit_elem, activity_id)
    _process_activity_agendapunten(session, activiteit_elem, activity_id)
    
    return activity_id


def _process_vlos_speakers_detailed(session, vergadering_elem: ET.Element, section_id: str) -> int:
    """Process all speakers within a vergadering element"""
    speakers_processed = 0
    
    # Find all spreker elements
    for spreker_elem in vergadering_elem.findall('.//vlos:spreker', NS_VLOS):
        speaker_id = _process_single_vlos_spreker(session, spreker_elem, section_id)
        if speaker_id:
            speakers_processed += 1
    
    return speakers_processed


def _process_single_vlos_spreker(session, spreker_elem: ET.Element, section_id: str) -> Optional[str]:
    """Process a single spreker element"""
    
    speaker_id = spreker_elem.get('objectid', f"vlos_spreker_{hash(ET.tostring(spreker_elem))}")
    speaker_soort = spreker_elem.get('soort', 'Unknown')
    
    # Extract speaker details
    aanhef_elem = spreker_elem.find('.//vlos:aanhef', NS_VLOS)
    voornaam_elem = spreker_elem.find('.//vlos:voornaam', NS_VLOS)
    achternaam_elem = spreker_elem.find('.//vlos:achternaam', NS_VLOS)
    functie_elem = spreker_elem.find('.//vlos:functie', NS_VLOS)
    fractie_elem = spreker_elem.find('.//vlos:fractie', NS_VLOS)
    
    aanhef = aanhef_elem.text if aanhef_elem is not None else ''
    voornaam = voornaam_elem.text if voornaam_elem is not None else ''
    achternaam = achternaam_elem.text if achternaam_elem is not None else ''
    functie = functie_elem.text if functie_elem is not None else ''
    fractie = fractie_elem.text if fractie_elem is not None else ''
    
    full_name = f"{aanhef} {voornaam} {achternaam}".strip()
    
    # Create speaker node
    speaker_props = {
        'id': speaker_id,
        'name': full_name,
        'voornaam': voornaam,
        'achternaam': achternaam,
        'aanhef': aanhef,
        'functie': functie,
        'fractie': fractie,
        'soort': speaker_soort,
        'source': 'vlos_xml'
    }
    session.execute_write(merge_node, 'VlosSpeaker', 'id', speaker_props)
    
    # Link to section
    session.execute_write(merge_rel, 'VlosSection', 'id', section_id,
                          'VlosSpeaker', 'id', speaker_id, 'HAS_SPEAKER')
    
    return speaker_id


def _process_vlos_agendapunten(session, vergadering_elem: ET.Element, section_id: str, canonical_vergadering_node):
    """Process agendapunten within a vergadering element"""
    
    for agendapunt_elem in vergadering_elem.findall('.//vlos:agendapunt', NS_VLOS):
        agendapunt_id = agendapunt_elem.get('objectid', f"vlos_agendapunt_{hash(ET.tostring(agendapunt_elem))}")
        
        # Get agendapunt details
        titel_elem = agendapunt_elem.find('.//vlos:titel', NS_VLOS)
        nummer_elem = agendapunt_elem.find('.//vlos:nummer', NS_VLOS)
        
        title = titel_elem.text if titel_elem is not None else 'Untitled Agendapunt'
        nummer = nummer_elem.text if nummer_elem is not None else ''
        
        # Create agendapunt node
        agendapunt_props = {
            'id': agendapunt_id,
            'titel': title,
            'nummer': nummer,
            'source': 'vlos_xml'
        }
        session.execute_write(merge_node, 'VlosAgendapunt', 'id', agendapunt_props)
        
        # Link to section
        session.execute_write(merge_rel, 'VlosSection', 'id', section_id,
                              'VlosAgendapunt', 'id', agendapunt_id, 'HAS_AGENDAPUNT')


def _process_woordvoerder_element(session, woordvoerder_elem: ET.Element, activity_id: str):
    """Process a complete woordvoerder element including speaker and speech content"""
    
    woordvoerder_id = woordvoerder_elem.get('objectid', f"vlos_woordvoerder_{hash(ET.tostring(woordvoerder_elem))}")
    
    # Get timing information
    markeertijdbegin_elem = woordvoerder_elem.find('.//vlos:markeertijdbegin', NS_VLOS)
    markeertijdeind_elem = woordvoerder_elem.find('.//vlos:markeertijdeind', NS_VLOS)
    
    start_time = markeertijdbegin_elem.text if markeertijdbegin_elem is not None else None
    end_time = markeertijdeind_elem.text if markeertijdeind_elem is not None else None
    
    # Get speaker information
    spreker_elem = woordvoerder_elem.find('.//vlos:spreker', NS_VLOS)
    speaker_id = None
    if spreker_elem is not None:
        speaker_id = _process_single_vlos_spreker(session, spreker_elem, activity_id)
    
    # Create woordvoerder node
    woordvoerder_props = {
        'id': woordvoerder_id,
        'start_time': start_time,
        'end_time': end_time,
        'is_voorzitter': woordvoerder_elem.find('.//vlos:isvoorzitter', NS_VLOS) is not None,
        'is_draad': woordvoerder_elem.find('.//vlos:isdraad', NS_VLOS) is not None,
        'source': 'vlos_xml'
    }
    session.execute_write(merge_node, 'VlosWoordvoerder', 'id', woordvoerder_props)
    
    # Link to activity
    session.execute_write(merge_rel, 'VlosActivity', 'id', activity_id,
                          'VlosWoordvoerder', 'id', woordvoerder_id, 'HAS_WOORDVOERDER')
    
    # Link to speaker if exists
    if speaker_id:
        session.execute_write(merge_rel, 'VlosWoordvoerder', 'id', woordvoerder_id,
                              'VlosSpeaker', 'id', speaker_id, 'SPOKEN_BY')
    
    # Process the main speech content (tekst elements)
    _process_tekst_elements(session, woordvoerder_elem, woordvoerder_id)
    
    # Process any interruptions (interrumpant elements)
    for interrumpant_elem in woordvoerder_elem.findall('.//vlos:interrumpant', NS_VLOS):
        _process_interrumpant_element(session, interrumpant_elem, woordvoerder_id)
    
    print(f"    📝 Processed woordvoerder: {woordvoerder_id} ({start_time} - {end_time})")


def _process_tekst_elements(session, parent_elem: ET.Element, parent_id: str):
    """Process tekst elements containing the actual speech content"""
    
    # Find direct tekst elements (not nested in interrumpant)
    tekst_elements = parent_elem.findall('./vlos:tekst', NS_VLOS)
    
    for i, tekst_elem in enumerate(tekst_elements):
        tekst_id = f"{parent_id}_tekst_{i}"
        
        # Extract all text content from alinea/alineaitem structure
        speech_content = []
        for alinea_elem in tekst_elem.findall('.//vlos:alinea', NS_VLOS):
            paragraph_lines = []
            for alineaitem_elem in alinea_elem.findall('.//vlos:alineaitem', NS_VLOS):
                if alineaitem_elem.text:
                    paragraph_lines.append(alineaitem_elem.text.strip())
            
            if paragraph_lines:
                speech_content.append(' '.join(paragraph_lines))
        
        full_speech_text = '\n\n'.join(speech_content)
        
        # Create tekst node
        tekst_props = {
            'id': tekst_id,
            'content': full_speech_text,
            'paragraph_count': len(speech_content),
            'source': 'vlos_xml'
        }
        session.execute_write(merge_node, 'VlosTekst', 'id', tekst_props)
        
        # Link to parent (woordvoerder or interrumpant)
        session.execute_write(merge_rel, 'VlosWoordvoerder', 'id', parent_id,
                              'VlosTekst', 'id', tekst_id, 'HAS_TEKST')
        
        print(f"      💬 Processed speech: {len(full_speech_text)} characters, {len(speech_content)} paragraphs")


def _process_interrumpant_element(session, interrumpant_elem: ET.Element, parent_woordvoerder_id: str):
    """Process interrumpant elements (interruptions during speeches)"""
    
    interrumpant_id = interrumpant_elem.get('objectid', f"vlos_interrumpant_{hash(ET.tostring(interrumpant_elem))}")
    
    # Get timing information
    markeertijdbegin_elem = interrumpant_elem.find('.//vlos:markeertijdbegin', NS_VLOS)
    markeertijdeind_elem = interrumpant_elem.find('.//vlos:markeertijdeind', NS_VLOS)
    
    start_time = markeertijdbegin_elem.text if markeertijdbegin_elem is not None else None
    end_time = markeertijdeind_elem.text if markeertijdeind_elem is not None else None
    
    # Get speaker information
    spreker_elem = interrumpant_elem.find('.//vlos:spreker', NS_VLOS)
    speaker_id = None
    if spreker_elem is not None:
        speaker_id = _process_single_vlos_spreker(session, spreker_elem, parent_woordvoerder_id)
    
    # Create interrumpant node
    interrumpant_props = {
        'id': interrumpant_id,
        'start_time': start_time,
        'end_time': end_time,
        'is_voorzitter': interrumpant_elem.find('.//vlos:isvoorzitter', NS_VLOS) is not None,
        'is_draad': interrumpant_elem.find('.//vlos:isdraad', NS_VLOS) is not None,
        'source': 'vlos_xml'
    }
    session.execute_write(merge_node, 'VlosInterrumpant', 'id', interrumpant_props)
    
    # Link to parent woordvoerder
    session.execute_write(merge_rel, 'VlosWoordvoerder', 'id', parent_woordvoerder_id,
                          'VlosInterrumpant', 'id', interrumpant_id, 'HAS_INTERRUPTION')
    
    # Link to speaker if exists
    if speaker_id:
        session.execute_write(merge_rel, 'VlosInterrumpant', 'id', interrumpant_id,
                              'VlosSpeaker', 'id', speaker_id, 'SPOKEN_BY')
    
    # Process the interruption speech content
    _process_tekst_elements_for_interrumpant(session, interrumpant_elem, interrumpant_id)
    
    print(f"      🔀 Processed interruption: {interrumpant_id} ({start_time} - {end_time})")


def _process_tekst_elements_for_interrumpant(session, interrumpant_elem: ET.Element, interrumpant_id: str):
    """Process tekst elements within an interrumpant"""
    
    tekst_elements = interrumpant_elem.findall('./vlos:tekst', NS_VLOS)
    
    for i, tekst_elem in enumerate(tekst_elements):
        tekst_id = f"{interrumpant_id}_tekst_{i}"
        
        # Extract all text content from alinea/alineaitem structure
        speech_content = []
        for alinea_elem in tekst_elem.findall('.//vlos:alinea', NS_VLOS):
            paragraph_lines = []
            for alineaitem_elem in alinea_elem.findall('.//vlos:alineaitem', NS_VLOS):
                if alineaitem_elem.text:
                    paragraph_lines.append(alineaitem_elem.text.strip())
            
            if paragraph_lines:
                speech_content.append(' '.join(paragraph_lines))
        
        full_speech_text = '\n\n'.join(speech_content)
        
        # Create tekst node
        tekst_props = {
            'id': tekst_id,
            'content': full_speech_text,
            'paragraph_count': len(speech_content),
            'source': 'vlos_xml'
        }
        session.execute_write(merge_node, 'VlosTekst', 'id', tekst_props)
        
        # Link to interrumpant
        session.execute_write(merge_rel, 'VlosInterrumpant', 'id', interrumpant_id,
                              'VlosTekst', 'id', tekst_id, 'HAS_TEKST')
        
        print(f"        💬 Processed interruption speech: {len(full_speech_text)} characters")


def _process_activity_speakers(session, activiteit_elem: ET.Element, activity_id: str):
    """Process speakers within a specific activity"""
    
    for woordvoerder_elem in activiteit_elem.findall('.//vlos:woordvoerder', NS_VLOS):
        # Process the full woordvoerder element (speaker + speech)
        _process_woordvoerder_element(session, woordvoerder_elem, activity_id)


def _process_activity_agendapunten(session, activiteit_elem: ET.Element, activity_id: str):
    """Process agendapunten within a specific activity"""
    
    for agendapunt_elem in activiteit_elem.findall('.//vlos:agendapunt', NS_VLOS):
        agendapunt_id = agendapunt_elem.get('objectid', f"vlos_agendapunt_{hash(ET.tostring(agendapunt_elem))}")
        
        # Link existing agendapunt to this activity
        session.execute_write(merge_rel, 'VlosActivity', 'id', activity_id,
                              'VlosAgendapunt', 'id', agendapunt_id, 'DISCUSSES_AGENDAPUNT')


def parse_vlos_xml_datetime(datetime_str: str) -> Optional[datetime]:
    """Parse VLOS XML datetime string to datetime object"""
    if not datetime_str:
        return None
    
    try:
        # Handle format like "2025-04-23T17:06:06"
        if 'T' in datetime_str:
            return datetime.fromisoformat(datetime_str.replace('T', ' '))
        else:
            # Try other common formats
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
                try:
                    return datetime.strptime(datetime_str, fmt)
                except ValueError:
                    continue
    except Exception:
        pass
    
    return None


def _find_best_api_activity_match(vlos_activity_props: Dict[str, Any], api_activities: List[Dict[str, Any]]) -> tuple[Optional[Dict], float]:
    """Find the best matching API activity for a VLOS activity"""
    
    best_match = None
    best_score = 0.0
    
    for api_activity in api_activities:
        # Use the sophisticated matching from vlos_matching.py
        from .processors.vlos_matching import calculate_vlos_activity_match_score
        
        # Prepare data for matching
        xml_activity_data = {
            'title': vlos_activity_props.get('title', ''),
            'soort': vlos_activity_props.get('soort', ''),
            'start_time': parse_vlos_xml_datetime(vlos_activity_props.get('start_time')) if vlos_activity_props.get('start_time') else None,
            'end_time': parse_vlos_xml_datetime(vlos_activity_props.get('end_time')) if vlos_activity_props.get('end_time') else None,
        }
        
        score, reasons = calculate_vlos_activity_match_score(xml_activity_data, api_activity)
        
        if score > best_score:
            best_score = score
            best_match = api_activity
    
    return best_match, best_score


def _process_vlos_vergadering_element(session, vergadering_elem: ET.Element, parent_id: str, 
                                    canonical_vergadering_node, api_activities) -> Optional[str]:
    """Process a VLOS vergadering element"""
    
    vergadering_id = vergadering_elem.get('objectid', f"vlos_vergadering_{hash(ET.tostring(vergadering_elem))}")
    vergadering_soort = vergadering_elem.get('soort', 'Unknown')
    vergadering_kamer = vergadering_elem.get('kamer', 'Unknown')
    
    print(f"🔍 DEBUG: Processing vergadering element:")
    print(f"  - ID: {vergadering_id}")
    print(f"  - Soort: {vergadering_soort}")
    print(f"  - Kamer: {vergadering_kamer}")
    
    # Analyze the structure of this vergadering element
    print(f"🔍 DEBUG: Vergadering element structure:")
    _analyze_xml_structure(vergadering_elem, max_depth=3)
    
    # Create vergadering section node
    section_props = {
        'id': vergadering_id,
        'title': f"{vergadering_soort} vergadering",
        'type': 'vergadering',
        'soort': vergadering_soort,
        'kamer': vergadering_kamer,
        'source': 'vlos_xml'
    }
    session.execute_write(merge_node, 'VlosSection', 'id', section_props)
    
    # Link to parent document
    session.execute_write(merge_rel, 'VlosDocument', 'id', parent_id,
                          'VlosSection', 'id', vergadering_id, 'CONTAINS_SECTION')
    
    # Count different types of elements
    activities_count = len(vergadering_elem.findall('.//vlos:activity', NS_VLOS))
    speakers_count = len(vergadering_elem.findall('.//vlos:spreker', NS_VLOS))
    sections_count = len(vergadering_elem.findall('./vlos:section', NS_VLOS))
    agendapunten_count = len(vergadering_elem.findall('.//vlos:agendapunt', NS_VLOS))
    
    print(f"📊 DEBUG: Found elements in vergadering:")
    print(f"  - Activities: {activities_count}")
    print(f"  - Speakers: {speakers_count}")
    print(f"  - Sections: {sections_count}")
    print(f"  - Agendapunten: {agendapunten_count}")
    
    # Process any activities within this vergadering element
    for activity_elem in vergadering_elem.findall('.//vlos:activity', NS_VLOS):
        process_vlos_activity_element(session, activity_elem, canonical_vergadering_node, 
                                    vergadering_id, api_activities)
    
    # Process speakers
    process_vlos_speakers(session, vergadering_elem, vergadering_id, 'VlosSection')
    
    # Process zaken
    process_vlos_zaken(session, vergadering_elem, vergadering_id, 'VlosSection')
    
    # Process any nested sections within the vergadering
    for nested_section in vergadering_elem.findall('./vlos:section', NS_VLOS):
        nested_section_id = _process_vlos_section(session, nested_section, vergadering_id, 
                                                canonical_vergadering_node, api_activities)
        if nested_section_id:
            # Link nested section to parent vergadering section
            session.execute_write(merge_rel, 'VlosSection', 'id', vergadering_id,
                                  'VlosSection', 'id', nested_section_id, 'CONTAINS_SUBSECTION')
    
    return vergadering_id


def _process_vlos_section(session, section_elem: ET.Element, parent_id: str, 
                         canonical_vergadering_node, api_activities) -> Optional[str]:
    """Process a VLOS section element"""
    
    section_id = section_elem.get('id', f"vlos_section_{hash(ET.tostring(section_elem))}")
    section_title = section_elem.get('title', 'Untitled Section')
    section_type = section_elem.get('type', 'general')
    
    # Create section node
    section_props = {
        'id': section_id,
        'title': section_title,
        'type': section_type,
        'source': 'vlos_xml'
    }
    session.execute_write(merge_node, 'VlosSection', 'id', section_props)
    
    # Link to parent document
    session.execute_write(merge_rel, 'VlosDocument', 'id', parent_id,
                          'VlosSection', 'id', section_id, 'CONTAINS_SECTION')
    
    # Process activities within this section
    for activity_elem in section_elem.findall('.//vlos:activity', NS_VLOS):
        process_vlos_activity_element(session, activity_elem, canonical_vergadering_node, 
                                    section_id, api_activities)
    
    # Process speakers
    process_vlos_speakers(session, section_elem, section_id, 'VlosSection')
    
    # Process zaken
    process_vlos_zaken(session, section_elem, section_id, 'VlosSection')
    
    # Process nested sections recursively
    for nested_section in section_elem.findall('./vlos:section', NS_VLOS):
        nested_section_id = _process_vlos_section(session, nested_section, section_id, 
                                                canonical_vergadering_node, api_activities)
        if nested_section_id:
            # Link nested section to parent section
            session.execute_write(merge_rel, 'VlosSection', 'id', section_id,
                                  'VlosSection', 'id', nested_section_id, 'CONTAINS_SUBSECTION')
    
    return section_id


# Backward compatibility function
def load_vlos_verslag_original(driver, xml_content: str, canonical_api_vergadering_id: str):
    """Original load_vlos_verslag function for backward compatibility."""
    return load_vlos_verslag(driver, xml_content, canonical_api_vergadering_id)