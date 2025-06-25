"""
VLOS Verslag Loader - Processes VLOS XML verslag content with interface support
"""
import xml.etree.ElementTree as ET
import time
from typing import Optional

from utils.helpers import merge_node, merge_rel
from core.connection.neo4j_connection import Neo4jConnection

# Import interface system
from core.interfaces import BaseLoader, LoaderConfig, LoaderResult, LoaderCapability, loader_registry

# Import VLOS processors
from .processors.vlos_processor import (
    NS_VLOS, get_candidate_api_activities, process_vlos_activity_element,
    process_vlos_speakers, process_vlos_zaken
)


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
            api_activities = get_candidate_api_activities(session, canonical_vergadering_node)
            print(f"📊 Found {len(api_activities)} API activities for matching")
            
            # Process main document structure
            _process_vlos_document_structure(session, root, canonical_vergadering_node, api_activities)
            
        print("✅ VLOS XML processing completed successfully")
        
    except ET.ParseError as e:
        print(f"❌ XML parsing error: {e}")
        raise
    except Exception as e:
        print(f"❌ Error processing VLOS XML: {e}")
        raise


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
    
    # Process sections
    sections_processed = 0
    for section in root.findall('.//vlos:section', NS_VLOS):
        section_id = _process_vlos_section(session, section, doc_id, canonical_vergadering_node, api_activities)
        if section_id:
            sections_processed += 1
    
    print(f"📊 Processed {sections_processed} VLOS sections")


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