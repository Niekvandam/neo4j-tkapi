import xml.etree.ElementTree as ET
from helpers import merge_node, merge_rel
from neo4j_connection import Neo4jConnection

# Namespace for the vlosCoreDocument XML
NS = {'vlos': 'http://www.tweedekamer.nl/ggm/vergaderverslag/v1.0'}

def _process_activity_element(conn: Neo4jConnection, element: ET.Element, parent_id: str, parent_label: str):
    """
    Recursively processes an activity-like XML element and writes it to Neo4j.
    """
    activity_id = element.get('objectid')
    if not activity_id:
        return
    with conn.driver.session(database=conn.database) as session:
        # 1. Create the Activiteit Node for the current element
        props = {
            'id': activity_id,
            'soort': element.get('soort'),
            'titel': element.findtext('vlos:titel', '', NS),
            'onderwerp': element.findtext('vlos:onderwerp', '', NS),
            'begin': element.findtext('vlos:aanvangstijd', '', NS) or element.findtext('vlos:markeertijdbegin', '', NS),
            'einde': element.findtext('vlos:eindtijd', '', NS) or element.findtext('vlos:markeertijdeind', '', NS),
            'source': 'vlos' # Mark that this data comes from the XML
        }
        session.execute_write(merge_node, 'Activiteit', 'id', props)

        # 2. Link to its parent
        if parent_id and parent_label:
            session.execute_write(merge_rel, 'Activiteit', 'id', activity_id, parent_label, 'id', parent_id, 'PART_OF')

        # 3. Process direct children (speakers, cases)
        _process_speakers(session, element, activity_id)
        _process_zaken(session, element, activity_id)

        # 4. Recursive Step: Process sub-activity elements
        sub_activity_tags = ['vlos:activiteit', 'vlos:activiteithoofd', 'vlos:activiteitdeel', 'vlos:woordvoerder', 'vlos:interrumpant']
        for tag in sub_activity_tags:
            for sub_element in element.findall(tag, NS):
                _process_activity_element(session, sub_element, activity_id, 'Activiteit')

def _process_speakers(conn: Neo4jConnection, element: ET.Element, activity_id: str):
    """Finds speakers and links them as actors to the activity."""
    with conn.driver.session(database=conn.database) as session:
        for spreker_el in element.findall('.//vlos:spreker', NS):
            persoon_id = spreker_el.get('objectid')
            if not persoon_id: continue

            fractie_naam = spreker_el.findtext('vlos:fractie', None, NS)
            
            persoon_props = {
                'id': persoon_id, 'soort': spreker_el.get('soort'),
                'functie': spreker_el.findtext('vlos:functie', '', NS),
                'achternaam': spreker_el.findtext('vlos:achternaam', '', NS),
                'voornaam': spreker_el.findtext('vlos:voornaam', '', NS).strip(),
                'verslagnaam': spreker_el.findtext('vlos:verslagnaam', '', NS),
            }
            session.execute_write(merge_node, 'Persoon', 'id', persoon_props)

            # Create a unique ActiviteitActor for this specific role
            actor_id = f"actor_{activity_id}_{persoon_id}"
            actor_props = {'id': actor_id, 'naam': f"{persoon_props['voornaam']} {persoon_props['achternaam']}"}
            session.execute_write(merge_node, 'ActiviteitActor', 'id', actor_props)
            
            session.execute_write(merge_rel, 'Activiteit', 'id', activity_id, 'ActiviteitActor', 'id', actor_id, 'HAS_ACTOR')
            session.execute_write(merge_rel, 'ActiviteitActor', 'id', actor_id, 'Persoon', 'id', persoon_id, 'ACTED_AS_PERSOON')

            if fractie_naam:
                session.execute_write(merge_node, 'Fractie', 'naam', {'naam': fractie_naam})
                session.execute_write(merge_rel, 'ActiviteitActor', 'id', actor_id, 'Fractie', 'naam', fractie_naam, 'ACTED_AS_FRACTIE')

def _process_zaken(conn: Neo4jConnection, element: ET.Element, activity_id: str):
    """Finds cases and links them to the activity."""
    with conn.driver.session(database=conn.database) as session:
        for zaak_el in element.findall('./vlos:zaken/vlos:zaak', NS): # Direct children only
            zaak_id = zaak_el.get('objectid')
            if not zaak_id: continue
            
            zaak_props = {
                'id': zaak_id,
                'soort': zaak_el.get('soort'),
                'onderwerp': zaak_el.findtext('vlos:onderwerp', '', NS),
                'dossiernummer': zaak_el.findtext('vlos:dossiernummer', None, NS)
            }
            session.execute_write(merge_node, 'Zaak', 'id', zaak_props)
            session.execute_write(merge_rel, 'Activiteit', 'id', activity_id, 'Zaak', 'id', zaak_id, 'PART_OF_ZAAK')

def load_vlos_verslag(conn: Neo4jConnection, xml_content: str):
    """
    Entry point for parsing the full XML content of a Verslag.
    """
    with conn.driver.session(database=conn.database) as session:
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            print(f"  ✕ ERROR: Could not parse XML. {e}")
            return

        vergadering_el = root.find('vlos:vergadering', NS)
        if vergadering_el is None:
            print("  ✕ ERROR: <vergadering> tag not found in XML.")
            return
            
        vergadering_id = vergadering_el.get('objectid')
        vergadering_props = {
            'id': vergadering_id, 'soort': vergadering_el.get('soort'),
            'kamer': vergadering_el.get('kamer'),
            'titel': vergadering_el.findtext('vlos:titel', '', NS),
            'zaal': vergadering_el.findtext('vlos:zaal', '', NS),
            'datum': vergadering_el.findtext('vlos:datum', '', NS),
        }
        session.execute_write(merge_node, 'Vergadering', 'id', vergadering_props)
        
        for activiteit_el in vergadering_el.findall('vlos:activiteit', NS):
            _process_activity_element(session, activiteit_el, vergadering_id, 'Vergadering')