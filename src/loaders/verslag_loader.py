from tkapi import TKApi
from tkapi.verslag import Verslag
from neo4j_connection import Neo4jConnection
from helpers import merge_node, merge_rel
from .vlos_verslag_loader import load_vlos_verslag
import requests
import xml.etree.ElementTree as ET

api = TKApi()

def download_xml(verslag_id):
    """Downloads the XML content for a given Verslag ID."""
    url = f"https://gegevensmagazijn.tweedekamer.nl/OData/v4/2.0/Verslag({verslag_id})/resource"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.content
    except requests.RequestException as e:
        print(f"  ✕ ERROR downloading XML for Verslag {verslag_id}: {e}")
        return None

def load_verslagen(conn: Neo4jConnection, batch_size: int = 50):
    """
    Loads Verslag metadata from TKApi and parses the full XML for each verslag
    to load detailed hierarchical data.
    """
    api = TKApi()
    verslagen = api.get_items(Verslag, max_items=batch_size)
    print(f"→ Fetched metadata for {len(verslagen)} Verslagen.")

    with conn.driver.session(database=conn.database) as session:
        for idx, v in enumerate(verslagen, 1):
            print(f"  → Processing Verslag {idx}/{len(verslagen)} (ID: {v.id})")
            
            # 1. Merge the Verslag node itself
            props = {
                'id': v.id,
                'soort': v.soort.name if v.soort else None,
                'status': v.status.name if v.status else None,
                'source': 'tkapi'
            }
            session.execute_write(merge_node, 'Verslag', 'id', props)

            # 2. Link to Vergadering (if available in metadata)
            if v.vergadering:
                session.execute_write(merge_node, 'Vergadering', 'id', {'id': v.vergadering.id})
                session.execute_write(merge_rel, 'Verslag', 'id', v.id, 'Vergadering', 'id', v.vergadering.id, 'RECORDED_IN')

            # 3. Download and parse the full XML content
            print(f"    - Downloading XML resource...")
            xml_content = download_xml(v.id)
            
            if xml_content:
                print(f"    - Parsing XML and loading into Neo4j...")
                # The new loader function takes care of the complex parsing
                load_vlos_verslag(session, xml_content)
                # Link the Verslag node to the Vergadering from the XML
                session.execute_write(merge_rel, 'Verslag', 'id', v.id, 'Vergadering', 'id', v.vergadering.id, 'DESCRIBES_CONTENT_OF')
                print(f"    ✔ Successfully parsed and loaded XML.")

    print("✅ Loaded Verslagen and their detailed XML content.")
