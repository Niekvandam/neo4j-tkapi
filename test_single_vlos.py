#!/usr/bin/env python3
"""
Test script to process a single VLOS XML file in detail
"""

import sys
import os
sys.path.append('src')

from core.connection.neo4j_connection import Neo4jConnection
from loaders.vlos_verslag_loader import load_vlos_verslag
import tkapi

def test_single_vlos_xml():
    """Test processing a single VLOS XML file"""
    
    # Initialize connection
    conn = Neo4jConnection()
    
    try:
        # Connect to TK API
        api = tkapi.TKApi()
        
        # Get a recent vergadering
        print("🔍 Fetching a recent vergadering...")
        from tkapi.vergadering import Vergadering
        from tkapi.util import util as tkapi_util
        from datetime import timezone
        import datetime
        
        # Create filter for recent vergaderingen
        start_datetime_obj = datetime.datetime.strptime('2025-01-01', "%Y-%m-%d")
        odata_start_date_str = tkapi_util.datetime_to_odata(start_datetime_obj.replace(tzinfo=timezone.utc))
        
        filter_obj = Vergadering.create_filter()
        filter_obj.add_filter_str(f"Datum ge {odata_start_date_str}")
        
        vergaderingen = api.get_items(Vergadering, filter=filter_obj, max_items=5)
        
        for vergadering in vergaderingen:
            print(f"📋 Checking vergadering: {vergadering.id} - {vergadering.titel}")
            
            # Check if this vergadering has a verslag directly
            if vergadering.verslag:
                verslag = vergadering.verslag
                print(f"📄 Found verslag: {verslag.id}")
                
                # Download the XML content using the existing function
                from loaders.processors.common_processors import download_verslag_xml
                
                try:
                    print(f"📥 Downloading XML resource for verslag {verslag.id}...")
                    xml_content = download_verslag_xml(verslag.id)
                    
                    print(f"📊 XML content length: {len(xml_content)} characters")
                    
                    # Handle bytes vs string encoding
                    if isinstance(xml_content, bytes):
                        xml_string = xml_content.decode('utf-8')
                        print("🔧 Converted bytes to string")
                    else:
                        xml_string = xml_content
                    
                    print(f"📊 XML content preview (first 500 chars):")
                    print(xml_string[:500])
                    print("...")
                    
                    # Save XML to file for inspection
                    xml_filename = f"sample_vlos_{vergadering.id}_{verslag.id}.xml"
                    with open(xml_filename, 'w', encoding='utf-8') as f:
                        f.write(xml_string)
                    print(f"💾 Saved XML to: {xml_filename}")
                    
                    # Process the XML
                    print(f"🔄 Processing VLOS XML...")
                    load_vlos_verslag(conn.driver, xml_string, vergadering.id)
                    
                    print(f"✅ Completed processing vergadering {vergadering.id}")
                    return  # Process only the first XML file
                    
                except Exception as e:
                    print(f"❌ Error downloading XML for verslag {verslag.id}: {e}")
                    continue
            else:
                print(f"⚠️ No verslag found for vergadering {vergadering.id}")
                continue
                        
        print("❌ No XML resources found in recent vergaderingen")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    test_single_vlos_xml() 