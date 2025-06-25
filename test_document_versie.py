#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from core.connection.neo4j_connection import Neo4jConnection
from loaders.document_loader import load_documents

def test_document_versie():
    """Test the document loader with DocumentVersie fixes"""
    conn = Neo4jConnection()
    
    try:
        print("🧪 Testing DocumentVersie handling...")
        # Test with yesterday's date to get minimal documents
        import datetime
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"📅 Testing with documents from: {yesterday}")
        
        load_documents(conn, batch_size=2, start_date_str=yesterday, skip_count=0)
        print("✅ Test completed successfully!")
        
    except KeyboardInterrupt:
        print("⚠️ Test interrupted by user")
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    test_document_versie() 