#!/usr/bin/env python3

try:
    from src.neo4j_connection import Neo4jConnection
    print("✅ Successfully imported Neo4jConnection")
    
    conn = Neo4jConnection()
    print("✅ Successfully created Neo4jConnection instance")
    
    # Test basic connection
    result = conn.query("RETURN 'Connection successful' AS message")
    print(f"✅ Connection test result: {result}")
    
    conn.close()
    print("✅ Connection closed successfully")
    
except Exception as e:
    print(f"❌ Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc() 