#!/usr/bin/env python3
"""
VLOS Neo4j Loader Example

Demonstrates how to load VLOS analysis results into Neo4j database.
This creates comprehensive parliamentary relationship graphs.
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from core.connection.neo4j_connection import Neo4jConnection
from loaders.vlos_neo4j_loader import load_vlos_analysis


def main():
    print("🏛️ VLOS Neo4j Loader Example")
    print("=" * 60)
    
    try:
        # Create Neo4j connection
        print("🔗 Connecting to Neo4j...")
        conn = Neo4jConnection()
        
        # Load VLOS analysis results
        print("📊 Loading VLOS analysis results into Neo4j...")
        load_vlos_analysis(conn)
        
        print("\n✅ VLOS Neo4j loading completed successfully!")
        print("\n📈 You can now explore the parliamentary relationships in Neo4j Browser:")
        print("   - Speaker-Zaak connections")
        print("   - Interruption patterns")
        print("   - Voting behavior")
        print("   - Activity participation")
        
        # Example queries
        print("\n🔍 Example Cypher queries to explore the data:")
        print()
        print("1. Find all speakers and what they discussed:")
        print("   MATCH (p:Persoon)-[:SPOKE_IN_CONNECTION]->(c:SpeakerZaakConnection)-[:DISCUSSES_DOSSIER]->(d:Dossier)")
        print("   RETURN p.achternaam, p.voornaam, d.titel, c.speech_preview")
        print()
        print("2. Find interruption patterns:")
        print("   MATCH (p1:Persoon)-[:INTERRUPTED_IN]->(i:InterruptionEvent)<-[:WAS_INTERRUPTED_IN]-(p2:Persoon)")
        print("   RETURN p1.achternaam AS interrupting, p2.achternaam AS interrupted, i.type, i.context")
        print()
        print("3. Find controversial voting topics:")
        print("   MATCH (v:VotingEvent)-[:HAS_FRACTIE_VOTE]->(fv:FractieVote)")
        print("   WHERE v.is_controversial = true")
        print("   RETURN v.besluit_titel, fv.fractie_naam, fv.stem")
        print()
        print("4. Find most active speakers:")
        print("   MATCH (p:Persoon)-[:SPOKE_IN_CONNECTION]->(c:SpeakerZaakConnection)")
        print("   RETURN p.achternaam, p.voornaam, COUNT(c) AS connections")
        print("   ORDER BY connections DESC LIMIT 10")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main()) 