from neo4j_connection import Neo4jConnection
from seed_enums import seed_enum_nodes

# Import anchor loaders
from loaders.document_loader import load_documents
from loaders.zaak_loader import load_zaken
from loaders.activiteit_loader import load_activiteiten
from loaders.agendapunt_loader import load_agendapunten
from loaders.vergadering_loader import load_vergaderingen

# Import other loaders that are still independent (or might be for now)
from loaders.persoon_loader import load_personen
from loaders.fractie_loader import load_fracties
from loaders.toezegging_loader import load_toezeggingen
from loaders.actor_loader import load_activiteit_actors # Assuming ActiviteitActor is fetched based on its own date or via Activiteit

# Import common processors only to use utility like clear_processed_ids
from loaders.common_processors import clear_processed_ids


# Define the start date for filtering
SHARED_START_DATE = "2024-01-01"

if __name__ == "__main__":
    conn = Neo4jConnection()
    try:
        print(f"🚀 Starting data load for items from {SHARED_START_DATE} onwards.")
        
        # Clear processed IDs at the beginning of the run
        clear_processed_ids()

        # 1. Seed Enum Nodes (independent of dates)
        seed_enum_nodes(conn)
        print("-" * 30)

        # 2. Load independent master data (like Personen, Fracties)
        # These typically don't depend on the date filter in the same way,
        # or have their own "active" filters.
        # Adjust batch_size as needed for production.
        load_personen(conn, batch_size=5000)
        print("-" * 30)
        load_fracties(conn, batch_size=500)
        print("-" * 30)

        # 3. Load dated "anchor" entities. These will then load their related
        #    "dateless" entities (Dossiers, Besluiten, Stemmingen, Verslagen).

        # Load Activiteiten (dated) - these might link to Zaken, Documenten, Agendapunten
        load_activiteiten(conn, start_date_str=SHARED_START_DATE)
        print("-" * 30)

        # Load Agendapunten (dated) - these will process related Besluiten (which process Stemmingen)
        load_agendapunten(conn, start_date_str=SHARED_START_DATE)
        print("-" * 30)

        # Load Zaken (dated) - these will process related Documenten, Besluiten (->Stemmingen), Dossiers
        load_zaken(conn, start_date_str=SHARED_START_DATE)
        print("-" * 30)
        
        # Load Documents (dated) - these will process related Dossiers, Zaken
        load_documents(conn, start_date_str=SHARED_START_DATE)
        print("-" * 30)

        # Load Vergaderingen (dated) - these will process related Verslagen (which includes XML parsing)
        load_vergaderingen(conn, start_date_str=SHARED_START_DATE)
        print("-" * 30)
        
        # Load Toezeggingen (has AanmaakDatum for filtering)
        load_toezeggingen(conn) # Assuming it uses its internal date filter
        print("-" * 30)



        print("🎉 All selected loaders completed.")

    finally:
        conn.close()
        print("🔌 Neo4j connection closed.")