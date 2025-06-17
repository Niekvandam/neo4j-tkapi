
from neo4j_connection import Neo4jConnection
from seed_enums import seed_enum_nodes
from loaders.persoon_loader import load_personen
from loaders.fractie_loader import load_fracties
from loaders.toezegging_loader import load_toezeggingen
from loaders.vergadering_loader import load_vergaderingen
from loaders.verslag_loader import load_verslagen
from loaders.document_loader import load_documents
from loaders.stemming_loader import load_stemmingen
from loaders.zaak_loader import load_zaken
from loaders.activiteit_loader import load_activiteiten
from loaders.actor_loader import load_activiteit_actors
from loaders.agendapunt_loader import load_agendapunten
from loaders.besluit_loader import load_besluiten
from loaders.dossier_loader import load_dossiers

if __name__ == "__main__":
    conn = Neo4jConnection()
    try:
        seed_enum_nodes(conn)
        load_personen(conn, batch_size=99999)
        load_fracties(conn, batch_size=99999)
        load_toezeggingen(conn, batch_size=99999)
        load_vergaderingen(conn, batch_size=99999)
        load_verslagen(conn, batch_size=99999)
        load_documents(conn, batch_size=99999)
        load_stemmingen(conn, batch_size=99999)
        load_zaken(conn, batch_size=99999)
        load_activiteiten(conn, batch_size=99999)
        load_activiteit_actors(conn, batch_size=99999)
        load_agendapunten(conn, batch_size=99999)
        load_besluiten(conn, batch_size=99999)
        load_dossiers(conn, batch_size=50)
    finally:
        conn.close()
        print("🔌 Closed.")
