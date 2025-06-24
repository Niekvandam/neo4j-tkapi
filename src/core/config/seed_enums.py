from core.connection.neo4j_connection import Neo4jConnection
from utils.helpers import merge_node
from tkapi.zaak import ZaakSoort, KabinetsAppreciatie, ZaakActorRelatieSoort
from tkapi.document import DocumentSoort
from tkapi.activiteit import ActiviteitSoort, ActiviteitStatus, DatumSoort, ActiviteitRelatieSoort
from tkapi.toezegging import ToezeggingStatus



def seed_enum_nodes(conn: Neo4jConnection):
    with conn.driver.session(database=conn.database) as session:
        # Zaak enums
        for enum_cls in (ZaakSoort, KabinetsAppreciatie, ZaakActorRelatieSoort):
            for member in enum_cls:
                session.execute_write(merge_node, enum_cls.__name__, 'key', {'key': member.name, 'prefLabel': member.value})
        # Document enum
        for member in DocumentSoort:
            session.execute_write(merge_node, 'DocumentSoort', 'key', {'key': member.name, 'prefLabel': member.value})
        # Activiteit enums
        for enum_cls in (ActiviteitSoort, ActiviteitStatus, DatumSoort, ActiviteitRelatieSoort):
            for member in enum_cls:
                session.execute_write(merge_node, enum_cls.__name__, 'key', {'key': member.name, 'prefLabel': member.value})
        for member in ToezeggingStatus:
            session.execute_write(merge_node, 'ToezeggingStatus', 'key', {'key': member.name, 'prefLabel': member.value})
    print("✅ Seeded all enum nodes.")

