import pytest
import sys
from pathlib import Path

# Ensure project src/ directory is on the Python path so that
# `import core...` works regardless of how pytest is executed.
ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core.connection.neo4j_connection import Neo4jConnection


@pytest.fixture(scope="module")
def neo4j_conn():
    """Provide a Neo4jConnection for the duration of the test module."""
    conn = Neo4jConnection()
    yield conn
    conn.close()


def _single_int(result):
    """Helper to extract the integer count from a one-row Cypher result."""
    if not result:
        return 0
    # record is a dict, take the first value (e.g. {'cnt': 123})
    return list(result[0].values())[0]


@pytest.mark.parametrize(
    "description, cypher",
    [
        (
            "Verslag → VlosDocument",
            "MATCH (v:Verslag)-[:IS_VLOS_FOR]->(:VlosDocument) RETURN count(v) AS cnt",
        ),
        (
            "VlosDocument ← Vergadering",
            "MATCH (:VlosDocument)<-[:HAS_VLOS_DOCUMENT]-(:Vergadering) RETURN count(*) AS cnt",
        ),
        (
            "Vergadering → Agendapunt",
            "MATCH (:Vergadering)-[:HAS_AGENDAPUNT]->(:Agendapunt) RETURN count(*) AS cnt",
        ),
        (
            "Agendapunt → Activiteit",
            "MATCH (:Agendapunt)-[:BELONGS_TO_ACTIVITEIT]->(:Activiteit) RETURN count(*) AS cnt",
        ),
        (
            "Vergadering → Activiteit",
            "MATCH (:Vergadering)-[:HAS_ACTIVITEIT]->(:Activiteit) RETURN count(*) AS cnt",
        ),
        (
            "ActiviteitActor → Persoon",
            "MATCH (:ActiviteitActor)-[:ACTED_AS_PERSOON]->(:Persoon) RETURN count(*) AS cnt",
        ),
        (
            "Persoon seat assignments",
            "MATCH (:Persoon)-[:HAS_SEAT_ASSIGNMENT]->(:FractieZetelPersoon) RETURN count(*) AS cnt",
        ),
    ],
)
def test_graph_link_exists(neo4j_conn, description, cypher):
    """Ensure each expected relationship exists at least once in the graph."""
    result = neo4j_conn.query(cypher)
    count = _single_int(result)
    assert (
        count > 0
    ), f"Expected at least one instance of '{description}', but found none." 