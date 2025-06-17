from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
load_dotenv()
class Neo4jConnection:
    def __init__(self):
        """
        Initialize the Neo4j driver and specify the target database.

        :param uri: Bolt URI of the Neo4j instance (default: bolt://localhost:7687)
        :param user: Username for authentication (default: tkapi)
        :param password: Password for authentication (default: tkapi123)
        :param database: Database name (default: neo4j)
        """
        uri = os.getenv("NEO4J_URI")
        user = os.getenv("NEO4J_USER")
        password = os.getenv("NEO4J_PASSWORD")
        database = os.getenv("NEO4J_DATABASE")
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    def close(self) -> None:
        """Close the Neo4j driver connection."""
        self.driver.close()

    def query(self, cypher: str, parameters: dict = None) -> list:
        """
        Execute a Cypher query against the specified database and return the results.

        :param cypher: The Cypher query string to execute.
        :param parameters: Optional dictionary of parameters for the query.
        :return: List of records returned by the query.
        """
        with self.driver.session(database=self.database) as session:
            result = session.run(cypher, parameters or {})
            return [record.data() for record in result]


if __name__ == "__main__":
    # Example usage
    conn = Neo4jConnection()
    try:
        result = conn.query("RETURN 'Connection successful' AS message")
        print(result[0]['message'])
    finally:
        conn.close()
