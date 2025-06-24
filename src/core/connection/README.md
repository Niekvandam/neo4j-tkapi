# Database Connection

This module handles Neo4j database connections for the TK API data loader project.

## 📁 Files

- **[`neo4j_connection.py`](neo4j_connection.py)** - Neo4j connection management class

## 🚀 Features

- **Connection Management** - Handles Neo4j driver initialization and cleanup
- **Environment Configuration** - Reads connection settings from environment variables
- **Session Management** - Provides clean session handling patterns
- **Error Handling** - Robust error handling for connection issues

## 📖 Usage

### Basic Connection

```python
from ..connection.neo4j_connection import Neo4jConnection

# Initialize connection
conn = Neo4jConnection()

# Use in session
with conn.driver.session() as session:
    result = session.run("MATCH (n) RETURN count(n)")
    print(result.single()[0])

# Cleanup when done
conn.close()
```

### Environment Configuration

Set the following environment variables:

```bash
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
```

## 🔧 Configuration

The connection class reads from environment variables:

- **`NEO4J_URI`** - Database URI (default: `bolt://localhost:7687`)
- **`NEO4J_USER`** - Username (default: `neo4j`)
- **`NEO4J_PASSWORD`** - Password (required)

## 🔗 Integration

Used by:

- **[Data Loaders](../../loaders/README.md)** - All loaders use this for database access
- **[Utility Functions](../../utils/README.md)** - Helper functions for database operations
- **[Configuration](../config/README.md)** - Seed data and enum loading

## 🧪 Testing

Test your connection:

```bash
# Test basic connection
python tests/test_connection.py

# Test in interactive mode
python -c "from src.core.connection.neo4j_connection import Neo4jConnection; conn = Neo4jConnection(); print('Connection successful')"
```

## 📝 Best Practices

1. **Always use context managers** for sessions
2. **Close connections** when done
3. **Handle connection errors** gracefully
4. **Use environment variables** for configuration
5. **Don't hardcode credentials** in source code

### Example Pattern

```python
def process_data(conn, data):
    """Process data with proper session handling"""
    with conn.driver.session() as session:
        for item in data:
            session.execute_write(lambda tx: tx.run(
                "MERGE (n:Item {id: $id}) SET n.name = $name",
                id=item.id, name=item.name
            ))
```

---

**Parent:** [Core System](../README.md) | **Related:** [Utils](../../utils/README.md) 