# Data Loaders

This directory contains all the data loader modules for importing TK API data into Neo4j. Each loader is responsible for a specific type of data entity and includes checkpoint support for resumable processing.

## 📁 Loader Files

### Core Entity Loaders
- **[`zaak_loader.py`](zaak_loader.py)** - Loads Zaken (cases) data
- **[`activiteit_loader.py`](activiteit_loader.py)** - Loads Activiteiten (activities) and their Agendapunten
- **[`document_loader.py`](document_loader.py)** - Loads Document entities
- **[`vergadering_loader.py`](vergadering_loader.py)** - Loads Vergadering (meeting) data

### Supporting Entity Loaders
- **[`persoon_loader.py`](persoon_loader.py)** - Loads Persoon (person) entities
- **[`fractie_loader.py`](fractie_loader.py)** - Loads Fractie (faction) data
- **[`actor_loader.py`](actor_loader.py)** - Loads Actor relationship data
- **[`toezegging_loader.py`](toezegging_loader.py)** - Loads Toezegging (commitment) data

### Specialized Loaders
- **[`agendapunt_loader.py`](agendapunt_loader.py)** - Loads Agendapunt (agenda item) data
- **[`stemming_loader.py`](stemming_loader.py)** - Loads Stemming (voting) data
- **[`besluit_loader.py`](besluit_loader.py)** - Loads Besluit (decision) data
- **[`dossier_loader.py`](dossier_loader.py)** - Loads Dossier data
- **[`verslag_loader.py`](verslag_loader.py)** - Loads Verslag (report) data
- **[`vlos_verslag_loader.py`](vlos_verslag_loader.py)** - Loads VLOS Verslag data

### Utility Files
- **[`common_processors.py`](common_processors.py)** - Shared processing utilities and functions
- **[`zaak_loader_refactored.py`](zaak_loader_refactored.py)** - Refactored version of zaak loader

## 🚀 Usage Patterns

### Using Checkpoint Decorators

Most loaders use the checkpoint decorator system for automatic progress tracking:

```python
from ..core.checkpoint.checkpoint_decorator import checkpoint_loader

@checkpoint_loader(checkpoint_interval=25)
def load_documents(conn, documents, _checkpoint_context=None):
    def process_single_document(doc):
        # Your processing logic here
        pass
    
    if _checkpoint_context:
        _checkpoint_context.process_items(documents, process_single_document)
    else:
        # Fallback for non-checkpoint usage
        for doc in documents:
            process_single_document(doc)
```

### Threading Support

Some loaders support multi-threaded processing:

```python
# Activiteiten loader with threading
load_activiteiten_threaded(conn, activiteiten, max_workers=10, checkpoint_manager=cm)

# Zaken loader with threading  
load_zaken_threaded(conn, zaken, max_workers=10, checkpoint_manager=cm)
```

### Skip Functionality

All loaders support skipping items:

```python
# Skip first 1000 items
load_documents(conn, documents[1000:], checkpoint_manager=cm)
```

## 🔧 Configuration

Loaders use configuration from the core config system:

```python
from ..core.config.constants import REL_MAP_ZAAK
from ..core.connection.neo4j_connection import Neo4jConnection
from ..utils.helpers import merge_node, merge_rel
```

## 📊 Monitoring

Each loader provides progress information:

- **Real-time progress updates** during processing
- **Error logging** for failed items
- **Statistics** on processed vs. failed items
- **Checkpoint status** for resumability

## 🔗 Dependencies

### Internal Dependencies
- **[Core Checkpoint System](../core/checkpoint/README.md)** - For resumable processing
- **[Core Connection](../core/connection/README.md)** - For Neo4j database access
- **[Core Config](../core/config/README.md)** - For constants and configuration
- **[Utils](../utils/README.md)** - For helper functions

### External Dependencies
- **tkapi** - TK API client library
- **neo4j** - Neo4j Python driver
- **concurrent.futures** - For threading support

## 🧪 Testing

Test your loaders:

```bash
# Test individual loader
python -c "from src.loaders.document_loader import load_documents; print('Import successful')"

# Test with checkpoint system
python tests/test_decorator.py

# Test threaded processing
python tests/test_threaded_activiteit.py
```

## 📝 Adding New Loaders

When creating a new loader:

1. **Use the checkpoint decorator** for automatic progress tracking
2. **Follow the naming convention**: `{entity}_loader.py`
3. **Import from the correct paths** using relative imports
4. **Add comprehensive error handling**
5. **Document your loader** in this README

### Template for New Loader

```python
from ..core.connection.neo4j_connection import Neo4jConnection
from ..core.checkpoint.checkpoint_decorator import checkpoint_loader
from ..utils.helpers import merge_node, merge_rel
from ..core.config.constants import REL_MAP_ENTITY

@checkpoint_loader(checkpoint_interval=25)
def load_entities(conn, entities, _checkpoint_context=None):
    """
    Load Entity data into Neo4j
    
    Args:
        conn: Neo4jConnection instance
        entities: List of Entity objects to process
        _checkpoint_context: Checkpoint context (provided by decorator)
    """
    def process_single_entity(entity):
        with conn.driver.session() as session:
            # Your processing logic here
            session.execute_write(merge_node, 'Entity', 'id', {
                'id': entity.id,
                'name': entity.name
            })
    
    if _checkpoint_context:
        _checkpoint_context.process_items(entities, process_single_entity)
    else:
        for entity in entities:
            process_single_entity(entity)
```

## 🔄 Migration Notes

If you're updating from the old structure:

1. **Update import statements** to use the new paths
2. **Use checkpoint decorators** instead of manual checkpoint management
3. **Follow the new directory structure** for any new files
4. **Update tests** to use the new import paths

---

**Related Documentation:**
- [Main README](../../README.md) - Project overview
- [Checkpoint System](../../docs/checkpoint-system.md) - Checkpoint functionality
- [Threading Guide](../../docs/threading-and-skip.md) - Threading and skip features 