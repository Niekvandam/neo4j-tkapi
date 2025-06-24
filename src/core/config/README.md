# Configuration System

This module contains configuration constants, enums, and seed data for the Neo4j TK API data loader project.

## 📁 Files

- **[`constants.py`](constants.py)** - Application constants and relationship mappings
- **[`seed_enums.py`](seed_enums.py)** - Enum seeding functionality for Neo4j

## 🚀 Features

- **Relationship Mappings** - Predefined mappings for different entity types
- **Enum Seeding** - Automatic loading of enum values into Neo4j
- **Constants Management** - Centralized configuration constants
- **Type Definitions** - Standard type mappings for entities

## 📖 Usage

### Using Constants

```python
from ..config.constants import REL_MAP_ZAAK, REL_MAP_ACTIVITEIT

# Use relationship mappings in loaders
for rel_name, rel_config in REL_MAP_ZAAK.items():
    # Process relationships
    pass
```

### Seeding Enums

```python
from ..config.seed_enums import seed_enum_nodes
from ..connection.neo4j_connection import Neo4jConnection

conn = Neo4jConnection()
seed_enum_nodes(conn)
```

## 🔧 Configuration Maps

### Available Relationship Maps

- **`REL_MAP_ZAAK`** - Relationship configuration for Zaak entities
- **`REL_MAP_ACTIVITEIT`** - Relationship configuration for Activiteit entities  
- **`REL_MAP_ACTOR`** - Relationship configuration for Actor entities
- **`REL_MAP_TOEZEGGING`** - Relationship configuration for Toezegging entities

### Map Structure

```python
REL_MAP_EXAMPLE = {
    'relationship_name': {
        'target_label': 'TargetNode',
        'target_key': 'id_field',
        'rel_type': 'RELATIONSHIP_TYPE',
        'properties': ['prop1', 'prop2']
    }
}
```

## 🌱 Enum Seeding

The enum seeding system automatically loads enumeration values from the TK API into Neo4j:

### Supported Enums

- **ZaakSoort** - Types of cases
- **KabinetsAppreciatie** - Cabinet appreciation values
- **ZaakActorRelatieSoort** - Case-actor relationship types
- **DocumentSoort** - Document types

### Seeding Process

1. **Fetch enum values** from TK API
2. **Create nodes** in Neo4j with appropriate labels
3. **Set properties** including ID, naam, and omschrijving
4. **Handle duplicates** gracefully with MERGE operations

## 🔗 Integration

Used by:

- **[Data Loaders](../../loaders/README.md)** - Import constants for relationship processing
- **[Main Application](../../main.py)** - Enum seeding during initialization
- **[Connection System](../connection/README.md)** - Database operations for seeding

## 🧪 Testing

Test configuration loading:

```bash
# Test constants import
python -c "from src.core.config.constants import REL_MAP_ZAAK; print('Constants loaded successfully')"

# Test enum seeding
python -c "from src.core.config.seed_enums import seed_enum_nodes; print('Enum seeding available')"
```

## 📝 Adding New Configuration

### Adding New Constants

```python
# In constants.py
NEW_CONSTANT = {
    'key': 'value',
    'another_key': 'another_value'
}

# New relationship map
REL_MAP_NEW_ENTITY = {
    'related_items': {
        'target_label': 'RelatedItem',
        'target_key': 'id',
        'rel_type': 'RELATED_TO',
        'properties': ['relationship_property']
    }
}
```

### Adding New Enum Types

```python
# In seed_enums.py
def seed_new_enum_type(conn):
    """Seed new enum type into Neo4j"""
    from tkapi.new_module import NewEnumType
    
    with conn.driver.session() as session:
        for enum_item in NewEnumType.get_all():
            session.execute_write(merge_node, 'NewEnumType', 'id', {
                'id': enum_item.id,
                'naam': enum_item.naam,
                'omschrijving': enum_item.omschrijving
            })
```

## 📚 Best Practices

1. **Centralize constants** - Keep all configuration in this module
2. **Use descriptive names** - Make constant names self-documenting
3. **Document relationships** - Comment complex relationship mappings
4. **Version control** - Track changes to configuration carefully
5. **Validate enum data** - Ensure enum seeding handles API changes

---

**Parent:** [Core System](../README.md) | **Related:** [Data Loaders](../../loaders/README.md) 