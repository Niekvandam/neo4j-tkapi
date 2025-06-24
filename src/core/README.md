# Core System Components

The core system provides the foundational components for the Neo4j TK API data loader project. These modules handle essential functionality like database connections, checkpoint management, and configuration.

## 📁 Structure

```
core/
├── checkpoint/          # Checkpoint management system
│   ├── checkpoint_manager.py
│   ├── checkpoint_decorator.py
│   ├── checkpoint_cli.py
│   └── README.md
├── connection/          # Database connection handling
│   ├── neo4j_connection.py
│   └── README.md
├── config/              # Configuration and constants
│   ├── constants.py
│   ├── seed_enums.py
│   └── README.md
└── README.md           # This file
```

## 🚀 Components

### [Checkpoint System](checkpoint/README.md)
Provides robust, resumable data loading capabilities:
- **Persistent progress tracking** with JSON file storage
- **Decorator-based integration** for easy adoption
- **CLI management tools** for monitoring and maintenance
- **Error handling** with detailed logging

### [Connection Management](connection/README.md)
Handles Neo4j database connections:
- **Environment-based configuration** for flexibility
- **Session management** with proper cleanup
- **Error handling** for connection issues
- **Driver lifecycle management**

### [Configuration System](config/README.md)
Manages application configuration and constants:
- **Relationship mappings** for different entity types
- **Enum seeding** from TK API into Neo4j
- **Centralized constants** for consistency
- **Type definitions** for standardization

## 🔗 Integration

The core system integrates with:

- **[Data Loaders](../loaders/README.md)** - Use all core components
- **[Main Application](../main.py)** - Orchestrates core functionality
- **[Utilities](../utils/README.md)** - Complementary helper functions
- **[Tests](../../tests/README.md)** - Validation and testing

## 📖 Usage Patterns

### Basic Setup

```python
# Initialize core components
from core.connection.neo4j_connection import Neo4jConnection
from core.checkpoint.checkpoint_manager import CheckpointManager
from core.config.seed_enums import seed_enum_nodes

# Setup
conn = Neo4jConnection()
checkpoint_manager = CheckpointManager()
seed_enum_nodes(conn)
```

### With Decorators

```python
from core.checkpoint.checkpoint_decorator import checkpoint_loader

@checkpoint_loader(checkpoint_interval=25)
def load_data(conn, items, _checkpoint_context=None):
    # Your loader implementation
    pass
```

### Configuration Usage

```python
from core.config.constants import REL_MAP_ZAAK
from core.connection.neo4j_connection import Neo4jConnection

conn = Neo4jConnection()
# Use relationship mappings
for rel_name, config in REL_MAP_ZAAK.items():
    # Process relationships
    pass
```

## 🧪 Testing

Test the core system:

```bash
# Test connection
python -c "from src.core.connection.neo4j_connection import Neo4jConnection; conn = Neo4jConnection(); print('✅ Connection OK')"

# Test checkpoint system
python tests/test_decorator.py

# Test configuration
python -c "from src.core.config.constants import REL_MAP_ZAAK; print('✅ Config OK')"
```

## 📝 Best Practices

1. **Use dependency injection** - Pass connections and managers as parameters
2. **Handle errors gracefully** - All core components include error handling
3. **Follow the decorator pattern** - Use checkpoint decorators for new loaders
4. **Centralize configuration** - Keep all constants in the config module
5. **Test thoroughly** - Core components are critical infrastructure

## 🔧 Development

When extending the core system:

1. **Maintain backwards compatibility** - Existing loaders depend on these APIs
2. **Add comprehensive tests** - Core functionality must be reliable
3. **Document thoroughly** - Update READMEs for any changes
4. **Follow existing patterns** - Consistency is key for maintainability

---

**Parent:** [Source Code](../README.md) | **Components:** [Checkpoint](checkpoint/README.md) • [Connection](connection/README.md) • [Config](config/README.md) 