# Utility Functions

This module contains helper functions and utilities used throughout the Neo4j TK API data loader project.

## 📁 Files

- **[`helpers.py`](helpers.py)** - Core helper functions for database operations and data processing

## 🚀 Features

- **Database Helpers** - Functions for Neo4j operations (merge_node, merge_rel)
- **Batch Processing** - Efficient batch operations for large datasets
- **ID Checking** - Utilities for checking existing data in Neo4j
- **Data Processing** - Common data transformation and validation functions

## 📖 Usage

### Database Operations

```python
from ..utils.helpers import merge_node, merge_rel

# Merge a node
with session.execute_write() as tx:
    merge_node(tx, 'Person', 'id', {
        'id': person.id,
        'name': person.name,
        'email': person.email
    })

# Merge a relationship
merge_rel(tx, 'Person', 'id', person_id, 'Company', 'id', company_id, 'WORKS_FOR', {
    'start_date': '2024-01-01',
    'position': 'Developer'
})
```

### Batch ID Checking

```python
from ..utils.helpers import batch_check_nodes_exist

# Check which IDs already exist in Neo4j
existing_ids = batch_check_nodes_exist(conn, 'Person', 'id', person_ids)
new_persons = [p for p in persons if p.id not in existing_ids]
```

## 🔧 Available Functions

### Node Operations

- **`merge_node(tx, label, key, properties)`** - Merge a single node
- **`batch_merge_nodes(session, label, key, items)`** - Batch merge multiple nodes
- **`batch_check_nodes_exist(conn, label, key, ids)`** - Check existence of multiple nodes

### Relationship Operations

- **`merge_rel(tx, from_label, from_key, from_id, to_label, to_key, to_id, rel_type, properties)`** - Merge relationship
- **`batch_merge_relationships(session, relationships)`** - Batch merge multiple relationships

### Data Processing

- **`clean_properties(properties)`** - Clean and validate property dictionaries
- **`format_datetime(dt)`** - Format datetime objects for Neo4j
- **`safe_get(obj, attr, default=None)`** - Safely get object attributes

## 🚀 Performance Features

### Batch Processing

The utilities support efficient batch processing:

```python
# Process 1000 items at a time
batch_size = 1000
for i in range(0, len(items), batch_size):
    batch = items[i:i + batch_size]
    batch_merge_nodes(session, 'Item', 'id', batch)
```

### ID Existence Checking

Efficient checking of existing data:

```python
# Check 15,000+ IDs per second
all_ids = [item.id for item in items]
existing_ids = batch_check_nodes_exist(conn, 'Item', 'id', all_ids)

# Only process new items
new_items = [item for item in items if item.id not in existing_ids]
```

## 🔗 Integration

Used by:

- **[Data Loaders](../loaders/README.md)** - All loaders use these helper functions
- **[Configuration System](../core/config/README.md)** - Enum seeding and setup
- **[Checkpoint System](../core/checkpoint/README.md)** - Progress tracking operations

## 🧪 Testing

Test utility functions:

```bash
# Test helper imports
python -c "from src.utils.helpers import merge_node; print('Helpers loaded successfully')"

# Test with actual database
python tests/test_connection.py
```

## 📝 Adding New Utilities

### Adding Helper Functions

```python
# In helpers.py
def new_helper_function(param1, param2):
    """
    Description of what this function does
    
    Args:
        param1: Description of parameter 1
        param2: Description of parameter 2
        
    Returns:
        Description of return value
    """
    # Implementation here
    return result
```

### Best Practices for Utilities

1. **Keep functions focused** - Each function should do one thing well
2. **Add comprehensive docstrings** - Document parameters and return values
3. **Handle errors gracefully** - Use try/catch for external operations
4. **Support batch operations** - Design for efficiency with large datasets
5. **Use type hints** - Add type annotations for better code clarity

### Example Utility Pattern

```python
def batch_process_items(session, items, processor_func, batch_size=1000):
    """
    Process items in batches for better performance
    
    Args:
        session: Neo4j session
        items: List of items to process
        processor_func: Function to process each batch
        batch_size: Size of each batch (default: 1000)
        
    Returns:
        Number of successfully processed items
    """
    processed_count = 0
    
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        try:
            processor_func(session, batch)
            processed_count += len(batch)
        except Exception as e:
            print(f"Error processing batch {i//batch_size + 1}: {e}")
            continue
    
    return processed_count
```

## 📊 Performance Tips

1. **Use batch operations** when processing large datasets
2. **Check existence** before creating to avoid duplicates
3. **Use transactions** for consistency
4. **Limit query complexity** for better performance
5. **Monitor memory usage** with large batches

---

**Parent:** [Source Code](../README.md) | **Related:** [Data Loaders](../loaders/README.md) 