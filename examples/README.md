# Usage Examples

This directory contains example scripts and code snippets demonstrating how to use the Neo4j TK API data loader project.

## 📁 Example Files

- **[`example_simple_loader.py`](example_simple_loader.py)** - Basic example of creating a data loader

## 🚀 Getting Started

### Basic Loader Example

The simple loader example demonstrates:
- Setting up a database connection
- Using checkpoint decorators
- Processing data with error handling
- Basic progress tracking

```bash
# Run the simple loader example
python examples/example_simple_loader.py
```

## 📖 Example Patterns

### 1. Basic Data Loader

```python
from src.core.connection.neo4j_connection import Neo4jConnection
from src.core.checkpoint.checkpoint_decorator import checkpoint_loader
from src.utils.helpers import merge_node

@checkpoint_loader(checkpoint_interval=25)
def load_simple_data(conn, items, _checkpoint_context=None):
    """Example of a basic data loader"""
    def process_single_item(item):
        with conn.driver.session() as session:
            session.execute_write(merge_node, 'Item', 'id', {
                'id': item['id'],
                'name': item['name'],
                'description': item.get('description', '')
            })
    
    if _checkpoint_context:
        _checkpoint_context.process_items(items, process_single_item)
    else:
        for item in items:
            process_single_item(item)

# Usage
conn = Neo4jConnection()
sample_data = [
    {'id': 1, 'name': 'Item 1', 'description': 'First item'},
    {'id': 2, 'name': 'Item 2', 'description': 'Second item'},
]
load_simple_data(conn, sample_data)
```

### 2. Loader with Relationships

```python
@checkpoint_loader(checkpoint_interval=10)
def load_data_with_relationships(conn, items, _checkpoint_context=None):
    """Example loader that creates nodes and relationships"""
    def process_single_item(item):
        with conn.driver.session() as session:
            # Create main node
            session.execute_write(merge_node, 'Person', 'id', {
                'id': item['id'],
                'name': item['name'],
                'email': item['email']
            })
            
            # Create relationships
            for company_id in item.get('companies', []):
                session.execute_write(merge_rel, 
                    'Person', 'id', item['id'],
                    'Company', 'id', company_id,
                    'WORKS_FOR', {'role': 'employee'}
                )
    
    if _checkpoint_context:
        _checkpoint_context.process_items(items, process_single_item)
```

### 3. Threaded Processing Example

```python
from concurrent.futures import ThreadPoolExecutor
import threading

def load_data_threaded(conn, items, max_workers=5):
    """Example of threaded data processing"""
    processed_count = 0
    failed_count = 0
    lock = threading.Lock()
    
    def process_batch(batch):
        nonlocal processed_count, failed_count
        local_processed = 0
        local_failed = 0
        
        for item in batch:
            try:
                with conn.driver.session() as session:
                    session.execute_write(merge_node, 'Item', 'id', item)
                local_processed += 1
            except Exception as e:
                print(f"Error processing {item}: {e}")
                local_failed += 1
        
        with lock:
            processed_count += local_processed
            failed_count += local_failed
            print(f"Progress: {processed_count}/{len(items)} processed")
    
    # Split items into batches
    batch_size = max(1, len(items) // max_workers)
    batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
    
    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process_batch, batches)
    
    print(f"Final results: {processed_count} processed, {failed_count} failed")
```

### 4. Custom Checkpoint Management

```python
from src.core.checkpoint.checkpoint_manager import CheckpointManager, LoaderCheckpoint

def load_data_with_custom_checkpoints(conn, items):
    """Example using manual checkpoint management"""
    checkpoint_manager = CheckpointManager()
    checkpoint_manager.start_new_run("Custom loader example")
    
    checkpoint = LoaderCheckpoint(checkpoint_manager, "load_custom_data")
    checkpoint.set_total_items(len(items))
    
    for i, item in enumerate(items, 1):
        item_id = f"item_{item['id']}"
        
        if checkpoint.is_processed(item_id):
            print(f"Skipping already processed item: {item_id}")
            continue
        
        try:
            # Process item
            with conn.driver.session() as session:
                session.execute_write(merge_node, 'CustomItem', 'id', item)
            
            checkpoint.mark_processed(item_id)
            
            # Save progress every 10 items
            if i % 10 == 0:
                checkpoint.save_progress()
                stats = checkpoint.get_progress_stats()
                print(f"Progress: {stats['processed_count']}/{stats['total_items']}")
                
        except Exception as e:
            checkpoint.mark_failed(item_id, str(e))
            print(f"Failed to process {item_id}: {e}")
    
    checkpoint.save_progress()
    checkpoint_manager.mark_loader_complete("load_custom_data")
```

## 🔧 Configuration Examples

### Environment Setup

```python
import os
from src.core.connection.neo4j_connection import Neo4jConnection

# Set environment variables programmatically
os.environ['NEO4J_URI'] = 'bolt://localhost:7687'
os.environ['NEO4J_USER'] = 'neo4j'
os.environ['NEO4J_PASSWORD'] = 'your_password'

# Initialize connection
conn = Neo4jConnection()
```

### Custom Configuration

```python
from src.core.config.constants import REL_MAP_ZAAK

# Example of using configuration constants
def process_zaak_relationships(zaak, conn):
    """Process relationships using configuration"""
    for rel_name, rel_config in REL_MAP_ZAAK.items():
        if hasattr(zaak, rel_name):
            target_items = getattr(zaak, rel_name)
            for target_item in target_items:
                # Create relationship using config
                merge_rel(
                    conn, 'Zaak', 'id', zaak.id,
                    rel_config['target_label'], 
                    rel_config['target_key'], 
                    getattr(target_item, rel_config['target_key']),
                    rel_config['rel_type'],
                    {}
                )
```

## 🧪 Testing Examples

### Unit Test Example

```python
import unittest
from src.core.connection.neo4j_connection import Neo4jConnection

class TestExampleLoader(unittest.TestCase):
    def setUp(self):
        self.conn = Neo4jConnection()
    
    def test_simple_loading(self):
        """Test the simple loader example"""
        test_data = [{'id': 999, 'name': 'Test Item'}]
        
        # This should not raise an exception
        load_simple_data(self.conn, test_data)
        
        # Verify data was loaded
        with self.conn.driver.session() as session:
            result = session.run("MATCH (n:Item {id: 999}) RETURN n.name")
            record = result.single()
            self.assertEqual(record['n.name'], 'Test Item')
    
    def tearDown(self):
        # Clean up test data
        with self.conn.driver.session() as session:
            session.run("MATCH (n:Item {id: 999}) DELETE n")
        self.conn.close()
```

## 📚 Additional Resources

### Documentation Links
- **[Main README](../README.md)** - Project overview
- **[Data Loaders](../src/loaders/README.md)** - Loader documentation
- **[Checkpoint System](../docs/checkpoint-system.md)** - Checkpoint guide
- **[Threading Guide](../docs/threading-and-skip.md)** - Threading features

### Best Practices
1. **Always use checkpoint decorators** for new loaders
2. **Handle errors gracefully** with try/catch blocks
3. **Use batch processing** for large datasets
4. **Monitor progress** with regular status updates
5. **Clean up resources** properly (close connections)

### Common Patterns
- **Decorator-based loaders** - Recommended approach
- **Manual checkpoint management** - For complex scenarios
- **Threaded processing** - For performance-critical loads
- **Relationship processing** - Using configuration maps

---

**Parent:** [Project Root](../README.md) | **Related:** [Source Code](../src/README.md) | **Tests:** [Test Suite](../tests/README.md) 