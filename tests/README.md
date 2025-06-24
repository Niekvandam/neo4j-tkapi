# Test Suite

This directory contains test files for validating the functionality of the Neo4j TK API data loader project.

## 📁 Test Files

- **[`test_connection.py`](test_connection.py)** - Tests Neo4j database connection functionality
- **[`test_decorator.py`](test_decorator.py)** - Tests checkpoint decorator system
- **[`test_threaded_activiteit.py`](test_threaded_activiteit.py)** - Tests threaded processing for activiteiten
- **[`simple_test.py`](simple_test.py)** - Basic smoke tests for core functionality
- **[`test.py`](test.py)** - Additional test scenarios

## 🧪 Running Tests

### Individual Tests

```bash
# Test database connection
python tests/test_connection.py

# Test checkpoint decorators
python tests/test_decorator.py

# Test threaded processing
python tests/test_threaded_activiteit.py

# Run simple smoke tests
python tests/simple_test.py
```

### All Tests

```bash
# Run all tests (if using pytest)
pytest tests/

# Or run manually
python -m tests.test_connection
python -m tests.test_decorator
python -m tests.test_threaded_activiteit
```

## 🔧 Test Categories

### Connection Tests
Validate database connectivity and session management:
- Neo4j driver initialization
- Environment variable configuration
- Session lifecycle management
- Error handling for connection failures

### Checkpoint Tests
Test the checkpoint system functionality:
- Decorator integration
- Progress tracking
- Resume functionality
- Error recovery
- CLI operations

### Threading Tests
Validate multi-threaded processing:
- Thread safety
- Performance improvements
- Error handling in threaded context
- Progress tracking across threads

### Integration Tests
End-to-end testing of complete workflows:
- Full data loading processes
- Checkpoint resume scenarios
- Error recovery workflows
- Performance benchmarks

## 📊 Test Data

Tests use various data sources:
- **Mock data** for unit tests
- **Sample TK API responses** for integration tests
- **Test Neo4j database** for database operations
- **Checkpoint files** for resume testing

## 🔧 Test Configuration

### Environment Setup

Create a `.env.test` file for test configuration:

```bash
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=test_password
NEO4J_DATABASE=test_database
```

### Test Database

Use a separate test database to avoid affecting production data:

```cypher
CREATE DATABASE test_database;
USE test_database;
```

## 📝 Writing New Tests

### Test Structure

```python
import unittest
from src.core.connection.neo4j_connection import Neo4jConnection

class TestNewFeature(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures"""
        self.conn = Neo4jConnection()
    
    def tearDown(self):
        """Clean up after tests"""
        self.conn.close()
    
    def test_feature_functionality(self):
        """Test specific functionality"""
        # Test implementation
        pass

if __name__ == '__main__':
    unittest.main()
```

### Best Practices

1. **Use descriptive test names** - Make it clear what is being tested
2. **Test both success and failure cases** - Include error scenarios
3. **Clean up after tests** - Ensure tests don't affect each other
4. **Use appropriate assertions** - Choose the right assertion methods
5. **Mock external dependencies** - Isolate units being tested

### Example Test

```python
def test_checkpoint_decorator_basic_functionality(self):
    """Test that checkpoint decorator tracks progress correctly"""
    items = [{'id': i, 'name': f'item_{i}'} for i in range(100)]
    processed_items = []
    
    @checkpoint_loader(checkpoint_interval=10)
    def test_loader(conn, items, _checkpoint_context=None):
        def process_item(item):
            processed_items.append(item)
        
        if _checkpoint_context:
            _checkpoint_context.process_items(items, process_item)
    
    # Run test
    test_loader(self.conn, items)
    
    # Assertions
    self.assertEqual(len(processed_items), 100)
    self.assertTrue(all(item in processed_items for item in items))
```

## 🚨 Test Coverage

Current test coverage areas:

### ✅ Covered
- Database connection establishment
- Basic checkpoint functionality
- Decorator integration
- Threading for activiteiten
- Error handling scenarios

### 🔄 Partial Coverage
- All loader functions
- Complete checkpoint resume scenarios
- Performance benchmarks
- CLI operations

### ❌ Needs Coverage
- Edge cases in data processing
- Memory usage under load
- Network failure scenarios
- Large dataset performance

## 🔍 Debugging Tests

### Common Issues

1. **Connection failures** - Check Neo4j is running and credentials are correct
2. **Import errors** - Ensure Python path includes src directory
3. **Checkpoint conflicts** - Clean up checkpoint files between tests
4. **Threading issues** - Use proper synchronization in threaded tests

### Debug Commands

```bash
# Run with verbose output
python -v tests/test_connection.py

# Run specific test method
python -m unittest tests.test_decorator.TestCheckpointDecorator.test_basic_functionality

# Debug with pdb
python -m pdb tests/test_connection.py
```

## 📈 Performance Testing

### Benchmarking

```python
import time

def test_performance_benchmark(self):
    """Benchmark data loading performance"""
    items = [{'id': i} for i in range(10000)]
    
    start_time = time.time()
    # Run loader
    end_time = time.time()
    
    processing_time = end_time - start_time
    items_per_second = len(items) / processing_time
    
    print(f"Processed {len(items)} items in {processing_time:.2f}s")
    print(f"Rate: {items_per_second:.2f} items/second")
    
    # Assert performance threshold
    self.assertGreater(items_per_second, 100)  # Minimum 100 items/second
```

---

**Parent:** [Project Root](../README.md) | **Related:** [Source Code](../src/README.md) 