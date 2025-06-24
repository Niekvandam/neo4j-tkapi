# Checkpoint System

The checkpoint system provides robust, resumable data loading capabilities for the Neo4j TK API project. It allows you to resume interrupted data loading processes from where they left off.

## 📁 Files

- **[`checkpoint_manager.py`](checkpoint_manager.py)** - Core checkpoint management functionality
- **[`checkpoint_decorator.py`](checkpoint_decorator.py)** - Decorators for easy checkpoint integration
- **[`checkpoint_cli.py`](checkpoint_cli.py)** - Command-line interface for checkpoint management

## 🚀 Key Features

- **Persistent Progress Tracking** - All progress saved to disk in JSON files
- **Granular Resumption** - Resume at the individual item level
- **Error Handling** - Failed items are logged but don't stop the entire process
- **Run Management** - Track multiple runs with detailed status information
- **Decorator System** - Easy integration with minimal code changes

## 📖 Quick Start

### Using Decorators (Recommended)

```python
from ..checkpoint.checkpoint_decorator import checkpoint_loader

@checkpoint_loader(checkpoint_interval=25)
def load_items(conn, items, _checkpoint_context=None):
    def process_single_item(item):
        # Your processing logic here
        pass
    
    if _checkpoint_context:
        _checkpoint_context.process_items(items, process_single_item)
```

### Manual Checkpoint Management

```python
from ..checkpoint.checkpoint_manager import CheckpointManager, LoaderCheckpoint

checkpoint_manager = CheckpointManager()
checkpoint = LoaderCheckpoint(checkpoint_manager, "load_items")

for item in items:
    if checkpoint.is_processed(item.id):
        continue
    
    try:
        # Process item
        checkpoint.mark_processed(item.id)
    except Exception as e:
        checkpoint.mark_failed(item.id, str(e))
```

## 🔧 Configuration

The checkpoint system automatically handles:

- **File Storage** - Checkpoints stored in `checkpoints/` directory
- **Run Identification** - Unique run IDs with timestamps
- **Progress Tracking** - Automatic progress statistics
- **Error Logging** - Detailed error information for debugging

## 📊 Monitoring

Use the CLI for detailed monitoring:

```bash
# List all runs
python src/core/checkpoint/checkpoint_cli.py list

# Show run details
python src/core/checkpoint/checkpoint_cli.py show --run-id run_20241201_143022

# Clean up old runs
python src/core/checkpoint/checkpoint_cli.py cleanup --keep 3
```

## 🔗 Integration

The checkpoint system integrates with:

- **[Main Application](../../main.py)** - Automatic checkpoint management
- **[Data Loaders](../../loaders/README.md)** - Decorator-based integration
- **[CLI Tools](checkpoint_cli.py)** - Management and monitoring

## 📚 Documentation

For detailed usage information, see:

- **[Checkpoint System Guide](../../../docs/checkpoint-system.md)** - Complete usage guide
- **[Decorator System Guide](../../../docs/decorator-system.md)** - Decorator usage patterns

---

**Parent:** [Core System](../README.md) | **Related:** [Data Loaders](../../loaders/README.md) 