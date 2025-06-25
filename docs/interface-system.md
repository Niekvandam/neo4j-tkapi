# Loader Interface System

This document describes the standardized interface system for data loaders in the Neo4j TK API project. The interface system provides consistency, extensibility, and better maintainability for all loaders.

## 🎯 Why Interfaces?

### Current Challenges
- **Inconsistent signatures**: Different loaders have different parameter patterns
- **Mixed capabilities**: Some support threading, others don't, with no clear indication
- **Duplicate code**: Similar functionality implemented multiple times
- **Hard to extend**: Adding new features requires modifying many files
- **Configuration chaos**: No standardized way to configure loaders

### Benefits of Interfaces
- **Consistency**: All loaders follow the same patterns
- **Discoverability**: Easy to see what each loader supports
- **Extensibility**: New capabilities can be added systematically
- **Testability**: Standardized testing approaches
- **Maintainability**: Clear separation of concerns

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Loader Interface System                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐  ┌─────────────────┐  ┌─────────────────────┐ │
│  │   Protocol   │  │   BaseLoader    │  │  LoaderRegistry     │ │
│  │              │  │   (Abstract)    │  │                     │ │
│  │ - load()     │  │                 │  │ - register()        │ │
│  │ - capabilities│  │ - validate()    │  │ - get_loader()      │ │
│  │ - validate() │  │ - capabilities  │  │ - by_capability()   │ │
│  └──────────────┘  └─────────────────┘  └─────────────────────┘ │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                    Configuration & Results                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐  ┌─────────────────┐  ┌─────────────────────┐ │
│  │LoaderConfig  │  │  LoaderResult   │  │ LoaderCapability    │ │
│  │              │  │                 │  │                     │ │
│  │ - dates      │  │ - success       │  │ - THREADING         │ │
│  │ - threading  │  │ - counts        │  │ - DATE_FILTERING    │ │
│  │ - batch_size │  │ - timing        │  │ - BATCH_PROCESSING  │ │
│  │ - skip_count │  │ - errors        │  │ - ID_CHECKING       │ │
│  └──────────────┘  └─────────────────┘  └─────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 📋 Core Components

### 1. LoaderProtocol

Defines the contract that all loaders must implement:

```python
class LoaderProtocol(Protocol):
    def load(self, conn: Any, config: LoaderConfig, 
             checkpoint_manager: Optional[Any] = None) -> LoaderResult:
        """Main loading method"""
        ...
    
    def get_capabilities(self) -> List[LoaderCapability]:
        """Return supported capabilities"""
        ...
    
    def validate_config(self, config: LoaderConfig) -> List[str]:
        """Validate configuration"""
        ...
```

### 2. LoaderCapability Enum

Standardized capabilities that loaders can support:

```python
class LoaderCapability(Enum):
    THREADING = "threading"                    # Multi-threaded processing
    BATCH_PROCESSING = "batch_processing"      # Batch operations
    DATE_FILTERING = "date_filtering"          # Date range filtering
    SKIP_FUNCTIONALITY = "skip_functionality"  # Skip N items
    ID_CHECKING = "id_checking"               # Check existing IDs
    INCREMENTAL_LOADING = "incremental_loading" # Resume from checkpoint
    RELATIONSHIP_PROCESSING = "relationship_processing" # Handle relationships
```

### 3. LoaderConfig

Standardized configuration object:

```python
@dataclass
class LoaderConfig:
    start_date: Optional[str] = None          # Start date (YYYY-MM-DD)
    end_date: Optional[str] = None            # End date (YYYY-MM-DD)
    batch_size: int = 50                      # Batch size for processing
    skip_count: int = 0                       # Items to skip
    max_workers: int = 10                     # Thread count
    checkpoint_interval: int = 25             # Checkpoint frequency
    overwrite_existing: bool = False          # Overwrite existing data
    enable_threading: bool = False            # Enable threading
    enable_id_checking: bool = True           # Check existing IDs
    custom_params: Optional[Dict[str, Any]] = None  # Loader-specific params
```

### 4. LoaderResult

Standardized result object:

```python
@dataclass
class LoaderResult:
    success: bool                             # Overall success
    processed_count: int                      # Items processed
    failed_count: int                         # Items failed
    skipped_count: int                        # Items skipped
    total_items: int                          # Total items
    execution_time_seconds: float             # Execution time
    error_messages: List[str]                 # Error details
    warnings: List[str]                       # Warning messages
    metadata: Optional[Dict[str, Any]] = None # Additional info
```

## 🔧 Implementation Guide

### Creating a Modern Loader

```python
from src.core.interfaces import BaseLoader, LoaderCapability, LoaderConfig, LoaderResult

class MyEntityLoader(BaseLoader):
    def __init__(self):
        super().__init__(
            name="my_entity_loader",
            description="Loads MyEntity data from TK API"
        )
        
        # Declare capabilities
        self._capabilities = [
            LoaderCapability.DATE_FILTERING,
            LoaderCapability.THREADING,
            LoaderCapability.SKIP_FUNCTIONALITY
        ]
    
    def load(self, conn: Any, config: LoaderConfig, 
             checkpoint_manager: Optional[Any] = None) -> LoaderResult:
        """Implementation of the main loading logic"""
        start_time = time.time()
        
        try:
            # Validate configuration
            errors = self.validate_config(config)
            if errors:
                return LoaderResult(
                    success=False,
                    processed_count=0,
                    failed_count=0,
                    skipped_count=0,
                    total_items=0,
                    execution_time_seconds=time.time() - start_time,
                    error_messages=errors,
                    warnings=[],
                    metadata={"validation_failed": True}
                )
            
            # Your loading logic here
            # ...
            
            return LoaderResult(
                success=True,
                processed_count=processed,
                failed_count=failed,
                skipped_count=config.skip_count,
                total_items=total,
                execution_time_seconds=time.time() - start_time,
                error_messages=[],
                warnings=[],
                metadata={"processing_mode": "modern"}
            )
            
        except Exception as e:
            return LoaderResult(
                success=False,
                processed_count=0,
                failed_count=0,
                skipped_count=0,
                total_items=0,
                execution_time_seconds=time.time() - start_time,
                error_messages=[str(e)],
                warnings=[],
                metadata={"exception": str(e)}
            )
```

### Registering Loaders

```python
from src.core.interfaces import loader_registry

# Register your loader
my_loader = MyEntityLoader()
loader_registry.register(my_loader, order=1)

# Get loader by name
loader = loader_registry.get_loader("my_entity_loader")

# Get loaders by capability
threaded_loaders = loader_registry.get_loaders_by_capability(LoaderCapability.THREADING)
```

### Using the Interface

```python
# Create configuration
config = LoaderConfig(
    start_date="2024-01-01",
    enable_threading=True,
    max_workers=10,
    skip_count=1000
)

# Get and run loader
loader = loader_registry.get_loader("my_entity_loader")
result = loader.load(conn, config, checkpoint_manager)

# Check results
if result.success:
    print(f"✅ Processed {result.processed_count} items in {result.execution_time_seconds:.2f}s")
else:
    print(f"❌ Failed: {', '.join(result.error_messages)}")
```

## 🔄 Migration Strategy

### Phase 1: Interface Introduction (Current)
- ✅ Create interface definitions
- ✅ Add example modern loader
- ✅ Document patterns and benefits

### Phase 2: Gradual Migration
- 🔄 Create compatibility adapters for existing loaders
- 🔄 Migrate one loader at a time to new interface
- 🔄 Maintain backward compatibility

### Phase 3: Modernization
- ⏳ Update main.py to use registry system
- ⏳ Standardize all configuration handling
- ⏳ Remove legacy compatibility code

### Phase 4: Advanced Features
- ⏳ Add loader dependency management
- ⏳ Implement loader composition patterns
- ⏳ Add advanced monitoring and metrics

## 🎯 Compatibility Strategy

### Adapter Pattern

Create adapters that allow modern loaders to work with existing systems:

```python
def create_compatibility_adapter(modern_loader: BaseLoader):
    """Convert modern loader to legacy function signature"""
    def adapter_function(conn, start_date_str="2024-01-01", skip_count=0, 
                        max_workers=10, enable_threading=False, checkpoint_manager=None):
        
        config = LoaderConfig(
            start_date=start_date_str,
            skip_count=skip_count,
            max_workers=max_workers,
            enable_threading=enable_threading
        )
        
        result = modern_loader.load(conn, config, checkpoint_manager)
        
        # Convert result to legacy format
        if result.success:
            print(f"✅ {modern_loader.name} completed successfully!")
        else:
            print(f"❌ {modern_loader.name} failed!")
            for error in result.error_messages:
                print(f"   Error: {error}")
        
        return result.success
    
    return adapter_function
```

### Decorator Bridge

Bridge modern loaders with existing decorator system:

```python
def modern_loader_decorator(modern_loader: BaseLoader):
    """Decorator that bridges modern loader with checkpoint system"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Extract parameters and convert to LoaderConfig
            config = LoaderConfig(
                start_date=kwargs.get('start_date_str', '2024-01-01'),
                skip_count=kwargs.get('skip_count', 0),
                # ... other mappings
            )
            
            # Run modern loader
            result = modern_loader.load(args[0], config, kwargs.get('checkpoint_manager'))
            
            # Handle result according to decorator expectations
            return result.success
        
        return wrapper
    return decorator
```

## 🧪 Testing Modern Loaders

### Unit Testing

```python
import unittest
from src.core.interfaces import LoaderConfig, LoaderCapability

class TestMyEntityLoader(unittest.TestCase):
    def setUp(self):
        self.loader = MyEntityLoader()
        self.config = LoaderConfig(start_date="2024-01-01")
    
    def test_capabilities(self):
        """Test that loader declares correct capabilities"""
        capabilities = self.loader.get_capabilities()
        self.assertIn(LoaderCapability.DATE_FILTERING, capabilities)
        self.assertIn(LoaderCapability.THREADING, capabilities)
    
    def test_config_validation(self):
        """Test configuration validation"""
        # Valid config
        errors = self.loader.validate_config(self.config)
        self.assertEqual(len(errors), 0)
        
        # Invalid config
        invalid_config = LoaderConfig(start_date="invalid-date")
        errors = self.loader.validate_config(invalid_config)
        self.assertGreater(len(errors), 0)
    
    def test_load_success(self):
        """Test successful loading"""
        result = self.loader.load(mock_conn, self.config)
        self.assertTrue(result.success)
        self.assertGreater(result.processed_count, 0)
```

### Integration Testing

```python
def test_loader_registry_integration():
    """Test loader registry functionality"""
    # Register loader
    loader = MyEntityLoader()
    loader_registry.register(loader)
    
    # Retrieve loader
    retrieved = loader_registry.get_loader("my_entity_loader")
    assert retrieved is not None
    assert retrieved.name == loader.name
    
    # Test capability filtering
    threaded_loaders = loader_registry.get_loaders_by_capability(LoaderCapability.THREADING)
    assert loader in threaded_loaders
```

## 📊 Benefits Analysis

### Before Interfaces
```python
# Inconsistent signatures
def load_zaken(conn, batch_size=50, start_date_str="2024-01-01", skip_count=0, _checkpoint_context=None):
def load_documents(conn, start_date_str="2024-01-01", skip_count=0, _checkpoint_context=None):
def load_activiteiten(conn, start_date_str="2024-01-01", skip_count=0, overwrite=False, _checkpoint_context=None):

# No capability discovery
# Hard to determine what each loader supports

# Mixed error handling
# Different result formats
```

### After Interfaces
```python
# Consistent interface
all_loaders = loader_registry.get_all_loaders()
for name, loader in all_loaders.items():
    config = LoaderConfig(start_date="2024-01-01", enable_threading=True)
    
    # Check if threading is supported
    if loader.supports_capability(LoaderCapability.THREADING):
        config.enable_threading = True
    
    # Validate configuration
    errors = loader.validate_config(config)
    if not errors:
        result = loader.load(conn, config, checkpoint_manager)
        print(f"{name}: {result.processed_count} items in {result.execution_time_seconds:.2f}s")
```

## 🚀 Future Enhancements

### Loader Composition
```python
class CompositeLoader(BaseLoader):
    """Loader that combines multiple loaders"""
    
    def __init__(self, loaders: List[BaseLoader]):
        self.loaders = loaders
        # Capabilities are union of all loader capabilities
        self._capabilities = list(set().union(*[l.get_capabilities() for l in loaders]))
```

### Dependency Management
```python
class LoaderDependency:
    """Define dependencies between loaders"""
    
    def __init__(self, loader_name: str, depends_on: List[str]):
        self.loader_name = loader_name
        self.depends_on = depends_on

# Usage
dependencies = [
    LoaderDependency("load_documents", ["load_zaken"]),
    LoaderDependency("load_activiteiten", ["load_vergaderingen"])
]
```

### Advanced Monitoring
```python
class LoaderMetrics:
    """Collect and analyze loader performance"""
    
    def record_execution(self, loader_name: str, result: LoaderResult):
        # Record metrics for analysis
        pass
    
    def get_performance_report(self) -> Dict[str, Any]:
        # Return performance analysis
        pass
```

---

**Related Documentation:**
- [Main README](../README.md) - Project overview
- [Checkpoint System](checkpoint-system.md) - Checkpoint functionality
- [Data Loaders](../src/loaders/README.md) - Current loader documentation 