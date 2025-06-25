"""
Interface definitions for the Neo4j TK API data loader system.

This module defines abstract base classes and protocols that all loaders should implement
to ensure consistency and enable future extensibility.
"""

from abc import ABC, abstractmethod
from typing import Protocol, Optional, List, Any, Dict
from dataclasses import dataclass
from enum import Enum


class LoaderCapability(Enum):
    """Capabilities that a loader can support"""
    THREADING = "threading"
    BATCH_PROCESSING = "batch_processing"
    DATE_FILTERING = "date_filtering"
    SKIP_FUNCTIONALITY = "skip_functionality"
    ID_CHECKING = "id_checking"
    INCREMENTAL_LOADING = "incremental_loading"
    RELATIONSHIP_PROCESSING = "relationship_processing"


@dataclass
class LoaderConfig:
    """Standard configuration for all loaders"""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    batch_size: int = 50
    skip_count: int = 0
    max_workers: int = 10
    checkpoint_interval: int = 25
    overwrite_existing: bool = False
    enable_threading: bool = False
    enable_id_checking: bool = True
    custom_params: Optional[Dict[str, Any]] = None


@dataclass
class LoaderResult:
    """Standard result object for loader operations"""
    success: bool
    processed_count: int
    failed_count: int
    skipped_count: int
    total_items: int
    execution_time_seconds: float
    error_messages: List[str]
    warnings: List[str]
    metadata: Optional[Dict[str, Any]] = None


class LoaderProtocol(Protocol):
    """Protocol that all loaders should implement"""
    
    def load(self, conn: Any, config: LoaderConfig, 
             checkpoint_manager: Optional[Any] = None) -> LoaderResult:
        """Main loading method that all loaders must implement"""
        ...
    
    def get_capabilities(self) -> List[LoaderCapability]:
        """Return list of capabilities this loader supports"""
        ...
    
    def validate_config(self, config: LoaderConfig) -> List[str]:
        """Validate configuration and return list of errors"""
        ...


class BaseLoader(ABC):
    """Abstract base class for all loaders"""
    
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self._capabilities: List[LoaderCapability] = []
    
    @abstractmethod
    def load(self, conn: Any, config: LoaderConfig, 
             checkpoint_manager: Optional[Any] = None) -> LoaderResult:
        """Main loading method - must be implemented by subclasses"""
        pass
    
    def get_capabilities(self) -> List[LoaderCapability]:
        """Return list of capabilities this loader supports"""
        return self._capabilities.copy()
    
    def supports_capability(self, capability: LoaderCapability) -> bool:
        """Check if loader supports a specific capability"""
        return capability in self._capabilities
    
    def validate_config(self, config: LoaderConfig) -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []
        
        # Basic validation
        if config.batch_size <= 0:
            errors.append("batch_size must be positive")
        if config.skip_count < 0:
            errors.append("skip_count cannot be negative")
        if config.max_workers <= 0:
            errors.append("max_workers must be positive")
        if config.checkpoint_interval <= 0:
            errors.append("checkpoint_interval must be positive")
            
        # Date validation
        if config.start_date:
            try:
                from datetime import datetime
                datetime.strptime(config.start_date, "%Y-%m-%d")
            except ValueError:
                errors.append("start_date must be in YYYY-MM-DD format")
                
        if config.end_date:
            try:
                from datetime import datetime
                datetime.strptime(config.end_date, "%Y-%m-%d")
            except ValueError:
                errors.append("end_date must be in YYYY-MM-DD format")
        
        # Threading validation
        if config.enable_threading and not self.supports_capability(LoaderCapability.THREADING):
            errors.append(f"Loader {self.name} does not support threading")
            
        return errors


class LoaderRegistry:
    """Registry for managing all available loaders"""
    
    def __init__(self):
        self._loaders: Dict[str, BaseLoader] = {}
        self._loader_order: List[str] = []
    
    def register(self, loader: BaseLoader, order: Optional[int] = None):
        """Register a loader with optional execution order"""
        self._loaders[loader.name] = loader
        
        if order is not None:
            if order < len(self._loader_order):
                self._loader_order.insert(order, loader.name)
            else:
                self._loader_order.append(loader.name)
        else:
            self._loader_order.append(loader.name)
    
    def get_loader(self, name: str) -> Optional[BaseLoader]:
        """Get a loader by name"""
        return self._loaders.get(name)
    
    def get_all_loaders(self) -> Dict[str, BaseLoader]:
        """Get all registered loaders"""
        return self._loaders.copy()
    
    def get_loaders_by_capability(self, capability: LoaderCapability) -> List[BaseLoader]:
        """Get all loaders that support a specific capability"""
        return [loader for loader in self._loaders.values() 
                if loader.supports_capability(capability)]


# Global registry instance
loader_registry = LoaderRegistry() 