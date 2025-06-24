import functools
import traceback
from typing import Callable, Any, Optional, List, Union
from .checkpoint_manager import CheckpointManager, LoaderCheckpoint


def with_checkpoint(
    checkpoint_interval: int = 25,
    get_item_id: Optional[Callable[[Any], str]] = None,
    get_total_items: Optional[Callable[[List[Any]], int]] = None
):
    """
    Decorator that adds automatic checkpoint functionality to loader functions.
    
    Args:
        checkpoint_interval: How often to save progress (default: every 25 items)
        get_item_id: Function to extract unique ID from each item (default: uses item.id)
        get_total_items: Function to get total count from items list (default: len(items))
    
    Usage:
        @with_checkpoint(checkpoint_interval=25, get_item_id=lambda item: item.nummer)
        def load_items(conn, items, checkpoint_manager=None):
            # Your processing logic here
            for item in items:
                # Process item
                process_item(item)
                yield item  # Yield processed items to track progress
    """
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Extract checkpoint_manager from kwargs
            checkpoint_manager = kwargs.pop('checkpoint_manager', None)
            
            # Get the function name for checkpoint identification
            loader_name = func.__name__
            
            # Initialize checkpoint if provided
            checkpoint = None
            if checkpoint_manager:
                checkpoint = LoaderCheckpoint(checkpoint_manager, loader_name)
            
            try:
                # Call the original function with a special checkpoint context
                return _execute_with_checkpoint(
                    func, args, kwargs, checkpoint, checkpoint_interval, 
                    get_item_id, get_total_items, loader_name
                )
            except Exception as e:
                if checkpoint_manager:
                    error_message = f"{str(e)}\n{traceback.format_exc()}"
                    checkpoint_manager.mark_loader_failed(loader_name, error_message)
                raise
                
        return wrapper
    return decorator


def _execute_with_checkpoint(
    func: Callable, 
    args: tuple, 
    kwargs: dict, 
    checkpoint: Optional[LoaderCheckpoint],
    checkpoint_interval: int,
    get_item_id: Optional[Callable],
    get_total_items: Optional[Callable],
    loader_name: str
):
    """
    Execute function with checkpoint tracking.
    """
    # Default ID extractor
    if get_item_id is None:
        get_item_id = lambda item: getattr(item, 'id', str(item))
    
    # Default total items calculator
    if get_total_items is None:
        get_total_items = lambda items: len(items) if hasattr(items, '__len__') else 0
    
    # Add checkpoint context to kwargs
    checkpoint_context = CheckpointContext(
        checkpoint, checkpoint_interval, get_item_id, get_total_items, loader_name
    )
    kwargs['_checkpoint_context'] = checkpoint_context
    
    # Execute the original function
    result = func(*args, **kwargs)
    
    # Final progress save
    if checkpoint:
        checkpoint.save_progress()
        stats = checkpoint.get_progress_stats()
        print(f"📊 Final Progress for {loader_name}: {stats['processed_count']}/{stats['total_items']} ({stats['completion_percentage']:.1f}%)")
        if stats['failure_count'] > 0:
            print(f"⚠️ {stats['failure_count']} items failed during processing")
    
    return result


class CheckpointContext:
    """
    Context object that provides checkpoint functionality to loader functions.
    """
    
    def __init__(
        self, 
        checkpoint: Optional[LoaderCheckpoint],
        checkpoint_interval: int,
        get_item_id: Callable,
        get_total_items: Callable,
        loader_name: str
    ):
        self.checkpoint = checkpoint
        self.checkpoint_interval = checkpoint_interval
        self.get_item_id = get_item_id
        self.get_total_items = get_total_items
        self.loader_name = loader_name
        self.processed_count = 0
        self.failed_count = 0
    
    def set_total_items(self, items: Union[List[Any], int]):
        """Set the total number of items to process."""
        if self.checkpoint:
            total = items if isinstance(items, int) else self.get_total_items(items)
            self.checkpoint.set_total_items(total)
    
    def is_processed(self, item: Any) -> bool:
        """Check if an item has already been processed."""
        if not self.checkpoint:
            return False
        item_id = self.get_item_id(item)
        return self.checkpoint.is_processed(item_id)
    
    def mark_processed(self, item: Any):
        """Mark an item as successfully processed."""
        if self.checkpoint:
            item_id = self.get_item_id(item)
            self.checkpoint.mark_processed(item_id)
        self.processed_count += 1
    
    def mark_failed(self, item: Any, error_message: str):
        """Mark an item as failed."""
        if self.checkpoint:
            item_id = self.get_item_id(item)
            self.checkpoint.mark_failed(item_id, error_message)
        self.failed_count += 1
    
    def save_progress_if_needed(self, current_index: int):
        """Save progress if checkpoint interval is reached."""
        if self.checkpoint and current_index % self.checkpoint_interval == 0:
            self.checkpoint.save_progress()
            stats = self.checkpoint.get_progress_stats()
            print(f"    📊 Progress: {stats['processed_count']}/{stats['total_items']} ({stats['completion_percentage']:.1f}%)")
    
    def process_items(self, items: List[Any], process_func: Callable[[Any], None]):
        """
        Convenience method to process a list of items with automatic checkpoint handling.
        
        Args:
            items: List of items to process
            process_func: Function that processes a single item
        """
        self.set_total_items(items)
        
        for i, item in enumerate(items, 1):
            # Skip if already processed
            if self.is_processed(item):
                continue
            
            try:
                # Process the item
                process_func(item)
                
                # Mark as processed
                self.mark_processed(item)
                
                # Save progress if needed
                self.save_progress_if_needed(i)
                
            except Exception as e:
                error_msg = f"Failed to process item {self.get_item_id(item)}: {str(e)}"
                print(f"    ❌ {error_msg}")
                self.mark_failed(item, error_msg)
                # Continue with next item
                continue


# Convenience decorators for common patterns
def checkpoint_loader(checkpoint_interval: int = 25):
    """
    Simple decorator for loaders that process items with .id attribute.
    """
    return with_checkpoint(
        checkpoint_interval=checkpoint_interval,
        get_item_id=lambda item: item.id
    )


def checkpoint_zaak_loader(checkpoint_interval: int = 25):
    """
    Decorator specifically for Zaak loaders that use nummer + id as identifier.
    """
    return with_checkpoint(
        checkpoint_interval=checkpoint_interval,
        get_item_id=lambda item: f"{item.nummer}_{item.id}"
    ) 