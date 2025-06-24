"""
Example of how to use the checkpoint decorator for simple loaders.
This shows how much cleaner and easier the code becomes.
"""

from core.checkpoint.checkpoint_decorator import checkpoint_loader, with_checkpoint
from core.connection.neo4j_connection import Neo4jConnection


@checkpoint_loader(checkpoint_interval=25)
def load_simple_items(conn: Neo4jConnection, items, _checkpoint_context=None):
    """
    Example of a simple loader using the checkpoint decorator.
    
    The decorator automatically handles:
    - Progress tracking every 25 items
    - Skipping already processed items
    - Error handling and logging
    - Final progress reporting
    """
    
    def process_single_item(item):
        # Your processing logic here
        with conn.driver.session() as session:
            # Process the item
            session.execute_write(
                lambda tx: tx.run("MERGE (n:Item {id: $id, name: $name})", 
                                id=item.id, name=item.name)
            )
    
    # The checkpoint context automatically handles the loop, error handling, and progress tracking
    if _checkpoint_context:
        _checkpoint_context.process_items(items, process_single_item)
    else:
        # Fallback when decorator is not used
        for item in items:
            process_single_item(item)


@with_checkpoint(
    checkpoint_interval=10,  # Custom interval
    get_item_id=lambda item: f"custom_{item.nummer}_{item.id}"  # Custom ID function
)
def load_custom_items(conn: Neo4jConnection, items, _checkpoint_context=None):
    """
    Example with custom checkpoint configuration.
    """
    
    def process_single_item(item):
        # Your custom processing logic
        print(f"Processing custom item: {item.nummer}")
        # ... processing code ...
    
    if _checkpoint_context:
        _checkpoint_context.process_items(items, process_single_item)


# Example of manual checkpoint handling for complex scenarios
@checkpoint_loader(checkpoint_interval=25)
def load_complex_items(conn: Neo4jConnection, items, _checkpoint_context=None):
    """
    Example showing manual checkpoint handling for complex scenarios.
    """
    if _checkpoint_context:
        _checkpoint_context.set_total_items(items)
    
    for i, item in enumerate(items, 1):
        # Check if already processed
        if _checkpoint_context and _checkpoint_context.is_processed(item):
            continue
        
        try:
            # Your complex processing logic here
            with conn.driver.session() as session:
                # Complex processing that might involve multiple steps
                result1 = process_step_1(session, item)
                result2 = process_step_2(session, item, result1)
                finalize_processing(session, item, result2)
            
            # Mark as processed
            if _checkpoint_context:
                _checkpoint_context.mark_processed(item)
            
            # Manual progress saving
            if _checkpoint_context:
                _checkpoint_context.save_progress_if_needed(i)
                
        except Exception as e:
            error_msg = f"Failed to process complex item {item.id}: {str(e)}"
            print(f"❌ {error_msg}")
            
            if _checkpoint_context:
                _checkpoint_context.mark_failed(item, error_msg)
            
            # Continue with next item
            continue


def process_step_1(session, item):
    # Dummy processing step
    return f"step1_result_for_{item.id}"

def process_step_2(session, item, result1):
    # Dummy processing step
    return f"step2_result_for_{item.id}_with_{result1}"

def finalize_processing(session, item, result2):
    # Dummy finalization step
    pass


# Usage example:
if __name__ == "__main__":
    # Example of how to use these loaders
    from core.checkpoint.checkpoint_manager import CheckpointManager
    
    # Mock items for demonstration
    class MockItem:
        def __init__(self, id, name, nummer=None):
            self.id = id
            self.name = name
            self.nummer = nummer or id
    
    items = [MockItem(f"item_{i}", f"Item {i}") for i in range(100)]
    
    # Initialize checkpoint manager
    checkpoint_manager = CheckpointManager()
    checkpoint_manager.start_new_run("Example loader run")
    
    # Create connection (mock)
    conn = Neo4jConnection()
    
    try:
        # Load items with automatic checkpoint support
        load_simple_items(conn, items, checkpoint_manager=checkpoint_manager)
        
        # The decorator automatically handles:
        # - Progress tracking every 25 items
        # - Skipping already processed items if resuming
        # - Error handling and logging
        # - Final progress reporting
        
    finally:
        conn.close() 