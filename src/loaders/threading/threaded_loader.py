"""
Reusable threaded loading utilities for TK API loaders
"""
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from core.checkpoint.checkpoint_manager import LoaderCheckpoint, CheckpointManager
from utils.helpers import batch_check_nodes_exist

# Global thread-safe counters
_thread_lock = threading.Lock()
_processed_count = 0
_failed_count = 0


class SimpleCheckpointContext:
    """Simple checkpoint context for thread compatibility"""
    
    def __init__(self, checkpoint_obj):
        self.checkpoint = checkpoint_obj
        
    def mark_processed(self, item):
        if self.checkpoint:
            self.checkpoint.mark_processed(item.id)
            
    def mark_failed(self, item, error_msg):
        if self.checkpoint:
            self.checkpoint.mark_failed(item.id, error_msg)


def reset_counters():
    """Reset global thread counters"""
    global _processed_count, _failed_count
    with _thread_lock:
        _processed_count = 0
        _failed_count = 0


def update_processed_count():
    """Thread-safe increment of processed count"""
    global _processed_count
    with _thread_lock:
        _processed_count += 1


def update_failed_count():
    """Thread-safe increment of failed count"""
    global _failed_count
    with _thread_lock:
        _failed_count += 1


def get_counts():
    """Get current thread-safe counts"""
    with _thread_lock:
        return _processed_count, _failed_count


def process_items_threaded(items, process_func, conn, max_workers=10, 
                          checkpoint_manager=None, loader_name="unknown",
                          skip_count=0, overwrite=False, node_label=None):
    """
    Generic threaded processor for API items.
    
    Args:
        items: List of items to process
        process_func: Function to process each item (item, conn, checkpoint_context)
        conn: Neo4j connection
        max_workers: Number of threads to use
        checkpoint_manager: Optional checkpoint manager
        loader_name: Name for checkpoint identification
        skip_count: Number of items to skip from beginning
        overwrite: Whether to overwrite existing items
        node_label: Neo4j node label for existence checking
    
    Returns:
        dict: Processing statistics
    """
    reset_counters()
    
    # Initialize checkpoint if provided
    checkpoint = None
    checkpoint_context = None
    if checkpoint_manager:
        checkpoint = LoaderCheckpoint(checkpoint_manager, loader_name)
        checkpoint.set_total_items(len(items))
        checkpoint_context = SimpleCheckpointContext(checkpoint)

    # Apply skip_count if specified
    if skip_count > 0:
        if skip_count >= len(items):
            print(f"⚠️ Skip count ({skip_count}) is greater than or equal to total items ({len(items)}). Nothing to process.")
            return {"processed": 0, "failed": 0, "skipped": len(items), "total": len(items)}
        items = items[skip_count:]
        print(f"⏭️ Skipping first {skip_count} items. Processing {len(items)} remaining items.")

    # Check which items already exist in Neo4j (unless overwrite is enabled)
    if not overwrite and items and node_label:
        print(f"🔍 Checking which {node_label} already exist in Neo4j...")
        item_ids = [item.id for item in items if item and item.id]
        
        with conn.driver.session(database=conn.database) as session:
            existing_ids = batch_check_nodes_exist(session, node_label, "id", item_ids)
        
        if existing_ids:
            # Filter out existing items
            original_count = len(items)
            items = [item for item in items if item.id not in existing_ids]
            filtered_count = len(items)
            print(f"📊 Found {len(existing_ids)} existing {node_label} in Neo4j")
            print(f"⏭️ Skipping {original_count - filtered_count} existing items. Processing {filtered_count} new items.")
            
            if not items:
                print(f"✅ All {node_label} already exist in Neo4j. Nothing to process.")
                return {"processed": 0, "failed": 0, "skipped": original_count, "total": original_count}
        else:
            print(f"📊 No existing {node_label} found in Neo4j. Processing all items.")
    elif overwrite:
        print("🔄 Overwrite mode enabled - processing all items regardless of existing data")

    # Filter out already processed items if checkpoint exists
    items_to_process = []
    if checkpoint:
        for item in items:
            if not checkpoint.is_processed(item.id):
                items_to_process.append(item)
        print(f"→ {len(items_to_process)} items remaining to process (skipped {len(items) - len(items_to_process)} already processed)")
    else:
        items_to_process = items

    if not items_to_process:
        print("✅ No items to process.")
        return {"processed": 0, "failed": 0, "skipped": len(items), "total": len(items)}

    print(f"🚀 Starting threaded processing with {max_workers} workers...")
    start_time = time.time()

    # Process with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(process_func, item, conn, checkpoint_context): item 
            for item in items_to_process
        }
        
        # Process completed tasks and show progress
        completed = 0
        for future in as_completed(futures):
            completed += 1
            item = futures[future]
            
            try:
                success = future.result()
                if success:
                    update_processed_count()
                else:
                    update_failed_count()
                    
                if completed % 25 == 0:  # Progress update every 25 items
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    processed, failed = get_counts()
                    print(f"    📊 Progress: {completed}/{len(items_to_process)} ({completed/len(items_to_process)*100:.1f}%) - Rate: {rate:.1f} items/sec - Success: {processed}, Failed: {failed}")
                    
                    # Save checkpoint progress
                    if checkpoint:
                        checkpoint.save_progress()
                        
            except Exception as e:
                update_failed_count()
                print(f"    ❌ Unexpected error processing {getattr(item, 'id', 'unknown')}: {e}")

    # Final statistics
    elapsed_time = time.time() - start_time
    avg_rate = len(items_to_process) / elapsed_time if elapsed_time > 0 else 0
    processed, failed = get_counts()
    
    print(f"✅ Completed threaded processing!")
    print(f"📊 Final Stats:")
    print(f"   • Total processed: {processed}")
    print(f"   • Failed: {failed}")
    print(f"   • Time elapsed: {elapsed_time:.2f} seconds")
    print(f"   • Average rate: {avg_rate:.2f} items/second")
    
    # Final checkpoint save
    if checkpoint:
        checkpoint.save_progress()
        stats = checkpoint.get_progress_stats()
        print(f"📊 Checkpoint Stats: {stats['processed_count']}/{stats['total_items']} ({stats['completion_percentage']:.1f}%)")
    
    return {
        "processed": processed,
        "failed": failed,
        "skipped": skip_count,
        "total": len(items) + skip_count,
        "elapsed_time": elapsed_time,
        "avg_rate": avg_rate
    } 