#!/usr/bin/env python3
"""
Test script for threaded Personen loading
"""
import time
from core.connection.neo4j_connection import Neo4jConnection
from core.interfaces import LoaderConfig
from src.loaders.persoon_loader import PersoonLoader, load_personen_threaded

def test_threaded_personen_loading():
    """Test the new threaded personen loading functionality"""
    
    # Create connection
    conn = Neo4jConnection()
    
    # Test 1: Small batch with threading
    print("🧪 Test 1: Loading 50 Personen with threading (10 workers)")
    print("=" * 60)
    
    start_time = time.time()
    stats = load_personen_threaded(
        conn=conn,
        batch_size=50,
        max_workers=10,
        skip_count=0,
        overwrite=False
    )
    
    elapsed_time = time.time() - start_time
    
    print(f"\n📊 Threading Results:")
    print(f"   • Processed: {stats['processed']}")
    print(f"   • Failed: {stats['failed']}")
    print(f"   • Skipped: {stats['skipped']}")
    print(f"   • Total: {stats['total']}")
    print(f"   • Time: {elapsed_time:.2f} seconds")
    print(f"   • Rate: {stats['processed'] / elapsed_time:.2f} items/sec")
    
    # Test 2: Using the loader interface with threading
    print("\n🧪 Test 2: Using PersoonLoader interface with threading")
    print("=" * 60)
    
    loader = PersoonLoader()
    config = LoaderConfig(
        batch_size=30,
        enable_threading=True,
        max_workers=8,
        skip_count=0,
        custom_params={'overwrite': False}
    )
    
    start_time = time.time()
    result = loader.load(conn, config)
    elapsed_time = time.time() - start_time
    
    print(f"\n📊 Interface Results:")
    print(f"   • Success: {result.success}")
    print(f"   • Processed: {result.processed_count}")
    print(f"   • Failed: {result.failed_count}")
    print(f"   • Skipped: {result.skipped_count}")
    print(f"   • Total: {result.total_items}")
    print(f"   • Time: {elapsed_time:.2f} seconds")
    if result.error_messages:
        print(f"   • Errors: {result.error_messages}")
    
    # Test 3: Compare threading vs non-threading
    print("\n🧪 Test 3: Comparing threaded vs non-threaded performance")
    print("=" * 60)
    
    config_no_threading = LoaderConfig(
        batch_size=20,
        enable_threading=False,
        skip_count=0,
        custom_params={'overwrite': True}  # Force reprocessing
    )
    
    print("Running non-threaded version...")
    start_time = time.time()
    result_no_threading = loader.load(conn, config_no_threading)
    time_no_threading = time.time() - start_time
    
    config_threading = LoaderConfig(
        batch_size=20,
        enable_threading=True,
        max_workers=5,
        skip_count=0,
        custom_params={'overwrite': True}  # Force reprocessing
    )
    
    print("Running threaded version...")
    start_time = time.time()
    result_threading = loader.load(conn, config_threading)
    time_threading = time.time() - start_time
    
    print(f"\n📊 Performance Comparison:")
    print(f"   • Non-threaded: {time_no_threading:.2f}s")
    print(f"   • Threaded: {time_threading:.2f}s")
    if time_no_threading > 0:
        speedup = time_no_threading / time_threading
        print(f"   • Speedup: {speedup:.2f}x")
    
    print("\n✅ All tests completed!")
    conn.close()

if __name__ == "__main__":
    test_threaded_personen_loading() 