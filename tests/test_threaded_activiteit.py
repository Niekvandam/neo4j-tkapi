#!/usr/bin/env python3
"""
Test script for the threaded activiteit loader.
This demonstrates the faster processing capabilities with multithreading.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from ..core.connection.neo4j_connection import Neo4jConnection
from loaders.activiteit_loader import load_activiteiten_threaded, load_activiteiten
from checkpoint_manager import CheckpointManager
import time

def main():
    print("🧪 Testing Threaded Activiteit Loader")
    print("=" * 50)
    
    # Initialize connections
    conn = Neo4jConnection()
    checkpoint_manager = CheckpointManager()
    
    try:
        # Test parameters
        start_date = "2024-01-01"
        max_workers = 10
        skip_count = 18000  # Skip first 18,000 items as requested
        
        print(f"📅 Start date: {start_date}")
        print(f"🧵 Max workers: {max_workers}")
        print(f"⏭️ Skip count: {skip_count}")
        print()
        
        # Test the threaded version
        print("🚀 Testing threaded version...")
        start_time = time.time()
        
        load_activiteiten_threaded(
            conn=conn,
            start_date_str=start_date,
            max_workers=max_workers,
            skip_count=skip_count,
            checkpoint_manager=checkpoint_manager
        )
        
        threaded_time = time.time() - start_time
        print(f"⏱️ Threaded version completed in: {threaded_time:.2f} seconds")
        print()
        
        # Optional: Compare with single-threaded version
        # Uncomment the following lines if you want to compare performance
        """
        print("🐌 Testing single-threaded version for comparison...")
        start_time = time.time()
        
        load_activiteiten(
            conn=conn,
            start_date_str=start_date
        )
        
        single_time = time.time() - start_time
        print(f"⏱️ Single-threaded version completed in: {single_time:.2f} seconds")
        
        if single_time > 0:
            speedup = single_time / threaded_time
            print(f"🚀 Speedup: {speedup:.2f}x faster with threading")
        """
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        conn.close()
        print("🔌 Connection closed")

if __name__ == "__main__":
    main() 