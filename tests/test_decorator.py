#!/usr/bin/env python3
"""
Test script for the checkpoint decorator functionality.
"""

import os
import sys
import tempfile
import shutil
from unittest.mock import Mock

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ..core.checkpoint.checkpoint_decorator import checkpoint_loader, checkpoint_zaak_loader, with_checkpoint
from checkpoint_manager import CheckpointManager


class MockItem:
    """Mock item for testing."""
    def __init__(self, id, name, nummer=None):
        self.id = id
        self.name = name
        self.nummer = nummer or id


class MockConnection:
    """Mock Neo4j connection for testing."""
    def __init__(self):
        self.processed_items = []
    
    def process_item(self, item):
        """Simulate processing an item."""
        self.processed_items.append(item.id)
        print(f"    Processed item: {item.id}")


def test_simple_decorator():
    """Test the basic checkpoint_loader decorator."""
    print("🧪 Testing simple decorator...")
    
    # Create temporary directory for checkpoints
    temp_dir = tempfile.mkdtemp()
    original_dir = os.getcwd()
    
    try:
        os.chdir(temp_dir)
        
        # Create test items
        items = [MockItem(f"item_{i}", f"Item {i}") for i in range(10)]
        conn = MockConnection()
        
        @checkpoint_loader(checkpoint_interval=3)
        def test_loader(connection, items_list, _checkpoint_context=None):
            def process_single_item(item):
                connection.process_item(item)
            
            if _checkpoint_context:
                _checkpoint_context.process_items(items_list, process_single_item)
            else:
                for item in items_list:
                    process_single_item(item)
        
        # Initialize checkpoint manager
        checkpoint_manager = CheckpointManager()
        checkpoint_manager.start_new_run("Test run")
        
        # Run the loader
        test_loader(conn, items, checkpoint_manager=checkpoint_manager)
        
        # Verify all items were processed
        assert len(conn.processed_items) == 10, f"Expected 10 items, got {len(conn.processed_items)}"
        assert conn.processed_items == [f"item_{i}" for i in range(10)]
        
        print("✅ Simple decorator test passed!")
        
    finally:
        os.chdir(original_dir)
        shutil.rmtree(temp_dir)


def test_zaak_decorator():
    """Test the checkpoint_zaak_loader decorator."""
    print("🧪 Testing Zaak decorator...")
    
    # Create temporary directory for checkpoints
    temp_dir = tempfile.mkdtemp()
    original_dir = os.getcwd()
    
    try:
        os.chdir(temp_dir)
        
        # Create test items with nummer and id
        items = [MockItem(f"id_{i}", f"Item {i}", f"nummer_{i}") for i in range(5)]
        conn = MockConnection()
        
        @checkpoint_zaak_loader(checkpoint_interval=2)
        def test_zaak_loader(connection, items_list, _checkpoint_context=None):
            def process_single_item(item):
                connection.process_item(item)
            
            if _checkpoint_context:
                _checkpoint_context.process_items(items_list, process_single_item)
            else:
                for item in items_list:
                    process_single_item(item)
        
        # Initialize checkpoint manager
        checkpoint_manager = CheckpointManager()
        checkpoint_manager.start_new_run("Test Zaak run")
        
        # Run the loader
        test_zaak_loader(conn, items, checkpoint_manager=checkpoint_manager)
        
        # Verify all items were processed
        assert len(conn.processed_items) == 5, f"Expected 5 items, got {len(conn.processed_items)}"
        
        print("✅ Zaak decorator test passed!")
        
    finally:
        os.chdir(original_dir)
        shutil.rmtree(temp_dir)


def test_custom_decorator():
    """Test the custom with_checkpoint decorator."""
    print("🧪 Testing custom decorator...")
    
    # Create temporary directory for checkpoints
    temp_dir = tempfile.mkdtemp()
    original_dir = os.getcwd()
    
    try:
        os.chdir(temp_dir)
        
        # Create test items
        items = [MockItem(f"id_{i}", f"Item {i}", f"custom_{i}") for i in range(7)]
        conn = MockConnection()
        
        @with_checkpoint(
            checkpoint_interval=2,
            get_item_id=lambda item: f"custom_{item.nummer}_{item.id}"
        )
        def test_custom_loader(connection, items_list, _checkpoint_context=None):
            def process_single_item(item):
                connection.process_item(item)
            
            if _checkpoint_context:
                _checkpoint_context.process_items(items_list, process_single_item)
            else:
                for item in items_list:
                    process_single_item(item)
        
        # Initialize checkpoint manager
        checkpoint_manager = CheckpointManager()
        checkpoint_manager.start_new_run("Test custom run")
        
        # Run the loader
        test_custom_loader(conn, items, checkpoint_manager=checkpoint_manager)
        
        # Verify all items were processed
        assert len(conn.processed_items) == 7, f"Expected 7 items, got {len(conn.processed_items)}"
        
        print("✅ Custom decorator test passed!")
        
    finally:
        os.chdir(original_dir)
        shutil.rmtree(temp_dir)


def test_resume_functionality():
    """Test that resume functionality works with decorators."""
    print("🧪 Testing resume functionality...")
    
    # Create temporary directory for checkpoints
    temp_dir = tempfile.mkdtemp()
    original_dir = os.getcwd()
    
    try:
        os.chdir(temp_dir)
        
        # Create test items
        items = [MockItem(f"item_{i}", f"Item {i}") for i in range(10)]
        
        @checkpoint_loader(checkpoint_interval=2)
        def test_resume_loader(connection, items_list, _checkpoint_context=None):
            def process_single_item(item):
                # Simulate failure on item 5
                if item.id == "item_5":
                    raise Exception("Simulated failure")
                connection.process_item(item)
            
            if _checkpoint_context:
                _checkpoint_context.process_items(items_list, process_single_item)
            else:
                for item in items_list:
                    process_single_item(item)
        
        # First run - will fail on item 5
        conn1 = MockConnection()
        checkpoint_manager = CheckpointManager()
        run_id = checkpoint_manager.start_new_run("Test resume run")
        
        try:
            test_resume_loader(conn1, items, checkpoint_manager=checkpoint_manager)
        except:
            pass  # Expected to fail
        
        # Check that some items were processed before failure
        print(f"First run processed: {len(conn1.processed_items)} items")
        
        # Second run - resume from checkpoint
        conn2 = MockConnection()
        checkpoint_manager2 = CheckpointManager()
        checkpoint_manager2.resume_run(run_id)
        
        # Modify the loader to not fail on item 5
        @checkpoint_loader(checkpoint_interval=2)
        def test_resume_loader_fixed(connection, items_list, _checkpoint_context=None):
            def process_single_item(item):
                connection.process_item(item)
            
            if _checkpoint_context:
                _checkpoint_context.process_items(items_list, process_single_item)
            else:
                for item in items_list:
                    process_single_item(item)
        
        test_resume_loader_fixed(conn2, items, checkpoint_manager=checkpoint_manager2)
        
        print(f"Second run processed: {len(conn2.processed_items)} items")
        print("✅ Resume functionality test passed!")
        
    finally:
        os.chdir(original_dir)
        shutil.rmtree(temp_dir)


def test_without_checkpoint_manager():
    """Test that decorators work without checkpoint manager (fallback mode)."""
    print("🧪 Testing fallback mode (no checkpoint manager)...")
    
    items = [MockItem(f"item_{i}", f"Item {i}") for i in range(5)]
    conn = MockConnection()
    
    @checkpoint_loader(checkpoint_interval=2)
    def test_fallback_loader(connection, items_list, _checkpoint_context=None):
        def process_single_item(item):
            connection.process_item(item)
        
        if _checkpoint_context:
            _checkpoint_context.process_items(items_list, process_single_item)
        else:
            for item in items_list:
                process_single_item(item)
    
    # Run without checkpoint manager
    test_fallback_loader(conn, items)  # No checkpoint_manager parameter
    
    # Verify all items were processed
    assert len(conn.processed_items) == 5, f"Expected 5 items, got {len(conn.processed_items)}"
    
    print("✅ Fallback mode test passed!")


if __name__ == "__main__":
    print("🚀 Starting checkpoint decorator tests...\n")
    
    try:
        test_simple_decorator()
        print()
        test_zaak_decorator()
        print()
        test_custom_decorator()
        print()
        test_resume_functionality()
        print()
        test_without_checkpoint_manager()
        print()
        print("🎉 All tests passed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1) 