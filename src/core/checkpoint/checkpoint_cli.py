#!/usr/bin/env python3
"""
Checkpoint Management CLI
Utility for managing data loading checkpoints
"""

import argparse
import json
from .checkpoint_manager import CheckpointManager
from datetime import datetime

def format_timestamp(timestamp_str):
    """Format ISO timestamp for display."""
    if not timestamp_str:
        return "Unknown"
    try:
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return timestamp_str

def list_runs(checkpoint_manager):
    """List all available runs with detailed information."""
    runs = checkpoint_manager.list_runs()
    
    if not runs:
        print("📋 No runs found")
        return
    
    print("\n📋 Available runs:")
    print("-" * 80)
    
    for run_info in runs:
        status_emoji = {
            "completed": "✅", 
            "running": "🔄", 
            "failed": "❌"
        }.get(run_info["status"], "❓")
        
        print(f"{status_emoji} {run_info['run_id']}")
        print(f"   Status: {run_info['status']}")
        print(f"   Started: {format_timestamp(run_info.get('start_time'))}")
        if run_info.get('description'):
            print(f"   Description: {run_info['description']}")
        
        # Load detailed progress if available
        try:
            checkpoint_manager.current_run_id = run_info['run_id']
            checkpoints = checkpoint_manager._load_checkpoints(run_info['run_id'])
            if checkpoints:
                print(f"   Loaders:")
                for loader_name, checkpoint_data in checkpoints.items():
                    loader_status = checkpoint_data.get('status', 'in_progress')
                    progress = checkpoint_data.get('progress', {})
                    
                    loader_emoji = {
                        "completed": "✅", 
                        "in_progress": "🔄", 
                        "failed": "❌"
                    }.get(loader_status, "❓")
                    
                    print(f"     {loader_emoji} {loader_name}: {loader_status}")
                    
                    if progress.get('processed_count') is not None:
                        processed = progress.get('processed_count', 0)
                        total = progress.get('total_items', 0)
                        percentage = (processed / total * 100) if total > 0 else 0
                        print(f"        Progress: {processed}/{total} ({percentage:.1f}%)")
                        
                        if progress.get('failure_count', 0) > 0:
                            print(f"        Failures: {progress['failure_count']}")
        except:
            pass  # Skip if can't load detailed info
        
        print("-" * 80)

def show_run_details(checkpoint_manager, run_id):
    """Show detailed information about a specific run."""
    if not checkpoint_manager._run_exists(run_id):
        print(f"❌ Run {run_id} not found")
        return
    
    checkpoint_manager.current_run_id = run_id
    run_info = checkpoint_manager._load_run_info()
    checkpoints = checkpoint_manager._load_checkpoints(run_id)
    
    print(f"\n📊 Run Details: {run_id}")
    print("=" * 60)
    
    print(f"Status: {run_info.get('status', 'unknown')}")
    print(f"Started: {format_timestamp(run_info.get('start_time'))}")
    if run_info.get('end_time'):
        print(f"Ended: {format_timestamp(run_info.get('end_time'))}")
    if run_info.get('description'):
        print(f"Description: {run_info['description']}")
    
    if checkpoints:
        print(f"\nLoaders ({len(checkpoints)}):")
        print("-" * 40)
        
        for loader_name, checkpoint_data in checkpoints.items():
            status = checkpoint_data.get('status', 'in_progress')
            progress = checkpoint_data.get('progress', {})
            
            status_emoji = {
                "completed": "✅", 
                "in_progress": "🔄", 
                "failed": "❌"
            }.get(status, "❓")
            
            print(f"{status_emoji} {loader_name}")
            print(f"   Status: {status}")
            print(f"   Last Updated: {format_timestamp(checkpoint_data.get('timestamp'))}")
            
            if progress:
                if progress.get('processed_count') is not None:
                    processed = progress.get('processed_count', 0)
                    total = progress.get('total_items', 0)
                    percentage = (processed / total * 100) if total > 0 else 0
                    print(f"   Progress: {processed}/{total} items ({percentage:.1f}%)")
                
                if progress.get('current_batch'):
                    print(f"   Current Batch: {progress['current_batch']}")
                
                if progress.get('failure_count', 0) > 0:
                    print(f"   Failures: {progress['failure_count']} items")
                    
                    # Show recent failures
                    failed_items = progress.get('failed_items', [])
                    if failed_items:
                        print(f"   Recent Failures:")
                        for failure in failed_items[-3:]:  # Show last 3 failures
                            print(f"     - {failure.get('item_id', 'Unknown')}: {failure.get('error', 'No error message')[:100]}...")
            
            if checkpoint_data.get('error'):
                print(f"   Error: {checkpoint_data['error'][:200]}...")
            
            print()
    else:
        print("\nNo loader checkpoints found")

def delete_run(checkpoint_manager, run_id):
    """Delete a specific run and all its checkpoints."""
    if not checkpoint_manager._run_exists(run_id):
        print(f"❌ Run {run_id} not found")
        return
    
    # Confirm deletion
    response = input(f"⚠️ Are you sure you want to delete run {run_id}? (y/N): ")
    if response.lower() != 'y':
        print("Deletion cancelled")
        return
    
    try:
        checkpoint_manager._delete_run_files(run_id)
        print(f"✅ Deleted run {run_id}")
    except Exception as e:
        print(f"❌ Failed to delete run {run_id}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Checkpoint Management CLI")
    parser.add_argument('command', choices=['list', 'show', 'delete', 'cleanup'], 
                       help='Command to execute')
    parser.add_argument('--run-id', type=str, help='Specific run ID for show/delete commands')
    parser.add_argument('--keep', type=int, default=5, help='Number of runs to keep during cleanup')
    
    args = parser.parse_args()
    
    checkpoint_manager = CheckpointManager()
    
    if args.command == 'list':
        list_runs(checkpoint_manager)
    
    elif args.command == 'show':
        if not args.run_id:
            print("❌ --run-id is required for show command")
            return
        show_run_details(checkpoint_manager, args.run_id)
    
    elif args.command == 'delete':
        if not args.run_id:
            print("❌ --run-id is required for delete command")
            return
        delete_run(checkpoint_manager, args.run_id)
    
    elif args.command == 'cleanup':
        print(f"🧹 Cleaning up old runs, keeping last {args.keep}...")
        checkpoint_manager.cleanup_old_runs(args.keep)

if __name__ == "__main__":
    main() 