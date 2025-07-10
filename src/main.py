"""
Main entry point for the Neo4j TK API Data Loader
"""
import sys

# Logging setup must come before any prints are emitted
from pathlib import Path
from utils.logging_utils import setup_logging

from core.connection.neo4j_connection import Neo4jConnection
from core.config.seed_enums import seed_enum_nodes
from core.checkpoint.checkpoint_manager import CheckpointManager
from core.config.cli_config import (
    create_argument_parser, args_to_config, print_configuration_summary,
    print_run_summary, list_available_runs
)
from core.loader_manager import execute_all_loaders


import traceback
import inspect

# Define the start date for filtering
SHARED_START_DATE = "2024-01-01"

def run_loader_with_checkpoint(checkpoint_manager, loader_name, loader_func, *args, **kwargs):
    """
    Run a loader function with checkpoint management and error handling.
    This function now works with both decorator-based and legacy loaders.
    """
    if checkpoint_manager.is_loader_completed(loader_name):
        print(f"⏭️ Skipping {loader_name} - already completed")
        return True
    
    print(f"🔄 Starting {loader_name}...")
    try:
        # Check if the loader supports the new decorator system
        sig = inspect.signature(loader_func)
        if '_checkpoint_context' in sig.parameters:
            # New decorator-based loader
            kwargs['checkpoint_manager'] = checkpoint_manager
        elif 'checkpoint_manager' in sig.parameters:
            # Legacy loader with manual checkpoint support
            kwargs['checkpoint_manager'] = checkpoint_manager
        
        # Run the loader function
        result = loader_func(*args, **kwargs)
        
        # Mark as completed
        checkpoint_manager.mark_loader_complete(loader_name)
        return True
        
    except Exception as e:
        error_message = f"{str(e)}\n{traceback.format_exc()}"
        print(f"❌ Error in {loader_name}: {str(e)}")
        checkpoint_manager.mark_loader_failed(loader_name, error_message)
        return False

def handle_run_management(checkpoint_manager, args, config):
    """Handle run management logic (start/resume/validate)"""
    run_started = False
    
    if args.new_run:
        # Force new run
        checkpoint_manager.start_new_run(f"Data load for items from {args.start_date} onwards", config)
        run_started = True
    elif args.resume_run:
        run_started = checkpoint_manager.resume_run(args.resume_run) is not None
    elif args.resume:
        run_started = checkpoint_manager.resume_run() is not None
    
    if not run_started:
        checkpoint_manager.start_new_run(f"Data load for items from {args.start_date} onwards", config)
    elif not args.new_run:
        # Validate configuration compatibility when resuming (but not for new runs)
        if not checkpoint_manager.validate_config_compatibility(config):
            print("\n❌ Configuration mismatch detected!")
            print("   You can either:")
            print("   1. Use the same configuration as the original run")
            print("   2. Start a new run with --new-run")
            stored_config = checkpoint_manager.get_run_config()
            if stored_config:
                print(f"   Original configuration: {checkpoint_manager._format_config_summary(stored_config)}")
            return False
        else:
            print("✅ Configuration validated - compatible with stored run")
    
    return True

def main():
    """Main entry point for the application"""
    # Set up logging (console + file)
    setup_logging(Path("logs"))
    # Parse command line arguments
    parser = create_argument_parser()
    args = parser.parse_args()
    
    # Initialize checkpoint manager
    checkpoint_manager = CheckpointManager()
    
    # Handle list runs command
    if args.list_runs:
        list_available_runs(checkpoint_manager)
        return
    
    # Handle cleanup command
    if args.cleanup:
        checkpoint_manager.cleanup_old_runs()
        return
    
    # Convert arguments to configuration
    config = args_to_config(args)
    
    # Handle run management
    if not handle_run_management(checkpoint_manager, args, config):
        return  # Exit on configuration mismatch
    
    # Show current run summary
    print_run_summary(checkpoint_manager)
    
    # Initialize database connection
    conn = Neo4jConnection()
    
    try:
        # Print configuration summary
        print_configuration_summary(config, checkpoint_manager)
        
        # Seed enum nodes first
        print("\n🌱 Seeding enum nodes...")
        seed_enum_nodes(conn)
        
        # Execute all loaders
        print("\n🚀 Starting data loading sequence...")
        success = execute_all_loaders(conn, checkpoint_manager, config)
        
        if success:
            # Mark the entire run as completed
            checkpoint_manager.complete_run()
            print("\n🎉 All loaders completed successfully!")
        else:
            print("\n⚠️ Some loaders failed. Check the logs above for details.")
            print("💡 You can resume this run later with --resume to retry failed loaders.")
            
    except KeyboardInterrupt:
        print("\n⏹️ Process interrupted by user")
        print("💾 Progress has been saved. You can resume with --resume")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        print("💾 Progress has been saved. You can resume with --resume")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()