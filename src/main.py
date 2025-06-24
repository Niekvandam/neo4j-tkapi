from core.connection.neo4j_connection import Neo4jConnection
from core.config.seed_enums import seed_enum_nodes
from core.checkpoint.checkpoint_manager import CheckpointManager

# Import anchor loaders (now with decorators)
from loaders.document_loader import load_documents
from loaders.zaak_loader import load_zaken, load_zaken_threaded
from loaders.activiteit_loader import load_activiteiten, load_activiteiten_threaded
# Note: agendapunten are processed through activiteiten (not standalone)
from loaders.vergadering_loader import load_vergaderingen

# Import other loaders that are still independent (or might be for now)
from loaders.persoon_loader import load_personen
from loaders.fractie_loader import load_fracties
from loaders.toezegging_loader import load_toezeggingen
from loaders.actor_loader import load_activiteit_actors # Assuming ActiviteitActor is fetched based on its own date or via Activiteit

# Import common processors only to use utility like clear_processed_ids
from loaders.common_processors import clear_processed_ids

import sys
import argparse
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

def main():
    parser = argparse.ArgumentParser(description="Neo4j TK API Data Loader with Checkpoint Support")
    parser.add_argument("--resume", action="store_true", help="Resume the most recent incomplete run")
    parser.add_argument("--resume-run", type=str, help="Resume a specific run by ID")
    parser.add_argument("--new-run", action="store_true", help="Force start a new run (ignore incomplete runs)")
    parser.add_argument("--list-runs", action="store_true", help="List all available runs")
    parser.add_argument("--start-date", type=str, default=SHARED_START_DATE, help="Start date for filtering (YYYY-MM-DD)")
    parser.add_argument("--cleanup", action="store_true", help="Clean up old checkpoint files")
    parser.add_argument("--threaded", action="store_true", help="Use threaded version of activiteit and zaak loaders for faster processing")
    parser.add_argument("--threaded-zaken", action="store_true", help="Use threaded version specifically for zaak loader")
    parser.add_argument("--max-workers", type=int, default=10, help="Number of threads for threaded processing (default: 10)")
    parser.add_argument("--skip-count", type=int, default=0, help="Number of items to skip from the beginning for each loader (default: 0)")
    parser.add_argument("--skip-activiteiten", type=int, help="Number of activiteiten to skip (overrides --skip-count for activiteiten)")
    parser.add_argument("--skip-zaken", type=int, help="Number of zaken to skip (overrides --skip-count for zaken)")
    parser.add_argument("--skip-documents", type=int, help="Number of documents to skip (overrides --skip-count for documents)")
    parser.add_argument("--skip-vergaderingen", type=int, help="Number of vergaderingen to skip (overrides --skip-count for vergaderingen)")
    # Note: agendapunten are processed through activiteiten, no separate skip option needed
    
    # Data processing arguments
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing data (skip ID existence checks for faster processing)")
    
    args = parser.parse_args()
    
    # Initialize checkpoint manager
    checkpoint_manager = CheckpointManager()
    
    # Handle different run modes
    if args.list_runs:
        runs = checkpoint_manager.list_runs()
        if runs:
            print("\n📋 Available runs:")
            for run_info in runs:
                status_emoji = {"completed": "✅", "running": "🔄", "failed": "❌"}.get(run_info["status"], "❓")
                print(f"  {status_emoji} {run_info['run_id']} - {run_info['status']} - {run_info.get('start_time', 'Unknown time')}")
                if run_info.get('description'):
                    print(f"      Description: {run_info['description']}")
                
                # Show configuration if available
                if run_info.get('config'):
                    config_summary = checkpoint_manager._format_config_summary(run_info['config'])
                    print(f"      Configuration: {config_summary}")
        else:
            print("📋 No runs found")
        return
    
    if args.cleanup:
        checkpoint_manager.cleanup_old_runs()
        return
    
    # Prepare configuration for storage
    config = {
        'start_date': args.start_date,
        'threaded': args.threaded,
        'threaded_zaken': args.threaded_zaken,
        'max_workers': args.max_workers,
        'skip_count': args.skip_count,
        'skip_activiteiten': args.skip_activiteiten,
        'skip_zaken': args.skip_zaken,
        'skip_documents': args.skip_documents,
        'skip_vergaderingen': args.skip_vergaderingen,
        'overwrite': args.overwrite
    }
    
    # Determine run mode
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
            return
        else:
            print("✅ Configuration validated - compatible with stored run")
    
    # Show current run summary
    summary = checkpoint_manager.get_run_summary()
    print(f"\n📊 Current run summary:")
    print(f"   Run ID: {summary.get('run_id', 'Unknown')}")
    print(f"   Status: {summary.get('status', 'Unknown')}")
    
    # Show configuration
    stored_config = summary.get("config", {})
    if stored_config:
        print(f"   Configuration: {checkpoint_manager._format_config_summary(stored_config)}")
    
    if summary.get("loaders"):
        print(f"\n📊 Loader progress:")
        for loader_name, loader_info in summary["loaders"].items():
            status_emoji = {"completed": "✅", "in_progress": "🔄", "failed": "❌"}.get(loader_info["status"], "❓")
            print(f"  {status_emoji} {loader_name}: {loader_info['status']}")
            if loader_info.get("progress", {}).get("processed_count"):
                progress = loader_info["progress"]
                print(f"      Progress: {progress.get('processed_count', 0)}/{progress.get('total_items', 0)} items")
    
    conn = Neo4jConnection()
    try:
        print(f"\n🚀 Processing data load for items from {args.start_date} onwards.")
        print("📋 Note: Agendapunten are processed through Activiteiten (hierarchical relationship)")
        if args.threaded:
            print(f"🧵 Using threaded processing with {args.max_workers} workers for activiteiten")
        if args.threaded_zaken or args.threaded:
            print(f"🧵 Using threaded processing with {args.max_workers} workers for zaken")
        if args.overwrite:
            print("🔄 Overwrite mode enabled - will process all items regardless of existing data")
        else:
            print("🔍 ID checking enabled - will skip items that already exist in Neo4j")
        if args.skip_count > 0:
            print(f"⏭️ Default skip count: {args.skip_count} items per loader")
        
        # Show specific skip counts if set
        skip_info = []
        if args.skip_activiteiten is not None:
            skip_info.append(f"activiteiten: {args.skip_activiteiten}")
        if args.skip_zaken is not None:
            skip_info.append(f"zaken: {args.skip_zaken}")
        if args.skip_documents is not None:
            skip_info.append(f"documents: {args.skip_documents}")
        if args.skip_vergaderingen is not None:
            skip_info.append(f"vergaderingen: {args.skip_vergaderingen}")
        
        if skip_info:
            print(f"⏭️ Specific skip counts: {', '.join(skip_info)}")
        
        # Only clear processed IDs if starting a new run (not resuming)
        if not args.resume and not args.resume_run:
            clear_processed_ids()
        
        # Define the loaders to run in order
        # Note: These loaders now use decorators and handle checkpoints automatically
        
        # Helper function to get skip count for a specific loader
        def get_skip_count(loader_name, specific_skip=None):
            return specific_skip if specific_skip is not None else args.skip_count
        
        loaders_config = [
            ("seed_enums", lambda: seed_enum_nodes(conn)),
            # ("load_personen", lambda: load_personen(conn, batch_size=5000)),
            # ("load_fracties", lambda: load_fracties(conn, batch_size=500)),
            ("load_activiteiten", lambda: load_activiteiten_threaded(
                conn, 
                start_date_str=args.start_date, 
                max_workers=args.max_workers, 
                skip_count=get_skip_count("activiteiten", args.skip_activiteiten),
                overwrite=args.overwrite,
                checkpoint_manager=checkpoint_manager
            ) if args.threaded else load_activiteiten(
                conn, 
                start_date_str=args.start_date, 
                skip_count=get_skip_count("activiteiten", args.skip_activiteiten),
                overwrite=args.overwrite,
                checkpoint_manager=checkpoint_manager
            )),
            # Note: agendapunten are processed through activiteiten (not standalone)
            ("load_zaken", lambda: load_zaken_threaded(
                conn, 
                start_date_str=args.start_date, 
                max_workers=args.max_workers,
                skip_count=get_skip_count("zaken", args.skip_zaken),
                checkpoint_manager=checkpoint_manager
            ) if (args.threaded_zaken or args.threaded) else load_zaken(
                conn, 
                start_date_str=args.start_date, 
                skip_count=get_skip_count("zaken", args.skip_zaken),
                checkpoint_manager=checkpoint_manager
            )),
            ("load_documents", lambda: load_documents(
                conn, 
                start_date_str=args.start_date, 
                skip_count=get_skip_count("documents", args.skip_documents),
                checkpoint_manager=checkpoint_manager
            )),
            ("load_vergaderingen", lambda: load_vergaderingen(
                conn, 
                start_date_str=args.start_date, 
                skip_count=get_skip_count("vergaderingen", args.skip_vergaderingen),
                checkpoint_manager=checkpoint_manager
            )),
            ("load_toezeggingen", lambda: load_toezeggingen(conn)),
        ]
        
        # Run each loader with checkpoint management
        all_successful = True
        for loader_name, loader_func in loaders_config:
            success = run_loader_with_checkpoint(checkpoint_manager, loader_name, loader_func)
            if not success:
                all_successful = False
                print(f"\n⚠️ {loader_name} failed. You can resume this run later using --resume")
                break
            print("-" * 50)
        
        if all_successful:
            checkpoint_manager.complete_run()
            print("\n🎉 All loaders completed successfully!")
        else:
            print(f"\n⚠️ Run incomplete. Resume with: python main.py --resume-run {checkpoint_manager.current_run_id}")
            
    except KeyboardInterrupt:
        print(f"\n⚠️ Run interrupted. Resume with: python main.py --resume-run {checkpoint_manager.current_run_id}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        print(f"Resume with: python main.py --resume-run {checkpoint_manager.current_run_id}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()
        print("🔌 Neo4j connection closed.")

if __name__ == "__main__":
    main()