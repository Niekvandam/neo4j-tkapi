"""
CLI Configuration management for the Neo4j TK API Data Loader
"""
import argparse
from typing import Dict, Any


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser for the CLI"""
    parser = argparse.ArgumentParser(description="Neo4j TK API Data Loader with Checkpoint Support")
    
    # Run management arguments
    parser.add_argument("--resume", action="store_true", help="Resume the most recent incomplete run")
    parser.add_argument("--resume-run", type=str, help="Resume a specific run by ID")
    parser.add_argument("--new-run", action="store_true", help="Force start a new run (ignore incomplete runs)")
    parser.add_argument("--list-runs", action="store_true", help="List all available runs")
    parser.add_argument("--cleanup", action="store_true", help="Clean up old checkpoint files")
    
    # Data filtering arguments
    parser.add_argument("--start-date", type=str, default="2024-01-01", 
                       help="Start date for filtering (YYYY-MM-DD)")
    
    # Performance arguments
    parser.add_argument("--threaded", action="store_true", 
                       help="Use threaded version of activiteit and zaak loaders for faster processing")
    parser.add_argument("--threaded-zaken", action="store_true", 
                       help="Use threaded version specifically for zaak loader")
    parser.add_argument("--max-workers", type=int, default=10, 
                       help="Number of threads for threaded processing (default: 10)")
    
    # Skip count arguments
    parser.add_argument("--skip-count", type=int, default=0, 
                       help="Number of items to skip from the beginning for each loader (default: 0)")
    parser.add_argument("--skip-activiteiten", type=int, 
                       help="Number of activiteiten to skip (overrides --skip-count for activiteiten)")
    parser.add_argument("--skip-zaken", type=int, 
                       help="Number of zaken to skip (overrides --skip-count for zaken)")
    parser.add_argument("--skip-documents", type=int, 
                       help="Number of documents to skip (overrides --skip-count for documents)")
    parser.add_argument("--skip-vergaderingen", type=int, 
                       help="Number of vergaderingen to skip (overrides --skip-count for vergaderingen)")
    
    # Data processing arguments
    parser.add_argument("--overwrite", action="store_true", 
                       help="Overwrite existing data (skip ID existence checks for faster processing)")
    
    # Selective loader arguments
    parser.add_argument("--only-vlos", action="store_true",
                       help="Run only the VLOS analysis loader")
    parser.add_argument("--only-loader", type=str, 
                       help="Run only the specified loader (e.g., 'vlos_analysis', 'activiteiten', 'zaken')")
    parser.add_argument("--skip-loaders", type=str, nargs="+",
                       help="Skip specific loaders (e.g., 'documents', 'zaken')")
    
    return parser


def args_to_config(args: argparse.Namespace) -> Dict[str, Any]:
    """Convert parsed arguments to a configuration dictionary"""
    return {
        'start_date': args.start_date,
        'threaded': args.threaded,
        'threaded_zaken': args.threaded_zaken,
        'max_workers': args.max_workers,
        'skip_count': args.skip_count,
        'skip_activiteiten': args.skip_activiteiten,
        'skip_zaken': args.skip_zaken,
        'skip_documents': args.skip_documents,
        'skip_vergaderingen': args.skip_vergaderingen,
        'overwrite': args.overwrite,
        'only_vlos': args.only_vlos,
        'only_loader': args.only_loader,
        'skip_loaders': args.skip_loaders or []
    }


def get_skip_count_for_loader(config: Dict[str, Any], loader_name: str, specific_skip: int = None) -> int:
    """Get the appropriate skip count for a specific loader"""
    if specific_skip is not None:
        return specific_skip
    return config.get('skip_count', 0)


def print_configuration_summary(config: Dict[str, Any], checkpoint_manager):
    """Print a summary of the current configuration"""
    print(f"\n🚀 Processing data load for items from {config['start_date']} onwards.")
    print("📋 Note: Agendapunten are processed through Activiteiten (hierarchical relationship)")
    
    if config['threaded']:
        print(f"🧵 Using threaded processing with {config['max_workers']} workers for activiteiten")
    if config['threaded_zaken'] or config['threaded']:
        print(f"🧵 Using threaded processing with {config['max_workers']} workers for zaken")
    
    if config['overwrite']:
        print("🔄 Overwrite mode enabled - will process all items regardless of existing data")
    else:
        print("🔍 ID checking enabled - will skip items that already exist in Neo4j")
    
    if config['skip_count'] > 0:
        print(f"⏭️ Default skip count: {config['skip_count']} items per loader")
    
    # Show specific skip counts if set
    skip_info = []
    if config['skip_activiteiten'] is not None:
        skip_info.append(f"activiteiten: {config['skip_activiteiten']}")
    if config['skip_zaken'] is not None:
        skip_info.append(f"zaken: {config['skip_zaken']}")
    if config['skip_documents'] is not None:
        skip_info.append(f"documents: {config['skip_documents']}")
    if config['skip_vergaderingen'] is not None:
        skip_info.append(f"vergaderingen: {config['skip_vergaderingen']}")
    
    if skip_info:
        print(f"⏭️ Specific skip counts: {', '.join(skip_info)}")


def print_run_summary(checkpoint_manager):
    """Print a summary of the current run"""
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


def list_available_runs(checkpoint_manager):
    """List all available runs"""
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