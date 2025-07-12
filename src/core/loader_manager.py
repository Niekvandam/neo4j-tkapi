"""
Loader Management for the Neo4j TK API Data Loader
"""

import inspect
import traceback
from typing import Dict, Any, Callable

# Import anchor loaders (now with decorators)
from loaders.document_loader import load_documents
from loaders.zaak_loader import load_zaken, load_zaken_threaded
from loaders.activiteit_loader import load_activiteiten, load_activiteiten_threaded
from loaders.vergadering_loader import load_vergaderingen

# Import other loaders that are still independent
from loaders.persoon_loader import load_personen
from loaders.fractie_loader import load_fracties
from loaders.toezegging_loader import load_toezeggingen
from loaders.actor_loader import load_activiteit_actors
from loaders.commissie_loader import load_commissies
from loaders.persoonfunctie_loader import load_persoon_functies
from loaders.kamerstukdossier_loader import load_kamerstukdossiers
from loaders.zaal_loader import load_zalen
from loaders.reservering_loader import load_reserveringen

# Import VLOS analysis loader
from loaders.vlos_neo4j_loader import load_vlos_analysis

# Import common processors for utility functions
from loaders.processors.common_processors import clear_processed_ids

from core.connection.neo4j_connection import Neo4jConnection
from core.config.cli_config import get_skip_count_for_loader


def filter_loaders_by_config(loaders: list, config: Dict[str, Any]) -> list:
    """Filter loaders based on configuration (--only-vlos, --only-loader, --skip-loaders)"""
    
    # Handle --only-vlos
    if config.get("only_vlos"):
        return [loader for loader in loaders if loader["name"] == "vlos_analysis"]
    
    # Handle --only-loader
    if config.get("only_loader"):
        target_loader = config["only_loader"]
        matching_loaders = [loader for loader in loaders if loader["name"] == target_loader]
        if not matching_loaders:
            print(f"❌ Loader '{target_loader}' not found. Available loaders:")
            for loader in loaders:
                print(f"   - {loader['name']}")
            return []
        return matching_loaders
    
    # Handle --skip-loaders
    skip_loaders = config.get("skip_loaders", [])
    if skip_loaders:
        filtered_loaders = [loader for loader in loaders if loader["name"] not in skip_loaders]
        if len(filtered_loaders) < len(loaders):
            skipped_names = [loader["name"] for loader in loaders if loader["name"] in skip_loaders]
            print(f"⏭️ Skipping loaders: {', '.join(skipped_names)}")
        return filtered_loaders
    
    # No filtering - return all loaders
    return loaders


def run_loader_with_checkpoint(
    checkpoint_manager, loader_name: str, loader_func: Callable, *args, **kwargs
) -> bool:
    """
    Run a loader function with checkpoint management and error handling.
    This function works with both decorator-based and legacy loaders.
    """
    if checkpoint_manager.is_loader_completed(loader_name):
        print(f"⏭️ Skipping {loader_name} - already completed")
        return True

    print(f"🔄 Starting {loader_name}...")
    try:
        # Check if the loader supports the new decorator system
        sig = inspect.signature(loader_func)
        if "_checkpoint_context" in sig.parameters:
            # New decorator-based loader
            kwargs["checkpoint_manager"] = checkpoint_manager
        elif "checkpoint_manager" in sig.parameters:
            # Legacy loader with manual checkpoint support
            kwargs["checkpoint_manager"] = checkpoint_manager

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


def execute_all_loaders(
    conn: Neo4jConnection, checkpoint_manager, config: Dict[str, Any]
) -> bool:
    """
    Execute all data loaders in the correct sequence.

    Returns:
        bool: True if all loaders completed successfully, False otherwise
    """
    success = True

    # Clear processed IDs at the start
    clear_processed_ids()

    # Define loader sequence with their configurations
    loaders = [
        # Core entity loaders (commented out for now)
        {"name": "personen", "func": load_personen, "args": (conn,), "kwargs": {}},
        {"name": "fracties", "func": load_fracties, "args": (conn,), "kwargs": {}},
        {"name": "commissies", "func": load_commissies, "args": (conn,), "kwargs": {}},
        {"name": "persoon_functies", "func": load_persoon_functies, "args": (conn,), "kwargs": {}},
        
        # Infrastructure loaders
        {"name": "zalen", "func": load_zalen, "args": (conn,), "kwargs": {}},
        {"name": "reserveringen", "func": load_reserveringen, "args": (conn,), "kwargs": {}},
        
        # Foundation loaders - these create the basic entities that others reference
        {
            "name": "vergaderingen",
            "func": load_vergaderingen,
            "args": (conn,),
            "kwargs": {
                "start_date_str": config["start_date"],
                "skip_count": get_skip_count_for_loader(
                    config, "vergaderingen", config["skip_vergaderingen"]
                ),
            },
        },
        
        # Main data loaders (order matters due to relationships)
        {
            "name": "activiteiten",
            "func": (
                load_activiteiten_threaded if config["threaded"] else load_activiteiten
            ),
            "args": (conn,),
            "kwargs": {
                "start_date_str": config["start_date"],
                "max_workers": config["max_workers"] if config["threaded"] else None,
                "skip_count": get_skip_count_for_loader(
                    config, "activiteiten", config["skip_activiteiten"]
                ),
                "overwrite": config["overwrite"],
            },
        },
        {
            "name": "activiteit_actors",
            "func": load_activiteit_actors,
            "args": (conn,),
            "kwargs": {"start_date_str": config["start_date"]},
        },
        {
            "name": "zaken",
            "func": (
                load_zaken_threaded
                if (config["threaded_zaken"] or config["threaded"])
                else load_zaken
            ),
            "args": (conn,),
            "kwargs": {
                "start_date_str": config["start_date"],
                "max_workers": (
                    config["max_workers"]
                    if (config["threaded_zaken"] or config["threaded"])
                    else None
                ),
                "skip_count": get_skip_count_for_loader(
                    config, "zaken", config["skip_zaken"]
                ),
                "overwrite": config["overwrite"],
            },
        },
        {
            "name": "documents",
            "func": load_documents,
            "args": (conn,),
            "kwargs": {
                "start_date_str": config["start_date"],
                "skip_count": get_skip_count_for_loader(
                    config, "documents", config["skip_documents"]
                ),
                "overwrite": config["overwrite"],
            },
        },
        {
            "name": "kamerstukdossiers",
            "func": load_kamerstukdossiers,
            "args": (conn,),
            "kwargs": {},
        },
        
        # VLOS Analysis loader - processes XML files for parliamentary analysis
        # NOTE: This DEPENDS on vergaderingen being loaded first!
        {
            "name": "vlos_analysis",
            "func": load_vlos_analysis,
            "args": (conn,),
            "kwargs": {
                "xml_files_pattern": config.get("vlos_pattern", "sample_vlos_*.xml"),
                "start_date_str": config["start_date"],
                "use_api": config.get("use_vlos_api", True),
            },
        },
        
        # Secondary loaders
        {
            "name": "toezeggingen",
            "func": load_toezeggingen,
            "args": (conn,),
            "kwargs": {"start_date_str": config["start_date"]},
        },

    ]

    # Filter loaders based on configuration
    filtered_loaders = filter_loaders_by_config(loaders, config)
    
    # Execute loaders in sequence
    for loader_config in filtered_loaders:
        # Filter out None values from kwargs
        kwargs = {k: v for k, v in loader_config["kwargs"].items() if v is not None}

        loader_success = run_loader_with_checkpoint(
            checkpoint_manager,
            loader_config["name"],
            loader_config["func"],
            *loader_config["args"],
            **kwargs,
        )

        if not loader_success:
            success = False
            print(
                f"❌ Loader {loader_config['name']} failed, but continuing with remaining loaders..."
            )

    # DEPRECATED: VLOS processing has been moved to new modular system
    # The old deferred VLOS processing is now deprecated
    try:
        from loaders.processors.common_processors import DEFERRED_VLOS_ITEMS, process_deferred_vlos_items

        if DEFERRED_VLOS_ITEMS:
            print(f"🔄 Processing deprecated VLOS items...")
            # Call the deprecated processing function that will skip the items
            process_deferred_vlos_items(conn.driver)
            print("✅ Completed deprecation notice for VLOS verslagen.")
        else:
            print("📋 No deprecated VLOS items to process.")
    except Exception as e:
        print(f"⚠️  Note: VLOS processing has been deprecated - {e}")

    return success
