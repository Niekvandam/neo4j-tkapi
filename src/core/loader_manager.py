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

# Import common processors for utility functions
from loaders.processors.common_processors import clear_processed_ids

from core.connection.neo4j_connection import Neo4jConnection
from core.config.cli_config import get_skip_count_for_loader


def run_loader_with_checkpoint(checkpoint_manager, loader_name: str, loader_func: Callable, *args, **kwargs) -> bool:
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


def execute_all_loaders(conn: Neo4jConnection, checkpoint_manager, config: Dict[str, Any]) -> bool:
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
        # Core entity loaders
        {
            'name': 'personen',
            'func': load_personen,
            'args': (conn,),
            'kwargs': {}
        },
        {
            'name': 'fracties', 
            'func': load_fracties,
            'args': (conn,),
            'kwargs': {}
        },
        
        # Main data loaders (order matters due to relationships)
        {
            'name': 'vergaderingen',
            'func': load_vergaderingen,
            'args': (conn,),
            'kwargs': {
                'start_date_str': config['start_date'],
                'skip_count': get_skip_count_for_loader(config, 'vergaderingen', config['skip_vergaderingen'])
            }
        },
        {
            'name': 'activiteiten',
            'func': load_activiteiten_threaded if config['threaded'] else load_activiteiten,
            'args': (conn,),
            'kwargs': {
                'start_date_str': config['start_date'],
                'max_workers': config['max_workers'] if config['threaded'] else None,
                'skip_count': get_skip_count_for_loader(config, 'activiteiten', config['skip_activiteiten']),
                'overwrite': config['overwrite']
            }
        },
        {
            'name': 'zaken',
            'func': load_zaken_threaded if (config['threaded_zaken'] or config['threaded']) else load_zaken,
            'args': (conn,),
            'kwargs': {
                'start_date_str': config['start_date'],
                'max_workers': config['max_workers'] if (config['threaded_zaken'] or config['threaded']) else None,
                'skip_count': get_skip_count_for_loader(config, 'zaken', config['skip_zaken']),
                'overwrite': config['overwrite']
            }
        },
        {
            'name': 'documents',
            'func': load_documents,
            'args': (conn,),
            'kwargs': {
                'start_date_str': config['start_date'],
                'skip_count': get_skip_count_for_loader(config, 'documents', config['skip_documents']),
                'overwrite': config['overwrite']
            }
        },
        
        # Secondary loaders
        {
            'name': 'toezeggingen',
            'func': load_toezeggingen,
            'args': (conn,),
            'kwargs': {
                'start_date_str': config['start_date']
            }
        },
        {
            'name': 'activiteit_actors',
            'func': load_activiteit_actors,
            'args': (conn,),
            'kwargs': {
                'start_date_str': config['start_date']
            }
        }
    ]
    
    # Execute loaders in sequence
    for loader_config in loaders:
        # Filter out None values from kwargs
        kwargs = {k: v for k, v in loader_config['kwargs'].items() if v is not None}
        
        loader_success = run_loader_with_checkpoint(
            checkpoint_manager,
            loader_config['name'],
            loader_config['func'],
            *loader_config['args'],
            **kwargs
        )
        
        if not loader_success:
            success = False
            print(f"❌ Loader {loader_config['name']} failed, but continuing with remaining loaders...")
    
    return success 