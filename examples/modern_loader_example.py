"""
Example of a modern loader implementation using the new interface system.

This shows how to create a loader that follows the standardized interface
while maintaining compatibility with the existing decorator system.
"""

import time
from typing import List, Any, Optional
from src.core.interfaces import BaseLoader, LoaderCapability, LoaderConfig, LoaderResult
from src.core.connection.neo4j_connection import Neo4jConnection
from src.core.checkpoint.checkpoint_manager import CheckpointManager
from src.utils.helpers import merge_node, merge_rel
from tkapi import TKApi
from tkapi.document import Document
from datetime import datetime, timezone
from tkapi.util import util as tkapi_util


class ModernDocumentLoader(BaseLoader):
    """
    Example of a modern document loader using the new interface system.
    
    This loader demonstrates:
    - Standardized configuration
    - Capability declaration
    - Consistent result reporting
    - Threading support
    - Date filtering
    """
    
    def __init__(self):
        super().__init__(
            name="modern_document_loader",
            description="Modern document loader with standardized interface"
        )
        
        # Declare capabilities this loader supports
        self._capabilities = [
            LoaderCapability.DATE_FILTERING,
            LoaderCapability.SKIP_FUNCTIONALITY,
            LoaderCapability.BATCH_PROCESSING,
            LoaderCapability.ID_CHECKING,
            LoaderCapability.THREADING,
            LoaderCapability.INCREMENTAL_LOADING
        ]
    
    def load(self, conn: Any, config: LoaderConfig, 
             checkpoint_manager: Optional[Any] = None) -> LoaderResult:
        """
        Main loading method implementing the standardized interface.
        """
        start_time = time.time()
        
        try:
            # Validate configuration
            config_errors = self.validate_config(config)
            if config_errors:
                return LoaderResult(
                    success=False,
                    processed_count=0,
                    failed_count=0,
                    skipped_count=0,
                    total_items=0,
                    execution_time_seconds=time.time() - start_time,
                    error_messages=config_errors,
                    warnings=[],
                    metadata={"validation_failed": True}
                )
            
            print(f"🔄 Modern loader processing with config: {config}")
            
            # Simulate fetching and processing data
            total_items = 100  # Simulated
            processed_count = 95  # Simulated
            failed_count = 5     # Simulated
            
            return LoaderResult(
                success=failed_count == 0,
                processed_count=processed_count,
                failed_count=failed_count,
                skipped_count=config.skip_count,
                total_items=total_items,
                execution_time_seconds=time.time() - start_time,
                error_messages=[],
                warnings=[],
                metadata={
                    "processing_mode": "threaded" if config.enable_threading else "sequential",
                    "capabilities_used": [cap.value for cap in self._capabilities]
                }
            )
            
        except Exception as e:
            return LoaderResult(
                success=False,
                processed_count=0,
                failed_count=0,
                skipped_count=0,
                total_items=0,
                execution_time_seconds=time.time() - start_time,
                error_messages=[f"Unexpected error: {str(e)}"],
                warnings=[],
                metadata={"exception": str(e)}
            )
    
    def _fetch_documents(self, config: LoaderConfig) -> List[Document]:
        """Fetch documents from the TK API based on configuration."""
        api = TKApi()
        
        # Create date filter if specified
        filter_obj = Document.create_filter()
        if config.start_date:
            start_datetime = datetime.strptime(config.start_date, "%Y-%m-%d")
            odata_start = tkapi_util.datetime_to_odata(start_datetime.replace(tzinfo=timezone.utc))
            filter_obj.add_filter_str(f"Datum ge {odata_start}")
        
        if config.end_date:
            end_datetime = datetime.strptime(config.end_date, "%Y-%m-%d")
            odata_end = tkapi_util.datetime_to_odata(end_datetime.replace(tzinfo=timezone.utc))
            filter_obj.add_filter_str(f"Datum le {odata_end}")
        
        # Fetch documents
        documents = api.get_documenten(filter=filter_obj)
        print(f"→ Fetched {len(documents)} documents from API")
        
        return documents
    
    def _process_sequential(self, conn: Neo4jConnection, documents: List[Document], 
                          config: LoaderConfig, checkpoint_manager: Optional[CheckpointManager]) -> LoaderResult:
        """Process documents sequentially."""
        processed_count = 0
        failed_count = 0
        errors = []
        warnings = []
        
        # Setup checkpoint if available
        checkpoint = None
        if checkpoint_manager:
            from src.core.checkpoint.checkpoint_manager import LoaderCheckpoint
            checkpoint = LoaderCheckpoint(checkpoint_manager, self.name)
            checkpoint.set_total_items(len(documents))
        
        for i, document in enumerate(documents, 1):
            # Skip if already processed (checkpoint functionality)
            if checkpoint and checkpoint.is_processed(document.id):
                continue
            
            try:
                success = self._process_single_document(conn, document)
                if success:
                    processed_count += 1
                    if checkpoint:
                        checkpoint.mark_processed(document.id)
                else:
                    failed_count += 1
                    if checkpoint:
                        checkpoint.mark_failed(document.id, "Processing returned False")
                
                # Save checkpoint progress periodically
                if checkpoint and i % config.checkpoint_interval == 0:
                    checkpoint.save_progress()
                    stats = checkpoint.get_progress_stats()
                    print(f"📊 Progress: {stats['processed_count']}/{stats['total_items']} ({stats['completion_percentage']:.1f}%)")
                
            except Exception as e:
                failed_count += 1
                error_msg = f"Error processing document {document.id}: {str(e)}"
                errors.append(error_msg)
                if checkpoint:
                    checkpoint.mark_failed(document.id, error_msg)
        
        # Final checkpoint save
        if checkpoint:
            checkpoint.save_progress()
        
        return LoaderResult(
            success=failed_count == 0,
            processed_count=processed_count,
            failed_count=failed_count,
            skipped_count=0,  # Will be set by caller
            total_items=0,    # Will be set by caller
            execution_time_seconds=0,  # Will be set by caller
            error_messages=errors,
            warnings=warnings,
            metadata={
                "processing_mode": "sequential",
                "checkpoint_used": checkpoint is not None
            }
        )
    
    def _process_threaded(self, conn: Neo4jConnection, documents: List[Document], 
                         config: LoaderConfig, checkpoint_manager: Optional[CheckpointManager]) -> LoaderResult:
        """Process documents using threading."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading
        
        processed_count = 0
        failed_count = 0
        errors = []
        warnings = []
        lock = threading.Lock()
        
        # Setup checkpoint if available
        checkpoint = None
        if checkpoint_manager:
            from src.core.checkpoint.checkpoint_manager import LoaderCheckpoint
            checkpoint = LoaderCheckpoint(checkpoint_manager, self.name)
            checkpoint.set_total_items(len(documents))
        
        def process_document_thread_safe(document):
            nonlocal processed_count, failed_count
            
            # Skip if already processed
            if checkpoint and checkpoint.is_processed(document.id):
                return True
            
            try:
                success = self._process_single_document(conn, document)
                
                with lock:
                    if success:
                        processed_count += 1
                        if checkpoint:
                            checkpoint.mark_processed(document.id)
                    else:
                        failed_count += 1
                        if checkpoint:
                            checkpoint.mark_failed(document.id, "Processing returned False")
                
                return success
                
            except Exception as e:
                error_msg = f"Error processing document {document.id}: {str(e)}"
                
                with lock:
                    failed_count += 1
                    errors.append(error_msg)
                    if checkpoint:
                        checkpoint.mark_failed(document.id, error_msg)
                
                return False
        
        # Process with ThreadPoolExecutor
        print(f"🧵 Starting threaded processing with {config.max_workers} workers")
        
        with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            # Submit all tasks
            futures = {executor.submit(process_document_thread_safe, doc): doc for doc in documents}
            
            # Process completed tasks
            completed = 0
            for future in as_completed(futures):
                completed += 1
                
                if completed % config.checkpoint_interval == 0:
                    print(f"📊 Progress: {completed}/{len(documents)} ({completed/len(documents)*100:.1f}%)")
                    
                    # Save checkpoint progress
                    if checkpoint:
                        with lock:
                            checkpoint.save_progress()
        
        # Final checkpoint save
        if checkpoint:
            checkpoint.save_progress()
        
        return LoaderResult(
            success=failed_count == 0,
            processed_count=processed_count,
            failed_count=failed_count,
            skipped_count=0,  # Will be set by caller
            total_items=0,    # Will be set by caller
            execution_time_seconds=0,  # Will be set by caller
            error_messages=errors,
            warnings=warnings,
            metadata={
                "processing_mode": "threaded",
                "max_workers": config.max_workers,
                "checkpoint_used": checkpoint is not None
            }
        )
    
    def _process_single_document(self, conn: Neo4jConnection, document: Document) -> bool:
        """Process a single document."""
        try:
            with conn.driver.session() as session:
                # Create document node
                doc_props = {
                    'id': document.id,
                    'titel': document.titel or '',
                    'datum': str(document.datum) if document.datum else None,
                    'onderwerp': document.onderwerp or ''
                }
                session.execute_write(merge_node, 'Document', 'id', doc_props)
                
                # Create relationships if document has related entities
                if hasattr(document, 'dossier') and document.dossier:
                    session.execute_write(merge_rel, 
                        'Document', 'id', document.id,
                        'Dossier', 'id', document.dossier.id,
                        'BELONGS_TO_DOSSIER', {}
                    )
                
                return True
                
        except Exception as e:
            print(f"Error processing document {document.id}: {e}")
            return False
    
    def validate_config(self, config: LoaderConfig) -> List[str]:
        """Validate configuration specific to document loading."""
        errors = super().validate_config(config)
        
        # Add document-specific validations
        if config.custom_params:
            if 'invalid_param' in config.custom_params:
                errors.append("invalid_param is not supported for document loading")
        
        return errors


# Example usage and compatibility adapter
def create_compatibility_adapter(modern_loader: BaseLoader):
    """
    Create a compatibility adapter that allows modern loaders to work with
    the existing decorator-based system.
    """
    def adapter_function(conn: Neo4jConnection, start_date_str: str = "2024-01-01",
                        skip_count: int = 0, max_workers: int = 10, 
                        enable_threading: bool = False, checkpoint_manager=None):
        """Adapter function that converts old-style parameters to new config."""
        
        config = LoaderConfig(
            start_date=start_date_str,
            skip_count=skip_count,
            max_workers=max_workers,
            enable_threading=enable_threading
        )
        
        result = modern_loader.load(conn, config, checkpoint_manager)
        
        # Print results in the old style for compatibility
        if result.success:
            print(f"✅ {modern_loader.name} completed successfully!")
            print(f"📊 Processed: {result.processed_count}, Failed: {result.failed_count}")
            print(f"⏱️ Time: {result.execution_time_seconds:.2f}s")
        else:
            print(f"❌ {modern_loader.name} failed!")
            for error in result.error_messages:
                print(f"   Error: {error}")
        
        return result.success
    
    return adapter_function


# Example of how to use the modern loader
if __name__ == "__main__":
    # Create the modern loader
    loader = ModernDocumentLoader()
    
    # Check capabilities
    print("Loader capabilities:")
    for capability in loader.get_capabilities():
        print(f"  ✅ {capability.value}")
    
    # Create configuration
    config = LoaderConfig(
        start_date="2024-01-01",
        skip_count=0,
        max_workers=5,
        enable_threading=True,
        checkpoint_interval=25
    )
    
    # Validate configuration
    errors = loader.validate_config(config)
    if errors:
        print("Configuration errors:")
        for error in errors:
            print(f"  ❌ {error}")
    else:
        print("✅ Configuration is valid")
    
    # Example of using the compatibility adapter
    adapter = create_compatibility_adapter(loader)
    
    # This can now be called like the old-style loaders
    # adapter(conn, start_date_str="2024-01-01", enable_threading=True) 