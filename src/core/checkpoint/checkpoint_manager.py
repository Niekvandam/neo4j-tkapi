import json
import os
import datetime
from typing import Dict, List, Optional, Set
from pathlib import Path

class CheckpointManager:
    """
    Manages checkpoints for data loading processes to enable resumption after failures.
    Stores progress information in JSON files for persistence across runs.
    """
    
    def __init__(self, checkpoint_dir: str = "checkpoints"):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True)
        
        # Current run state
        self.current_run_id = None
        self.checkpoints = {}
        
    def start_new_run(self, run_description: str = None, config: Dict = None) -> str:
        """Start a new data loading run and return the run ID."""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.current_run_id = f"run_{timestamp}"
        
        run_info = {
            "run_id": self.current_run_id,
            "start_time": datetime.datetime.now().isoformat(),
            "description": run_description or "Data loading run",
            "status": "running",
            "config": config or {},
            "loaders": {}
        }
        
        self._save_run_info(run_info)
        print(f"🚀 Started new run: {self.current_run_id}")
        if config:
            print(f"📋 Saved configuration: {self._format_config_summary(config)}")
        return self.current_run_id
    
    def resume_run(self, run_id: str = None) -> Optional[str]:
        """Resume the most recent incomplete run or a specific run."""
        if run_id:
            if self._run_exists(run_id):
                self.current_run_id = run_id
                self.checkpoints = self._load_checkpoints(run_id)
                print(f"📂 Resuming run: {run_id}")
                return run_id
            else:
                print(f"❌ Run {run_id} not found")
                return None
        else:
            # Find the most recent incomplete run
            latest_run = self._find_latest_incomplete_run()
            if latest_run:
                self.current_run_id = latest_run
                self.checkpoints = self._load_checkpoints(latest_run)
                print(f"📂 Resuming latest incomplete run: {latest_run}")
                return latest_run
            else:
                print("ℹ️ No incomplete runs found")
                return None
    
    def save_loader_progress(self, loader_name: str, progress_info: Dict):
        """Save progress for a specific loader."""
        if not self.current_run_id:
            raise ValueError("No active run. Call start_new_run() first.")
        
        checkpoint_data = {
            "loader_name": loader_name,
            "timestamp": datetime.datetime.now().isoformat(),
            "progress": progress_info
        }
        
        self.checkpoints[loader_name] = checkpoint_data
        self._save_checkpoint(loader_name, checkpoint_data)
        
    def get_loader_progress(self, loader_name: str) -> Optional[Dict]:
        """Get saved progress for a specific loader."""
        return self.checkpoints.get(loader_name, {}).get("progress", None)
    
    def mark_loader_complete(self, loader_name: str):
        """Mark a loader as completed."""
        if loader_name in self.checkpoints:
            self.checkpoints[loader_name]["status"] = "completed"
            self.checkpoints[loader_name]["completed_at"] = datetime.datetime.now().isoformat()
            self._save_checkpoint(loader_name, self.checkpoints[loader_name])
        
        print(f"✅ Marked {loader_name} as completed")
    
    def mark_loader_failed(self, loader_name: str, error_message: str):
        """Mark a loader as failed with error details."""
        if loader_name in self.checkpoints:
            self.checkpoints[loader_name]["status"] = "failed"
            self.checkpoints[loader_name]["error"] = error_message
            self.checkpoints[loader_name]["failed_at"] = datetime.datetime.now().isoformat()
            self._save_checkpoint(loader_name, self.checkpoints[loader_name])
        
        print(f"❌ Marked {loader_name} as failed: {error_message}")
    
    def is_loader_completed(self, loader_name: str) -> bool:
        """Check if a loader has been completed."""
        return self.checkpoints.get(loader_name, {}).get("status") == "completed"
    
    def complete_run(self):
        """Mark the current run as completed."""
        if not self.current_run_id:
            return
        
        run_info = self._load_run_info()
        run_info["status"] = "completed"
        run_info["end_time"] = datetime.datetime.now().isoformat()
        self._save_run_info(run_info)
        
        print(f"🎉 Run {self.current_run_id} completed successfully")
    
    def get_run_summary(self) -> Dict:
        """Get a summary of the current run's progress."""
        if not self.current_run_id:
            return {}
        
        run_info = self._load_run_info()
        summary = {
            "run_id": self.current_run_id,
            "status": run_info.get("status", "unknown"),
            "start_time": run_info.get("start_time"),
            "config": run_info.get("config", {}),
            "loaders": {}
        }
        
        for loader_name, checkpoint in self.checkpoints.items():
            summary["loaders"][loader_name] = {
                "status": checkpoint.get("status", "in_progress"),
                "timestamp": checkpoint.get("timestamp"),
                "progress": checkpoint.get("progress", {})
            }
        
        return summary
    
    def list_runs(self) -> List[Dict]:
        """List all available runs."""
        runs = []
        for run_file in self.checkpoint_dir.glob("run_*.json"):
            try:
                with open(run_file, 'r') as f:
                    run_info = json.load(f)
                    runs.append({
                        "run_id": run_info["run_id"],
                        "start_time": run_info.get("start_time"),
                        "status": run_info.get("status", "unknown"),
                        "description": run_info.get("description", ""),
                        "config": run_info.get("config", {})
                    })
            except (json.JSONDecodeError, KeyError):
                continue
        
        return sorted(runs, key=lambda x: x.get("start_time", ""), reverse=True)
    
    def cleanup_old_runs(self, keep_last_n: int = 5):
        """Clean up old run files, keeping only the last N runs."""
        runs = self.list_runs()
        if len(runs) <= keep_last_n:
            return
        
        runs_to_delete = runs[keep_last_n:]
        for run_info in runs_to_delete:
            run_id = run_info["run_id"]
            self._delete_run_files(run_id)
            print(f"🗑️ Cleaned up old run: {run_id}")
    
    def get_run_config(self) -> Dict:
        """Get the configuration for the current run."""
        if not self.current_run_id:
            return {}
        
        run_info = self._load_run_info()
        return run_info.get("config", {})
    
    def validate_config_compatibility(self, current_config: Dict) -> bool:
        """
        Validate that the current configuration is compatible with the stored configuration.
        Returns True if compatible, False otherwise.
        """
        stored_config = self.get_run_config()
        if not stored_config:
            return True  # No stored config, so any config is compatible
        
        # Check critical configuration parameters that affect data consistency
        critical_params = [
            'start_date', 'threaded', 'threaded_zaken', 'max_workers',
            'skip_count', 'skip_activiteiten', 'skip_zaken', 
            'skip_documents', 'skip_vergaderingen', 'overwrite'
        ]
        
        incompatible_params = []
        for param in critical_params:
            stored_value = stored_config.get(param)
            current_value = current_config.get(param)
            
            # Only check if both values exist and are different
            if stored_value is not None and current_value is not None and stored_value != current_value:
                incompatible_params.append(f"{param}: stored={stored_value}, current={current_value}")
        
        if incompatible_params:
            print("⚠️ Configuration incompatibility detected:")
            for param in incompatible_params:
                print(f"   • {param}")
            return False
        
        return True
    
    # Private methods
    def _run_exists(self, run_id: str) -> bool:
        """Check if a run exists."""
        return (self.checkpoint_dir / f"{run_id}.json").exists()
    
    def _find_latest_incomplete_run(self) -> Optional[str]:
        """Find the most recent incomplete run."""
        runs = self.list_runs()
        for run_info in runs:
            if run_info["status"] in ["running", "failed"]:
                return run_info["run_id"]
        return None
    
    def _save_run_info(self, run_info: Dict):
        """Save run information to disk."""
        run_file = self.checkpoint_dir / f"{self.current_run_id}.json"
        with open(run_file, 'w') as f:
            json.dump(run_info, f, indent=2)
    
    def _load_run_info(self) -> Dict:
        """Load run information from disk."""
        run_file = self.checkpoint_dir / f"{self.current_run_id}.json"
        if run_file.exists():
            with open(run_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_checkpoint(self, loader_name: str, checkpoint_data: Dict):
        """Save checkpoint data to disk."""
        checkpoint_file = self.checkpoint_dir / f"{self.current_run_id}_{loader_name}.json"
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
    
    def _load_checkpoints(self, run_id: str) -> Dict:
        """Load all checkpoints for a run."""
        checkpoints = {}
        pattern = f"{run_id}_*.json"
        for checkpoint_file in self.checkpoint_dir.glob(pattern):
            # Extract loader name from filename
            filename = checkpoint_file.stem
            loader_name = filename.replace(f"{run_id}_", "")
            
            try:
                with open(checkpoint_file, 'r') as f:
                    checkpoints[loader_name] = json.load(f)
            except (json.JSONDecodeError, KeyError):
                continue
        
        return checkpoints
    
    def _delete_run_files(self, run_id: str):
        """Delete all files related to a run."""
        # Delete main run file
        run_file = self.checkpoint_dir / f"{run_id}.json"
        if run_file.exists():
            run_file.unlink()
        
        # Delete checkpoint files
        pattern = f"{run_id}_*.json"
        for checkpoint_file in self.checkpoint_dir.glob(pattern):
            checkpoint_file.unlink()
    
    def _format_config_summary(self, config: Dict) -> str:
        """Format configuration for display."""
        summary_parts = []
        
        if config.get('start_date'):
            summary_parts.append(f"start_date={config['start_date']}")
        
        if config.get('threaded'):
            summary_parts.append(f"threaded=True (workers={config.get('max_workers', 10)})")
        
        if config.get('threaded_zaken'):
            summary_parts.append(f"threaded-zaken=True (workers={config.get('max_workers', 10)})")
        
        if config.get('overwrite'):
            summary_parts.append("overwrite=True")
        
        skip_parts = []
        if config.get('skip_count', 0) > 0:
            skip_parts.append(f"global={config['skip_count']}")
        
        for loader in ['activiteiten', 'zaken', 'documents', 'vergaderingen']:
            skip_key = f'skip_{loader}'
            if config.get(skip_key) is not None:
                skip_parts.append(f"{loader}={config[skip_key]}")
        
        if skip_parts:
            summary_parts.append(f"skip=({', '.join(skip_parts)})")
        
        return ', '.join(summary_parts) if summary_parts else "default settings"


class LoaderCheckpoint:
    """
    Helper class for individual loaders to manage their specific checkpoints.
    """
    
    def __init__(self, checkpoint_manager: CheckpointManager, loader_name: str):
        self.checkpoint_manager = checkpoint_manager
        self.loader_name = loader_name
        self.processed_ids = set()
        self.current_batch = 0
        self.total_items = 0
        self.failed_items = []
        
        # Load existing progress if resuming
        self._load_progress()
    
    def _load_progress(self):
        """Load existing progress from checkpoint."""
        progress = self.checkpoint_manager.get_loader_progress(self.loader_name)
        if progress:
            self.processed_ids = set(progress.get("processed_ids", []))
            self.current_batch = progress.get("current_batch", 0)
            self.total_items = progress.get("total_items", 0)
            self.failed_items = progress.get("failed_items", [])
            print(f"📂 Loaded {self.loader_name} progress: {len(self.processed_ids)} items processed, batch {self.current_batch}")
    
    def save_progress(self):
        """Save current progress to checkpoint."""
        progress_info = {
            "processed_ids": list(self.processed_ids),
            "current_batch": self.current_batch,
            "total_items": self.total_items,
            "failed_items": self.failed_items,
            "processed_count": len(self.processed_ids),
            "failure_count": len(self.failed_items)
        }
        
        self.checkpoint_manager.save_loader_progress(self.loader_name, progress_info)
    
    def mark_processed(self, item_id: str):
        """Mark an item as processed."""
        self.processed_ids.add(item_id)
    
    def is_processed(self, item_id: str) -> bool:
        """Check if an item has been processed."""
        return item_id in self.processed_ids
    
    def mark_failed(self, item_id: str, error_message: str):
        """Mark an item as failed."""
        self.failed_items.append({
            "item_id": item_id,
            "error": error_message,
            "timestamp": datetime.datetime.now().isoformat()
        })
    
    def set_total_items(self, total: int):
        """Set the total number of items to process."""
        self.total_items = total
    
    def increment_batch(self):
        """Increment the current batch number."""
        self.current_batch += 1
    
    def get_progress_stats(self) -> Dict:
        """Get current progress statistics."""
        return {
            "processed_count": len(self.processed_ids),
            "total_items": self.total_items,
            "current_batch": self.current_batch,
            "failure_count": len(self.failed_items),
            "completion_percentage": (len(self.processed_ids) / self.total_items * 100) if self.total_items > 0 else 0
        } 