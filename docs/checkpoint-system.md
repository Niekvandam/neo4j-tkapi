# Checkpoint System for Neo4j TK API Data Loader

The checkpoint system provides robust, resumable data loading capabilities for the Neo4j TK API project. When the data loading process encounters errors or is interrupted, you can resume from where it left off without starting over.

## Features

- **Persistent Progress Tracking**: All progress is saved to disk in JSON files
- **Granular Resumption**: Resume at the individual item level (Zaak, Document, etc.)
- **Error Handling**: Failed items are logged but don't stop the entire process
- **Run Management**: Track multiple runs with detailed status information
- **Progress Monitoring**: Real-time progress updates with statistics

## Quick Start

### Basic Usage

```bash
# Start a new data loading run
python main.py

# Resume the most recent incomplete run
python main.py --resume

# Resume a specific run by ID
python main.py --resume-run run_20241201_143022

# List all available runs
python main.py --list-runs

# Clean up old checkpoint files
python main.py --cleanup
```

### Advanced Usage

```bash
# Start with a custom date range
python main.py --start-date 2024-06-01

# Force start a new run (don't resume)
python main.py --new-run --start-date 2024-01-01
```

## Checkpoint Management CLI

Use the dedicated CLI tool for detailed checkpoint management:

```bash
# List all runs with detailed progress
python checkpoint_cli.py list

# Show detailed information about a specific run
python checkpoint_cli.py show --run-id run_20241201_143022

# Delete a specific run
python checkpoint_cli.py delete --run-id run_20241201_143022

# Clean up old runs, keeping only the last 3
python checkpoint_cli.py cleanup --keep 3
```

## How It Works

### File Structure

Checkpoints are stored in the `checkpoints/` directory:

```
checkpoints/
├── run_20241201_143022.json          # Main run information
├── run_20241201_143022_load_zaken.json    # Zaak loader progress
├── run_20241201_143022_load_documents.json # Document loader progress
└── run_20241201_143022_load_vergaderingen.json # Vergadering loader progress
```

### Run States

- **running**: Currently in progress
- **completed**: Successfully finished
- **failed**: Encountered a critical error

### Loader States

- **in_progress**: Currently processing items
- **completed**: Successfully finished all items
- **failed**: Encountered a critical error that stopped the loader

## Supported Loaders

Currently, the following loaders support checkpoint functionality:

- ✅ **load_zaken**: Full checkpoint support with item-level tracking
- ✅ **load_documents**: Full checkpoint support with item-level tracking
- ✅ **load_vergaderingen**: Full checkpoint support with item-level tracking
- ✅ **load_agendapunten**: Full checkpoint support with item-level tracking
- ✅ **load_activiteiten**: Full checkpoint support with item-level tracking

## Error Handling

When errors occur:

1. **Individual Item Failures**: Logged but processing continues with remaining items
2. **Loader Failures**: The entire loader is marked as failed, but other loaders can still run
3. **System Failures**: The run is marked as incomplete and can be resumed later

### Example Error Recovery

```bash
# If load_zaken fails after processing 500/1000 items:
python main.py --resume

# The system will:
# 1. Skip the 500 already processed items
# 2. Continue from item 501
# 3. Maintain all existing progress
```

## Progress Monitoring

The system provides detailed progress information:

```
📊 Progress: 750/1000 (75.0%)
⚠️ 5 items failed during processing
```

### Progress Statistics

- **Processed Count**: Number of successfully processed items
- **Total Items**: Total number of items to process
- **Completion Percentage**: Overall progress percentage
- **Failure Count**: Number of items that failed to process
- **Current Batch**: Current processing batch (for batch-based loaders)

## Best Practices

### Regular Checkpointing

The system automatically saves progress:
- Every 25 items processed
- At the end of each loader
- When errors occur

### Monitoring Long-Running Processes

For large datasets:
1. Use `--list-runs` to monitor progress
2. Check the checkpoint CLI for detailed status
3. Monitor log output for error patterns

### Cleanup

Regularly clean up old checkpoints:
```bash
# Keep only the last 5 runs
python main.py --cleanup

# Or use the CLI for more control
python checkpoint_cli.py cleanup --keep 3
```

## Troubleshooting

### Common Issues

1. **"No incomplete runs found"**
   - All previous runs completed successfully
   - Start a new run with `python main.py`

2. **"Run not found"**
   - The specified run ID doesn't exist
   - Use `--list-runs` to see available runs

3. **Checkpoint files corrupted**
   - Delete the corrupted run files
   - Restart with a new run

### Recovery Procedures

If checkpoints become corrupted:

```bash
# List all runs to identify the problematic one
python checkpoint_cli.py list

# Delete the corrupted run
python checkpoint_cli.py delete --run-id run_20241201_143022

# Start fresh
python main.py --new-run
```

## Performance Considerations

### Checkpoint Frequency

- Progress is saved every 25 items by default
- More frequent saves = better resumability but slower performance
- Less frequent saves = faster performance but more potential rework

### Storage Usage

- Each checkpoint file is typically 1-10 KB
- Large runs with many failures may have larger checkpoint files
- Use cleanup regularly to manage disk usage

## Integration with Existing Code

### Adding Checkpoint Support to New Loaders

```python
from checkpoint_manager import LoaderCheckpoint, CheckpointManager

def my_loader(conn, checkpoint_manager: CheckpointManager = None):
    # Initialize checkpoint
    checkpoint = None
    if checkpoint_manager:
        checkpoint = LoaderCheckpoint(checkpoint_manager, "my_loader")
    
    # Set total items
    if checkpoint:
        checkpoint.set_total_items(len(items))
    
    for item in items:
        # Skip if already processed
        if checkpoint and checkpoint.is_processed(item.id):
            continue
        
        try:
            # Process item
            process_item(item)
            
            # Mark as processed
            if checkpoint:
                checkpoint.mark_processed(item.id)
                
        except Exception as e:
            # Mark as failed
            if checkpoint:
                checkpoint.mark_failed(item.id, str(e))
            continue
        
        # Save progress every 25 items
        if checkpoint and i % 25 == 0:
            checkpoint.save_progress()
```

## Future Enhancements

- [ ] Parallel processing with checkpoint coordination
- [ ] Web dashboard for monitoring runs
- [ ] Automatic retry mechanisms for failed items
- [ ] Integration with external monitoring systems
- [ ] Checkpoint compression for large runs 