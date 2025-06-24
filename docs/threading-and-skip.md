# Skip, Threading, and ID Checking Features

This document describes the skip functionality, threading improvements, and ID checking optimization added to the TK API data loaders.

## Skip Functionality

All major loaders now support skipping a specified number of items from the beginning of the dataset. This is useful for:

- Resuming processing from a specific point
- Avoiding already processed data
- Testing with smaller datasets
- Working around API limits or timeouts

### Command Line Options

#### Global Skip Count
```bash
python src/main.py --skip-count 18000
```
This will skip the first 18,000 items for all loaders.

#### Loader-Specific Skip Counts
```bash
python src/main.py --skip-activiteiten 18000 --skip-zaken 5000
```

Available loader-specific options:
- `--skip-activiteiten N` - Skip N activiteiten (agendapunten are processed through activiteiten)
- `--skip-zaken N` - Skip N zaken  
- `--skip-documents N` - Skip N documents
- `--skip-vergaderingen N` - Skip N vergaderingen

Note: Agendapunten are not processed as a standalone loader since every Agendapunt belongs to exactly one Activiteit.

Loader-specific skip counts override the global `--skip-count` for that particular loader.

### Examples

1. **Skip first 18,000 activiteiten and zaken:**
   ```bash
   python src/main.py --skip-activiteiten 18000 --skip-zaken 18000
   ```

2. **Skip first 10,000 items for all loaders:**
   ```bash
   python src/main.py --skip-count 10000
   ```

3. **Mixed approach - global skip with specific overrides:**
   ```bash
   python src/main.py --skip-count 5000 --skip-activiteiten 18000
   ```
   This skips 18,000 activiteiten but only 5,000 items for other loaders.

## Threading for Activiteiten

The activiteit loader now supports multithreading for significantly faster processing.

### Command Line Options

```bash
python src/main.py --threaded --max-workers 10
```

- `--threaded` - Enable threaded processing for activiteiten
- `--max-workers N` - Number of threads to use (default: 10)

### Performance Benefits

Threading can provide 3-10x speedup depending on:
- Network latency to Neo4j
- Database performance
- System resources
- Number of related entities per activiteit

### Thread Safety

The threaded implementation includes:
- Thread-safe Neo4j session handling (each thread gets its own session)
- Synchronized access to shared resources (like `PROCESSED_ZAAK_IDS`)
- Thread-safe progress tracking and error handling
- Proper checkpoint management across threads

## ID Checking Optimization

By default, the loader now checks which items already exist in Neo4j before processing them. This provides significant performance benefits for incremental loads.

### How It Works

1. **ID Collection**: Extract IDs from all items fetched from the API
2. **Batch Checking**: Query Neo4j to find which IDs already exist (processed in batches of 1,000)
3. **Filtering**: Only process items that don't already exist in the database
4. **Performance**: Typically 15,000+ IDs/second checking speed

### Command Line Options

```bash
# Default behavior - skip existing items
python src/main.py

# Force overwrite existing data
python src/main.py --overwrite
```

- **Default**: Check Neo4j for existing IDs and skip them
- `--overwrite` - Process all items regardless of whether they exist in Neo4j

### Benefits

- **Faster Incremental Loads**: Only process new data
- **Reduced API Calls**: Skip items that are already in the database
- **Resource Efficiency**: Save bandwidth, CPU, and database writes
- **Idempotent Operations**: Safe to run multiple times

### Performance Impact

The ID checking adds a small upfront cost but typically saves significant time:

- **ID Check Cost**: ~0.06 seconds for 1,000 IDs
- **Processing Savings**: Skip potentially thousands of unnecessary API calls and database writes
- **Net Benefit**: Usually 10-100x faster for incremental loads with existing data

## Combined Usage

You can combine all features:

```bash
python src/main.py --threaded --max-workers 10 --skip-activiteiten 18000 --overwrite
```

This will:
1. Skip the first 18,000 activiteiten
2. Process ALL remaining activiteiten (ignoring existing data due to `--overwrite`)
3. Use 10 threads for activiteiten processing
4. Use single-threaded processing for other loaders with ID checking enabled

## Testing

Use the test script to verify functionality:

```bash
python test_threaded_activiteit.py
```

This script demonstrates:
- Threaded processing with 10 workers
- Skipping the first 18,000 items
- Performance monitoring and statistics

## Implementation Details

### Supported Loaders
- `load_activiteiten` / `load_activiteiten_threaded` (includes agendapunten processing)
  - ✅ Skip functionality
  - ✅ Threading support
  - ✅ ID checking optimization
- `load_zaken`
  - ✅ Skip functionality
  - ❌ Threading (single-threaded)
  - ❌ ID checking (can be added if needed)
- `load_documents`
  - ✅ Skip functionality
  - ❌ Threading (single-threaded)
  - ❌ ID checking (can be added if needed)
- `load_vergaderingen`
  - ✅ Skip functionality
  - ❌ Threading (single-threaded)
  - ❌ ID checking (can be added if needed)

Note: `load_agendapunten` is deprecated since agendapunten should be processed through their parent activiteiten.

### Skip Implementation
The skip functionality works by:
1. Fetching the full dataset from the API
2. Slicing the list to skip the specified number of items: `items[skip_count:]`
3. Processing only the remaining items
4. Providing clear logging about what was skipped

### Threading Implementation
The threaded activiteit loader:
1. Uses `ThreadPoolExecutor` for thread management
2. Submits all items as separate tasks
3. Processes tasks as they complete
4. Provides real-time progress updates
5. Maintains thread-safe statistics and error handling

## Checkpoint Integration

All features work seamlessly with the checkpoint system:

- **Configuration Storage**: Skip counts, threading settings, and overwrite mode are stored in checkpoints
- **Compatibility Validation**: When resuming, the system validates that your current configuration matches the stored configuration
- **Resume Safety**: ID checking works with checkpoint resume - items are filtered based on both checkpoint progress AND Neo4j existence

### Configuration Conflicts

If you try to resume with different settings, you'll get a helpful error:

```
❌ Configuration mismatch detected!
   • skip_count: stored=18000, current=10000
   • overwrite: stored=False, current=True
   
   You can either:
   1. Use the same configuration as the original run
   2. Start a new run with --new-run
```

## Notes

- Skip functionality is applied after fetching data from the API but before processing
- ID checking is applied after skip but before checkpoint filtering
- Threading is currently only available for activiteiten (can be extended to other loaders)
- The skip count is validated against the total number of items to prevent errors
- ID checking adds ~0.06 seconds per 1,000 IDs but can save hours of unnecessary processing 