# Neo4j TK API Data Loader

A robust, resumable data loading system for importing TK (Tweede Kamer) API data into Neo4j with advanced checkpoint functionality, threading support, and comprehensive error handling.

## 🚀 Features

- **Resumable Loading**: Checkpoint system allows resuming interrupted data loads
- **Threading Support**: Multi-threaded processing for improved performance
- **Skip Functionality**: Skip already processed items or start from specific points
- **Error Handling**: Graceful error handling with detailed logging
- **ID Checking**: Automatic detection of existing data to avoid duplicates
- **Modular Design**: Clean separation of concerns with dedicated loaders

## 📁 Project Structure

```
neo4j-tkapi/
├── README.md                           # This file - project overview
├── requirements.txt                    # Python dependencies
├── src/                               # Main source code
│   ├── core/                          # Core system components
│   │   ├── checkpoint/                # Checkpoint management system
│   │   ├── connection/                # Database connection handling
│   │   └── config/                    # Configuration and constants
│   ├── loaders/                       # Data loader modules
│   │   ├── README.md                  # Loader documentation
│   │   └── [various loader files]
│   ├── utils/                         # Utility functions and helpers
│   └── main.py                        # Main entry point
├── docs/                              # Documentation
│   ├── checkpoint-system.md           # Checkpoint system guide
│   ├── threading-and-skip.md          # Threading and skip functionality
│   └── decorator-system.md            # Decorator usage guide
├── tests/                             # Test files
├── examples/                          # Usage examples
└── checkpoints/                       # Checkpoint data storage
```

## 🏃 Quick Start

### Basic Usage

```bash
# Start a new data loading run
python src/main.py

# Resume the most recent incomplete run
python src/main.py --resume

# List all available runs
python src/main.py --list-runs
```

### Advanced Usage

```bash
# Start with threading enabled
python src/main.py --threaded --max-workers 10

# Skip first 1000 items for all loaders
python src/main.py --skip-count 1000

# Start from a specific date
python src/main.py --start-date 2024-01-01
```

## 📚 Documentation

- **[Checkpoint System](docs/checkpoint-system.md)** - Complete guide to the checkpoint system
- **[Threading & Skip Features](docs/threading-and-skip.md)** - Threading and skip functionality
- **[Decorator System](docs/decorator-system.md)** - Using checkpoint decorators
- **[Data Loaders](src/loaders/README.md)** - Individual loader documentation

## 🔧 Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure your Neo4j connection (see [connection documentation](src/core/connection/README.md))
4. Run the data loader:
   ```bash
   python src/main.py
   ```

## 🛠️ Configuration

The system supports various configuration options:

- **Date Range**: `--start-date YYYY-MM-DD`
- **Threading**: `--threaded --max-workers N`
- **Skip Counts**: `--skip-count N` or loader-specific skips
- **Resume Options**: `--resume` or `--resume-run RUN_ID`
- **Data Handling**: `--overwrite` to ignore existing data

See individual documentation files for detailed configuration options.

## 🧪 Testing

```bash
# Run basic connection test
python test_connection.py

# Test threaded processing
python test_threaded_activiteit.py

# Run simple test
python simple_test.py
```

## 📊 Monitoring

The system provides comprehensive progress monitoring:

- Real-time progress updates
- Detailed error logging
- Checkpoint status tracking
- Performance statistics

Use the checkpoint CLI for detailed monitoring:

```bash
python src/core/checkpoint/checkpoint_cli.py list
```

## 🤝 Contributing

1. Follow the existing code structure
2. Add appropriate tests for new features
3. Update documentation for any changes
4. Use the checkpoint decorator system for new loaders

## 📄 License

[Add your license information here]

## 🆘 Support

For issues and questions:

1. Check the [documentation](docs/)
2. Review existing [issues](../../issues)
3. Create a new issue with detailed information

---

**Next Steps**: See the [Checkpoint System Guide](docs/checkpoint-system.md) to get started with advanced features. 