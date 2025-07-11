# DEPRECATION NOTICE

## Deprecated Files in This Directory

This directory contains deprecated VLOS (Verslag Online System) and Verslag/Vergadering loaders that have been superseded by a new, more modular architecture.

### Deprecated Files:

#### Main Loaders:
- `enhanced_vlos_verslag_loader.py` - Enhanced VLOS verslag loader with comprehensive analysis
- `vlos_verslag_loader.py` - Basic VLOS verslag loader
- `verslag_loader.py` - Basic verslag loader
- `vergadering_loader.py` - Vergadering loader

#### Processors:
- `processors/enhanced_vlos_matching.py` - Comprehensive parliamentary discourse analysis
- `processors/vlos_processor.py` - VLOS XML processing logic
- `processors/vlos_matching.py` - VLOS activity matching utilities
- `processors/vlos_speaker_matching.py` - VLOS speaker matching logic

## Why These Were Deprecated

### 1. **Lack of Separation of Concerns**
- Mixed responsibilities: XML parsing, API matching, Neo4j operations, and analysis all in single files
- Difficult to test individual components
- Hard to maintain and extend

### 2. **Tight Coupling**
- Processors tightly coupled to specific XML schemas and API structures
- Hard to swap out components or add new functionality
- Difficult to reuse components in different contexts

### 3. **Complex Monolithic Architecture**
- Large files with multiple responsibilities
- Complex dependency chains
- Hard to understand and modify

### 4. **Limited Extensibility**
- Adding new analysis features required modifying core processing logic
- No clear plugin or extension mechanism
- Difficult to add new data sources or output formats

## New Architecture Principles

The new system will be built with:

### 1. **Clear Separation of Concerns**
- **Extractors**: Pure data extraction from XML/API sources
- **Transformers**: Data transformation and normalization
- **Matchers**: Entity matching and linking logic
- **Analyzers**: Parliamentary behavior analysis
- **Loaders**: Neo4j data persistence

### 2. **Modular Design**
- Independent, testable components
- Clear interfaces between modules
- Easy to swap out implementations

### 3. **Pipeline Architecture**
- Data flows through well-defined stages
- Each stage has clear inputs and outputs
- Easy to add new processing stages

### 4. **Configurable Processing**
- YAML-based configuration
- Pluggable components
- Different processing modes (full, incremental, analysis-only)

### 5. **Comprehensive Testing**
- Unit tests for each component
- Integration tests for full pipelines
- Performance benchmarks

## Migration Path

If you need to reference the old implementation:

1. **For XML Processing Logic**: See `processors/vlos_processor.py`
2. **For Matching Algorithms**: See `processors/enhanced_vlos_matching.py`
3. **For Speaker Matching**: See `processors/vlos_speaker_matching.py`
4. **For Analysis Logic**: See the comprehensive analysis functions in `enhanced_vlos_matching.py`

## New Architecture Location

The new modular architecture will be located in:
- `src/vlos/` - New VLOS processing system
- `src/vlos/extractors/` - Data extraction components
- `src/vlos/transformers/` - Data transformation components
- `src/vlos/matchers/` - Entity matching components
- `src/vlos/analyzers/` - Parliamentary analysis components
- `src/vlos/loaders/` - Data persistence components

---

**Date Deprecated**: January 2025
**Reason**: Architecture refactoring for better maintainability and extensibility
**Replacement**: New modular VLOS processing system in `src/vlos/` 