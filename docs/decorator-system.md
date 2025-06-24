# Checkpoint Decorator System

De checkpoint decorator maakt het veel eenvoudiger om loaders te maken met automatische checkpoint ondersteuning. In plaats van handmatig checkpoint logica in elke loader te implementeren, kun je gewoon een decorator gebruiken.

## Voordelen van de Decorator

### Zonder Decorator (Oude Manier)
```python
def load_items(conn, items, checkpoint_manager=None):
    checkpoint = None
    if checkpoint_manager:
        checkpoint = LoaderCheckpoint(checkpoint_manager, "load_items")
        checkpoint.set_total_items(len(items))
    
    processed_count = 0
    for i, item in enumerate(items, 1):
        if checkpoint and checkpoint.is_processed(f"{item.nummer}_{item.id}"):
            continue
        
        try:
            # Process item
            process_item(item)
            
            if checkpoint:
                checkpoint.mark_processed(f"{item.nummer}_{item.id}")
            processed_count += 1
            
            if checkpoint and processed_count % 25 == 0:
                checkpoint.save_progress()
                stats = checkpoint.get_progress_stats()
                print(f"Progress: {stats['processed_count']}/{stats['total_items']}")
                
        except Exception as e:
            if checkpoint:
                checkpoint.mark_failed(f"{item.nummer}_{item.id}", str(e))
            continue
    
    if checkpoint:
        checkpoint.save_progress()
```

### Met Decorator (Nieuwe Manier)
```python
@checkpoint_loader(checkpoint_interval=25)
def load_items(conn, items, _checkpoint_context=None):
    def process_single_item(item):
        # Process item
        process_item(item)
    
    if _checkpoint_context:
        _checkpoint_context.process_items(items, process_single_item)
    else:
        for item in items:
            process_single_item(item)
```

**Resultaat**: 90% minder code, automatische error handling, en geen kans op bugs in checkpoint logica!

## Beschikbare Decorators

### 1. `@checkpoint_loader` - Voor standaard items met `.id` attribute
```python
@checkpoint_loader(checkpoint_interval=25)
def load_documents(conn, documents, _checkpoint_context=None):
    def process_single_document(doc):
        # Your processing logic
        pass
    
    if _checkpoint_context:
        _checkpoint_context.process_items(documents, process_single_document)
```

### 2. `@checkpoint_zaak_loader` - Voor Zaken met `nummer + id` identifier
```python
@checkpoint_zaak_loader(checkpoint_interval=25)
def load_zaken(conn, zaken, _checkpoint_context=None):
    def process_single_zaak(zaak):
        # Your processing logic
        pass
    
    if _checkpoint_context:
        _checkpoint_context.process_items(zaken, process_single_zaak)
```

### 3. `@with_checkpoint` - Voor custom configuratie
```python
@with_checkpoint(
    checkpoint_interval=10,
    get_item_id=lambda item: f"custom_{item.custom_field}_{item.id}"
)
def load_custom_items(conn, items, _checkpoint_context=None):
    def process_single_item(item):
        # Your processing logic
        pass
    
    if _checkpoint_context:
        _checkpoint_context.process_items(items, process_single_item)
```

## Automatische Functionaliteit

De decorator zorgt automatisch voor:

✅ **Progress Tracking** - Elke N items wordt progress opgeslagen
✅ **Skip Processed** - Al verwerkte items worden overgeslagen bij herstart
✅ **Error Handling** - Individuele item fouten stoppen niet de hele batch
✅ **Progress Reporting** - Realtime progress updates in console
✅ **Final Statistics** - Eindrapport met totalen en fouten
✅ **Loader Registration** - Automatische registratie bij CheckpointManager

## Gebruik Patronen

### Patroon 1: Eenvoudige Items Processing
```python
@checkpoint_loader()
def load_simple_items(conn, items, _checkpoint_context=None):
    def process_item(item):
        with conn.driver.session() as session:
            session.execute_write(merge_node, 'Item', 'id', {'id': item.id, 'name': item.name})
    
    if _checkpoint_context:
        _checkpoint_context.process_items(items, process_item)
```

### Patroon 2: Complexe Processing met Handmatige Controle
```python
@checkpoint_loader()
def load_complex_items(conn, items, _checkpoint_context=None):
    if _checkpoint_context:
        _checkpoint_context.set_total_items(items)
    
    for i, item in enumerate(items, 1):
        if _checkpoint_context and _checkpoint_context.is_processed(item):
            continue
        
        try:
            # Multi-step processing
            result1 = step1(item)
            result2 = step2(item, result1)
            step3(item, result2)
            
            if _checkpoint_context:
                _checkpoint_context.mark_processed(item)
                _checkpoint_context.save_progress_if_needed(i)
                
        except Exception as e:
            if _checkpoint_context:
                _checkpoint_context.mark_failed(item, str(e))
            continue
```

### Patroon 3: Batch Processing
```python
@checkpoint_loader()
def load_batch_items(conn, items, batch_size=100, _checkpoint_context=None):
    if _checkpoint_context:
        _checkpoint_context.set_total_items(items)
    
    # Filter out already processed items
    items_to_process = []
    if _checkpoint_context:
        items_to_process = [item for item in items if not _checkpoint_context.is_processed(item)]
    else:
        items_to_process = items
    
    # Process in batches
    for i in range(0, len(items_to_process), batch_size):
        batch = items_to_process[i:i + batch_size]
        
        try:
            process_batch(conn, batch)
            
            # Mark all items in batch as processed
            if _checkpoint_context:
                for item in batch:
                    _checkpoint_context.mark_processed(item)
                _checkpoint_context.save_progress_if_needed(i + len(batch))
                
        except Exception as e:
            # Handle batch failure - could mark all as failed or process individually
            if _checkpoint_context:
                for item in batch:
                    _checkpoint_context.mark_failed(item, str(e))
```

## Migratie van Bestaande Loaders

Om een bestaande loader te converteren:

1. **Voeg decorator toe**:
   ```python
   @checkpoint_loader(checkpoint_interval=25)
   def load_items(conn, items, _checkpoint_context=None):  # Add _checkpoint_context parameter
   ```

2. **Verwijder handmatige checkpoint code**:
   - Verwijder `LoaderCheckpoint` initialisatie
   - Verwijder handmatige `is_processed` checks
   - Verwijder handmatige `mark_processed` calls
   - Verwijder handmatige `save_progress` calls

3. **Gebruik CheckpointContext**:
   ```python
   def process_single_item(item):
       # Your existing processing logic
       pass
   
   if _checkpoint_context:
       _checkpoint_context.process_items(items, process_single_item)
   else:
       # Fallback for when decorator is not used
       for item in items:
           process_single_item(item)
   ```

4. **Test de migratie**:
   ```bash
   python main.py --resume  # Should work exactly the same
   ```

## Debugging en Troubleshooting

### Checkpoint Context Niet Beschikbaar
Als `_checkpoint_context` None is, betekent dit dat de loader niet via de decorator wordt aangeroepen. Zorg ervoor dat:
- De decorator correct is toegepast
- De loader wordt aangeroepen vanuit `main.py` met `checkpoint_manager` parameter

### Custom ID Functions
Voor complexe ID requirements:
```python
@with_checkpoint(
    get_item_id=lambda item: f"{item.type}_{item.nummer}_{item.version}"
)
def load_versioned_items(conn, items, _checkpoint_context=None):
    # Implementation
```

### Performance Tuning
```python
@checkpoint_loader(checkpoint_interval=50)  # Save less frequently for better performance
def load_large_dataset(conn, items, _checkpoint_context=None):
    # Implementation
```

## Toekomstige Uitbreidingen

De decorator architectuur maakt het eenvoudig om nieuwe functionaliteit toe te voegen:

- **Retry Logic**: Automatische retry van gefaalde items
- **Parallel Processing**: Multi-threaded processing met checkpoint synchronisatie
- **Memory Management**: Automatische garbage collection tussen batches
- **Metrics Collection**: Gedetailleerde performance metrics
- **Alert Integration**: Notifications bij fouten of voltooiing

## Voorbeelden in de Codebase

Zie deze bestanden voor concrete voorbeelden:
- `src/example_simple_loader.py` - Basis voorbeelden
- `src/loaders/zaak_loader_refactored.py` - Complexe refactored loader
- `src/checkpoint_decorator.py` - Implementatie details 