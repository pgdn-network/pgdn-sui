# Sui Data Extraction Add-on Integration

## Task
Integrate the new `sui_data_addon.py` file into the Sui library to work seamlessly with discovery data from the general scanning library.

## What's Been Added
A new file `advanced.py` has been added to the repository. This is a focused Sui protocol data extraction add-on that:
- Takes discovery JSON from the general scanner as input
- Extracts Sui-specific protocol data (versions, epochs, validator data, consensus metrics)
- Outputs structured JSON data for external analysis systems
- Works with the discovered ports and endpoints, no redundant discovery

## Integration Requirements

### 1. File Placement
- Ensure `sui_data_addon.py` is in the correct location within the Sui library structure
- Update any necessary `__init__.py` files to include the new module

### 2. Dependencies
- Verify all required imports are available in the Sui library environment
- Check that `httpx`, `asyncio`, and other dependencies are properly handled
- Ensure compatibility with existing logging setup

### 3. CLI Integration
The add-on should be callable as:
```bash
python sui_data_addon.py --discovery-json discovery_results.json --output sui_protocol_data.json
```

### 4. Discovery JSON Compatibility
The add-on needs to handle discovery JSON from the general scanning library in formats:
- Data/meta format: `{"data": [{"type": "sui_node", "payload": {...}}], "meta": {...}}`
- Legacy format: `{"nodes": [...]}`
- Direct array format: `[...]`

### 5. Testing
- Test with sample discovery JSON files
- Verify data extraction works for different node types (validator, public_rpc, hybrid, monitoring)
- Confirm output formats (structured, flat, metrics_only) work correctly

## Expected Workflow
```
General Library Discovery → discovery.json → sui_data_addon.py → sui_protocol_data.json
```

The add-on should seamlessly extract Sui protocol data from nodes discovered by the general scanner, focusing purely on Sui-specific metrics and protocol information.

## Notes
- This is an add-on, not a replacement for existing functionality
- Focus is on data extraction, not security analysis
- Should work with the ports and endpoints discovered by the general scanner
- Output is structured JSON for consumption by external systems
