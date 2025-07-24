# PGDN-SUI

Enhanced Sui network scanner with discovery, deep analysis, and advanced protocol data extraction modes.

You should not be using this library without consent from the Sui network operators. This tool is intended for educational and research purposes only.

## Features

- **Discovery Mode**: Fast node type detection and classification
- **Deep Analysis Mode**: Comprehensive endpoint scanning with service versions, SSL certificates, and security assessment
- **Advanced Mode**: Sui protocol data extraction including consensus metrics, validator data, and chain state
- **Rich Endpoint Data**: SSH banners, HTTP headers, Docker API detection, and service identification
- **Concurrent Execution**: High-performance scanning with configurable worker pools
- **Mode-Based Operation**: Choose between quick discovery, detailed analysis, or protocol data extraction
- **CLI and Library**: Use as Python package or command-line tool
- **JSON Output**: Structured results with multiple export formats

## Installation

```bash
pip install pgdn-sui
```

## Quick Start

### CLI Usage

#### Discovery Mode (Node Detection)
```bash
# Scan single hostname
pgdn-sui --mode discovery --hostnames sui.wizardfiction.com

# Scan multiple hostnames
pgdn-sui --mode discovery --hostnames sui.wizardfiction.com validator.sui.io

# Save results to file
pgdn-sui --mode discovery --hostnames sui.wizardfiction.com --output discovery.json
```

#### Deep Analysis Mode (Full Endpoint Scanning)
```bash
# Fresh deep scan
pgdn-sui --mode deep --hostnames sui.wizardfiction.com

# Deep analysis on existing discovery results
pgdn-sui --mode deep --input-file discovery.json

# With custom timeout and workers
pgdn-sui --mode deep --hostnames sui.wizardfiction.com --timeout 5 --workers 20
```

#### Advanced Mode (Sui Protocol Data Extraction)
```bash
# Fresh advanced scan (runs discovery first, then extracts protocol data)
pgdn-sui --mode advanced --hostnames sui.wizardfiction.com

# Advanced analysis on existing discovery results
pgdn-sui --mode advanced --input-file discovery.json

# Advanced analysis from JSON string
pgdn-sui --mode advanced --input-json '[{"ip":"node.com","type":"validator",...}]'

# Save results to file
pgdn-sui --mode advanced --hostnames sui.wizardfiction.com --output advanced.json

# Include enhanced data (activeValidators, committee info)  
pgdn-sui --mode advanced --hostnames sui.wizardfiction.com --enhanced
```

#### Output Options
```bash
# JSON format (default)
pgdn-sui --mode discovery --hostnames sui.wizardfiction.com --format json

# Summary format
pgdn-sui --mode discovery --hostnames sui.wizardfiction.com --format summary

# Save to file
pgdn-sui --mode deep --hostnames sui.wizardfiction.com --output results.json
```

### Library Usage

```python
from pgdn_sui import EnhancedSuiScanner

# Create scanner
scanner = EnhancedSuiScanner(timeout=3, max_workers=10)

# Discovery mode (fast node detection)
discovery_results = scanner.scan_targets_discovery_mode([
    "sui.wizardfiction.com",
    "validator.sui.io"
])

# Deep analysis mode (comprehensive scanning)
deep_results = scanner.scan_targets_deep_mode([
    "sui.wizardfiction.com"
])

# Process existing discovery results with deep analysis
enhanced_results = scanner.process_existing_results_deep_mode(discovery_results)

# Export results
json_output = scanner.export_results(results, format="json")
summary_output = scanner.export_results(results, format="summary")

# Load results from file
existing_results = EnhancedSuiScanner.load_results_from_file("discovery.json")
```

#### Advanced Protocol Data Extraction
```python
from pgdn_sui import SuiDataExtractor, SuiDataResult

# Method 1: Initialize from hostnames (runs discovery first)
extractor = SuiDataExtractor.from_hostnames(['fullnode.mainnet.sui.io'], {'timeout': 10})

# Method 2: Initialize extractor from discovery JSON file
extractor = SuiDataExtractor.from_discovery_json('discovery.json')

# Method 3: Load discovery data manually
discovery_data = [
    {
        "ip": "fullnode.mainnet.sui.io",
        "type": "hybrid",
        "network": "mainnet",
        "capabilities": ["json_rpc", "grpc"],
        "accessible_ports": [9000, 50051],
        "working_endpoints": ["https://fullnode.mainnet.sui.io/"]
    }
]
extractor = SuiDataExtractor({'timeout': 8})
extractor.discovery_data = discovery_data

# Extract Sui protocol data
results = extractor.extract_all_nodes()

# Export as flat JSON
flat_json = extractor.export_data(results)

# Access extracted data
for result in results:
    print(f"Node: {result.ip}")
    print(f"Sui Version: {result.sui_version}")
    print(f"Protocol Version: {result.protocol_version}")
    print(f"Current Epoch: {result.current_epoch}")
    print(f"Data Completeness: {result.data_completeness}")
```

## Scanner Modes

### Discovery Mode
- **Purpose**: Fast node type detection and basic capability assessment
- **Speed**: ~5-10 seconds per target
- **Output**: Node type, network, capabilities, accessible ports, working endpoints
- **Use Case**: Initial reconnaissance, network mapping, bulk scanning

### Deep Analysis Mode
- **Purpose**: Comprehensive endpoint analysis with detailed service information
- **Speed**: ~30-60 seconds per target
- **Output**: All discovery data plus:
  - Service banners and versions (SSH, HTTP, Docker)
  - SSL/TLS certificate details
  - HTTP headers and technology detection
  - Port categorization and security assessment
  - Sui-specific metrics and version information
- **Use Case**: Detailed security assessment, vulnerability research, infrastructure analysis

### Advanced Mode
- **Purpose**: Extract Sui-specific protocol data and metrics for analysis
- **Speed**: ~5-15 seconds per target (uses existing discovery data)
- **Output**: Sui protocol data including:
  - Protocol versions and chain state
  - Consensus metrics and validator information
  - Network peers and uptime statistics
  - Transaction rates and performance data
  - RPC method availability and metrics access
- **Use Case**: Blockchain analysis, network monitoring, protocol research

## Output Format

### Discovery Results
```json
{
  "scan_summary": {
    "total_nodes": 1,
    "scanner_version": "Enhanced Rich Endpoint v2.0 (Discovery Mode)",
    "by_type": {"validator": 1},
    "by_network": {"mainnet": 1}
  },
  "nodes": [
    {
      "ip": "sui.wizardfiction.com",
      "type": "validator",
      "network": "mainnet",
      "confidence": 0.95,
      "capabilities": ["grpc", "consensus", "prometheus_metrics"],
      "accessible_ports": [22, 443, 9100],
      "working_endpoints": ["grpc://sui.wizardfiction.com:8080"],
      "analysis_level": "discovery"
    }
  ]
}
```

### Deep Analysis Results
```json
{
  "scan_summary": {
    "total_nodes": 1,
    "scanner_version": "Enhanced Rich Endpoint v2.0 (Deep Analysis)",
    "endpoint_statistics": {
      "total_services_detected": 3,
      "total_ssl_certificates": 1,
      "nodes_with_deep_analysis": 1
    }
  },
  "nodes": [
    {
      "ip": "sui.wizardfiction.com",
      "type": "validator",
      "analysis_level": "deep",
      "service_versions": {
        "ssh_version": "SSH-2.0-OpenSSH_8.9p1 Ubuntu-3ubuntu0.1",
        "ssh_software": "OpenSSH_8.9p1 Ubuntu-3ubuntu0.1"
      },
      "ssl_info": {
        "certificate_count": 1,
        "certificates": {
          "443": {
            "subject": {"CN": "sui.wizardfiction.com"},
            "issuer": {"CN": "Let's Encrypt Authority X3"}
          }
        }
      },
      "version": {
        "build_info_version": "1.14.2",
        "sui_current_protocol_version": "48",
        "sui_epoch": "125"
      }
    }
  ]
}
```

### Advanced Results
```json
{
  "extraction_metadata": {
    "timestamp": "2025-07-22T20:32:30.371814",
    "total_nodes": 1,
    "successful_extractions": 1,
    "average_completeness": 1.0,
    "extractor_version": "SuiDataExtractor-v1.0"
  },
  "node_data": [
    {
      "ip": "fullnode.mainnet.sui.io",
      "port": 9000,
      "node_type": "hybrid",
      "network": "mainnet",
      "sui_version": null,
      "protocol_version": "87",
      "current_epoch": null,
      "checkpoint_height": 170549957,
      "validator_count": null,
      "consensus_round": null,
      "network_peers": null,
      "transaction_rate": null,
      "rpc_methods_available": [
        "sui_getChainIdentifier",
        "sui_getProtocolConfig",
        "sui_getTotalTransactionBlocks",
        "sui_getLatestCheckpointSequenceNumber"
      ],
      "metrics_data": {
        "grpc_accessible": true,
        "grpc_port": 50051
      },
      "data_completeness": 1.0,
      "extraction_errors": []
    }
  ]
}
```

## Development

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest

# Format code
black pgdn_sui/

# Type checking
mypy pgdn_sui/

# Lint
flake8 pgdn_sui/
```

## License

MIT License
