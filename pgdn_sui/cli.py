#!/usr/bin/env python3
"""
PGDN-SUI CLI Interface
"""

import click
import json
import sys
import logging
from pathlib import Path
from typing import List, Dict, Optional

from .scanner import EnhancedSuiScanner, SuiNodeResult, setup_logging
from .advanced import SuiDataExtractor, SuiDataResult
from .exceptions import PgdnSuiException, ScannerError, NetworkError, ValidationError


def display_batch_statistics(results):
    """Display throughput batch statistics to stderr"""
    if not results:
        return
    
    # Find a result with batch_stats to display
    batch_stats = None
    for result in results:
        if hasattr(result, 'batch_stats') and result.batch_stats:
            batch_stats = result.batch_stats
            break
    
    if not batch_stats:
        click.echo("No batch statistics available", err=True)
        return
    
    click.echo("\n--- Throughput Batch Statistics ---", err=True)
    click.echo(f"Batch size: {batch_stats.get('batch_size', 0)} nodes", err=True)
    click.echo(f"Valid nodes: {batch_stats.get('valid_nodes', 0)} with throughput data", err=True)
    
    tps_stats = batch_stats.get('tps')
    if tps_stats:
        click.echo(f"TPS: p10={tps_stats['p10']:.2f}, p50={tps_stats['p50']:.2f}, p90={tps_stats['p90']:.2f}, p95={tps_stats['p95']:.2f} (n={tps_stats['count']})", err=True)
    
    cps_stats = batch_stats.get('cps')
    if cps_stats:
        click.echo(f"CPS: p10={cps_stats['p10']:.2f}, p50={cps_stats['p50']:.2f}, p90={cps_stats['p90']:.2f}, p95={cps_stats['p95']:.2f} (n={cps_stats['count']})", err=True)
    
    # Show band distribution
    tps_bands = {"low": 0, "normal": 0, "high": 0, "none": 0}
    cps_bands = {"low": 0, "normal": 0, "high": 0, "none": 0}
    
    for result in results:
        if hasattr(result, 'tps_band'):
            tps_bands[result.tps_band or "none"] += 1
        if hasattr(result, 'cps_band'):
            cps_bands[result.cps_band or "none"] += 1
    
    if any(count > 0 for count in tps_bands.values() if count):
        click.echo(f"TPS bands: low={tps_bands['low']}, normal={tps_bands['normal']}, high={tps_bands['high']}", err=True)
    if any(count > 0 for count in cps_bands.values() if count):
        click.echo(f"CPS bands: low={cps_bands['low']}, normal={cps_bands['normal']}, high={cps_bands['high']}", err=True)
    
    click.echo("", err=True)


def format_json(data, pretty=False):
    """Helper function to format JSON consistently"""
    if pretty:
        return json.dumps(data, indent=2)
    return json.dumps(data, separators=(',', ':'))


@click.command()
@click.option('--mode', type=click.Choice(['discovery', 'deep', 'advanced']), required=True,
              help='Scan mode: discovery (node detection only), deep (full endpoint analysis), or advanced (Sui protocol data extraction)')
@click.option('--hostnames', multiple=True, help='List of hostnames to scan')
@click.option('--input-file', type=click.Path(exists=True), 
              help='JSON file with discovery results for deep mode enhancement')
@click.option('--input-json', help='JSON string with discovery results for deep mode enhancement')
@click.option('--timeout', type=int, default=5, help='Request timeout in seconds')
@click.option('--workers', type=int, default=3, help='Concurrent workers')
@click.option('--output', type=click.Path(), help='Save results to file')
@click.option('--format', type=click.Choice(['json', 'summary']), default='json',
              help='Output format (default: json)')
@click.option('--enhanced', is_flag=True, help='Include enhanced data (activeValidators, committee info) in advanced mode')
@click.option('--max-sample-length', type=int, default=200, help='Maximum sample length for truncation (default: 200, debug mode avoids truncation)')
@click.option('--show-batch-stats', is_flag=True, help='Display throughput percentile statistics for the batch')
@click.option('--pretty', is_flag=True, help='Pretty-print JSON output (default: compact)')
@click.option('--debug', is_flag=True, help='Enable debug logging')
@click.option('--quiet', is_flag=True, help='Suppress output except results')
def cli(mode, hostnames, input_file, input_json, timeout, workers, output, format, enhanced, max_sample_length, show_batch_stats, pretty, debug, quiet):
    """PGDN-SUI: Enhanced Sui network scanner with discovery, deep analysis, and advanced data extraction modes"""
    
    # Set up logging
    if debug:
        setup_logging(logging.DEBUG)
    elif quiet:
        setup_logging(logging.ERROR)
    else:
        setup_logging(logging.INFO)
    
    try:
        scanner = EnhancedSuiScanner(timeout=timeout, max_workers=workers)
        
        # Handle deep mode with existing results
        if mode == 'deep' and (input_file or input_json):
            if input_file and input_json:
                click.echo("Error: Cannot specify both --input-file and --input-json", err=True)
                sys.exit(1)
            
            if hostnames:
                click.echo("Warning: --hostnames ignored when using --input-file or --input-json", err=True)
            
            # Load existing results
            if input_file:
                if not quiet:
                    click.echo(f"Loading discovery results from {input_file}...", err=True)
                existing_results = EnhancedSuiScanner.load_results_from_file(input_file)
            else:
                if not quiet:
                    click.echo("Loading discovery results from JSON string...", err=True)
                try:
                    data = json.loads(input_json)
                    # Convert JSON to SuiNodeResult objects
                    existing_results = []
                    
                    # Handle new data/meta format and legacy formats
                    if isinstance(data, dict) and "data" in data:
                        # New data/meta format
                        nodes_data = []
                        for item in data["data"]:
                            if isinstance(item, dict) and "payload" in item:
                                nodes_data.append(item["payload"])
                            else:
                                nodes_data.append(item)
                    elif isinstance(data, dict) and "nodes" in data:
                        # Legacy format
                        nodes_data = data["nodes"]
                    elif isinstance(data, list):
                        # Direct array format
                        nodes_data = data
                    else:
                        nodes_data = data.get("nodes", data) if isinstance(data, dict) else data
                    
                    for node_data in nodes_data:
                        result = SuiNodeResult(
                            ip=node_data["ip"],
                            hostname=node_data.get("hostname", node_data["ip"]),
                            discovered_at=node_data.get("discovered_at", ""),
                            node_type=node_data.get("type", node_data.get("node_type", "unknown")),
                            confidence=node_data.get("confidence", 0.0),
                            capabilities=node_data.get("capabilities", []),
                            network=node_data.get("network", "unknown"),
                            discovery_time=node_data.get("discovery_time", 0.0),
                            classification_time=node_data.get("classification_time", 0.0),
                            total_scan_time=node_data.get("scan_time", node_data.get("total_scan_time", 0.0)),
                            accessible_ports=node_data.get("accessible_ports", []),
                            working_endpoints=node_data.get("working_endpoints", []),
                            analysis_level=node_data.get("analysis_level", "discovery")
                        )
                        existing_results.append(result)
                except (json.JSONDecodeError, KeyError) as e:
                    click.echo(f"Error parsing JSON input: {e}", err=True)
                    sys.exit(1)
            
            if not quiet:
                click.echo(f"Running deep analysis on {len(existing_results)} existing results...", err=True)
            
            results = scanner.process_existing_results_deep_mode(existing_results)
        
        # Handle advanced mode for Sui protocol data extraction
        elif mode == 'advanced':
            # Advanced mode with hostnames (run discovery first, then advanced)
            if hostnames and not (input_file or input_json):
                targets = list(hostnames)
                
                if not quiet:
                    click.echo(f"Running discovery scan for advanced mode on {len(targets)} targets...", err=True)
                
                scanner = EnhancedSuiScanner(timeout=timeout, max_workers=workers)
                discovery_results = scanner.scan_targets_discovery_mode(targets)
                
                if not discovery_results:
                    click.echo("No nodes discovered for advanced mode", err=True)
                    sys.exit(1)
                
                if not quiet:
                    click.echo(f"Discovery complete: {len(discovery_results)} nodes found, running deep analysis...", err=True)
                
                # Run deep analysis on discovery results to get detailed endpoint data
                deep_results = scanner.process_existing_results_deep_mode(discovery_results)
                
                if not quiet:
                    click.echo(f"Deep analysis complete: {len(deep_results)} nodes analyzed, extracting protocol data...", err=True)
                
                # Convert SuiNodeResult objects to dict format for extractor
                nodes_data = []
                for result in deep_results:
                    node_dict = {
                        "ip": result.ip,
                        "hostname": result.hostname,
                        "type": result.node_type,
                        "network": result.network,
                        "capabilities": result.capabilities,
                        "accessible_ports": result.accessible_ports,
                        "working_endpoints": result.working_endpoints
                    }
                    nodes_data.append(node_dict)
                
                # Create extractor with the data
                extractor_config = {
                    'timeout': timeout, 
                    'enhanced': enhanced,
                    'debug': debug,
                    'max_sample_length': max_sample_length,
                    'show_batch_stats': show_batch_stats
                }
                extractor = SuiDataExtractor(extractor_config)
                extractor.discovery_data = nodes_data
                
                # Extract advanced data
                advanced_results = extractor.extract_all_nodes()
                
            # Advanced mode with input file/json
            elif input_file or input_json:
                if hostnames:
                    click.echo("Warning: --hostnames ignored when using --input-file or --input-json", err=True)
                
                extractor_config = {
                    'timeout': timeout, 
                    'enhanced': enhanced,
                    'debug': debug,
                    'max_sample_length': max_sample_length,
                    'show_batch_stats': show_batch_stats
                }
                
                if input_file:
                    if not quiet:
                        click.echo(f"Loading discovery results from {input_file} for advanced data extraction...", err=True)
                    extractor = SuiDataExtractor.from_discovery_json(input_file, extractor_config)
                else:
                    if not quiet:
                        click.echo("Loading discovery results from JSON string for advanced data extraction...", err=True)
                    import tempfile
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
                        tmp_file.write(input_json)
                        tmp_file.flush()
                        extractor = SuiDataExtractor.from_discovery_json(tmp_file.name, extractor_config)
                    
                    import os
                    os.unlink(tmp_file.name)
                
                if not quiet:
                    click.echo("Extracting Sui protocol data from discovered nodes...", err=True)
                
                advanced_results = extractor.extract_all_nodes()
            
            else:
                click.echo("Error: --hostnames or --input-file/--input-json is required for advanced mode", err=True)
                sys.exit(1)
            
            # Output results
            if advanced_results:
                # Display batch statistics if requested
                if show_batch_stats and advanced_results:
                    display_batch_statistics(advanced_results)
                
                output_data = extractor.export_data(advanced_results, pretty=pretty)
                
                if output:
                    with open(output, 'w') as f:
                        f.write(output_data)
                    if not quiet:
                        click.echo(f"Advanced data extraction results saved to {output}", err=True)
                else:
                    click.echo(output_data)
            else:
                error_result = {
                    "error": "No Sui protocol data extracted"
                }
                
                if output:
                    with open(output, 'w') as f:
                        f.write(format_json(error_result, pretty))
                else:
                    click.echo(format_json(error_result, pretty))
                
                if not quiet:
                    click.echo("No Sui protocol data extracted", err=True)
                sys.exit(1)
            
            return
        
        # Handle fresh scan (discovery or deep mode)
        else:
            if not hostnames:
                click.echo("Error: --hostnames is required for fresh scans", err=True)
                sys.exit(1)
            
            if input_file or input_json:
                click.echo("Warning: --input-file and --input-json ignored for fresh scans", err=True)
            
            targets = list(hostnames)
            
            if not quiet:
                mode_desc = "discovery" if mode == "discovery" else "deep analysis"
                click.echo(f"Starting {mode_desc} scan of {len(targets)} targets...", err=True)
            
            if mode == 'discovery':
                results = scanner.scan_targets_discovery_mode(targets)
            else:  # mode == 'deep'
                results = scanner.scan_targets_deep_mode(targets)
        
        # Output results
        if results:
            output_data = scanner.export_results(results, format, pretty=pretty)
            
            if output:
                with open(output, 'w') as f:
                    f.write(output_data)
                if not quiet:
                    success_msg = f"Results saved to {output}"
                    if mode == 'discovery':
                        success_msg += f"\nTo run deep analysis: pgdn-sui --mode deep --input-file {output}"
                    click.echo(success_msg, err=True)
            else:
                click.echo(output_data)
        else:
            error_result = {
                "error": "No Sui nodes found"
            }
            
            if output:
                with open(output, 'w') as f:
                    f.write(format_json(error_result, pretty))
            else:
                click.echo(format_json(error_result, pretty))
            
            if not quiet:
                click.echo("No Sui nodes found", err=True)
            sys.exit(1)
    
    except Exception as e:
        error_result = {
            "error": str(e)
        }
        
        if output:
            with open(output, 'w') as f:
                f.write(format_json(error_result, pretty))
        else:
            click.echo(format_json(error_result, pretty))
        
        if not quiet:
            click.echo(f"Scan failed: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli()