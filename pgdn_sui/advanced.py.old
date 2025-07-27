#!/usr/bin/env python3
"""
Targeted Sui Node Interrogator - Refactored
Deep blockchain intelligence extraction using native Sui protocols and local SDK
"""

import json
import logging
import time
import asyncio
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import asdict
from datetime import datetime

from .models import SuiDataResult
from .protobuf_manager import SuiProtobufManager
from .utils import get_primary_port, calculate_gini_coefficient
from .extractors import (
    RpcExtractor, 
    GrpcExtractor, 
    MetricsExtractor, 
    GraphqlExtractor,
    WebsocketExtractor, 
    SuiClientExtractor
)

# gRPC imports - will be available after protobuf compilation
try:
    import grpc
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False

# Sui SDK imports
try:
    from pysui import SuiConfig, SyncClient
    SUI_SDK_AVAILABLE = True
except ImportError:
    SUI_SDK_AVAILABLE = False

logger = logging.getLogger(__name__)


class SuiDataExtractor:
    """
    Enhanced Sui node interrogator with native protocol support
    """
    
    # Enhanced Sui port intelligence
    SUI_PORT_INTEL = {
        9000: {"name": "JSON-RPC", "expected_for": ["public_rpc", "hybrid", "validator"], "critical": True},
        9100: {"name": "Prometheus", "expected_for": ["validator", "hybrid"], "critical": False}, 
        8080: {"name": "gRPC", "expected_for": ["validator", "hybrid"], "critical": True},
        50051: {"name": "Consensus-gRPC", "expected_for": ["validator", "hybrid"], "critical": True},
        9184: {"name": "WebSocket", "expected_for": ["public_rpc", "hybrid"], "critical": False},
        3030: {"name": "GraphQL", "expected_for": ["public_rpc", "hybrid"], "critical": False},
        8084: {"name": "Indexer", "expected_for": ["indexer", "hybrid"], "critical": False},
    }
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.timeout = self.config.get('timeout', 10)
        self.use_local_sui = self.config.get('use_local_sui', True)
        self.sui_binary = self.config.get('sui_binary', 'sui')
        self.logger = logging.getLogger(__name__)
        
        # Initialize protobuf manager
        self.protobuf_manager = SuiProtobufManager(
            self.config.get('sui_repo_path')
        )
        
        # Setup protobufs for gRPC
        if GRPC_AVAILABLE:
            self.grpc_ready = self.protobuf_manager.setup_protobufs()
            self.sui_grpc_stubs = self.protobuf_manager.get_sui_grpc_stubs()
        else:
            self.grpc_ready = False
            self.sui_grpc_stubs = None
        
        # Initialize Sui SDK if available
        self.sui_client = None
        if SUI_SDK_AVAILABLE and self.config.get('use_sdk', True):
            try:
                self.sui_client = SyncClient(SuiConfig.default_config())
                self.logger.info("SUCCESS: Sui SDK client initialized")
            except Exception as e:
                self.logger.warning(f"WARNING: Failed to initialize Sui SDK: {e}")

        # Initialize extractors
        self.rpc_extractor = RpcExtractor(self.timeout, self.config)
        self.grpc_extractor = GrpcExtractor(self.timeout, self.config, self.sui_grpc_stubs)
        self.metrics_extractor = MetricsExtractor(self.timeout, self.config)
        self.graphql_extractor = GraphqlExtractor(self.timeout, self.config)
        self.websocket_extractor = WebsocketExtractor(self.timeout, self.config)
        self.sui_client_extractor = SuiClientExtractor(self.timeout, self.config, self.sui_client)

    @classmethod
    def from_discovery_json(cls, json_file_path: str, config: Dict = None):
        """Initialize from discovery JSON file"""
        extractor = cls(config)
        extractor.discovery_data = extractor._load_discovery_data(json_file_path)
        extractor.logger.info(f"LOADED: {len(extractor.discovery_data)} Sui nodes for intelligence extraction")
        return extractor
    
    @classmethod
    def from_hostnames(cls, hostnames: List[str], config: Dict = None):
        """Initialize by running discovery and deep analysis on hostnames first, then prepare for advanced extraction"""
        if config is None:
            config = {}
        
        # Import here to avoid circular import
        from .scanner import EnhancedSuiScanner
        
        # Run discovery scan first
        scanner_config = {
            'timeout': config.get('timeout', 5),
            'max_workers': config.get('max_workers', 3)
        }
        scanner = EnhancedSuiScanner(timeout=scanner_config['timeout'], max_workers=scanner_config['max_workers'])
        discovery_results = scanner.scan_targets_discovery_mode(hostnames)
        
        if not discovery_results:
            raise ValueError(f"No Sui nodes discovered from hostnames: {hostnames}")
        
        # Run deep analysis on discovery results to get detailed endpoint data
        deep_results = scanner.process_existing_results_deep_mode(discovery_results)
        
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
        
        # Create instance and load the discovery data
        extractor = cls(config)
        extractor.discovery_data = nodes_data
        extractor.logger.info(f"DISCOVERED: {len(nodes_data)} Sui nodes from {len(hostnames)} hostnames for intelligence extraction (with deep analysis)")
        
        return extractor

    def _load_discovery_data(self, json_file_path: str) -> List[Dict]:
        """Load discovery data and focus on Sui nodes"""
        if not Path(json_file_path).exists():
            raise FileNotFoundError(f"Discovery file not found: {json_file_path}")
        
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        nodes = []
        if "data" in data:
            for item in data["data"]:
                if item.get("type") == "sui_node":
                    nodes.append(item["payload"])
        elif "nodes" in data:
            nodes = data["nodes"]
        elif isinstance(data, list):
            nodes = data
        
        # Only process confirmed Sui nodes
        sui_nodes = [node for node in nodes if self._is_sui_node(node)]
        self.logger.info(f"CONFIRMED: Found {len(sui_nodes)} confirmed Sui nodes for intelligence extraction")
        return sui_nodes

    def _is_sui_node(self, node: Dict) -> bool:
        """Confirm this is actually a Sui node worth interrogating"""
        node_type = node.get("node_type", node.get("type", ""))
        capabilities = node.get("capabilities", [])
        
        # Must be a known Sui node type
        if node_type not in ["validator", "hybrid", "public_rpc", "indexer"]:
            return False
            
        # Must have Sui-relevant capabilities
        sui_capabilities = ["grpc", "prometheus_metrics", "json_rpc", "websocket"]
        if not any(cap in capabilities for cap in sui_capabilities):
            return False
            
        return True

    def extract_all_nodes(self) -> List[SuiDataResult]:
        """Extract intelligence from all Sui nodes using async processing"""
        if not hasattr(self, 'discovery_data'):
            raise ValueError("No discovery data loaded. Use from_discovery_json() first.")
        
        # Run async extraction in event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            results = loop.run_until_complete(self._async_extract_all())
            return results
        finally:
            loop.close()

    async def _async_extract_all(self) -> List[SuiDataResult]:
        """Async extraction of all nodes"""
        results = []
        start_time = time.time()
        
        self.logger.info(f"STARTING: Intelligent Sui blockchain analysis of {len(self.discovery_data)} nodes")
        
        # Process nodes with controlled concurrency
        semaphore = asyncio.Semaphore(4)  # Limit concurrent connections
        
        async def extract_with_semaphore(node_data: Dict) -> Optional[SuiDataResult]:
            async with semaphore:
                try:
                    result = await self._extract_node_intelligence_async(node_data)
                    return result
                except Exception as e:
                    self.logger.error(f"EXTRACTION FAILED: Intelligence extraction failed for {node_data.get('ip', 'unknown')}: {e}")
                    # Return None instead of raising exception to continue processing other nodes
                    return None
        
        # Create tasks for all nodes
        tasks = [extract_with_semaphore(node) for node in self.discovery_data]
        completed_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for result in completed_results:
            if isinstance(result, SuiDataResult):
                results.append(result)
            elif isinstance(result, Exception):
                self.logger.error(f"TASK FAILED: Task failed with exception: {result}")
            # Handle None results (failed extractions)
            elif result is None:
                self.logger.debug("Task returned None (failed extraction)")
        
        extraction_time = time.time() - start_time
        self.logger.info(f"EXTRACTION COMPLETE: {len(results)} nodes analyzed in {extraction_time:.2f}s")
        
        return results

    async def _extract_node_intelligence_async(self, node_data: Dict) -> SuiDataResult:
        """Extract comprehensive intelligence from a single Sui node"""
        ip = node_data["ip"]
        node_type = node_data.get("node_type", node_data.get("type", "unknown"))
        capabilities = node_data.get("capabilities", [])
        discovered_ports = node_data.get("accessible_ports", [])
        
        self.logger.info(f"ANALYZING: Extracting intelligence from Sui {node_type} at {ip}")
        
        result = SuiDataResult(
            ip=ip,
            port=get_primary_port(discovered_ports, node_type),
            timestamp=datetime.utcnow(),
            node_type=node_type,
            network="unknown"
        )
        
        # Intelligence extraction pipeline using modular extractors
        intelligence_tasks = [
            ("rpc", self.rpc_extractor.extract_rpc_intelligence_async(result, ip, discovered_ports)),
            ("websocket", self.websocket_extractor.extract_websocket_intelligence_async(result, ip)),
            ("graphql", self.graphql_extractor.extract_graphql_intelligence_async(result, ip)),
            ("grpc", self.grpc_extractor.extract_grpc_intelligence_async(result, ip, discovered_ports)),
            ("metrics", self.metrics_extractor.extract_metrics_intelligence_async(result, ip)),
            ("local_sui", self.sui_client_extractor.extract_local_sui_intelligence_async(result, ip)),
        ]
        
        # Execute intelligence gathering tasks
        successful_extractions = 0
        total_extractions = len(intelligence_tasks)
        
        for source_name, task in intelligence_tasks:
            try:
                success = await task
                if success:
                    successful_extractions += 1
                    result.intelligence_sources.append(source_name)
                    self.logger.info(f"SUCCESS: {source_name} intelligence extracted from {ip}")
                else:
                    self.logger.debug(f"FAILED: {source_name} intelligence extraction failed for {ip}")
            except Exception as e:
                self.logger.error(f"EXTRACTION ERROR: {source_name} extraction error for {ip}: {e}")
                result.extraction_errors.append(f"{source_name}_extraction_error: {str(e)}")
        
        # Calculate intelligence completeness
        result.data_completeness = successful_extractions / total_extractions if total_extractions > 0 else 0.0
        
        # Post-processing analysis with error handling
        try:
            await self._post_process_intelligence(result)
        except Exception as e:
            self.logger.error(f"POST-PROCESSING ERROR: Post-processing error for {ip}: {e}")
            result.extraction_errors.append(f"post_processing_error: {str(e)}")
        
        self.logger.info(f"ANALYSIS COMPLETE: Intelligence extraction for {ip} complete: {result.data_completeness:.2f} completeness")
        return result

    async def _post_process_intelligence(self, result: SuiDataResult):
        """Enhanced post-processing with node health assessment"""
        
        # Standard post-processing
        if result.metrics_exposed and not result.rpc_exposed:
            result.metrics_snapshot["node_classification"] = "validator_only"
            self.logger.info(f"Node classified as validator-only (metrics exposed, RPC not accessible)")
            
            # For validator nodes, try to infer more information from available data
            if result.sui_version and not result.network:
                # Try to map version to likely network
                version_parts = result.sui_version.split('.')
                if len(version_parts) >= 2:
                    major_minor = f"{version_parts[0]}.{version_parts[1]}"
                    # This is heuristic - newer versions more likely on mainnet
                    try:
                        if float(major_minor) >= 1.8:
                            result.network = "mainnet"  # Most likely for recent versions
                            result.metrics_snapshot["network_inference"] = "version_heuristic"
                            self.logger.info(f"Inferred network as mainnet based on version {result.sui_version}")
                    except ValueError:
                        pass
        
        # Node Health Assessment
        await self._assess_node_health(result)
        
        # Standard validations
        await self._validate_intelligence_consistency(result)
        await self._calculate_derived_metrics(result)
        
        # Additional validator-specific analysis
        if result.is_active_validator:
            result.metrics_snapshot["validator_confidence"] = "high" if result.metrics_exposed else "medium"

    async def _assess_node_health(self, result: SuiDataResult):
        """Assess node health and flag problematic validators"""
        
        health_issues = []
        health_score = 1.0  # Start with perfect health
        
        # Check RPC responsiveness
        rpc_timeout_count = 0
        for key, value in result.service_response_times.items():
            if "rpc_" in key and value > 5.0:  # >5s response time
                rpc_timeout_count += 1
                health_issues.append(f"slow_rpc_response_{key}")
        
        if rpc_timeout_count >= 3:  # Multiple slow/timeout RPC endpoints
            health_issues.append("rpc_endpoints_unresponsive")
            health_score -= 0.4
            self.logger.warning(f"Multiple RPC endpoints unresponsive")
        
        # Check metrics quality
        metrics_size = result.metrics_snapshot.get("size_bytes", 0)
        if result.metrics_exposed and metrics_size == 0:
            health_issues.append("empty_metrics_response")
            health_score -= 0.3
            self.logger.warning(f"Metrics endpoint returns empty data")
        elif result.metrics_exposed and metrics_size < 1000:  # Very small metrics
            health_issues.append("minimal_metrics_data")
            health_score -= 0.2
            self.logger.warning(f"Metrics endpoint returns minimal data ({metrics_size} bytes)")
        
        # Check for complete lack of blockchain intelligence
        blockchain_data_fields = [
            result.current_epoch, result.checkpoint_height, result.chain_identifier,
            result.protocol_version, result.network
        ]
        populated_fields = sum(1 for field in blockchain_data_fields if field is not None and field != "unknown")
        
        if populated_fields == 0:
            health_issues.append("no_blockchain_intelligence")
            health_score -= 0.4
            self.logger.warning(f"No blockchain intelligence extracted")
        elif populated_fields <= 2:
            health_issues.append("limited_blockchain_intelligence")
            health_score -= 0.2
            self.logger.warning(f"Limited blockchain intelligence ({populated_fields}/5 fields)")
        
        # Check service availability pattern
        services_working = sum([
            result.rpc_exposed,
            result.websocket_available, 
            result.graphql_available,
            result.grpc_available,
            result.metrics_exposed
        ])
        
        if services_working <= 1:
            health_issues.append("minimal_service_availability")
            health_score -= 0.3
            self.logger.warning(f"Minimal service availability ({services_working}/5 services)")
        
        # Check for extraction errors
        if len(result.extraction_errors) > 3:
            health_issues.append("multiple_extraction_errors")
            health_score -= 0.2
        
        # Determine overall health status
        if health_score >= 0.8:
            health_status = "healthy"
        elif health_score >= 0.6:
            health_status = "degraded"  
        elif health_score >= 0.4:
            health_status = "problematic"
        else:
            health_status = "critical"
        
        # Store health assessment
        result.metrics_snapshot["node_health"] = {
            "status": health_status,
            "score": round(health_score, 2),
            "issues": health_issues,
            "assessment_timestamp": datetime.utcnow().isoformat()
        }
        
        # Log health assessment
        if health_status in ["problematic", "critical"]:
            self.logger.error(f"Node health: {health_status.upper()} (score: {health_score:.2f}) - Issues: {', '.join(health_issues)}")
        elif health_status == "degraded":
            self.logger.warning(f"Node health: {health_status.upper()} (score: {health_score:.2f}) - Issues: {', '.join(health_issues)}")
        else:
            self.logger.info(f"Node health: {health_status.upper()} (score: {health_score:.2f})")

    async def _validate_intelligence_consistency(self, result: SuiDataResult):
        """Validate intelligence for consistency and flag anomalies"""
        # Check epoch and checkpoint consistency
        if result.current_epoch and result.checkpoint_height:
            try:
                # Ensure both are integers for comparison
                epoch = int(result.current_epoch) if result.current_epoch else 0
                checkpoint = int(result.checkpoint_height) if result.checkpoint_height else 0
                
                if checkpoint < epoch:
                    result.extraction_errors.append("checkpoint_epoch_inconsistency")
            except (ValueError, TypeError):
                # If conversion fails, skip the comparison
                pass
        
        # Check validator stake consistency
        if result.is_active_validator and result.validator_stake and result.total_stake:
            try:
                stake_percentage = (float(result.validator_stake) / float(result.total_stake)) * 100
                if stake_percentage > 50:  # Single validator has >50% stake
                    result.extraction_errors.append("validator_majority_stake_warning")
                    
                result.voting_power_percentage = stake_percentage
            except (ValueError, ZeroDivisionError):
                pass

    async def _calculate_derived_metrics(self, result: SuiDataResult):
        """Calculate derived intelligence metrics"""
        # Calculate service availability score
        services = [
            result.rpc_exposed,
            result.websocket_available,
            result.graphql_available,
            result.grpc_available,
            result.metrics_exposed
        ]
        result.service_availability_score = sum(services) / len(services)
        
        # Calculate response time statistics
        if result.service_response_times:
            response_times = list(result.service_response_times.values())
            result.avg_response_time = sum(response_times) / len(response_times)
            result.max_response_time = max(response_times)
            result.min_response_time = min(response_times)

    def export_data(self, results: List[SuiDataResult], pretty: bool = False) -> str:
        """Export Sui data as flat JSON"""
        if pretty:
            return json.dumps([asdict(result) for result in results], indent=2, default=str)
        return json.dumps([asdict(result) for result in results], separators=(',', ':'), default=str)


# CLI Interface - maintaining the same entry point as original
def main():
    """CLI for Intelligent Sui Node Interrogator"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Intelligent Sui Node Interrogator - Deep blockchain intelligence extraction using native protocols",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Advanced intelligence extraction from discovery file
  python sui_data_extractor.py --mode advanced --input-file fn.json

  # Basic intelligence extraction
  python sui_data_extractor.py --mode basic --input-file fn.json --output basic_analysis.json

  # Validator-focused analysis
  python sui_data_extractor.py --mode validator --input-file fn.json --format validator_focused

  # Comprehensive analysis with local Sui tools
  python sui_data_extractor.py --mode advanced --input-file fn.json --use-local-sui --sui-repo-path /path/to/sui
        """
    )
    
    parser.add_argument('--mode', 
                       choices=['basic', 'advanced'],
                       default='advanced',
                       help='Analysis mode (default: advanced)')
    
    parser.add_argument('--input-file', required=True, 
                       help='Input file with discovered Sui nodes (JSON)')
    
    parser.add_argument('--output', default='sui_intelligence_data.json', 
                       help='Output file')
    
    parser.add_argument('--timeout', type=int, default=10, 
                       help='Request timeout seconds')
    
    parser.add_argument('--use-local-sui', action='store_true', 
                       help='Use local Sui CLI and SDK')
    
    parser.add_argument('--sui-binary', default='sui', 
                       help='Sui binary path')
    
    parser.add_argument('--sui-repo-path', 
                       help='Path to Sui repository for protobuf compilation')
    
    parser.add_argument('--enhanced', action='store_true',
                       help='Include full activeValidators array and committee info (large output)')
    
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Verbose logging')
    
    parser.add_argument('--quiet', '-q', action='store_true', 
                       help='Quiet mode')
    
    args = parser.parse_args()
    
    # Configure logging
    if args.quiet:
        level = logging.WARNING
    elif args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO
    
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')
    
    try:
        # Initialize scanner with configuration
        config = {
            'timeout': args.timeout,
            'use_local_sui': args.use_local_sui,
            'sui_binary': args.sui_binary,
            'sui_repo_path': args.sui_repo_path,
            'use_sdk': True,
            'enhanced': args.enhanced,
        }
        
        logger.info("Initializing Intelligent Sui Node Interrogator")
        extractor = SuiDataExtractor.from_discovery_json(args.input_file, config)
        
        # Extract intelligence
        logger.info("Starting comprehensive Sui blockchain intelligence extraction")
        results = extractor.extract_all_nodes()
        
        # Export intelligence
        output = extractor.export_data(results)
        
        # Save results
        with open(args.output, 'w') as f:
            f.write(output)
        
        logger.info(f"Intelligence extraction complete! Results saved to {args.output}")
        
        # Intelligence summary
        if results:
            avg_completeness = sum(r.data_completeness for r in results) / len(results)
            validators = len([r for r in results if r.is_active_validator])
            rpc_nodes = len([r for r in results if r.rpc_exposed])
            grpc_nodes = len([r for r in results if r.grpc_available])
            
            logger.info(f"Intelligence Extraction Summary:")
            logger.info(f"  Nodes analyzed: {len(results)}")
            logger.info(f"  Average intelligence completeness: {avg_completeness:.2f}")
            logger.info(f"  Active validators identified: {validators}")
            logger.info(f"  RPC nodes: {rpc_nodes}")
            logger.info(f"  gRPC nodes: {grpc_nodes}")
        
    except KeyboardInterrupt:
        logger.info("Intelligence extraction interrupted")
    except Exception as e:
        logger.error(f"Intelligence extraction failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        raise


if __name__ == "__main__":
    main()