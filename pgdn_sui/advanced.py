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
from .response_format import standard_response, format_json
from .throughput_calculator import ThroughputCalculator
from .throughput_bands import get_band_summary_for_evidence
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
    
    # Port intelligence removed - now using capability-first probing approach
    
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
        
        # Setup protobufs for gRPC (can be disabled via config)
        if GRPC_AVAILABLE and not self.config.get('disable_grpc', False):
            self.grpc_ready = self.protobuf_manager.setup_protobufs()
            self.sui_grpc_stubs = self.protobuf_manager.get_sui_grpc_stubs()
        else:
            self.grpc_ready = False
            self.sui_grpc_stubs = None
            if self.config.get('disable_grpc', False):
                self.logger.debug("gRPC functionality disabled via configuration")
        
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
        
        # Initialize throughput calculator
        self.throughput_calculator = ThroughputCalculator(
            state_file_path=self.config.get('throughput_state_file'),
            ttl_hours=self.config.get('throughput_ttl_hours', 24)
        )

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
        
        # Calculate batch-level metrics (checkpoint lag, transaction throughput)
        try:
            self.rpc_extractor.calculate_batch_metrics(results)
            self.logger.info("Batch metrics calculation completed")
        except Exception as e:
            self.logger.error(f"Batch metrics calculation failed: {e}")
        
        # Update evidence strings with band information after batch processing
        try:
            self._update_throughput_evidence_with_bands(results)
            self.logger.debug("Throughput evidence updated with band information")
        except Exception as e:
            self.logger.error(f"Failed to update evidence with bands: {e}")
        
        return results

    def _update_throughput_evidence_with_bands(self, results: List[SuiDataResult]) -> None:
        """Update throughput evidence strings to include band information after batch processing"""
        for result in results:
            if (result.network_throughput and hasattr(result, 'set_evidence')):
                tps = result.network_throughput.get('tps')
                cps = result.network_throughput.get('cps')
                window = result.network_throughput.get('calculation_window_seconds')
                
                # Get band information 
                tps_band = getattr(result, 'tps_band', None)
                cps_band = getattr(result, 'cps_band', None)
                
                # Update evidence string with band information
                if tps is not None or cps is not None:
                    band_summary = get_band_summary_for_evidence(tps, cps, tps_band, cps_band)
                    
                    if window:
                        result.set_evidence("throughput", f"delta ok: {band_summary}/{window:.2f}s")
                    else:
                        result.set_evidence("throughput", f"delta ok: {band_summary}")
                        
                    self.logger.debug(f"Updated throughput evidence for {result.ip}: {band_summary}")

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
            network="unknown",
            hostname=node_data.get("hostname")
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
        
        # Capability-driven classification (before post-processing)
        try:
            self._classify_node_role(result)
        except Exception as e:
            self.logger.error(f"CLASSIFICATION ERROR: Node classification error for {ip}: {e}")
            result.extraction_errors.append(f"classification_error: {str(e)}")
        
        # Post-processing analysis with error handling
        try:
            await self._post_process_intelligence(result)
        except Exception as e:
            self.logger.error(f"POST-PROCESSING ERROR: Post-processing error for {ip}: {e}")
            result.extraction_errors.append(f"post_processing_error: {str(e)}")
        
        self.logger.info(f"ANALYSIS COMPLETE: Intelligence extraction for {ip} complete: {result.data_completeness:.2f} completeness")
        return result

    def _classify_node_role(self, result: SuiDataResult):
        """
        Rule 3: Exact node role classification with the specified names
        
        Derive booleans:
        grpc_detected := TCP connect success to 50051 or 8080 (or grpc health OK)
        has_narwhal_metrics := metrics_exposed == true AND metrics text contains prefixes {"narwhal_","consensus_"}
        
        Classification:
        1) If has_narwhal_metrics → "validator"
        2) Else if rpc_reachable and grpc_detected → "rpc_node_with_grpc"
        3) Else if rpc_reachable → "rpc_node"
        4) Else if metrics_exposed → "metrics"
        5) Else → "unknown"
        """
        
        # Rule 3: Detect gRPC presence via TCP connect to 50051 or 8080 (or grpc health OK)
        grpc_detected = (
            result.grpc_available or 
            (hasattr(result, 'open_ports') and result.open_ports and 
             result.open_ports.get('grpc', []))
        )
        
        # Rule 3: Use rpc_reachable from Rule 1 (HTTP 200 or 429/rate limit)
        rpc_reachable = result.rpc_reachable
        
        # Rule 3: has_narwhal_metrics requires both metrics_exposed AND narwhal/consensus prefixes
        has_narwhal_metrics = result.metrics_exposed and result.has_narwhal_metrics
        
        # Rule 3: Classification logic with exact names
        if has_narwhal_metrics:
            result.node_role = "validator"
            self.logger.info(f"Node classified as VALIDATOR: Narwhal metrics detected")
                
        elif rpc_reachable and grpc_detected:
            result.node_role = "rpc_node_with_grpc"
            self.logger.info(f"Node classified as RPC_NODE_WITH_GRPC: RPC reachable + gRPC detected")
            
        elif rpc_reachable:
            result.node_role = "rpc_node"
            self.logger.info(f"Node classified as RPC_NODE: RPC reachable only")
            
        elif result.metrics_exposed:
            result.node_role = "metrics"
            self.logger.info(f"Node classified as METRICS: Metrics exposed, RPC not reachable")
            
        else:
            result.node_role = "unknown"
            self.logger.info(f"Node classified as UNKNOWN: No clear capabilities detected")
        
        # Apply uptime expectation rules for RPC nodes with closed metrics
        if result.node_role in ["rpc_node", "rpc_node_with_grpc"] and not result.metrics_exposed:
            result.uptime_expected = False
            self.logger.info(f"Setting uptime_expected=false for {result.node_role} with closed metrics")
        
        # Set narwhal_missing_reason based on role and capabilities
        self._set_narwhal_missing_reason(result, grpc_detected)
        
        # Rule 4: Store classification evidence block (consistent format)
        result.metrics_snapshot["classification"] = {
            "evidence": {
                "rpc_reachable": rpc_reachable,
                "grpc_detected": grpc_detected,
                "has_narwhal_metrics": has_narwhal_metrics,
                "metrics_exposed": result.metrics_exposed
            }
        }

    def _set_narwhal_missing_reason(self, result: SuiDataResult, grpc_detected: bool):
        """
        Rule E: Set narwhal_missing_reason based on capability detection
        - has_narwhal_metrics drives validator expectation; grpc presence alone does NOT imply validator
        - Only set reason for validator_candidate nodes that are missing narwhal data
        """
        
        # Only set reason if narwhal metrics are missing
        if result.has_narwhal_metrics:
            result.narwhal_missing_reason = None  # No reason needed
            return
        
        # Rule E: Only check narwhal expectations for validator nodes
        if result.node_role == "validator":
            # This shouldn't happen since validator requires has_narwhal_metrics=true
            result.narwhal_missing_reason = "missing_metrics_data"
            return
        
        # For other node roles, check if they have validator-like characteristics but missing narwhal
        if result.node_role in ["rpc_node", "rpc_node_with_grpc"] and not result.metrics_exposed:
            # RPC nodes with no metrics - can't determine narwhal presence
            result.narwhal_missing_reason = "metrics_closed"
        elif result.node_role in ["rpc_node", "rpc_node_with_grpc"] and result.metrics_exposed:
            # RPC nodes with metrics but no narwhal data
            result.narwhal_missing_reason = "missing_metrics_data"
        else:
            # For metrics_only or unknown nodes, leave null
            result.narwhal_missing_reason = None

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
        
        # Calculate network throughput (TPS & CPS)
        await self._calculate_network_throughput(result)
        
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
        
        # Check metrics quality - improved logic with better error categorization
        metrics_size = result.metrics_snapshot.get("size_bytes", 0)
        tdd_metrics_size = result.metrics_snapshot.get("tdd_metrics_size_bytes", 0)
        
        # Check for HTML error pages in metrics (not a health issue, just wrong endpoint)
        has_html_error = (
            result.metrics_snapshot.get("tdd_metrics_html_error", False) or
            any(key.endswith("_html_error") for key in result.metrics_snapshot.keys())
        )
        
        # Check for empty metrics responses
        has_empty_metrics = (
            result.metrics_snapshot.get("tdd_metrics_empty", False) or
            any(key.endswith("_minimal_response") for key in result.metrics_snapshot.keys())
        )
        
        if has_html_error:
            health_issues.append("html_error_instead_of_metrics")
            health_score -= 0.1  # Less severe - just wrong endpoint type
            self.logger.warning(f"Metrics endpoints return HTML error pages instead of metrics")
        elif has_empty_metrics:
            health_issues.append("empty_metrics_response")
            health_score -= 0.2  # Moderate issue - endpoint exists but no data
            self.logger.warning(f"Metrics endpoints return empty responses")
        elif result.metrics_exposed and tdd_metrics_size == 0 and metrics_size == 0:
            health_issues.append("no_metrics_data")
            health_score -= 0.3
            self.logger.warning(f"Metrics endpoint returns no data")
        elif result.metrics_exposed and (tdd_metrics_size < 1000 and metrics_size < 1000):  # Very small metrics
            effective_size = max(tdd_metrics_size, metrics_size)
            if effective_size > 10:  # If we have some data, it's less concerning
                health_issues.append("minimal_metrics_data")
                health_score -= 0.1  # Reduced penalty for minimal but present data
                self.logger.warning(f"Metrics endpoint returns minimal data ({effective_size} bytes)")
            else:
                health_issues.append("empty_metrics_response") 
                health_score -= 0.2
                self.logger.warning(f"Metrics endpoint returns almost no data ({effective_size} bytes)")
        
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
        
        # Check for extraction errors - improved categorization
        critical_errors = [err for err in result.extraction_errors if "critical_failure" in err]
        html_errors = [err for err in result.extraction_errors if "html_error" in err]
        connection_errors = [err for err in result.extraction_errors if any(term in err for term in ["connection", "timeout", "network"])]
        
        total_errors = len(result.extraction_errors)
        
        if len(critical_errors) > 2:
            health_issues.append("multiple_critical_failures")
            health_score -= 0.3  # Critical failures are more serious
        elif len(html_errors) > 2:
            health_issues.append("multiple_html_errors")
            health_score -= 0.1  # HTML errors are less serious (wrong endpoint type)
        elif len(connection_errors) > 2:
            health_issues.append("multiple_connection_errors") 
            health_score -= 0.2  # Connection errors are moderately serious
        elif total_errors > 5:
            health_issues.append("multiple_extraction_errors")
            health_score -= 0.15  # Reduced penalty for mixed error types
        
        # Determine overall health status
        if health_score >= 0.8:
            health_status = "healthy"
        elif health_score >= 0.6:
            health_status = "degraded"  
        elif health_score >= 0.4:
            health_status = "problematic"
        else:
            health_status = "critical"
        
        # Apply extend.md scoring criteria
        health_score = await self._apply_extended_scoring(result, health_score, health_issues)
        
        # Re-evaluate health status after extended scoring
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

    async def _calculate_network_throughput(self, result: SuiDataResult):
        """
        Rule F: Calculate TPS and CPS with enhanced rate limit and guardrail support
        - Role-independent calculation
        - Stable delta_key with lowercased hostname/IP
        - Δt >= 1.0s guardrail
        - Independent reasons for TPS vs CPS
        """
        try:
            # Rule F: Handle unreachable RPC
            if result.rpc_status == "unreachable":
                result.network_throughput = {
                    "tps": None,
                    "cps": None,
                    "calculation_window_seconds": None,
                    "reason": "unreachable_rpc"
                }
                self.logger.info(f"Setting TPS/CPS to null for {result.ip}: RPC unreachable")
                return
            
            # Rule F: Handle rate-limited RPC - compute what we can
            if result.rpc_status == "rate_limited":
                # If we have some data despite rate limiting, try to compute
                if result.total_transactions is None and result.checkpoint_height is None:
                    result.network_throughput = {
                        "tps": None,
                        "cps": None,
                        "calculation_window_seconds": None,
                        "reason": "rate_limited"
                    }
                    self.logger.info(f"Setting TPS/CPS to null for {result.ip}: Rate limited, no data")
                    return
            
            # Extract required data from result
            network = result.network if result.network != "unknown" else "mainnet"  # Default fallback
            ip = result.ip
            port = result.port
            total_transactions = result.total_transactions
            checkpoint_height = result.checkpoint_height
            timestamp = result.timestamp
            
            # Rule F: Calculate throughput using ThroughputCalculator with stable delta_key
            hostname = getattr(result, 'hostname', None)
            throughput_data = self.throughput_calculator.calculate_throughput(
                network=network,
                ip=ip,
                port=port,
                total_transactions=total_transactions,
                checkpoint_height=checkpoint_height,
                timestamp=timestamp,
                hostname=hostname
            )
            
            # Rule F: Handle rate limiting in throughput data
            if result.rpc_status == "rate_limited":
                # If one counter is missing due to rate limit, set appropriate reason
                if total_transactions is None and checkpoint_height is not None:
                    throughput_data["tps"] = None
                    if "reason" not in throughput_data or throughput_data["reason"] is None:
                        throughput_data["reason"] = "rate_limited"
                elif checkpoint_height is None and total_transactions is not None:
                    throughput_data["cps"] = None
                    if "reason" not in throughput_data or throughput_data["reason"] is None:
                        throughput_data["reason"] = "rate_limited"
            
            # Store in result
            result.network_throughput = throughput_data
            
            # Log successful calculation (but not null values) and set evidence strings
            if throughput_data.get("tps") is not None or throughput_data.get("cps") is not None:
                tps = throughput_data.get('tps')
                cps = throughput_data.get('cps')
                window = throughput_data.get('calculation_window_seconds')
                
                # Create evidence string for successful calculation
                if tps is not None and cps is not None:
                    if hasattr(result, 'set_evidence'):
                        result.set_evidence("throughput", f"delta ok: tps={tps}/cps={cps}/{window:.2f}s")
                elif cps is not None:
                    if hasattr(result, 'set_evidence'):
                        result.set_evidence("throughput", f"delta ok: cps={cps}/{window:.2f}s")
                elif tps is not None:
                    if hasattr(result, 'set_evidence'):
                        result.set_evidence("throughput", f"delta ok: tps={tps}/{window:.2f}s")
                
                self.logger.info(f"Throughput calculated for {ip}: TPS={tps}, CPS={cps}, window={window}s")
            else:
                reason = throughput_data.get('reason', 'no_data')
                if hasattr(result, 'set_evidence'):
                    result.set_evidence("throughput", f"delta failed ({reason})")
                self.logger.debug(f"Throughput calculation returned null values for {ip}: {reason}")
                
        except Exception as e:
            self.logger.error(f"Failed to calculate network throughput for {result.ip}: {e}")
            # Set default empty throughput data on error
            result.network_throughput = {
                "tps": None,
                "cps": None, 
                "calculation_window_seconds": None,
                "reason": "calculation_error"
            }

    async def _apply_extended_scoring(self, result: SuiDataResult, current_score: float, health_issues: List[str]) -> float:
        """Apply scoring criteria from extend.md with public node classification"""
        extended_score = current_score
        
        # Check if this is a public node (neutral scoring)
        is_public = getattr(result, 'is_public_node', False)
        
        if is_public:
            # Public nodes: neutral scoring for uptime, don't mark down for closed metrics
            if result.metrics_surface and result.metrics_surface.get("http_status") == "closed":
                # Public nodes with closed metrics: neutral (don't penalize)
                result.metrics_snapshot["extended_scoring_neutral"] = "public_node_closed_metrics"
                self.logger.info(f"Public node neutral scoring: Closed metrics are acceptable for public RPC providers")
            elif result.metrics_surface and result.metrics_surface.get("http_status") in [401, 403]:
                # Public nodes with gated metrics: neutral (don't penalize)
                result.metrics_snapshot["extended_scoring_neutral"] = "public_node_gated_metrics"
                self.logger.info(f"Public node neutral scoring: Gated metrics for public RPC provider")
            
            # Don't apply uptime penalties to public nodes
            result.metrics_snapshot["extended_scoring_note"] = "public_node_uptime_neutral"
            
        elif result.uptime_expected is False:
            # RPC nodes with closed metrics: don't penalize for uptime issues
            result.metrics_snapshot["extended_scoring_neutral"] = "rpc_node_closed_metrics_no_uptime_penalty"
            self.logger.info(f"RPC node neutral scoring: uptime_expected=false, no uptime penalties applied")
            
        else:
            # Validator/private nodes: apply strict scoring
            
            # Mark down nodes that have no reachable RPC (cannot participate in network queries)
            if not result.rpc_reachable:
                health_issues.append("rpc_unreachable_marked_down")
                extended_score -= 0.3
                self.logger.warning(f"Validator marked down: No reachable RPC endpoints")
            
            # Rule 7: If rpc_status="rate_limited" but rpc_reachable=true → no penalty
            if result.rpc_status == "rate_limited" and result.rpc_reachable:
                result.metrics_snapshot["extended_scoring_neutral"] = "rate_limited_but_reachable"
                self.logger.info(f"Validator neutral scoring: Rate limited but still reachable")
            
            # Check for nodes that hide all uptime metrics while exposing other metrics publicly
            # This indicates partial hardening, not full security
            if result.metrics_exposed and result.uptime_status in ["unavailable_public_metrics", "closed"]:
                # Check if other metrics are available
                has_other_metrics = any(key in result.metrics_snapshot for key in [
                    "sui_metrics_total", "consensus_metrics_count", "narwhal_metrics_count"
                ])
                if has_other_metrics:
                    health_issues.append("partial_hardening_marked_down")
                    extended_score -= 0.2
                    self.logger.warning(f"Validator marked down: Hiding uptime metrics while exposing other metrics (partial hardening)")
            
            # Neutral scoring for nodes that gate metrics entirely (403/401) but have healthy RPC
            if (result.rpc_reachable and 
                not result.metrics_exposed and 
                any(result.metrics_snapshot.get(f"port_{port}_gated", False) for port in [9184, 9100])):
                # This is neutral - don't penalize or boost
                result.metrics_snapshot["extended_scoring_neutral"] = "gated_metrics_healthy_rpc"
                self.logger.info(f"Validator neutral scoring: Gated metrics but healthy RPC")
            
            # Rule 7: Mark up nodes that have both RPC and uptime metrics available and correct
            if (result.rpc_reachable and 
                result.uptime_status == "available" and 
                result.uptime_seconds_total is not None):
                health_issues.append("rpc_and_uptime_available_marked_up")
                extended_score += 0.1  # Boost score for fully accessible nodes
                self.logger.info(f"Validator marked up: Both RPC and uptime metrics are exposed and correct")
            
            # Rule 7: Boost score for validators with exposed metrics
            if result.node_role == "validator" and result.metrics_exposed:
                extended_score += 0.1
                health_issues.append("validator_with_metrics_marked_up")
                self.logger.info(f"Validator marked up: Validator role with exposed metrics")
        
        # Store extended scoring metadata
        result.metrics_snapshot["extended_scoring"] = {
            "rpc_status": result.rpc_status,
            "rpc_reachable": result.rpc_reachable,
            "uptime_status": result.uptime_status,
            "uptime_source": result.uptime_source,
            "uptime_expected": result.uptime_expected,
            "metrics_exposed": result.metrics_exposed,
            "is_public_node": is_public,
            "score_adjustment": round(extended_score - current_score, 2)
        }
        
        return max(0.0, min(1.0, extended_score))  # Clamp between 0 and 1

    def export_data(self, results: List[SuiDataResult], pretty: bool = False) -> str:
        """Export Sui data using standard response format"""
        data = [asdict(result) for result in results]
        
        response = standard_response(
            data=data,
            operation="data_extraction", 
            data_type="sui_data",
            stage="extract",
            total_nodes=len(results)
        )
        
        return format_json(response, pretty)


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
