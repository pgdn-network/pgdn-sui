#!/usr/bin/env python3
"""
Metrics Extractor
Handles Prometheus metrics extraction from Sui nodes
"""

import re
import time
import logging
import requests
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from ..models import SuiDataResult

logger = logging.getLogger(__name__)


class MetricsExtractor:
    """Handles Prometheus metrics extraction from Sui nodes"""
    
    def __init__(self, timeout: int = 10, config: Dict = None):
        self.timeout = timeout
        self.config = config or {}
        self.logger = logger

    async def extract_metrics_intelligence_async(self, result: SuiDataResult, ip: str) -> bool:
        """Enhanced metrics intelligence extraction with dual port fallback (extend.md)"""
        
        # First try dual port metrics extraction as specified in extend.md
        dual_port_success = await self._extract_dual_port_metrics(result, ip)
        
        # If dual port extraction succeeds, we're done
        if dual_port_success:
            return True
        
        # Fall back to original TDD high-impact metrics extraction
        tdd_success = await self._extract_tdd_prometheus_metrics(result, ip)
        
        # Then fall back to comprehensive metrics extraction
        comprehensive_success = await self._extract_comprehensive_metrics(result, ip)
        
        return tdd_success or comprehensive_success
    
    async def _extract_dual_port_metrics(self, result: SuiDataResult, ip: str) -> bool:
        """Extract metrics from dual ports (:9184 and :9100) as specified in extend.md"""
        
        # Try both ports as specified in extend.md
        metrics_ports = [
            {"port": 9184, "type": "sui_app", "url": f"http://{ip}:9184/metrics"},
            {"port": 9100, "type": "node_exporter", "url": f"http://{ip}:9100/metrics"}
        ]
        
        for port_info in metrics_ports:
            try:
                start_time = time.time()
                headers = {"Accept": "text/plain"}
                
                response = requests.get(
                    port_info["url"],
                    timeout=(2, 2),  # (connect_timeout, read_timeout)
                    headers=headers,
                    verify=False
                )
                
                response_time = time.time() - start_time
                
                if response.status_code == 200:
                    metrics_text = response.text
                    
                    # Check if we received valid metrics
                    if self._is_valid_metrics_response(metrics_text):
                        result.metrics_exposed = True
                        result.service_response_times[f"metrics_dual_{port_info['port']}"] = response_time
                        
                        self.logger.info(f"Dual port metrics extracted from {port_info['url']} ({len(metrics_text)} bytes)")
                        
                        # Extract uptime with priority-based approach (extend.md requirement)
                        uptime_value, evidence = await self._extract_uptime_with_priority(
                            result, metrics_text, response.status_code
                        )
                        result.uptime_evidence = evidence[:128]
                        
                        # For node_exporter, mark the uptime source
                        if port_info["type"] == "node_exporter" and uptime_value:
                            result.uptime_source = "node_exporter"
                        
                        # Extract other metrics
                        await self._extract_sui_blockchain_intelligence_from_metrics(result, metrics_text)
                        
                        # Classify public node and detect edge node
                        await self._classify_public_node(result, ip, response.headers)
                        await self._detect_edge_node(result, ip, response.headers)
                        
                        result.metrics_snapshot["dual_port_metrics_source"] = port_info["url"]
                        result.metrics_snapshot["dual_port_metrics_type"] = port_info["type"]
                        result.metrics_snapshot["dual_port_success"] = True
                        
                        return True
                    else:
                        self.logger.debug(f"Invalid metrics response from {port_info['url']}")
                        
                elif response.status_code in [401, 403]:
                    # Gated metrics
                    result.metrics_snapshot[f"port_{port_info['port']}_gated"] = True
                    self.logger.info(f"Metrics gated on port {port_info['port']} (HTTP {response.status_code})")
                    
                elif response.status_code == 404:
                    # Closed metrics
                    result.metrics_snapshot[f"port_{port_info['port']}_closed"] = True
                    self.logger.debug(f"Metrics endpoint not found on port {port_info['port']} (HTTP 404)")
                    
            except requests.exceptions.Timeout:
                result.metrics_snapshot[f"port_{port_info['port']}_timeout"] = True
                self.logger.debug(f"Metrics timeout on port {port_info['port']}")
                
            except requests.exceptions.ConnectionError as e:
                if "Connection refused" in str(e):
                    result.metrics_snapshot[f"port_{port_info['port']}_connection_refused"] = True
                    self.logger.debug(f"Metrics connection refused on port {port_info['port']}")
                else:
                    self.logger.debug(f"Metrics connection error on port {port_info['port']}: {e}")
                    
            except Exception as e:
                self.logger.debug(f"Metrics extraction error on port {port_info['port']}: {e}")
        
        # No successful metrics extraction from either port
        return False
    
    def _is_valid_metrics_response(self, metrics_text: str) -> bool:
        """Check if metrics response is valid"""
        if not metrics_text or len(metrics_text.strip()) < 10:
            return False
        
        # Check for HTML error pages
        if metrics_text.startswith('<!DOCTYPE html') or metrics_text.startswith('<html'):
            return False
        
        # Check for prometheus indicators or target metrics
        has_target_metrics = any(metric in metrics_text for metric in [
            "narwhal_primary_network_peers",
            "consensus_round", 
            "uptime_seconds_total",
            "sui_current_protocol_version",
            "build_info"
        ])
        
        has_prometheus_indicators = any(indicator in metrics_text for indicator in [
            "# HELP", "# TYPE"
        ])
        
        return has_target_metrics or has_prometheus_indicators
    
    async def _extract_tdd_prometheus_metrics(self, result: SuiDataResult, ip: str) -> bool:
        """Extract 5 high-impact TDD metrics using exact prompt.md specifications"""
        
        # Default endpoint as specified in prompt.md
        primary_endpoint = f"http://{ip}:9184/metrics"
        
        # Budget: ≤ 1 HTTP request per node for metrics
        try:
            start_time = time.time()
            
            # Headers and timeouts as specified in prompt.md
            headers = {"Accept": "text/plain"}
            
            # Timeouts: connect 2s, read 2s as specified
            response = requests.get(
                primary_endpoint, 
                timeout=(2, 2),  # (connect_timeout, read_timeout)
                headers=headers,
                verify=False
            )
            
            response_time = time.time() - start_time
            
            # Success condition: HTTP 200 + body contains at least one targeted metric or HELP/TYPE
            if response.status_code == 200:
                metrics_text = response.text
                
                # First, check if we received an HTML error page instead of metrics
                if metrics_text.startswith('<!DOCTYPE html') or metrics_text.startswith('<html'):
                    self.logger.warning(f"TDD metrics endpoint {primary_endpoint} returned HTML error page instead of metrics")
                    result.metrics_exposed = True  # Endpoint exists but returns wrong content
                    result.metrics_snapshot["tdd_metrics_html_error"] = True
                    
                    # Extract uptime with priority-based approach for HTML error
                    uptime_value, evidence = await self._extract_uptime_with_priority(result, "", response.status_code, "html_error")
                    result.uptime_evidence = evidence[:128]
                    
                    # Still detect edge node
                    await self._detect_edge_node(result, ip, response.headers)
                    return False
                
                # Check if response is empty or too small to be valid metrics
                if not metrics_text or len(metrics_text.strip()) < 10:
                    self.logger.warning(f"TDD metrics endpoint {primary_endpoint} returned empty or minimal response ({len(metrics_text)} bytes)")
                    result.metrics_exposed = True  # Endpoint exists but empty
                    result.metrics_snapshot["tdd_metrics_empty"] = True
                    
                    # Extract uptime with priority-based approach for empty response
                    uptime_value, evidence = await self._extract_uptime_with_priority(result, "", response.status_code, "empty_response")
                    result.uptime_evidence = evidence[:128]
                    
                    # Still detect edge node
                    await self._detect_edge_node(result, ip, response.headers)
                    return False
                
                # Check success condition: contains target metrics or prometheus indicators
                has_target_metrics = any(metric in metrics_text for metric in [
                    "narwhal_primary_network_peers",
                    "consensus_round", 
                    "uptime_seconds_total",
                    "sui_current_protocol_version",
                    "build_info"
                ])
                
                has_prometheus_indicators = any(indicator in metrics_text for indicator in [
                    "# HELP", "# TYPE"
                ])
                
                if has_target_metrics or has_prometheus_indicators:
                    result.metrics_exposed = True
                    result.service_response_times[f"metrics_tdd_{primary_endpoint}"] = response_time
                    
                    self.logger.info(f"TDD metrics extracted from {primary_endpoint} ({len(metrics_text)} bytes)")
                    
                    # Store metrics size for health assessment
                    result.metrics_snapshot["tdd_metrics_size_bytes"] = len(metrics_text)
                    
                    # Extract uptime with priority-based approach
                    uptime_value, evidence = await self._extract_uptime_with_priority(result, metrics_text, response.status_code)
                    result.uptime_evidence = evidence[:128]  # Ensure ≤128 chars
                    
                    # Classify public node and detect edge node
                    await self._classify_public_node(result, ip, response.headers)
                    await self._detect_edge_node(result, ip, response.headers)
                    
                    # Extract the other high-impact metrics
                    await self._extract_sui_blockchain_intelligence_from_metrics(result, metrics_text)
                    
                    result.metrics_snapshot["tdd_metrics_source"] = primary_endpoint
                    result.metrics_snapshot["tdd_metrics_success"] = True
                    
                    return True
                else:
                    # No target metrics found but HTTP 200
                    result.metrics_snapshot["tdd_metrics_no_targets"] = True
                    result.metrics_snapshot["tdd_metrics_size_bytes"] = len(metrics_text)
                    
                    uptime_value, evidence = await self._extract_uptime_with_priority(result, metrics_text, response.status_code)
                    result.uptime_evidence = evidence[:128]
                    
                    # Still detect edge node even without target metrics
                    await self._detect_edge_node(result, ip, response.headers)
                    
                    self.logger.debug(f"TDD metrics endpoint returned data ({len(metrics_text)} bytes) but no target metrics found")
                    return False
                    
            else:
                # Handle non-200 responses with uptime classification
                uptime_value, evidence = await self._extract_uptime_with_priority(result, "", response.status_code)
                result.uptime_evidence = evidence[:128]
                
                if response.status_code in [401, 403]:
                    result.metrics_exposed = False
                    result.metrics_snapshot["tdd_metrics_gated"] = True
                    self.logger.info(f"TDD metrics gated (HTTP {response.status_code}) - validator likely secured")
                elif response.status_code == 404:
                    result.metrics_exposed = True  # exposed=1 but closed
                    result.metrics_snapshot["tdd_metrics_closed"] = True
                    self.logger.info(f"TDD metrics endpoint not found (HTTP 404) - validator present but closed")
                else:
                    self.logger.debug(f"TDD metrics returned unexpected status: {response.status_code}")
                
                return False
                
        except requests.exceptions.Timeout:
            # Timeout - treat as closed per prompt.md  
            result.metrics_exposed = True  # exposed=1 but closed
            result.metrics_snapshot["tdd_metrics_timeout"] = True
            
            # Handle uptime classification for timeout
            uptime_value, evidence = await self._extract_uptime_with_priority(result, "", 0, "timeout")
            result.uptime_evidence = evidence[:128]
            
            self.logger.info(f"TDD metrics timeout - validator present but unresponsive")
            return False
            
        except requests.exceptions.ConnectionError as e:
            if "Connection refused" in str(e):
                # Connection refused - treat as closed per prompt.md
                result.metrics_exposed = True  # exposed=1 but closed  
                result.metrics_snapshot["tdd_metrics_connection_refused"] = True
                
                # Handle uptime classification for connection refused
                uptime_value, evidence = await self._extract_uptime_with_priority(result, "", 0, "connection_refused")
                result.uptime_evidence = evidence[:128]
                
                self.logger.info(f"TDD metrics connection refused - validator present but closed")
                return False
            else:
                # Network error - one retry as specified
                try:
                    self.logger.debug(f"TDD metrics network error, retrying once: {e}")
                    response = requests.get(
                        primary_endpoint,
                        timeout=(2, 2),
                        headers=headers, 
                        verify=False
                    )
                    
                    if response.status_code == 200:
                        # Same success logic as above
                        metrics_text = response.text
                        has_target_metrics = any(metric in metrics_text for metric in [
                            "narwhal_primary_network_peers", "consensus_round", "uptime_seconds_total",
                            "sui_current_protocol_version", "build_info"
                        ])
                        has_prometheus_indicators = any(indicator in metrics_text for indicator in [
                            "# HELP", "# TYPE"
                        ])
                        
                        if has_target_metrics or has_prometheus_indicators:
                            result.metrics_exposed = True
                            
                            # Extract uptime with priority-based approach
                            uptime_value, evidence = await self._extract_uptime_with_priority(result, metrics_text, response.status_code)
                            result.uptime_evidence = evidence[:128]
                            
                            # Detect edge node
                            await self._detect_edge_node(result, ip, response.headers)
                            
                            # Extract other metrics
                            await self._extract_sui_blockchain_intelligence_from_metrics(result, metrics_text)
                            result.metrics_snapshot["tdd_metrics_success_retry"] = True
                            self.logger.info(f"TDD metrics extracted on retry")
                            return True
                            
                except Exception as retry_error:
                    self.logger.debug(f"TDD metrics retry failed: {retry_error}")
                
                # If retry failed, still set uptime classification
                uptime_value, evidence = await self._extract_uptime_with_priority(result, "", 0, "connection_error")
                result.uptime_evidence = evidence[:128]
                
                return False
                
        except Exception as e:
            # Handle general extraction errors
            uptime_value, evidence = await self._extract_uptime_with_priority(result, "", 0, "error")
            result.uptime_evidence = evidence[:128]
            
            self.logger.debug(f"TDD metrics extraction error: {e}")
            return False
    
    async def _extract_comprehensive_metrics(self, result: SuiDataResult, ip: str) -> bool:
        """Enhanced metrics intelligence extraction with better validator detection (original logic)"""
        # Skip comprehensive extraction if TDD was already successful
        if result.metrics_snapshot.get("tdd_metrics_success"):
            self.logger.debug("TDD metrics already extracted, skipping comprehensive extraction")
            return True
            
        # Comprehensive metrics endpoint mapping (original logic)
        metrics_endpoints = [
            # Standard Sui validator metrics
            {"url": f"http://{ip}:9090/metrics", "type": "sui_validator", "critical": True},
            {"url": f"https://{ip}:9090/metrics", "type": "sui_validator", "critical": True},
            
            # Node Exporter system metrics (common on validators)
            {"url": f"http://{ip}:9100/metrics", "type": "node_exporter", "critical": False},
            {"url": f"https://{ip}:9100/metrics", "type": "node_exporter", "critical": False},
            
            # Alternative gRPC metrics
            {"url": f"http://{ip}:8080/metrics", "type": "grpc_metrics", "critical": False},
            {"url": f"https://{ip}:8080/metrics", "type": "grpc_metrics", "critical": False},
            
            # Alternative ports sometimes used
            {"url": f"http://{ip}:3000/metrics", "type": "alternative", "critical": False},
            {"url": f"http://{ip}:8000/metrics", "type": "alternative", "critical": False},
        ]
        
        intelligence_extracted = False
        metrics_endpoints_found = []
        
        for endpoint_info in metrics_endpoints:
            endpoint = endpoint_info["url"]
            endpoint_type = endpoint_info["type"]
            is_critical = endpoint_info["critical"]
            
            # Extract port from endpoint URL for consistent reference
            port = endpoint.split(':')[-1].split('/')[0]
            
            try:
                start_time = time.time()
                response = requests.get(endpoint, timeout=self.timeout, verify=False)
                response_time = time.time() - start_time
                
                if response.status_code == 200:
                    result.metrics_exposed = True
                    result.service_response_times[f"metrics_{endpoint}"] = response_time
                    
                    metrics_text = response.text
                    metrics_size = len(metrics_text)
                    
                    # Check if we got HTML error page instead of metrics
                    if metrics_text.startswith('<!DOCTYPE html') or metrics_text.startswith('<html'):
                        self.logger.warning(f"Port {port} returned HTML error page instead of metrics ({metrics_size} bytes)")
                        result.metrics_snapshot[f"port_{port}_html_error"] = True
                        continue
                    
                    # Check if response is empty or too small
                    if metrics_size < 10:
                        self.logger.warning(f"Port {port} returned empty or minimal metrics response ({metrics_size} bytes)")
                        result.metrics_snapshot[f"port_{port}_minimal_response"] = True
                        continue
                    
                    self.logger.info(f"Retrieved {metrics_size} bytes from {endpoint_type} on port {port}")
                    
                    # Analyze metrics content
                    metrics_analysis = await self._analyze_metrics_content(result, metrics_text, endpoint_type, port)
                    
                    if metrics_analysis["is_sui_metrics"]:
                        self.logger.info(f"Found Sui validator metrics on port {port}")
                        result.metrics_snapshot["sui_metrics_port"] = port
                        result.metrics_snapshot["sui_metrics_endpoint"] = endpoint
                        
                        # Extract uptime if not already set by TDD extraction
                        if result.uptime_status is None:
                            uptime_value, evidence = await self._extract_uptime_with_priority(result, metrics_text, response.status_code)
                            result.uptime_evidence = evidence[:128]
                            
                            # Detect edge node
                            await self._detect_edge_node(result, ip, response.headers)
                        
                        await self._extract_sui_blockchain_intelligence_from_metrics(result, metrics_text)
                        intelligence_extracted = True
                        
                    elif metrics_analysis["is_node_exporter"]:
                        self.logger.info(f"Found Node Exporter on port {port} - indicates monitored infrastructure")
                        result.metrics_snapshot["node_exporter_port"] = port
                        result.metrics_snapshot["infrastructure_monitoring"] = True
                        
                        # Extract uptime from node exporter if not already set
                        if result.uptime_status is None:
                            uptime_value, evidence = await self._extract_uptime_with_priority(result, metrics_text, response.status_code)
                            result.uptime_evidence = evidence[:128]
                            
                            # Detect edge node
                            await self._detect_edge_node(result, ip, response.headers)
                        
                        await self._analyze_system_metrics(result, metrics_text)
                        intelligence_extracted = True
                        
                    else:
                        self.logger.info(f"Found generic metrics on port {port}")
                        result.metrics_snapshot[f"generic_metrics_port_{port}"] = True
                        intelligence_extracted = True
                    
                    # Store common metrics info
                    result.metrics_snapshot[f"endpoint_{port}"] = {
                        "type": endpoint_type,
                        "size_bytes": metrics_size,
                        "response_time": response_time,
                        "metrics_count": metrics_analysis["metrics_count"],
                        "sui_indicators": metrics_analysis["sui_indicators"],
                    }
                    
                    metrics_endpoints_found.append({
                        "port": port,
                        "type": endpoint_type,
                        "size": metrics_size,
                        "is_sui": metrics_analysis["is_sui_metrics"]
                    })
                    
                elif response.status_code == 401:
                    self.logger.info(f"Port {port} requires authentication - likely secured validator")
                    result.metrics_snapshot[f"port_{port}_auth_required"] = True
                    
                elif response.status_code == 403:
                    self.logger.info(f"Port {port} access forbidden - likely secured validator")
                    result.metrics_snapshot[f"port_{port}_forbidden"] = True
                    
                else:
                    self.logger.debug(f"Port {port} returned status {response.status_code}")
                    
            except requests.exceptions.ConnectionError as e:
                if "Connection refused" in str(e):
                    result.metrics_snapshot[f"port_{port}_connection_refused"] = True
                    if is_critical:
                        self.logger.info(f"Critical port {port} connection refused - likely secured validator")
                else:
                    self.logger.debug(f"Connection error for port {port}: {e}")
                    
            except requests.exceptions.Timeout:
                self.logger.debug(f"Port {port} timed out")
                result.metrics_snapshot[f"port_{port}_timeout"] = True
                
            except Exception as e:
                self.logger.debug(f"Error checking port {port}: {e}")
                continue
        
        # Analyze the overall metrics pattern for validator detection
        await self._analyze_metrics_pattern_for_validator_detection(result, metrics_endpoints_found)
        
        return intelligence_extracted

    async def _analyze_metrics_content(self, result: SuiDataResult, metrics_text: str, endpoint_type: str, port: str) -> Dict:
        """Analyze metrics content to determine type and extract intelligence"""
        analysis = {
            "is_sui_metrics": False,
            "is_node_exporter": False,
            "metrics_count": 0,
            "sui_indicators": 0,
            "blockchain_metrics": 0,
        }
        
        # Count total metrics
        metric_lines = [line for line in metrics_text.split('\n') if line and not line.startswith('#')]
        analysis["metrics_count"] = len(metric_lines)
        
        # Check for Sui-specific indicators
        sui_patterns = [
            r'sui_\w+',
            r'consensus_\w+', 
            r'narwhal_\w+',
            r'validator_\w+',
            r'epoch\w*\s+\d+',
            r'checkpoint\w*\s+\d+',
        ]
        
        for pattern in sui_patterns:
            matches = re.findall(pattern, metrics_text, re.IGNORECASE)
            analysis["sui_indicators"] += len(matches)
        
        # Check for Node Exporter indicators
        node_exporter_patterns = [
            r'node_\w+',
            r'go_gc_duration_seconds',
            r'process_\w+',
            r'promhttp_\w+',
        ]
        
        node_exporter_count = 0
        for pattern in node_exporter_patterns:
            matches = re.findall(pattern, metrics_text)
            node_exporter_count += len(matches)
        
        # Determine metrics type
        if analysis["sui_indicators"] > 5:
            analysis["is_sui_metrics"] = True
            self.logger.info(f"Detected Sui metrics: {analysis['sui_indicators']} Sui indicators")
            
        elif node_exporter_count > 10:
            analysis["is_node_exporter"] = True
            self.logger.info(f"Detected Node Exporter: {node_exporter_count} system indicators")
        
        return analysis

    async def _extract_sui_blockchain_intelligence_from_metrics(self, result: SuiDataResult, metrics_text: str):
        """Extract blockchain intelligence directly from Sui metrics"""
        # Enhanced metrics patterns for comprehensive blockchain intelligence
        blockchain_patterns = {
            # Core blockchain state
            "current_epoch": (r'sui_epoch\s+(\d+)', int),
            "checkpoint_height": (r'sui_checkpoint_sequence_number\s+(\d+)', int),
            "protocol_version": (r'sui_current_protocol_version\s+(\d+)', str),
            "total_transactions": (r'sui_total_transaction_blocks\s+(\d+)', int),
            "reference_gas_price": (r'sui_reference_gas_price\s+(\d+)', int),
            
            # Enhanced Consensus metrics (from prompt.md)
            "consensus_round": (r'consensus_round\s+(\d+)', int),
            "consensus_commit_latency_seconds": (r'consensus_commit_latency_seconds\s+([0-9.]+)', float),
            "narwhal_round": (r'narwhal_round\s+(\d+)', int),
            "narwhal_certificate_created_total": (r'narwhal_certificate_created_total\s+(\d+)', int),
            "narwhal_primary_network_peers": (r'narwhal_primary_network_peers\s+(\d+)', int),
            "consensus_proposals_in_queue": (r'consensus_proposals_in_queue\s+(\d+)', int),
            "consensus_rejected_transactions_total": (r'consensus_rejected_transactions_total\s+(\d+)', int),
            
            
            # Enhanced Mempool / Transaction Pipeline (from prompt.md)
            "mempool_transactions_total": (r'mempool_transactions_total\s+(\d+)', int),
            "mempool_pending_transactions": (r'mempool_pending_transactions\s+(\d+)', int),
            
            # Legacy patterns (keep for compatibility)
            "narwhal_round_legacy": (r'narwhal_primary_current_round\s+(\d+)', int),
            "consensus_latency": (r'consensus_commit_latency_ms\s+([0-9.]+)', float),
            "narwhal_certificates": (r'narwhal_certificate_created_total\s+(\d+)', int),
            
            # Network metrics
            "network_peers": (r'sui_network_peers\s+(\d+)', int),
            "mempool_size": (r'narwhal_mempool_size\s+(\d+)', int),
            "transaction_rate": (r'sui_transaction_rate\s+([0-9.]+)', float),
            
            # Validator metrics
            "validator_count": (r'sui_validator_committee_size\s+(\d+)', int),
            "validator_stake": (r'sui_validator_stake\s+(\d+)', int),
            "voting_power": (r'sui_validator_voting_power\s+(\d+)', int),
        }
        
        metrics_extracted = 0
        
        for field_name, (pattern, data_type) in blockchain_patterns.items():
            # Use multiline mode for TDD patterns that need ^ and $ anchors
            if field_name in ["uptime_seconds_total"]:
                match = re.search(pattern, metrics_text, re.MULTILINE)
            else:
                match = re.search(pattern, metrics_text, re.IGNORECASE)
            if match:
                try:
                    value = data_type(match.group(1))
                    
                    # Map to result fields
                    if field_name == "current_epoch" and not result.current_epoch:
                        result.current_epoch = value
                        self.logger.info(f"Epoch from metrics: {value}")
                        
                    elif field_name == "checkpoint_height" and not result.checkpoint_height:
                        result.checkpoint_height = value
                        self.logger.info(f"Checkpoint from metrics: {value}")
                        
                    elif field_name == "protocol_version" and not result.protocol_version:
                        result.protocol_version = str(value)
                        self.logger.info(f"Protocol version from metrics: {value}")
                        
                    elif field_name == "total_transactions" and not result.total_transactions:
                        result.total_transactions = value
                        self.logger.info(f"Total transactions from metrics: {value}")
                        
                    # Enhanced consensus metrics (from prompt.md)
                    elif field_name == "consensus_round" and not result.consensus_round:
                        result.consensus_round = value
                        
                    elif field_name == "consensus_commit_latency_seconds" and not result.consensus_commit_latency_seconds:
                        result.consensus_commit_latency_seconds = value
                        
                    elif field_name == "narwhal_round" and not result.narwhal_round:
                        result.narwhal_round = value
                        
                    elif field_name == "narwhal_certificate_created_total" and not result.narwhal_certificate_created:
                        result.narwhal_certificate_created = value
                        
                    elif field_name == "narwhal_primary_network_peers" and not result.narwhal_primary_network_peers:
                        result.narwhal_primary_network_peers = value
                        
                    elif field_name == "consensus_proposals_in_queue" and not result.consensus_proposals_in_queue:
                        result.consensus_proposals_in_queue = value
                        
                    elif field_name == "consensus_rejected_transactions_total" and not result.consensus_rejected_transactions_total:
                        result.consensus_rejected_transactions_total = value
                        
                    # Enhanced mempool metrics (from prompt.md)
                    elif field_name == "mempool_transactions_total" and not result.mempool_transactions_total:
                        result.mempool_transactions_total = value
                        
                    elif field_name == "mempool_pending_transactions" and not result.mempool_pending_transactions:
                        result.mempool_pending_transactions = value
                        
                        
                    # Legacy compatibility
                    elif field_name == "narwhal_round_legacy" and not result.narwhal_round:
                        result.narwhal_round = value
                        
                    elif field_name == "network_peers" and not result.network_peers:
                        result.network_peers = value
                    
                    # Store in metrics snapshot
                    result.metrics_snapshot[f"extracted_{field_name}"] = value
                    metrics_extracted += 1
                    
                except (ValueError, TypeError) as e:
                    self.logger.debug(f"Failed to parse {field_name}: {e}")
        
        # Extract build_info using exact TDD pattern from prompt.md
        # Pattern needs to handle version and git_commit in any order
        build_info_pattern = r'^build_info\{([^}]*)\}\s+1\s*$'
        for line in metrics_text.split('\n'):
            build_info_match = re.match(build_info_pattern, line, re.MULTILINE)
            if build_info_match:
                labels_text = build_info_match.group(1)
                
                # Extract version and git_commit from labels using separate patterns
                version_match = re.search(r'version="([^"]+)"', labels_text)
                commit_match = re.search(r'git_commit="([a-f0-9]+)"', labels_text)
                
                if version_match and not result.build_info_version:
                    result.build_info_version = version_match.group(1)
                    result.metrics_snapshot["build_info_version_from_metrics"] = result.build_info_version
                    metrics_extracted += 1
                    self.logger.info(f"Build version from metrics: {result.build_info_version}")
                    
                    # Also set sui_version for backward compatibility
                    if not result.sui_version:
                        result.sui_version = version_match.group(1)
                        result.metrics_snapshot["sui_version_from_metrics"] = result.sui_version
                
                if commit_match and not result.build_info_git_commit:
                    result.build_info_git_commit = commit_match.group(1)
                    result.metrics_snapshot["build_info_git_commit_from_metrics"] = result.build_info_git_commit
                    metrics_extracted += 1
                    self.logger.info(f"Git commit from metrics: {result.build_info_git_commit}")
                
                break  # Use first match per prompt.md specification
        
        # Look for validator-specific evidence
        validator_evidence = await self._detect_validator_evidence_from_metrics(result, metrics_text)
        if validator_evidence:
            metrics_extracted += len(validator_evidence)
        
        result.metrics_snapshot["blockchain_metrics_extracted"] = metrics_extracted
        self.logger.info(f"Extracted {metrics_extracted} blockchain metrics")
        
        # Calculate derived metrics
        await self._calculate_transaction_throughput(result, metrics_text)
        await self._calculate_checkpoint_lag(result)

    async def _detect_validator_evidence_from_metrics(self, result: SuiDataResult, metrics_text: str) -> Dict:
        """Detect evidence that this node is a validator from metrics"""
        evidence = {}
        
        # Validator-specific metric patterns
        validator_patterns = {
            "consensus_participation": r'consensus_committed_certificates\s+(\d+)',
            "validator_rewards": r'sui_validator_epoch_rewards\s+(\d+)',
            "committee_membership": r'sui_validator_in_committee\s+1',
            "stake_delegation": r'sui_validator_total_stake\s+(\d+)',
            "narwhal_primary": r'narwhal_primary_.*\s+\d+',
            "consensus_authority": r'consensus_authority_.*\s+\d+',
        }
        
        for evidence_type, pattern in validator_patterns.items():
            matches = re.findall(pattern, metrics_text, re.IGNORECASE)
            if matches:
                evidence[evidence_type] = len(matches)
                self.logger.info(f"Validator evidence - {evidence_type}: {len(matches)} indicators")
        
        # Strong validator indicators
        strong_indicators = [
            "consensus_participation",
            "committee_membership", 
            "narwhal_primary",
            "consensus_authority"
        ]
        
        strong_evidence_count = sum(1 for indicator in strong_indicators if indicator in evidence)
        
        if strong_evidence_count >= 2:
            result.is_active_validator = True
            result.metrics_snapshot["validator_confidence"] = "high_metrics_evidence"
            self.logger.info(f"HIGH CONFIDENCE: Node is active validator ({strong_evidence_count} strong indicators)")
            
        elif len(evidence) >= 3:
            result.is_active_validator = True
            result.metrics_snapshot["validator_confidence"] = "moderate_metrics_evidence"
            self.logger.info(f"MODERATE CONFIDENCE: Node likely validator ({len(evidence)} indicators)")
        
        # Store evidence
        if evidence:
            result.metrics_snapshot["validator_evidence_from_metrics"] = evidence
        
        return evidence

    async def _analyze_metrics_pattern_for_validator_detection(self, result: SuiDataResult, endpoints_found: List[Dict]):
        """Analyze overall metrics exposure pattern to detect validator characteristics"""
        
        # Analyze the pattern of metrics endpoints
        sui_endpoints = [e for e in endpoints_found if e.get("is_sui")]
        node_exporter_endpoints = [e for e in endpoints_found if e.get("type") == "node_exporter"]
        
        # Classification logic
        if sui_endpoints:
            # Direct Sui metrics = definitely a Sui node
            result.metrics_snapshot["node_classification"] = "sui_node_with_metrics"
            if result.is_active_validator is None:
                result.is_active_validator = True
                result.metrics_snapshot["validator_confidence"] = "sui_metrics_available"
                
        elif node_exporter_endpoints and not result.rpc_exposed:
            # Node Exporter only + no RPC = likely secured validator with monitoring
            result.metrics_snapshot["node_classification"] = "monitored_infrastructure_no_rpc"
            
            # Check for connection refused on critical ports
            critical_ports_refused = any(
                result.metrics_snapshot.get(f"port_{port}_connection_refused", False)
                for port in ["9090", "9000", "8080"]
            )
            
            if critical_ports_refused:
                result.is_active_validator = True
                result.metrics_snapshot["validator_confidence"] = "secured_validator_pattern"
                result.network = "mainnet"  # Secured validators typically on mainnet
                self.logger.info(f"SECURED VALIDATOR: Monitoring present + critical ports secured")
                
        elif not endpoints_found:
            # No metrics at all
            result.metrics_snapshot["node_classification"] = "no_metrics_available"
            
        # Store endpoints summary
        result.metrics_snapshot["metrics_endpoints_summary"] = {
            "total_found": len(endpoints_found),
            "sui_endpoints": len(sui_endpoints),
            "node_exporter_endpoints": len(node_exporter_endpoints),
            "endpoints": endpoints_found
        }

    async def _analyze_system_metrics(self, result: SuiDataResult, metrics_text: str):
        """Analyze system metrics from Node Exporter for infrastructure intelligence"""
        system_info = {}
        
        # Extract system information
        patterns = {
            "go_version": r'go_info\{version="([^"]+)"\}',
            "cpu_cores": r'node_cpu_seconds_total\{cpu="(\d+)"',
            "memory_total": r'node_memory_MemTotal_bytes\s+(\d+)',
            "filesystem_size": r'node_filesystem_size_bytes.*\s+(\d+)',
            "network_interfaces": r'node_network_info\{device="([^"]+)"',
            "uptime": r'node_time_seconds\s+([0-9.]+)',
        }
        
        for info_type, pattern in patterns.items():
            matches = re.findall(pattern, metrics_text)
            if matches:
                if info_type == "cpu_cores":
                    # Count unique CPU cores
                    unique_cores = set(matches)
                    system_info["cpu_cores"] = len(unique_cores)
                elif info_type == "memory_total":
                    # Convert to GB
                    try:
                        memory_gb = int(matches[0]) / (1024**3)
                        system_info["memory_gb"] = round(memory_gb, 1)
                    except (ValueError, IndexError):
                        pass
                elif info_type == "network_interfaces":
                    system_info["network_interfaces"] = list(set(matches))
                else:
                    system_info[info_type] = matches[0] if matches else None
        
        if system_info:
            result.metrics_snapshot["system_info"] = system_info
            self.logger.info(f"System info: {system_info.get('cpu_cores', '?')} cores, {system_info.get('memory_gb', '?')}GB RAM")
        
        # Infrastructure quality assessment
        if system_info.get("cpu_cores", 0) >= 8 and system_info.get("memory_gb", 0) >= 16:
            result.metrics_snapshot["infrastructure_class"] = "high_performance"
        elif system_info.get("cpu_cores", 0) >= 4 and system_info.get("memory_gb", 0) >= 8:
            result.metrics_snapshot["infrastructure_class"] = "production_grade"
        else:
            result.metrics_snapshot["infrastructure_class"] = "basic"
        
        # This level of monitoring suggests serious infrastructure
        result.metrics_snapshot["infrastructure_monitoring_quality"] = "professional"

    async def _calculate_transaction_throughput(self, result: SuiDataResult, metrics_text: str):
        """Calculate transaction throughput TPS from total_transactions delta over scan interval"""
        # This is a single-node calculation - for full delta calculation across scan batch,
        # we would need access to previous scan data or scan start time
        # For now, store the raw total and let higher-level logic handle delta calculations
        
        if result.total_transactions:
            # Store scan timestamp for later delta calculations
            result.metrics_snapshot["transaction_count_timestamp"] = time.time()
            result.metrics_snapshot["transaction_count"] = result.total_transactions
            
            # If we have mempool data, we can estimate current throughput
            if result.mempool_transactions_total and result.mempool_pending_transactions:
                # Simple heuristic: assume pending transactions represent ~1-2 seconds of throughput
                pending = result.mempool_pending_transactions
                if pending > 0:
                    estimated_tps = pending / 2.0  # Assume 2-second processing window
                    result.transaction_throughput_tps = estimated_tps
                    self.logger.info(f"Estimated TPS from mempool: {estimated_tps:.1f}")

    async def _calculate_checkpoint_lag(self, result: SuiDataResult):
        """Calculate checkpoint lag - difference from max seen checkpoint in scan batch"""
        # This requires coordination across the scan batch to find the maximum checkpoint
        # For now, just store the checkpoint info for later batch processing
        
        if result.checkpoint_height:
            result.metrics_snapshot["checkpoint_for_lag_calculation"] = {
                "height": result.checkpoint_height,
                "timestamp": time.time(),
                "node_ip": result.ip
            }
            
            # Note: The actual lag calculation should be done at the batch level
            # where we can compare against the maximum checkpoint seen across all nodes
            # This would be: result.checkpoint_lag = max_checkpoint_in_batch - result.checkpoint_height

    async def _extract_uptime_with_priority(self, result: SuiDataResult, metrics_text: str, response_status: int, error_type: Optional[str] = None) -> Tuple[Optional[int], str]:
        """
        Extract uptime using priority-based approach from parsing.md
        Returns: (uptime_seconds, evidence_string)
        
        Priority order:
        1. uptime_seconds_total (validator app)
        2. process_start_time_seconds (derive uptime = now - start)
        3. process_uptime_seconds (if present)
        4. node_boot_time_seconds (if node-exporter is public; treat as host uptime)
        """
        current_time = time.time()
        
        # Initialize metrics surface if not already set
        if result.metrics_surface is None:
            result.metrics_surface = {}
        
        # Record HTTP status for metrics surface analysis
        if response_status == 200:
            result.metrics_surface["http_status"] = 200
        elif response_status in [401, 403]:
            result.metrics_surface["http_status"] = response_status
        elif response_status == 404 or error_type in ["timeout", "connection_refused"]:
            result.metrics_surface["http_status"] = "closed"
        else:
            result.metrics_surface["http_status"] = response_status
        
        if response_status != 200:
            # Handle non-200 responses
            if response_status in [401, 403]:
                result.uptime_status = "gated"
                result.uptime_seconds_total = None
                return None, f"metrics gated {response_status}"
            elif response_status == 404 or error_type in ["timeout", "connection_refused"]:
                result.uptime_status = "closed"
                result.uptime_seconds_total = None
                if error_type == "timeout":
                    return None, "metrics closed (timeout)"
                elif error_type == "connection_refused":
                    return None, "metrics closed (connection refused)"
                else:
                    return None, "metrics closed (404)"
            else:
                result.uptime_status = "closed"
                result.uptime_seconds_total = None
                return None, f"metrics closed ({response_status})"
        
        # Handle special error types for HTTP 200 responses
        if error_type in ["html_error", "empty_response"]:
            result.uptime_status = "unavailable_public_metrics"
            result.uptime_seconds_total = None
            if error_type == "html_error":
                return None, "metrics 200 but HTML error page"
            else:
                return None, "metrics 200 but empty response"
        
        # Priority 1: uptime_seconds_total (validator app)
        uptime_pattern = r'^uptime_seconds_total\s+(\d+)\s*$'
        match = re.search(uptime_pattern, metrics_text, re.MULTILINE)
        if match:
            uptime_seconds = int(match.group(1))
            result.uptime_status = "available"
            result.uptime_seconds_total = uptime_seconds
            return uptime_seconds, f"uptime={uptime_seconds}s"
        
        # Priority 2: process_start_time_seconds (derive uptime = now - start)
        process_start_pattern = r'process_start_time_seconds\s+([0-9.]+)'
        match = re.search(process_start_pattern, metrics_text)
        if match:
            try:
                start_time = float(match.group(1))
                uptime_seconds = int(current_time - start_time)
                if uptime_seconds > 0:
                    result.uptime_status = "available"
                    result.uptime_seconds_total = uptime_seconds
                    start_datetime = datetime.fromtimestamp(start_time).strftime("%Y-%m-%dT%H:%M:%SZ")
                    return uptime_seconds, f"start={start_datetime} (process_start_time_seconds)"
            except (ValueError, OSError):
                pass
        
        # Priority 3: process_uptime_seconds (if present)
        process_uptime_pattern = r'process_uptime_seconds\s+([0-9.]+)'
        match = re.search(process_uptime_pattern, metrics_text)
        if match:
            try:
                uptime_seconds = int(float(match.group(1)))
                result.uptime_status = "available"
                result.uptime_seconds_total = uptime_seconds
                return uptime_seconds, f"uptime={uptime_seconds}s (process_uptime_seconds)"
            except ValueError:
                pass
        
        # Priority 4: node_boot_time_seconds (if node-exporter is public; treat as host uptime)
        node_boot_pattern = r'node_boot_time_seconds\s+([0-9.]+)'
        match = re.search(node_boot_pattern, metrics_text)
        if match:
            try:
                boot_time = float(match.group(1))
                uptime_seconds = int(current_time - boot_time)
                if uptime_seconds > 0:
                    result.uptime_status = "available"
                    result.uptime_seconds_total = uptime_seconds
                    result.metrics_snapshot["uptime_source"] = "node_exporter"
                    boot_datetime = datetime.fromtimestamp(boot_time).strftime("%Y-%m-%dT%H:%M:%SZ")
                    return uptime_seconds, f"start={boot_datetime} (node_boot_time_seconds)"
            except (ValueError, OSError):
                pass
        
        # No uptime metrics found
        result.uptime_status = "unavailable_public_metrics"
        result.uptime_seconds_total = None
        return None, "metrics 200 but no uptime metric"

    async def _detect_edge_node(self, result: SuiDataResult, ip: str, headers: Optional[Dict] = None) -> bool:
        """
        Detect if node is a provider edge based on parsing.md criteria:
        - Known public RPC hostname
        - CDN/LB headers  
        - No gRPC
        - No validator metrics
        """
        edge_indicators = []
        
        # Check for public RPC hostname patterns (common cloud/CDN providers)
        public_rpc_patterns = [
            r'.*\.amazonaws\.com$',
            r'.*\.cloudflare\.com$', 
            r'.*\.google\.com$',
            r'.*\.azure\.com$',
            r'.*rpc.*\.com$',
            r'.*api.*\.com$',
            r'.*gateway.*\.com$',
            r'.*sui.*rpc.*',
        ]
        
        # Note: In real implementation, we'd need the actual hostname, not just IP
        # For now, we'll rely on other indicators
        
        # Check for CDN/Load Balancer headers
        if headers:
            cdn_headers = [
                'cf-ray',           # Cloudflare
                'x-amz-cf-id',      # AWS CloudFront
                'x-served-by',      # Fastly
                'x-cache',          # Various CDNs
                'x-cdn-provider',   # Generic CDN
                'x-forwarded-for',  # Load balancer
                'x-real-ip',        # Proxy/LB
            ]
            
            for header in cdn_headers:
                if header.lower() in [h.lower() for h in headers.keys()]:
                    edge_indicators.append(f"cdn_header:{header}")
        
        # Check if gRPC is available
        if not result.grpc_available:
            edge_indicators.append("no_grpc")
        
        # Check if validator metrics are absent
        validator_metrics_present = (
            result.consensus_round is not None or
            result.narwhal_round is not None or
            result.is_active_validator is True or
            result.validator_address is not None
        )
        
        if not validator_metrics_present:
            edge_indicators.append("no_validator_metrics")
        
        # Check if only RPC is exposed (typical for public endpoints)
        services_count = sum([
            result.rpc_exposed,
            result.grpc_available, 
            result.websocket_available,
            result.graphql_available
        ])
        
        if result.rpc_exposed and services_count == 1:
            edge_indicators.append("rpc_only")
        
        # Edge classification criteria
        is_edge = len(edge_indicators) >= 2 or "cdn_header:cf-ray" in edge_indicators
        
        if is_edge:
            result.edge = True
            result.uptime_expected = False
            result.metrics_snapshot["edge_indicators"] = edge_indicators
            self.logger.info(f"Edge node detected: {edge_indicators}")
        
        return is_edge

    async def _classify_public_node(self, result: SuiDataResult, ip: str, headers: Optional[Dict] = None) -> bool:
        """
        Classify node as public RPC provider based on hostname patterns and characteristics.
        
        Public node criteria:
        - Known provider hostname patterns (*.sui.io, *.onfinality.io, *.chainstack.com, etc.)
        - CDN/LB headers present AND grpc_available=false
        - If classified as public: is_public_node=true, uptime_expected=false
        """
        public_indicators = []
        provider_name = None
        
        # Get hostname from result if available (would need to be set by caller)
        hostname = getattr(result, 'hostname', None) or ip
        
        # Known public RPC provider patterns
        provider_patterns = {
            "sui_foundation": [r'.*\.sui\.io$', r'.*sui-.*\.com$'],
            "onfinality": [r'.*\.onfinality\.io$', r'.*onfinality.*\.com$'],
            "chainstack": [r'.*\.chainstack\.com$', r'.*chainstack.*\.io$'],
            "alchemy": [r'.*\.alchemy\.com$', r'.*alchemy.*\.io$'],
            "blockdaemon": [r'.*\.blockdaemon\.com$', r'.*blockdaemon.*\.io$'],
            "quicknode": [r'.*\.quicknode\.pro$', r'.*quicknode.*\.com$'],
            "ankr": [r'.*\.ankr\.com$', r'.*ankr.*\.io$'],
            "nodereal": [r'.*\.nodereal\.io$', r'.*nodereal.*\.com$'],
            "public_rpc": [r'.*rpc.*\.com$', r'.*api.*\.com$', r'.*gateway.*\.com$', r'.*node.*\.com$']
        }
        
        # Check hostname against known patterns
        for provider, patterns in provider_patterns.items():
            for pattern in patterns:
                if re.search(pattern, hostname, re.IGNORECASE):
                    public_indicators.append(f"hostname_pattern:{provider}")
                    if not provider_name:  # Use first match as primary provider
                        provider_name = provider
        
        # Check for CDN/Load Balancer headers (indicates infrastructure provider)
        cdn_detected = False
        if headers:
            cdn_headers = {
                'cf-ray': 'cloudflare',
                'x-amz-cf-id': 'aws_cloudfront', 
                'x-served-by': 'fastly',
                'x-cache': 'cdn',
                'x-cdn-provider': 'cdn',
                'x-forwarded-for': 'load_balancer',
                'x-real-ip': 'proxy'
            }
            
            for header, source in cdn_headers.items():
                if header.lower() in [h.lower() for h in headers.keys()]:
                    public_indicators.append(f"cdn_header:{source}")
                    cdn_detected = True
                    if not provider_name:
                        provider_name = source
        
        # Check if gRPC is unavailable (typical for public RPC endpoints)
        if not result.grpc_available:
            public_indicators.append("no_grpc_service")
        
        # Check if node lacks validator-specific metrics
        validator_metrics_present = (
            result.consensus_round is not None or
            result.narwhal_round is not None or
            result.is_active_validator is True or
            result.validator_address is not None
        )
        
        if not validator_metrics_present:
            public_indicators.append("no_validator_metrics")
        
        # Classification logic: known provider OR (CDN + no gRPC)
        is_public = (
            len([i for i in public_indicators if "hostname_pattern" in i]) > 0 or
            (cdn_detected and not result.grpc_available)
        )
        
        if is_public:
            result.is_public_node = True
            result.uptime_expected = False  # Public nodes don't need uptime SLA
            result.public_node_provider = provider_name
            result.metrics_snapshot["public_node_indicators"] = public_indicators
            
            self.logger.info(f"Public RPC node detected: {provider_name or 'unknown'} - {public_indicators}")
            
            # Set metrics surface for public nodes
            if result.metrics_surface is None:
                result.metrics_surface = {}
            result.metrics_surface["node_classification"] = "public_rpc_provider"
            result.metrics_surface["provider"] = provider_name
        else:
            result.is_public_node = False
            result.uptime_expected = True  # Validators/private nodes should have uptime
            
            # Set metrics surface for non-public nodes
            if result.metrics_surface is None:
                result.metrics_surface = {}
            result.metrics_surface["node_classification"] = "validator_or_private"
        
        return is_public