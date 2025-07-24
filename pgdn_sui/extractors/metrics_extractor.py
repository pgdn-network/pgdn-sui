#!/usr/bin/env python3
"""
Metrics Extractor
Handles Prometheus metrics extraction from Sui nodes
"""

import re
import time
import logging
import requests
from typing import Dict, List
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
        """Enhanced metrics intelligence extraction with better validator detection"""
        
        # Comprehensive metrics endpoint mapping
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
                    
                    self.logger.info(f"Retrieved {metrics_size} bytes from {endpoint_type} on port {port}")
                    
                    # Analyze metrics content
                    metrics_analysis = await self._analyze_metrics_content(result, metrics_text, endpoint_type, port)
                    
                    if metrics_analysis["is_sui_metrics"]:
                        self.logger.info(f"Found Sui validator metrics on port {port}")
                        result.metrics_snapshot["sui_metrics_port"] = port
                        result.metrics_snapshot["sui_metrics_endpoint"] = endpoint
                        await self._extract_sui_blockchain_intelligence_from_metrics(result, metrics_text)
                        intelligence_extracted = True
                        
                    elif metrics_analysis["is_node_exporter"]:
                        self.logger.info(f"Found Node Exporter on port {port} - indicates monitored infrastructure")
                        result.metrics_snapshot["node_exporter_port"] = port
                        result.metrics_snapshot["infrastructure_monitoring"] = True
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
            
            # Consensus metrics
            "consensus_round": (r'consensus_round\s+(\d+)', int),
            "narwhal_round": (r'narwhal_primary_current_round\s+(\d+)', int),
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
                        
                    elif field_name == "consensus_round" and not result.consensus_round:
                        result.consensus_round = value
                        
                    elif field_name == "narwhal_round" and not result.narwhal_round:
                        result.narwhal_round = value
                        
                    elif field_name == "network_peers" and not result.network_peers:
                        result.network_peers = value
                    
                    # Store in metrics snapshot
                    result.metrics_snapshot[f"extracted_{field_name}"] = value
                    metrics_extracted += 1
                    
                except (ValueError, TypeError) as e:
                    self.logger.debug(f"Failed to parse {field_name}: {e}")
        
        # Extract Sui version from build_info if available
        version_pattern = r'build_info\{[^}]*version="([^"]+)"'
        version_match = re.search(version_pattern, metrics_text)
        if version_match and not result.sui_version:
            result.sui_version = version_match.group(1)
            result.metrics_snapshot["sui_version_from_metrics"] = result.sui_version
            metrics_extracted += 1
            self.logger.info(f"Sui version from metrics: {result.sui_version}")
        
        # Look for validator-specific evidence
        validator_evidence = await self._detect_validator_evidence_from_metrics(result, metrics_text)
        if validator_evidence:
            metrics_extracted += len(validator_evidence)
        
        result.metrics_snapshot["blockchain_metrics_extracted"] = metrics_extracted
        self.logger.info(f"Extracted {metrics_extracted} blockchain metrics")

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