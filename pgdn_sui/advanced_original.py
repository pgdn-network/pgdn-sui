#!/usr/bin/env python3
"""
Targeted Sui Node Interrogator
Deep blockchain intelligence extraction using native Sui protocols and local SDK
"""

import json
import logging
import time
import asyncio
import subprocess
import os
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import requests
from urllib.parse import urlparse

# gRPC imports - will be available after protobuf compilation
try:
    import grpc
    from grpc_reflection.v1alpha import reflection_pb2_grpc, reflection_pb2
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False

# Sui SDK imports
try:
    from pysui import SuiConfig, SyncClient, AsyncClient
    from pysui.sui.sui_txn import SuiTransaction
    from pysui.sui.sui_types.address import SuiAddress
    SUI_SDK_AVAILABLE = True
except ImportError:
    SUI_SDK_AVAILABLE = False

logger = logging.getLogger(__name__)

@dataclass
class SuiDataResult:
    """Enhanced Sui protocol intelligence result"""
    ip: str
    port: int
    timestamp: datetime
    node_type: str
    network: str
    
    # Core Sui blockchain data
    sui_version: Optional[str] = None
    protocol_version: Optional[str] = None
    current_epoch: Optional[int] = None
    checkpoint_height: Optional[int] = None
    chain_identifier: Optional[str] = None
    genesis_checkpoint: Optional[str] = None
    
    # Enhanced validator intelligence
    validator_count: Optional[int] = None
    total_stake: Optional[float] = None
    voting_power_gini: Optional[float] = None
    is_active_validator: Optional[bool] = None
    validator_address: Optional[str] = None
    validator_name: Optional[str] = None
    validator_stake: Optional[int] = None
    validator_commission_rate: Optional[int] = None
    validator_next_epoch_stake: Optional[int] = None
    validator_rewards: Optional[int] = None
    
    # Enhanced consensus intelligence  
    consensus_round: Optional[int] = None
    consensus_engine: Optional[str] = None
    narwhal_round: Optional[int] = None
    narwhal_certificate_created: Optional[int] = None
    consensus_latency_ms: Optional[float] = None
    consensus_commit_latency: Optional[float] = None
    mempool_transactions: Optional[int] = None
    
    # Enhanced network intelligence
    network_peers: Optional[int] = None 
    peer_info: List[Dict] = None
    sync_status: Optional[str] = None
    checkpoint_lag: Optional[int] = None
    transaction_throughput: Optional[float] = None
    reference_gas_price: Optional[int] = None
    total_transactions: Optional[int] = None
    
    # Service availability intelligence
    rpc_exposed: bool = False
    rpc_authenticated: bool = False
    rpc_methods_available: List[str] = None
    websocket_available: bool = False
    graphql_available: bool = False
    grpc_available: bool = False
    grpc_services: List[str] = None
    metrics_exposed: bool = False
    
    # Security & configuration
    tls_enabled: bool = False
    cors_enabled: bool = False
    rate_limiting: bool = False
    certificate_info: Optional[Dict] = None
    
    # Performance metrics
    response_times: Dict[str, float] = None
    service_response_times: Dict[str, float] = None
    
    # Port analysis
    expected_ports: Dict[str, bool] = None
    unexpected_ports: List[int] = None
    
    # Raw intelligence data
    system_state: Dict[str, Any] = None
    committee_info: Dict[str, Any] = None
    metrics_snapshot: Dict[str, Any] = None
    grpc_reflection_data: Dict[str, Any] = None
    
    # Intelligence quality metrics
    data_completeness: float = 0.0
    intelligence_sources: List[str] = None
    extraction_errors: List[str] = None

    def __post_init__(self):
        if self.peer_info is None:
            self.peer_info = []
        if self.rpc_methods_available is None:
            self.rpc_methods_available = []
        if self.grpc_services is None:
            self.grpc_services = []
        if self.response_times is None:
            self.response_times = {}
        if self.service_response_times is None:
            self.service_response_times = {}
        if self.expected_ports is None:
            self.expected_ports = {}
        if self.unexpected_ports is None:
            self.unexpected_ports = []
        if self.system_state is None:
            self.system_state = {}
        if self.committee_info is None:
            self.committee_info = {}
        if self.metrics_snapshot is None:
            self.metrics_snapshot = {}
        if self.grpc_reflection_data is None:
            self.grpc_reflection_data = {}
        if self.intelligence_sources is None:
            self.intelligence_sources = []
        if self.extraction_errors is None:
            self.extraction_errors = []


class SuiProtobufManager:
    """
    Manages Sui protobuf compilation and gRPC stub generation
    """
    
    def __init__(self, sui_repo_path: Optional[str] = None):
        self.sui_repo_path = sui_repo_path or self._find_sui_repo()
        self.proto_dir = None
        self.generated_dir = Path("./generated_protos")
        self.stubs_ready = False
        
    def _find_sui_repo(self) -> Optional[str]:
        """Find Sui repository in common locations"""
        common_paths = [
            "./sui",
            "../sui", 
            "~/sui",
            "~/code/sui",
            "/opt/sui",
            os.environ.get("SUI_REPO_PATH", "")
        ]
        
        for path in common_paths:
            expanded_path = Path(path).expanduser()
            if expanded_path.exists() and (expanded_path / "crates").exists():
                logger.info(f"Found Sui repository at: {expanded_path}")
                return str(expanded_path)
        
        return None
    
    def setup_protobufs(self) -> bool:
        """Setup and compile Sui protobufs for gRPC"""
        if not self.sui_repo_path:
            logger.warning("Sui repository not found. gRPC functionality will be limited.")
            return False
        
        try:
            # Find proto files in Sui repository
            sui_path = Path(self.sui_repo_path)
            proto_files = list(sui_path.rglob("*.proto"))
            
            if not proto_files:
                logger.warning(f"No .proto files found in {sui_path}")
                return False
            
            logger.info(f"Found {len(proto_files)} proto files")
            
            # Create output directory
            self.generated_dir.mkdir(exist_ok=True)
            (self.generated_dir / "__init__.py").touch()
            
            # Compile proto files
            for proto_file in proto_files:
                self._compile_proto(proto_file)
            
            # Add generated directory to Python path
            if str(self.generated_dir) not in sys.path:
                sys.path.insert(0, str(self.generated_dir))
            
            self.stubs_ready = True
            logger.info("✅ Sui protobufs compiled successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup protobufs: {e}")
            return False
    
    def _compile_proto(self, proto_file: Path):
        """Compile a single proto file"""
        try:
            # Determine the relative path and package structure
            proto_dir = proto_file.parent
            relative_path = proto_file.relative_to(self.sui_repo_path)
            
            # Use grpcio-tools to compile
            from grpc_tools import protoc
            
            protoc_args = [
                "grpc_tools.protoc",
                f"--python_out={self.generated_dir}",
                f"--grpc_python_out={self.generated_dir}",
                f"--proto_path={self.sui_repo_path}",
                str(proto_file)
            ]
            
            result = protoc.main(protoc_args)
            if result == 0:
                logger.debug(f"Compiled: {relative_path}")
            else:
                logger.warning(f"Failed to compile: {relative_path}")
                
        except Exception as e:
            logger.debug(f"Error compiling {proto_file}: {e}")
    
    def get_sui_grpc_stubs(self):
        """Get compiled Sui gRPC stubs"""
        if not self.stubs_ready:
            return None
        
        try:
            # Try to import generated stubs
            # This would depend on the actual Sui proto structure
            stubs = {}
            
            # Look for common Sui gRPC services
            for proto_file in self.generated_dir.glob("*_pb2_grpc.py"):
                module_name = proto_file.stem.replace("_pb2_grpc", "")
                try:
                    # Dynamic import of generated stubs
                    import importlib.util
                    spec = importlib.util.spec_from_file_location(
                        f"{module_name}_grpc", proto_file
                    )
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    stubs[module_name] = module
                except Exception as e:
                    logger.debug(f"Failed to import {module_name}: {e}")
            
            return stubs if stubs else None
            
        except Exception as e:
            logger.error(f"Failed to get gRPC stubs: {e}")
            return None


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
    
    # Comprehensive Sui RPC methods organized by intelligence category
    CORE_BLOCKCHAIN_METHODS = {
        "sui_getChainIdentifier": {"extracts": ["chain_id"], "critical": True},
        "suix_getLatestSuiSystemState": {"extracts": ["epoch", "validators", "stake"], "critical": True},
        "sui_getLatestCheckpointSequenceNumber": {"extracts": ["checkpoint"], "critical": True},
        "sui_getCheckpoint": {"extracts": ["checkpoint_detail"], "critical": False},
        "sui_getTotalTransactionBlocks": {"extracts": ["transaction_count"], "critical": False},
        "suix_getReferenceGasPrice": {"extracts": ["gas_price"], "critical": False},
        "sui_getProtocolConfig": {"extracts": ["protocol_config"], "critical": False},
    }
    
    VALIDATOR_METHODS = {
        "suix_getValidatorsApy": {"extracts": ["validator_apy"], "critical": False},
        "sui_getValidators": {"extracts": ["validator_set"], "critical": True},
        "suix_getCommitteeInfo": {"extracts": ["committee"], "critical": True},
        "suix_getStakes": {"extracts": ["stakes"], "critical": False},
    }
    
    NETWORK_METHODS = {
        "sui_getNetworkMetrics": {"extracts": ["network_metrics"], "critical": False},
        "suix_subscribeEvent": {"extracts": ["event_capability"], "critical": False},
        "suix_subscribeTransaction": {"extracts": ["tx_stream_capability"], "critical": False},
    }
    
    # GraphQL queries for deep intelligence
    GRAPHQL_QUERIES = {
        "network_info": """
        query NetworkInfo {
            chainIdentifier
            epoch {
                epochId
                referenceGasPrice
                startTimestamp
                endTimestamp
                validatorSet {
                    totalCount
                }
            }
            protocolConfig {
                protocolVersion
            }
        }
        """,
        
        "validator_info": """
        query ValidatorInfo($first: Int) {
            epoch {
                validatorSet(first: $first) {
                    totalCount
                    nodes {
                        name
                        address {
                            address
                        }
                        nextEpochStake
                        votingPower
                        commissionRate
                        apy
                    }
                }
            }
        }
        """,
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
                self.logger.info("✅ Sui SDK client initialized")
            except Exception as e:
                self.logger.warning(f"⚠️ Failed to initialize Sui SDK: {e}")

    @classmethod
    def from_discovery_json(cls, json_file_path: str, config: Dict = None):
        """Initialize from discovery JSON file"""
        extractor = cls(config)
        extractor.discovery_data = extractor._load_discovery_data(json_file_path)
        extractor.logger.info(f"Loaded {len(extractor.discovery_data)} Sui nodes for intelligence extraction")
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
        extractor.logger.info(f"Discovered {len(nodes_data)} Sui nodes from {len(hostnames)} hostnames for intelligence extraction (with deep analysis)")
        
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
        self.logger.info(f"Found {len(sui_nodes)} confirmed Sui nodes for intelligence extraction")
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
        
        self.logger.info(f"🧠 Starting intelligent Sui blockchain analysis of {len(self.discovery_data)} nodes")
        
        # Process nodes with controlled concurrency
        semaphore = asyncio.Semaphore(4)  # Limit concurrent connections
        
        async def extract_with_semaphore(node_data: Dict) -> Optional[SuiDataResult]:
            async with semaphore:
                try:
                    result = await self._extract_node_intelligence_async(node_data)
                    return result
                except Exception as e:
                    self.logger.error(f"Intelligence extraction failed for {node_data.get('ip', 'unknown')}: {e}")
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
                self.logger.error(f"Task failed with exception: {result}")
            # Handle None results (failed extractions)
            elif result is None:
                self.logger.debug("Task returned None (failed extraction)")
        
        extraction_time = time.time() - start_time
        self.logger.info(f"🎉 Intelligence extraction complete: {len(results)} nodes analyzed in {extraction_time:.2f}s")
        
        return results

    async def _extract_node_intelligence_async(self, node_data: Dict) -> SuiDataResult:
        """Extract comprehensive intelligence from a single Sui node"""
        ip = node_data["ip"]
        node_type = node_data.get("node_type", node_data.get("type", "unknown"))
        capabilities = node_data.get("capabilities", [])
        discovered_ports = node_data.get("accessible_ports", [])
        
        self.logger.info(f"🧠 Extracting intelligence from Sui {node_type} at {ip}")
        
        result = SuiDataResult(
            ip=ip,
            port=self._get_primary_port(discovered_ports, node_type),
            timestamp=datetime.utcnow(),
            node_type=node_type,
            network="unknown"
        )
        
        # Intelligence extraction pipeline
        intelligence_tasks = [
            ("rpc", self._extract_rpc_intelligence_async(result, ip, discovered_ports)),
            ("websocket", self._extract_websocket_intelligence_async(result, ip)),
            ("graphql", self._extract_graphql_intelligence_async(result, ip)),
            ("grpc", self._extract_grpc_intelligence_async(result, ip, discovered_ports)),
            ("metrics", self._extract_metrics_intelligence_async(result, ip)),
            ("local_sui", self._extract_local_sui_intelligence_async(result, ip)),
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
                    self.logger.info(f"✅ {source_name} intelligence extracted from {ip}")
                else:
                    self.logger.debug(f"❌ {source_name} intelligence extraction failed for {ip}")
            except Exception as e:
                self.logger.error(f"💥 {source_name} extraction error for {ip}: {e}")
                result.extraction_errors.append(f"{source_name}_extraction_error: {str(e)}")
        
        # Calculate intelligence completeness
        result.data_completeness = successful_extractions / total_extractions if total_extractions > 0 else 0.0
        
        # Post-processing analysis with error handling
        try:
            await self._post_process_intelligence(result)
        except Exception as e:
            self.logger.error(f"💥 Post-processing error for {ip}: {e}")
            result.extraction_errors.append(f"post_processing_error: {str(e)}")
        
        self.logger.info(f"🎯 Intelligence extraction for {ip} complete: {result.data_completeness:.2f} completeness")
        return result

    def _get_primary_port(self, ports: List[int], node_type: str) -> int:
        """Get primary port based on node type and Sui port intelligence"""
        priority_ports = {
            "validator": [9100, 8080, 9000],
            "public_rpc": [9000, 9184, 3030],
            "hybrid": [9000, 9100, 8080],
            "indexer": [8084, 9000, 3030]
        }
        
        node_priorities = priority_ports.get(node_type, [9000, 9100, 8080])
        
        for port in node_priorities:
            if port in ports:
                return port
        
        return ports[0] if ports else 9000

    async def _extract_rpc_intelligence_async(self, result: SuiDataResult, ip: str, ports: List[int]) -> bool:
        """Extract comprehensive intelligence via JSON-RPC with enhanced debugging"""
        rpc_endpoints = []
        
        # Build RPC endpoints based on discovered ports and common patterns
        if 9000 in ports:
            rpc_endpoints.extend([f"https://{ip}:9000", f"http://{ip}:9000"])
        if 443 in ports:
            rpc_endpoints.extend([f"https://{ip}", f"https://{ip}/rpc"])
        if 80 in ports:
            rpc_endpoints.extend([f"http://{ip}", f"http://{ip}/rpc"])
        
        # Also try common patterns regardless of discovered ports
        if not rpc_endpoints:
            rpc_endpoints = [
                f"https://{ip}:9000",
                f"http://{ip}:9000",
                f"https://{ip}",
                f"http://{ip}",
            ]
        
        for endpoint in rpc_endpoints:
            try:
                self.logger.info(f"🌐 Testing RPC endpoint: {endpoint}")
                
                # Test basic connectivity and authentication with detailed logging
                start_time = time.time()
                test_response = requests.post(endpoint, json={
                    "jsonrpc": "2.0",
                    "method": "sui_getChainIdentifier",
                    "params": [],
                    "id": 1
                }, timeout=self.timeout, verify=False)
                
                response_time = time.time() - start_time
                result.service_response_times[f"rpc_{endpoint}"] = response_time
                
                self.logger.info(f"📡 RPC {endpoint} responded with status {test_response.status_code} in {response_time:.3f}s")
                
                if test_response.status_code in [401, 403]:
                    result.rpc_authenticated = True
                    self.logger.info(f"🔒 RPC endpoint {endpoint} requires authentication")
                    continue
                elif test_response.status_code == 200:
                    result.rpc_exposed = True
                    result.rpc_authenticated = False
                    
                    # Parse response to see if we got actual data
                    try:
                        response_data = test_response.json()
                        self.logger.info(f"📋 RPC response: {str(response_data)[:200]}...")
                        
                        if "result" in response_data and response_data["result"]:
                            self.logger.info(f"✅ RPC endpoint {endpoint} returned valid data: {response_data['result']}")
                            
                            # Process the chain identifier immediately
                            result.chain_identifier = str(response_data["result"])
                            chain_mappings = {
                                "35834a8a": "mainnet",
                                "4c78adac": "testnet", 
                                "devnet": "devnet",
                                "localnet": "localnet"
                            }
                            result.network = chain_mappings.get(result.chain_identifier, f"unknown_{result.chain_identifier}")
                            
                            # Extract comprehensive intelligence from this working endpoint
                            await self._extract_comprehensive_rpc_intelligence(result, endpoint)
                            return True
                        elif "error" in response_data:
                            error_info = response_data["error"]
                            self.logger.warning(f"⚠️ RPC endpoint {endpoint} returned error: {error_info}")
                            
                            # Still mark as exposed since we got a valid JSON-RPC response
                            result.rpc_exposed = True
                            
                            # Some errors still indicate the node is working
                            if error_info.get("code") in [-32602, -32601]:  # Invalid params/method not found
                                self.logger.info(f"💡 RPC endpoint {endpoint} is working but method not supported")
                                # Try other methods on this endpoint
                                await self._extract_comprehensive_rpc_intelligence(result, endpoint)
                                return True
                        else:
                            self.logger.debug(f"⚠️ RPC endpoint {endpoint} returned unexpected format: {response_data}")
                            
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"⚠️ RPC endpoint {endpoint} returned non-JSON response: {test_response.text[:100]}")
                        self.logger.debug(f"JSON decode error: {e}")
                elif test_response.status_code == 404:
                    self.logger.debug(f"❌ RPC endpoint {endpoint} returned 404 - endpoint not found")
                elif test_response.status_code >= 500:
                    self.logger.warning(f"⚠️ RPC endpoint {endpoint} returned server error {test_response.status_code}")
                else:
                    self.logger.debug(f"❌ RPC endpoint {endpoint} returned status {test_response.status_code}")
                    continue
                    
            except requests.exceptions.ConnectionError as e:
                self.logger.debug(f"❌ RPC endpoint {endpoint} connection failed: {e}")
                continue
            except requests.exceptions.Timeout as e:
                self.logger.debug(f"⏰ RPC endpoint {endpoint} timed out: {e}")
                continue
            except requests.exceptions.SSLError as e:
                self.logger.debug(f"🔒 RPC endpoint {endpoint} SSL error: {e}")
                continue
            except Exception as e:
                self.logger.debug(f"💥 RPC endpoint {endpoint} error: {e}")
                continue
        
        self.logger.warning(f"❌ No working RPC endpoints found for {ip}")
        return False

    async def _extract_comprehensive_rpc_intelligence(self, result: SuiDataResult, endpoint: str):
        """Extract comprehensive intelligence from a working RPC endpoint with corrected method names"""
        
        # CORRECTED: Use proper suix_ prefixes for most methods
        critical_methods = [
            # Core blockchain state (highest priority)
            ("sui_getChainIdentifier", []),
            ("suix_getLatestSuiSystemState", []),  # CORRECTED from sui_
            ("sui_getLatestCheckpointSequenceNumber", []),
            ("suix_getReferenceGasPrice", []),  # CORRECTED from sui_
            ("sui_getTotalTransactionBlocks", []),
            ("sui_getProtocolConfig", []),
            ("suix_getCommitteeInfo", []),  # CORRECTED from sui_
            ("suix_getValidatorsApy", []),
        ]
        
        # Methods that require parameters - handle more carefully
        parameterized_methods = [
            ("suix_getAllBalances", ["0x0000000000000000000000000000000000000000000000000000000000000000"]),
            ("sui_tryGetPastObject", ["0x0000000000000000000000000000000000000000000000000000000000000000", 1]),
            ("suix_getOwnedObjects", ["0x0000000000000000000000000000000000000000000000000000000000000000"]),
            ("suix_getCoinMetadata", ["0x2::sui::SUI"]),
        ]
        
        # Discovery methods
        discovery_methods = [
            ("rpc.discover", []),
        ]
        
        # Methods that may not exist or need special handling
        optional_methods = [
            ("sui_getValidators", []),  # Doesn't exist on mainnet
            ("suix_getStakes", ["0x0000000000000000000000000000000000000000000000000000000000000000"]),  # Needs valid address
            ("sui_getMoveFunctionArgTypes", ["0x2", "coin", "transfer"]),  # May fail with specific params
            ("suix_queryObjects", [{"MatchAll": []}]),  # May not exist
            ("sui_getCheckpoint", []),  # Needs checkpoint ID parameter
        ]
        
        # Combine all methods in priority order
        all_methods = critical_methods + parameterized_methods + discovery_methods + optional_methods
        
        successful_methods = 0
        total_methods = len(all_methods)
        
        self.logger.info(f"🔍 Starting comprehensive RPC extraction with {total_methods} methods")
        
        for method, params in all_methods:
            try:
                self.logger.info(f"🔍 Calling RPC method: {method} with params: {params}")
                
                response = requests.post(endpoint, json={
                    "jsonrpc": "2.0",
                    "method": method,  
                    "params": params,
                    "id": hash(method) % 1000
                }, timeout=self.timeout, verify=False)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if "result" in data and data["result"] is not None:
                        await self._process_rpc_method_result(result, method, data["result"])
                        result.rpc_methods_available.append(method)
                        successful_methods += 1
                        self.logger.info(f"✅ RPC method {method} successful")
                        
                    elif "error" in data:
                        error = data["error"]
                        error_code = error.get("code", "unknown")
                        error_message = error.get("message", "")
                        
                        # Log but continue - some errors are expected
                        if error_code in [-32602, -32601]:
                            self.logger.warning(f"❌ RPC method {method} error [{error_code}]: {error_message}")
                        else:
                            self.logger.debug(f"❌ RPC method {method} error [{error_code}]: {error_message}")
                            
            except Exception as e:
                self.logger.debug(f"💥 RPC method {method} failed: {e}")
                continue
        
        # Store extraction statistics
        result.metrics_snapshot["rpc_extraction"] = {
            "successful_methods": successful_methods,
            "total_methods_attempted": total_methods,
            "success_rate": successful_methods / total_methods if total_methods > 0 else 0
        }
        
        self.logger.info(f"✅ RPC extraction complete: {successful_methods}/{total_methods} methods successful")

    async def _process_rpc_method_result(self, result: SuiDataResult, method: str, data: Any):
        """Enhanced RPC method result processing with better data extraction"""
        try:
            if method == "sui_getChainIdentifier":
                result.chain_identifier = str(data)
                chain_mappings = {
                    "35834a8a": "mainnet",
                    "4c78adac": "testnet", 
                    "devnet": "devnet",
                    "localnet": "localnet"
                }
                result.network = chain_mappings.get(data, f"unknown_{data}")
                self.logger.info(f"🌐 Network: {result.network} (chain: {result.chain_identifier})")
                
            elif method == "suix_getLatestSuiSystemState" and isinstance(data, dict):
                # Store system state but conditionally process activeValidators
                system_state_copy = data.copy()
                
                # Only process activeValidators if enhanced mode is enabled
                if self.config.get('enhanced', False):
                    # Enhanced validator set analysis
                    active_validators = data.get("activeValidators", [])
                    if active_validators:
                        result.validator_count = len(active_validators)
                        
                        # Calculate total stake and voting power distribution
                        total_stake = 0
                        stakes = []
                        
                        for validator in active_validators:
                            stake = validator.get("stakingPoolSuiBalance")
                            if stake:
                                try:
                                    stake_amount = int(stake)
                                    stakes.append(stake_amount)
                                    total_stake += stake_amount
                                except (ValueError, TypeError):
                                    pass
                        
                        if stakes:
                            result.total_stake = total_stake
                            result.voting_power_gini = self._calculate_gini_coefficient(stakes)
                        
                        # Check if this node is a validator
                        await self._identify_node_as_validator(result, active_validators)
                        
                        self.logger.info(f"🏛️ Processed activeValidators array ({len(active_validators)} validators)")
                else:
                    # Get validator count without processing the full array
                    active_validators = data.get("activeValidators", [])
                    if active_validators:
                        result.validator_count = len(active_validators)
                    
                    # Remove activeValidators completely when not in enhanced mode
                    system_state_copy.pop('activeValidators', None)
                    self.logger.info(f"🏛️ Skipped activeValidators processing ({result.validator_count} validators, use --enhanced to include)")
                
                result.system_state.update(system_state_copy)
                
                # Extract comprehensive epoch information
                result.current_epoch = data.get("epoch")
                result.protocol_version = str(data.get("protocolVersion", ""))
                result.reference_gas_price = data.get("referenceGasPrice")
                
                self.logger.info(f"🏛️ System state: epoch {result.current_epoch}, protocol {result.protocol_version}, {result.validator_count} validators")
                
            elif method == "sui_getLatestCheckpointSequenceNumber":
                try:
                    result.checkpoint_height = int(data)
                    self.logger.info(f"🏁 Checkpoint: {result.checkpoint_height}")
                except (ValueError, TypeError):
                    pass
                    
            elif method == "sui_getTotalTransactionBlocks":
                try:
                    result.total_transactions = int(data)
                    self.logger.info(f"📈 Total transactions: {result.total_transactions}")
                except (ValueError, TypeError):
                    pass
                    
            elif method == "suix_getReferenceGasPrice":
                try:
                    result.reference_gas_price = int(data)
                    self.logger.info(f"⛽ Gas price: {result.reference_gas_price}")
                except (ValueError, TypeError):
                    pass
                    
            elif method == "sui_getProtocolConfig" and isinstance(data, dict):
                if not result.protocol_version:
                    result.protocol_version = str(data.get("protocolVersion", ""))
                    
                # Store detailed protocol configuration
                result.metrics_snapshot["protocol_config"] = {
                    "version": data.get("protocolVersion"),
                    "max_tx_size": data.get("attributes", {}).get("max_tx_size_bytes", {}).get("u64"),
                    "max_gas": data.get("attributes", {}).get("max_gas_budget", {}).get("u64"),
                    "min_supported": data.get("minSupportedProtocolVersion"),
                    "max_supported": data.get("maxSupportedProtocolVersion"),
                }
                
                self.logger.info(f"⚙️ Protocol: {result.protocol_version}")
                
            elif method == "suix_getCommitteeInfo" and isinstance(data, dict):
                committee_members = data.get("validators", [])
                
                # Only process committee_info if enhanced mode is enabled  
                if self.config.get('enhanced', False):
                    result.committee_info.update(data)
                    if committee_members and not result.validator_count:
                        result.validator_count = len(committee_members)
                    self.logger.info(f"🏛️ Committee: {len(committee_members)} members processed")
                else:
                    # Only get the count without storing the full committee data
                    if committee_members and not result.validator_count:
                        result.validator_count = len(committee_members)
                    self.logger.info(f"🏛️ Committee: {len(committee_members)} members (data skipped, use --enhanced to include)")
                
            elif method == "suix_getValidatorsApy" and isinstance(data, dict):
                apys = data.get("apys", [])
                if apys:
                    result.metrics_snapshot["validator_apys"] = {
                        "epoch": data.get("epoch"),
                        "validator_count": len(apys),
                        "avg_apy": sum(v.get("apy", 0) for v in apys) / len(apys) if apys else 0,
                        "apy_range": {
                            "min": min(v.get("apy", 0) for v in apys) if apys else 0,
                            "max": max(v.get("apy", 0) for v in apys) if apys else 0,
                        }
                    }
                    self.logger.info(f"📊 Validator APYs: {len(apys)} validators, avg {result.metrics_snapshot['validator_apys']['avg_apy']:.2f}%")
                    
            elif method == "rpc.discover" and isinstance(data, dict):
                # Extract available methods from OpenRPC discovery
                if "methods" in data:
                    available_methods = [m.get("name") for m in data["methods"] if m.get("name")]
                    result.rpc_methods_available.extend(available_methods)
                    result.metrics_snapshot["rpc_discovery"] = {
                        "openrpc_version": data.get("openrpc"),
                        "api_version": data.get("info", {}).get("version"),
                        "total_methods": len(available_methods),
                        "api_title": data.get("info", {}).get("title"),
                    }
                    self.logger.info(f"🔍 RPC Discovery: {len(available_methods)} methods available")
                    
            elif method == "suix_getCoinMetadata" and isinstance(data, dict):
                result.metrics_snapshot["sui_coin_metadata"] = {
                    "name": data.get("name"),
                    "symbol": data.get("symbol"),
                    "decimals": data.get("decimals"),
                    "description": data.get("description"),
                }
                self.logger.info(f"🪙 SUI coin metadata: {data.get('symbol')} ({data.get('decimals')} decimals)")
                
            # Store raw data for debugging (but limit size)
            if method in ["suix_getLatestSuiSystemState", "suix_getCommitteeInfo"] and not self.config.get('enhanced', False):
                # Store a smaller sample for debugging when not in enhanced mode
                sample_data = str(data)[:500] + "... [truncated]" if len(str(data)) > 500 else str(data)
                result.metrics_snapshot[f"rpc_{method}_sample"] = sample_data
            else:
                result.metrics_snapshot[f"rpc_{method}_sample"] = str(data)[:200] if isinstance(data, (dict, list)) else str(data)
            
        except Exception as e:
            result.extraction_errors.append(f"rpc_processing_error_{method}: {str(e)}")
            self.logger.error(f"💥 Error processing RPC method {method}: {e}")

    async def _identify_node_as_validator(self, result: SuiDataResult, validators: List[Dict]):
        """Enhanced validator identification using multiple heuristics"""
        for validator in validators:
            validator_info = {
                "address": validator.get("suiAddress"),
                "name": validator.get("name", ""),
                "description": validator.get("description", ""),
                "network_address": validator.get("networkAddress", ""),
                "primary_address": validator.get("primaryAddress", ""),
            }
            
            # Multiple identification strategies
            identification_methods = [
                self._check_ip_in_validator_field,
                self._check_hostname_resolution,
                self._check_network_address_pattern,
            ]
            
            for method in identification_methods:
                if method(result.ip, validator_info):
                    result.is_active_validator = True
                    result.validator_address = validator_info["address"]
                    result.validator_name = validator_info["name"]
                    result.validator_stake = validator.get("stakingPoolSuiBalance")
                    result.validator_commission_rate = validator.get("commissionRate")
                    return
    
    def _check_ip_in_validator_field(self, ip: str, validator_info: Dict) -> bool:
        """Check if IP appears in any validator field"""
        for field, value in validator_info.items():
            if value and ip in str(value):
                return True
        return False
    
    def _check_hostname_resolution(self, ip: str, validator_info: Dict) -> bool:
        """Check if validator hostname resolves to this IP"""
        # Implementation would require DNS resolution
        return False
    
    def _check_network_address_pattern(self, ip: str, validator_info: Dict) -> bool:
        """Check network address patterns"""
        network_addr = validator_info.get("network_address", "")
        if network_addr and ip in network_addr:
            return True
        return False

    async def _extract_websocket_intelligence_async(self, result: SuiDataResult, ip: str) -> bool:
        """Extract intelligence via WebSocket connections"""
        ws_ports = [9000, 9184]
        
        for port in ws_ports:
            try:
                # Test WebSocket upgrade capability
                import socket
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(3)
                if sock.connect_ex((ip, port)) == 0:
                    result.websocket_available = True
                    sock.close()
                    return True
                sock.close()
            except:
                continue
        
        return False

    async def _extract_graphql_intelligence_async(self, result: SuiDataResult, ip: str) -> bool:
        """Extract intelligence via GraphQL"""
        graphql_endpoints = [
            f"http://{ip}:9000/graphql",
            f"https://{ip}:9000/graphql",
            f"http://{ip}:3030/graphql",
            f"https://{ip}:3030/graphql",
        ]
        
        for endpoint in graphql_endpoints:
            try:
                # Test GraphQL introspection
                introspection_query = {
                    "query": "query { __schema { queryType { name } } }"
                }
                
                response = requests.post(endpoint, json=introspection_query, timeout=self.timeout)
                
                if response.status_code == 200:
                    data = response.json()
                    if "data" in data and "__schema" in data["data"]:
                        result.graphql_available = True
                        
                        # Execute intelligence queries
                        await self._execute_graphql_intelligence_queries(result, endpoint)
                        return True
                        
            except Exception as e:
                self.logger.debug(f"GraphQL endpoint {endpoint} failed: {e}")
                continue
        
        return False

    async def _execute_graphql_intelligence_queries(self, result: SuiDataResult, endpoint: str):
        """Execute GraphQL queries for intelligence extraction"""
        for query_name, query in self.GRAPHQL_QUERIES.items():
            try:
                variables = {}
                if query_name == "validator_info":
                    variables = {"first": 100}  # Limit validators returned
                
                response = requests.post(endpoint, json={
                    "query": query,
                    "variables": variables
                }, timeout=self.timeout)
                
                if response.status_code == 200:
                    data = response.json()
                    if "data" in data:
                        await self._process_graphql_intelligence(result, query_name, data["data"])
                        
            except Exception as e:
                self.logger.debug(f"GraphQL query {query_name} failed: {e}")

    async def _process_graphql_intelligence(self, result: SuiDataResult, query_name: str, data: Dict):
        """Process GraphQL intelligence data"""
        try:
            if query_name == "network_info":
                if "chainIdentifier" in data:
                    result.chain_identifier = data["chainIdentifier"]
                
                if "epoch" in data and data["epoch"]:
                    epoch_data = data["epoch"]
                    result.current_epoch = epoch_data.get("epochId")
                    result.reference_gas_price = epoch_data.get("referenceGasPrice")
                    
                    if "validatorSet" in epoch_data:
                        result.validator_count = epoch_data["validatorSet"].get("totalCount")
                
                if "protocolConfig" in data:
                    result.protocol_version = str(data["protocolConfig"].get("protocolVersion"))
                    
            elif query_name == "validator_info":
                if "epoch" in data and "validatorSet" in data["epoch"]:
                    validator_set = data["epoch"]["validatorSet"]
                    result.validator_count = validator_set.get("totalCount")
                    
                    validators = validator_set.get("nodes", [])
                    if validators:
                        await self._analyze_validator_set_from_graphql(result, validators)
                        
        except Exception as e:
            result.extraction_errors.append(f"graphql_intelligence_processing_error_{query_name}: {str(e)}")

    async def _analyze_validator_set_from_graphql(self, result: SuiDataResult, validators: List[Dict]):
        """Analyze validator set from GraphQL data"""
        try:
            stakes = []
            total_stake = 0
            
            for validator in validators:
                next_epoch_stake = validator.get("nextEpochStake")
                if next_epoch_stake:
                    try:
                        stake_amount = int(next_epoch_stake)
                        stakes.append(stake_amount)
                        total_stake += stake_amount
                    except (ValueError, TypeError):
                        pass
            
            if stakes:
                result.total_stake = total_stake
                result.voting_power_gini = self._calculate_gini_coefficient(stakes)
                
        except Exception as e:
            result.extraction_errors.append(f"graphql_validator_analysis_error: {str(e)}")

    async def _extract_grpc_intelligence_async(self, result: SuiDataResult, ip: str, ports: List[int]) -> bool:
        """Extract intelligence via gRPC with protobuf support"""
        if not GRPC_AVAILABLE:
            return False
        
        grpc_ports = [8080, 50051, 9090]
        
        for port in grpc_ports:
            if port not in ports:
                continue
                
            try:
                channel_address = f"{ip}:{port}"
                channel = grpc.insecure_channel(channel_address)
                
                # Test basic connectivity
                try:
                    grpc.channel_ready_future(channel).result(timeout=5)
                    result.grpc_available = True
                    
                    # Try reflection if available
                    if self._extract_grpc_reflection(result, channel):
                        result.intelligence_sources.append("grpc_reflection")
                    
                    # Try Sui-specific gRPC services if stubs are available
                    if self.sui_grpc_stubs:
                        await self._extract_sui_grpc_intelligence(result, channel)
                    
                    channel.close()
                    return True
                    
                except grpc.FutureTimeoutError:
                    channel.close()
                    continue
                    
            except Exception as e:
                self.logger.debug(f"gRPC port {port} failed: {e}")
                continue
        
        return False

    def _extract_grpc_reflection(self, result: SuiDataResult, channel) -> bool:
        """Extract gRPC service information via reflection"""
        try:
            stub = reflection_pb2_grpc.ServerReflectionStub(channel)
            request = reflection_pb2.ServerReflectionRequest()
            request.list_services = ""
            
            responses = stub.ServerReflectionInfo(iter([request]))
            
            for response in responses:
                if response.HasField('list_services_response'):
                    services = [s.name for s in response.list_services_response.service]
                    result.grpc_services = services
                    result.grpc_reflection_data["services"] = services
                    
                    # Identify Sui-specific services
                    sui_services = [s for s in services if 'sui' in s.lower() or 'consensus' in s.lower()]
                    if sui_services:
                        result.grpc_reflection_data["sui_services"] = sui_services
                    
                    return True
                    
        except Exception as e:
            self.logger.debug(f"gRPC reflection failed: {e}")
            return False

    async def _extract_sui_grpc_intelligence(self, result: SuiDataResult, channel):
        """Extract Sui-specific intelligence via gRPC stubs"""
        if not self.sui_grpc_stubs:
            return
        
        try:
            # This would use the compiled Sui protobuf stubs
            # Implementation depends on actual Sui gRPC service definitions
            
            for stub_name, stub_module in self.sui_grpc_stubs.items():
                try:
                    # Create service stub
                    service_stub = getattr(stub_module, f"{stub_name}Stub", None)
                    if service_stub:
                        stub_instance = service_stub(channel)
                        
                        # Call Sui-specific gRPC methods
                        # This would be implemented based on actual Sui proto definitions
                        result.grpc_reflection_data[f"{stub_name}_available"] = True
                        
                except Exception as e:
                    self.logger.debug(f"Sui gRPC stub {stub_name} failed: {e}")
                    
        except Exception as e:
            result.extraction_errors.append(f"sui_grpc_intelligence_error: {str(e)}")

    async def _extract_metrics_intelligence_async(self, result: SuiDataResult, ip: str) -> bool:
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
                    
                    self.logger.info(f"📊 Retrieved {metrics_size} bytes from {endpoint_type} on port {port}")
                    
                    # Analyze metrics content
                    metrics_analysis = await self._analyze_metrics_content(result, metrics_text, endpoint_type, port)
                    
                    if metrics_analysis["is_sui_metrics"]:
                        self.logger.info(f"🎯 Found Sui validator metrics on port {port}")
                        result.metrics_snapshot["sui_metrics_port"] = port
                        result.metrics_snapshot["sui_metrics_endpoint"] = endpoint
                        await self._extract_sui_blockchain_intelligence_from_metrics(result, metrics_text)
                        intelligence_extracted = True
                        
                    elif metrics_analysis["is_node_exporter"]:
                        self.logger.info(f"🖥️ Found Node Exporter on port {port} - indicates monitored infrastructure")
                        result.metrics_snapshot["node_exporter_port"] = port
                        result.metrics_snapshot["infrastructure_monitoring"] = True
                        await self._analyze_system_metrics(result, metrics_text)
                        intelligence_extracted = True
                        
                    else:
                        self.logger.info(f"📊 Found generic metrics on port {port}")
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
                    self.logger.info(f"🔒 Port {port} requires authentication - likely secured validator")
                    result.metrics_snapshot[f"port_{port}_auth_required"] = True
                    
                elif response.status_code == 403:
                    self.logger.info(f"🔒 Port {port} access forbidden - likely secured validator")
                    result.metrics_snapshot[f"port_{port}_forbidden"] = True
                    
                else:
                    self.logger.debug(f"❌ Port {port} returned status {response.status_code}")
                    
            except requests.exceptions.ConnectionError as e:
                if "Connection refused" in str(e):
                    result.metrics_snapshot[f"port_{port}_connection_refused"] = True
                    if is_critical:
                        self.logger.info(f"🔒 Critical port {port} connection refused - likely secured validator")
                else:
                    self.logger.debug(f"❌ Connection error for port {port}: {e}")
                    
            except requests.exceptions.Timeout:
                self.logger.debug(f"⏰ Port {port} timed out")
                result.metrics_snapshot[f"port_{port}_timeout"] = True
                
            except Exception as e:
                self.logger.debug(f"💥 Error checking port {port}: {e}")
                continue
        
        # Analyze the overall metrics pattern for validator detection
        await self._analyze_metrics_pattern_for_validator_detection(result, metrics_endpoints_found)
        
        return intelligence_extracted

    async def _analyze_metrics_content(self, result: SuiDataResult, metrics_text: str, endpoint_type: str, port: str) -> Dict:
        """Analyze metrics content to determine type and extract intelligence"""
        import re
        
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
            self.logger.info(f"🎯 Detected Sui metrics: {analysis['sui_indicators']} Sui indicators")
            
        elif node_exporter_count > 10:
            analysis["is_node_exporter"] = True
            self.logger.info(f"🖥️ Detected Node Exporter: {node_exporter_count} system indicators")
        
        return analysis

    async def _extract_sui_blockchain_intelligence_from_metrics(self, result: SuiDataResult, metrics_text: str):
        """Extract blockchain intelligence directly from Sui metrics"""
        import re
        
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
                        self.logger.info(f"📊 Epoch from metrics: {value}")
                        
                    elif field_name == "checkpoint_height" and not result.checkpoint_height:
                        result.checkpoint_height = value
                        self.logger.info(f"📊 Checkpoint from metrics: {value}")
                        
                    elif field_name == "protocol_version" and not result.protocol_version:
                        result.protocol_version = str(value)
                        self.logger.info(f"📊 Protocol version from metrics: {value}")
                        
                    elif field_name == "total_transactions" and not result.total_transactions:
                        result.total_transactions = value
                        self.logger.info(f"📊 Total transactions from metrics: {value}")
                        
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
            self.logger.info(f"📊 Sui version from metrics: {result.sui_version}")
        
        # Look for validator-specific evidence
        validator_evidence = await self._detect_validator_evidence_from_metrics(result, metrics_text)
        if validator_evidence:
            metrics_extracted += len(validator_evidence)
        
        result.metrics_snapshot["blockchain_metrics_extracted"] = metrics_extracted
        self.logger.info(f"📊 Extracted {metrics_extracted} blockchain metrics")

    async def _detect_validator_evidence_from_metrics(self, result: SuiDataResult, metrics_text: str) -> Dict:
        """Detect evidence that this node is a validator from metrics"""
        import re
        
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
                self.logger.info(f"🏛️ Validator evidence - {evidence_type}: {len(matches)} indicators")
        
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
            self.logger.info(f"🏛️ HIGH CONFIDENCE: Node is active validator ({strong_evidence_count} strong indicators)")
            
        elif len(evidence) >= 3:
            result.is_active_validator = True
            result.metrics_snapshot["validator_confidence"] = "moderate_metrics_evidence"
            self.logger.info(f"🏛️ MODERATE CONFIDENCE: Node likely validator ({len(evidence)} indicators)")
        
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
                self.logger.info(f"🏛️ SECURED VALIDATOR: Monitoring present + critical ports secured")
                
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
        import re
        
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
            self.logger.info(f"🖥️ System info: {system_info.get('cpu_cores', '?')} cores, {system_info.get('memory_gb', '?')}GB RAM")
        
        # Infrastructure quality assessment
        if system_info.get("cpu_cores", 0) >= 8 and system_info.get("memory_gb", 0) >= 16:
            result.metrics_snapshot["infrastructure_class"] = "high_performance"
        elif system_info.get("cpu_cores", 0) >= 4 and system_info.get("memory_gb", 0) >= 8:
            result.metrics_snapshot["infrastructure_class"] = "production_grade"
        else:
            result.metrics_snapshot["infrastructure_class"] = "basic"
        
        # This level of monitoring suggests serious infrastructure
        result.metrics_snapshot["infrastructure_monitoring_quality"] = "professional"

    async def _extract_local_sui_intelligence_async(self, result: SuiDataResult, ip: str) -> bool:
        """Extract comprehensive intelligence using local Sui client"""
        if not self.use_local_sui:
            return False
        
        intelligence_extracted = False
        
        # Primary: Use Sui client to interrogate the node directly
        if await self._extract_sui_client_intelligence(result, ip):
            intelligence_extracted = True
        
        # Secondary: Try Sui CLI intelligence
        if await self._extract_sui_cli_intelligence(result, ip):
            intelligence_extracted = True
        
        # Tertiary: Try Sui SDK intelligence
        if self.sui_client and await self._extract_sui_sdk_intelligence(result, ip):
            intelligence_extracted = True
        
        return intelligence_extracted

    async def _extract_sui_client_intelligence(self, result: SuiDataResult, ip: str) -> bool:
        """Extract comprehensive intelligence using local Sui client"""
        try:
            # Test if sui client is available
            version_result = subprocess.run([self.sui_binary, "client", "--version"], 
                                          capture_output=True, text=True, timeout=5)
            
            if version_result.returncode != 0:
                self.logger.debug(f"Sui client not available: {version_result.stderr}")
                return False
            
            # Build RPC URL - try multiple combinations
            rpc_urls = [
                f"https://{ip}:9000",
                f"http://{ip}:9000", 
                f"https://{ip}",
                f"http://{ip}",
            ]
            
            intelligence_extracted = False
            
            for rpc_url in rpc_urls:
                self.logger.info(f"🌐 Testing Sui client against: {rpc_url}")
                
                # Test basic connectivity first
                if not await self._test_sui_client_connectivity(rpc_url):
                    continue
                
                # Extract comprehensive blockchain intelligence
                if await self._extract_comprehensive_sui_client_data(result, rpc_url):
                    intelligence_extracted = True
                    result.intelligence_sources.append(f"sui_client_{rpc_url}")
                    self.logger.info(f"✅ Sui client intelligence extracted from {rpc_url}")
                    break  # Found working endpoint
                else:
                    self.logger.debug(f"❌ Sui client failed to extract from {rpc_url}")
            
            return intelligence_extracted
            
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            self.logger.debug(f"Sui client not available: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Sui client intelligence extraction failed: {e}")
            return False

    async def _test_sui_client_connectivity(self, rpc_url: str) -> bool:
        """Test if Sui client can connect to the RPC URL"""
        try:
            cmd = [
                self.sui_binary, "client", 
                "--client.rpc-url", rpc_url,
                "gas"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
            
            # Even if gas command fails, if we get a proper RPC response it means connectivity works
            if result.returncode == 0:
                return True
            elif "error" in result.stderr.lower() and "rpc" in result.stderr.lower():
                # RPC error means we connected but got an error response (still good)
                return True
            elif "connection" in result.stderr.lower() or "timeout" in result.stderr.lower():
                return False
            else:
                # Unknown error, assume we can't connect
                return False
                
        except subprocess.TimeoutExpired:
            return False
        except Exception:
            return False

    async def _extract_comprehensive_sui_client_data(self, result: SuiDataResult, rpc_url: str) -> bool:
        """Extract comprehensive blockchain data using Sui client commands"""
        
        # First, let's check the actual Sui CLI syntax
        help_result = subprocess.run([self.sui_binary, "client", "--help"], 
                                   capture_output=True, text=True, timeout=5)
        
        # Determine the correct RPC URL parameter
        rpc_param = "--client.rpc-url" if "--client.rpc-url" in help_result.stdout else "--rpc-url"
        
        # Check if we should use environment setup instead
        use_env_setup = "--client.rpc-url" not in help_result.stdout and "--rpc-url" not in help_result.stdout
        
        # For now, let's skip the complex environment setup and use curl primarily
        # This is more reliable across Sui versions
        use_env_setup = False
        env_params = [rpc_param, rpc_url] if not use_env_setup else []
        
        # Define Sui client commands for comprehensive intelligence extraction
        sui_commands = []
        
        # Only add Sui client commands if we have proper parameters
        if env_params:
            sui_commands.extend([
                # Gas command to test connectivity and get basic info
                {
                    "cmd": ["sui", "client"] + env_params + ["gas"],
                    "extract": "gas_info",
                    "critical": False
                },
                
                # Active address to test client connectivity
                {
                    "cmd": ["sui", "client"] + env_params + ["active-address"],
                    "extract": "active_address",
                    "critical": False
                },
                
                # Try to get network configuration
                {
                    "cmd": ["sui", "client"] + env_params + ["switch", "--list"],
                    "extract": "network_list",
                    "critical": False
                },
            ])
        
        # Add direct curl-based RPC calls as backup
        curl_commands = [
            {
                "cmd": ["curl", "-s", "-X", "POST", "-H", "Content-Type: application/json", 
                       "-d", '{"jsonrpc":"2.0","method":"sui_getChainIdentifier","params":[],"id":1}', 
                       rpc_url],
                "extract": "chain_id_curl",
                "critical": True
            },
            {
                "cmd": ["curl", "-s", "-X", "POST", "-H", "Content-Type: application/json", 
                       "-d", '{"jsonrpc":"2.0","method":"suix_getLatestSuiSystemState","params":[],"id":1}', 
                       rpc_url],
                "extract": "system_state_curl", 
                "critical": True
            },
            {
                "cmd": ["curl", "-s", "-X", "POST", "-H", "Content-Type: application/json", 
                       "-d", '{"jsonrpc":"2.0","method":"sui_getLatestCheckpointSequenceNumber","params":[],"id":1}', 
                       rpc_url],
                "extract": "checkpoint_curl",
                "critical": True
            },
            {
                "cmd": ["curl", "-s", "-X", "POST", "-H", "Content-Type: application/json", 
                       "-d", '{"jsonrpc":"2.0","method":"sui_getValidators","params":[],"id":1}', 
                       rpc_url],
                "extract": "validators_curl",
                "critical": True
            },
        ]
        
        # Combine both approaches - prioritize curl for reliability
        all_commands = curl_commands + sui_commands
        
        intelligence_extracted = False
        successful_extractions = 0
        critical_failures = 0
        
        for command_info in all_commands:
            try:
                self.logger.debug(f"🔍 Executing command: {command_info['extract']}")
                
                cmd_result = subprocess.run(
                    command_info["cmd"], 
                    capture_output=True, 
                    text=True, 
                    timeout=15
                )
                
                if cmd_result.returncode == 0:
                    # Process the successful result
                    await self._process_sui_client_result(
                        result, 
                        command_info["extract"], 
                        cmd_result.stdout,
                        rpc_url
                    )
                    successful_extractions += 1
                    intelligence_extracted = True
                    self.logger.info(f"✅ Extracted {command_info['extract']} via command")
                else:
                    # Log the failure
                    if command_info["critical"]:
                        critical_failures += 1
                        result.extraction_errors.append(f"sui_client_critical_failure_{command_info['extract']}: {cmd_result.stderr[:100]}")
                        self.logger.warning(f"❌ Critical command failed: {command_info['extract']}")
                    else:
                        self.logger.debug(f"⚠️ Command failed: {command_info['extract']} - {cmd_result.stderr[:50]}")
                        
            except subprocess.TimeoutExpired:
                if command_info["critical"]:
                    critical_failures += 1
                    result.extraction_errors.append(f"sui_client_timeout_{command_info['extract']}")
                self.logger.debug(f"⏰ Command timeout: {command_info['extract']}")
            except Exception as e:
                if command_info["critical"]:
                    critical_failures += 1
                    result.extraction_errors.append(f"sui_client_error_{command_info['extract']}: {str(e)}")
                self.logger.debug(f"💥 Command error {command_info['extract']}: {e}")
        
        # Update metrics
        result.metrics_snapshot["sui_client_extractions"] = successful_extractions
        result.metrics_snapshot["sui_client_critical_failures"] = critical_failures
        
        self.logger.info(f"📊 Sui client extraction: {successful_extractions} successful, {critical_failures} critical failures")
        
        return intelligence_extracted

    async def _process_sui_client_result(self, result: SuiDataResult, extract_type: str, output: str, rpc_url: str):
        """Process Sui client command results for intelligence extraction"""
        try:
            output = output.strip()
            
            if extract_type in ["chain_id_curl", "chain_id"]:
                # Extract chain identifier from JSON response or direct output
                if output.startswith('{'):
                    import json
                    try:
                        data = json.loads(output)
                        if "result" in data:
                            result.chain_identifier = str(data["result"]).strip('"')
                    except json.JSONDecodeError:
                        pass
                elif output and len(output) > 2:
                    result.chain_identifier = output.strip('"')
                
                if result.chain_identifier:
                    # Map to network names
                    chain_mappings = {
                        "35834a8a": "mainnet",
                        "4c78adac": "testnet", 
                        "devnet": "devnet",
                        "localnet": "localnet"
                    }
                    result.network = chain_mappings.get(result.chain_identifier, f"unknown_{result.chain_identifier}")
                    self.logger.info(f"🌐 Network identified: {result.network} (chain: {result.chain_identifier})")
            
            elif extract_type in ["checkpoint_curl", "checkpoint"]:
                # Extract checkpoint number
                if output.startswith('{'):
                    import json
                    try:
                        data = json.loads(output)
                        if "result" in data:
                            result.checkpoint_height = int(data["result"])
                    except (json.JSONDecodeError, ValueError, TypeError):
                        pass
                else:
                    try:
                        result.checkpoint_height = int(output.strip('"'))
                        self.logger.info(f"🏁 Checkpoint height: {result.checkpoint_height}")
                    except (ValueError, TypeError):
                        pass
            
            elif extract_type in ["system_state_curl", "epoch_state"]:
                # Parse system state JSON
                if output.startswith('{'):
                    import json
                    try:
                        data = json.loads(output)
                        
                        if "result" in data and isinstance(data["result"], dict):
                            epoch_data = data["result"]
                            result.system_state.update(epoch_data)
                            
                            # Extract epoch information
                            result.current_epoch = epoch_data.get("epoch")
                            result.protocol_version = str(epoch_data.get("protocolVersion", ""))
                            result.reference_gas_price = epoch_data.get("referenceGasPrice")
                            
                            # Only process validator information if enhanced mode is enabled
                            if self.config.get('enhanced', False):
                                # Extract validator information
                                active_validators = epoch_data.get("activeValidators", [])
                                if active_validators:
                                    result.validator_count = len(active_validators)
                                    await self._analyze_validator_set(result, active_validators)
                                    await self._identify_node_as_validator(result, active_validators)
                            else:
                                # Remove activeValidators from epoch_data when not in enhanced mode
                                epoch_data_copy = epoch_data.copy()
                                epoch_data_copy.pop('activeValidators', None)
                                result.system_state.update(epoch_data_copy)
                                
                            self.logger.info(f"🏛️ Epoch state: epoch {result.current_epoch}, {result.validator_count} validators")
                            
                    except json.JSONDecodeError:
                        # If not JSON, treat as raw output
                        result.system_state["raw_output"] = output[:500]
                        
            # Store the raw output for debugging
            result.metrics_snapshot[f"sui_client_{extract_type}_raw"] = output[:200] if len(output) > 200 else output
            
        except Exception as e:
            result.extraction_errors.append(f"sui_client_processing_error_{extract_type}: {str(e)}")
            self.logger.error(f"💥 Error processing command result {extract_type}: {e}")

    async def _analyze_validator_set(self, result: SuiDataResult, validators: List[Dict]):
        """Analyze validator set for comprehensive metrics"""
        try:
            stakes = []
            total_stake = 0
            
            for validator in validators:
                stake_balance = validator.get("stakingPoolSuiBalance")
                if stake_balance:
                    try:
                        stake_amount = int(stake_balance)
                        stakes.append(stake_amount)
                        total_stake += stake_amount
                    except (ValueError, TypeError):
                        pass
            
            if stakes:
                result.total_stake = total_stake
                result.voting_power_gini = self._calculate_gini_coefficient(stakes)
                
        except Exception as e:
            result.extraction_errors.append(f"validator_set_analysis_error: {str(e)}")

    async def _extract_sui_cli_intelligence(self, result: SuiDataResult, ip: str) -> bool:
        """Extract intelligence using Sui CLI"""
        try:
            # Test if sui CLI is available
            version_result = subprocess.run([self.sui_binary, "--version"], 
                                          capture_output=True, text=True, timeout=5)
            
            if version_result.returncode != 0:
                return False
            
            rpc_url = f"http://{ip}:9000"
            
            # Enhanced CLI commands for comprehensive intelligence
            cli_intelligence_commands = [
                (["sui", "client", "gas", "--client.rpc-url", rpc_url], "gas_objects"),
                (["sui", "client", "addresses", "--client.rpc-url", rpc_url], "addresses"),
                (["sui", "client", "active-address", "--client.rpc-url", rpc_url], "active_address"),
                (["sui", "validator", "metadata", "--client.rpc-url", rpc_url], "validator_metadata"),
            ]
            
            intelligence_extracted = False
            
            for cmd, intel_type in cli_intelligence_commands:
                try:
                    result_cmd = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
                    
                    if result_cmd.returncode == 0:
                        await self._process_sui_cli_intelligence(result, intel_type, result_cmd.stdout)
                        intelligence_extracted = True
                        
                except subprocess.TimeoutExpired:
                    continue
                except Exception as e:
                    self.logger.debug(f"Sui CLI command {intel_type} failed: {e}")
            
            return intelligence_extracted
            
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False

    async def _process_sui_cli_intelligence(self, result: SuiDataResult, output_type: str, output: str):
        """Process Sui CLI output for enhanced intelligence"""
        try:
            if output_type == "version_info":
                # Extract version information
                if "sui" in output.lower() and len(output.strip()) > 0:
                    result.sui_version = output.strip()
                    result.metrics_snapshot["cli_version"] = output.strip()
                    self.logger.info(f"🔢 Sui version from CLI: {result.sui_version}")
                    
        except Exception as e:
            result.extraction_errors.append(f"cli_intelligence_processing_error_{output_type}: {str(e)}")

    async def _extract_sui_sdk_intelligence(self, result: SuiDataResult, ip: str) -> bool:
        """Extract intelligence using Sui SDK"""
        if not self.sui_client:
            return False
        
        try:
            # Configure client to connect to this specific node
            rpc_url = f"http://{ip}:9000"
            
            # Create a new client configured for this node
            node_config = SuiConfig.default_config()
            node_config.rpc_url = rpc_url
            node_client = SyncClient(node_config)
            
            # Extract intelligence using SDK methods
            sdk_intelligence_extracted = False
            
            try:
                # Get chain identifier
                chain_id = node_client.get_chain_identifier()
                if chain_id:
                    result.chain_identifier = chain_id
                    result.metrics_snapshot["sdk_chain_id"] = chain_id
                    sdk_intelligence_extracted = True
                    
            except Exception as e:
                self.logger.debug(f"SDK chain identifier failed: {e}")
            
            try:
                # Get latest checkpoint
                checkpoint = node_client.get_latest_checkpoint_sequence_number()
                if checkpoint:
                    result.checkpoint_height = int(checkpoint)
                    result.metrics_snapshot["sdk_checkpoint"] = checkpoint
                    sdk_intelligence_extracted = True
                    
            except Exception as e:
                self.logger.debug(f"SDK checkpoint failed: {e}")
            
            return sdk_intelligence_extracted
            
        except Exception as e:
            self.logger.debug(f"Sui SDK intelligence extraction failed: {e}")
            return False

    async def _post_process_intelligence(self, result: SuiDataResult):
        """Enhanced post-processing with node health assessment"""
        
        # Standard post-processing
        if result.metrics_exposed and not result.rpc_exposed:
            result.metrics_snapshot["node_classification"] = "validator_only"
            self.logger.info(f"🏛️ Node classified as validator-only (metrics exposed, RPC not accessible)")
            
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
                            self.logger.info(f"🌐 Inferred network as mainnet based on version {result.sui_version}")
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
            self.logger.warning(f"🚨 Multiple RPC endpoints unresponsive")
        
        # Check metrics quality
        metrics_size = result.metrics_snapshot.get("size_bytes", 0)
        if result.metrics_exposed and metrics_size == 0:
            health_issues.append("empty_metrics_response")
            health_score -= 0.3
            self.logger.warning(f"🚨 Metrics endpoint returns empty data")
        elif result.metrics_exposed and metrics_size < 1000:  # Very small metrics
            health_issues.append("minimal_metrics_data")
            health_score -= 0.2
            self.logger.warning(f"🚨 Metrics endpoint returns minimal data ({metrics_size} bytes)")
        
        # Check for complete lack of blockchain intelligence
        blockchain_data_fields = [
            result.current_epoch, result.checkpoint_height, result.chain_identifier,
            result.protocol_version, result.network
        ]
        populated_fields = sum(1 for field in blockchain_data_fields if field is not None and field != "unknown")
        
        if populated_fields == 0:
            health_issues.append("no_blockchain_intelligence")
            health_score -= 0.4
            self.logger.warning(f"🚨 No blockchain intelligence extracted")
        elif populated_fields <= 2:
            health_issues.append("limited_blockchain_intelligence")
            health_score -= 0.2
            self.logger.warning(f"🚨 Limited blockchain intelligence ({populated_fields}/5 fields)")
        
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
            self.logger.warning(f"🚨 Minimal service availability ({services_working}/5 services)")
        
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
            self.logger.error(f"🚨 Node health: {health_status.upper()} (score: {health_score:.2f}) - Issues: {', '.join(health_issues)}")
        elif health_status == "degraded":
            self.logger.warning(f"⚠️ Node health: {health_status.upper()} (score: {health_score:.2f}) - Issues: {', '.join(health_issues)}")
        else:
            self.logger.info(f"✅ Node health: {health_status.upper()} (score: {health_score:.2f})")

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

    def _calculate_gini_coefficient(self, stakes: List[float]) -> float:
        """Calculate Gini coefficient for stake distribution analysis"""
        if not stakes or len(stakes) < 2:
            return 0.0
        
        sorted_stakes = sorted(stakes)
        n = len(sorted_stakes)
        cumsum = []
        running_sum = 0
        
        for stake in sorted_stakes:
            running_sum += stake
            cumsum.append(running_sum)
        
        return (n + 1 - 2 * sum(cumsum) / cumsum[-1]) / n

    def export_data(self, results: List[SuiDataResult]) -> str:
        """Export Sui data as flat JSON"""
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
        
        logger.info("🧠 Initializing Intelligent Sui Node Interrogator")
        extractor = SuiDataExtractor.from_discovery_json(args.input_file, config)
        
        # Extract intelligence
        logger.info("🎯 Starting comprehensive Sui blockchain intelligence extraction")
        results = extractor.extract_all_nodes()
        
        # Export intelligence
        output = extractor.export_data(results)
        
        # Save results
        with open(args.output, 'w') as f:
            f.write(output)
        
        logger.info(f"🎉 Intelligence extraction complete! Results saved to {args.output}")
        
        # Intelligence summary
        if results:
            avg_completeness = sum(r.data_completeness for r in results) / len(results)
            validators = len([r for r in results if r.is_active_validator])
            rpc_nodes = len([r for r in results if r.rpc_exposed])
            grpc_nodes = len([r for r in results if r.grpc_available])
            
            logger.info(f"📊 Intelligence Extraction Summary:")
            logger.info(f"  🎯 Nodes analyzed: {len(results)}")
            logger.info(f"  🧠 Average intelligence completeness: {avg_completeness:.2f}")
            logger.info(f"  👥 Active validators identified: {validators}")
            logger.info(f"  🌐 RPC nodes: {rpc_nodes}")
            logger.info(f"  🔗 gRPC nodes: {grpc_nodes}")
        
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
