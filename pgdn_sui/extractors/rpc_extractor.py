#!/usr/bin/env python3
"""
RPC Extractor
Handles JSON-RPC extraction from Sui nodes
"""

import json
import time
import random
import logging
import requests
from typing import List, Dict, Any, Optional
from ..models import SuiDataResult

logger = logging.getLogger(__name__)


class RpcExtractor:
    """Handles JSON-RPC extraction from Sui nodes"""
    
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
    
    def __init__(self, timeout: int = 10, config: Dict = None):
        self.timeout = timeout
        self.config = config or {}
        self.logger = logger
        self.backoff_delay = 0.5  # Rule 6: Base 500ms backoff
        self.max_backoff_delay = 5.0  # Rule 6: Cap at 5s
    
    def _build_rpc_permutations(self, ip: str) -> List[str]:
        """Build RPC endpoint permutations exactly as specified in user requirements"""
        # Schemes/ports: https://<host>[:443], https://<host>:9000, http://<host>:9000
        base_urls = [
            f"https://{ip}",      # https://<host> (default port 443)
            f"https://{ip}:443",  # https://<host>:443
            f"https://{ip}:9000", # https://<host>:9000
            f"http://{ip}:9000",  # http://<host>:9000
        ]
        
        # Paths to try per base: "/", "/rpc", "/json-rpc"
        paths = ["/", "/rpc", "/json-rpc"]
        
        endpoints = []
        for base_url in base_urls:
            for path in paths:
                endpoint = base_url + path
                endpoints.append(endpoint)
        
        return endpoints
    
    def _detect_rate_limit(self, response, response_data: dict = None) -> bool:
        """
        Rule C: Detect rate limiting via HTTP 429 OR JSON body containing rate limit patterns
        """
        # Check HTTP 429 status code
        if hasattr(response, 'status_code') and response.status_code == 429:
            return True
        
        # Check JSON body for rate limit patterns
        if response_data and isinstance(response_data, dict):
            # Check for specific rate limit error message patterns
            error_msg = response_data.get("error_msg", "")
            if "Your request is too frequent" in error_msg:
                return True
            
            # Check error object for rate limit indicators
            if "error" in response_data:
                error_obj = response_data["error"]
                if isinstance(error_obj, dict):
                    error_message = error_obj.get("message", "")
                    if "too frequent" in error_message.lower() or "rate limit" in error_message.lower():
                        return True
        
        return False
    
    def _apply_exponential_backoff(self) -> None:
        """
        Rule 6: Apply exponential backoff with jitter (base 500ms, max 5s)
        On first 429 per host, switch subsequent RPC calls in this run to exponential backoff
        """
        # Add jitter (±25% random variation)
        jitter = random.uniform(0.75, 1.25)
        actual_delay = min(self.backoff_delay * jitter, self.max_backoff_delay)
        
        self.logger.info(f"Rate limit detected, applying backoff: {actual_delay:.3f}s")
        time.sleep(actual_delay)
        
        # Double the backoff for next time (exponential)
        self.backoff_delay = min(self.backoff_delay * 2, self.max_backoff_delay)
        
    async def extract_rpc_intelligence_async(self, result: SuiDataResult, ip: str, ports: List[int]) -> bool:
        """Extract comprehensive intelligence via JSON-RPC with enhanced debugging and extend.md permutations"""
        
        # Extended RPC endpoint permutations as specified in extend.md
        rpc_endpoints = self._build_rpc_permutations(ip)
        
        # Set initial RPC status and reachability
        result.rpc_status = "unreachable"
        result.rpc_reachable = False
        
        for endpoint in rpc_endpoints:
            try:
                self.logger.info(f"Testing RPC endpoint: {endpoint}")
                
                # Test basic connectivity and authentication with short timeouts
                start_time = time.time()
                test_response = requests.post(endpoint, json={
                    "jsonrpc": "2.0",
                    "method": "sui_getChainIdentifier",
                    "params": [],
                    "id": 1
                }, timeout=(1.5, 2.0), verify=False)  # connect=1.5s, read=2.0s, retries=1
                
                response_time = time.time() - start_time
                result.service_response_times[f"rpc_{endpoint}"] = response_time
                
                self.logger.info(f"RPC {endpoint} responded with status {test_response.status_code} in {response_time:.3f}s")
                
                # Rule 1a: Check for HTTP 429 rate limiting first - this counts as reachable
                if test_response.status_code == 429:
                    result.rpc_rate_limit_events += 1
                    result.rpc_status = "rate_limited"
                    result.rpc_reachable = True  # Rule 1: HTTP 429 = reachable
                    self.logger.warning(f"Rate limit detected (HTTP 429) on RPC endpoint {endpoint}")
                    self._apply_exponential_backoff()
                    continue
                elif test_response.status_code in [401, 403]:
                    result.rpc_authenticated = True
                    self.logger.info(f"SECURED: RPC endpoint {endpoint} requires authentication")
                    continue
                elif test_response.status_code == 200:
                    result.rpc_exposed = True
                    result.rpc_authenticated = False
                    
                    # Parse response to see if we got actual data
                    try:
                        response_data = test_response.json()
                        self.logger.info(f"RPC response: {str(response_data)[:200]}...")
                        
                        # Rule 1b: Check for rate limiting in JSON body - this counts as reachable
                        if self._detect_rate_limit(test_response, response_data):
                            result.rpc_rate_limit_events += 1
                            result.rpc_status = "rate_limited"
                            result.rpc_reachable = True  # Rule 1: Rate limit after successful TLS = reachable
                            self.logger.warning(f"Rate limit detected on RPC endpoint {endpoint}")
                            # Apply backoff for remaining requests in this run
                            self._apply_exponential_backoff()
                            continue  # Try next endpoint
                        
                        if "result" in response_data and response_data["result"]:
                            self.logger.info(f"SUCCESS: RPC endpoint {endpoint} returned valid data: {response_data['result']}")
                            
                            # Set RPC status to reachable
                            result.rpc_status = "reachable"
                            result.rpc_reachable = True  # Rule 1: HTTP 200 JSON-RPC response = reachable
                            
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
                            self.logger.warning(f"ERROR: RPC endpoint {endpoint} returned error: {error_info}")
                            
                            # Still mark as exposed since we got a valid JSON-RPC response
                            result.rpc_exposed = True
                            
                            # Some errors still indicate the node is working
                            if error_info.get("code") in [-32602, -32601]:  # Invalid params/method not found
                                self.logger.info(f"PARTIAL: RPC endpoint {endpoint} is working but method not supported")
                                # Set RPC status to reachable
                                result.rpc_status = "reachable"
                                result.rpc_reachable = True  # Rule 1: HTTP 200 JSON-RPC response = reachable
                                # Try other methods on this endpoint
                                await self._extract_comprehensive_rpc_intelligence(result, endpoint)
                                return True
                        else:
                            self.logger.debug(f"UNEXPECTED: RPC endpoint {endpoint} returned unexpected format: {response_data}")
                            
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"NON-JSON: RPC endpoint {endpoint} returned non-JSON response: {test_response.text[:100]}")
                        self.logger.debug(f"JSON decode error: {e}")
                elif test_response.status_code == 404:
                    self.logger.debug(f"NOT FOUND: RPC endpoint {endpoint} returned 404 - endpoint not found")
                elif test_response.status_code >= 500:
                    self.logger.warning(f"SERVER ERROR: RPC endpoint {endpoint} returned server error {test_response.status_code}")
                else:
                    self.logger.debug(f"FAILED: RPC endpoint {endpoint} returned status {test_response.status_code}")
                    continue
                    
            except requests.exceptions.ConnectionError as e:
                self.logger.debug(f"CONNECTION FAILED: RPC endpoint {endpoint} connection failed: {e}")
                continue
            except requests.exceptions.Timeout as e:
                self.logger.debug(f"RPC endpoint {endpoint} timed out: {e}")
                continue
            except requests.exceptions.SSLError as e:
                self.logger.debug(f"SSL ERROR: RPC endpoint {endpoint} SSL error: {e}")
                continue
            except Exception as e:
                self.logger.debug(f"RPC endpoint {endpoint} error: {e}")
                continue
        
        # Rule 1: Only set unreachable if ALL attempts failed with timeout/TLS/DNS errors/connection refused
        # If we got any HTTP responses (even 404/500), that means TLS worked
        if not result.rpc_reachable:
            result.rpc_status = "unreachable"  # All attempts failed with connection-level errors
        
        self.logger.warning(f"FAILED: No working RPC endpoints found for {ip}, final status: {result.rpc_status}")
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
        
        self.logger.info(f"RPC EXTRACTION: Starting comprehensive analysis with {total_methods} methods")
        
        for method, params in all_methods:
            try:
                self.logger.info(f"TESTING: RPC method {method} with params: {params}")
                
                response = requests.post(endpoint, json={
                    "jsonrpc": "2.0",
                    "method": method,  
                    "params": params,
                    "id": hash(method) % 1000
                }, timeout=self.timeout, verify=False)
                
                # Rule 1: Check for rate limiting - maintain reachable status
                if response.status_code == 429:
                    result.rpc_rate_limit_events += 1
                    if result.rpc_status != "rate_limited":
                        result.rpc_status = "rate_limited"
                        result.rpc_reachable = True  # Rule 1: HTTP 429 = reachable
                    self.logger.warning(f"Rate limit hit on method {method}, applying backoff")
                    self._apply_exponential_backoff()
                    continue  # Skip this method but continue with others
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Rule 1: Check JSON body for rate limit patterns - maintain reachable status
                    if self._detect_rate_limit(response, data):
                        result.rpc_rate_limit_events += 1
                        if result.rpc_status != "rate_limited":
                            result.rpc_status = "rate_limited"
                            result.rpc_reachable = True  # Rule 1: Rate limit after successful TLS = reachable
                        self.logger.warning(f"Rate limit detected in JSON response for method {method}")
                        self._apply_exponential_backoff()
                        continue  # Skip this method but continue with others
                    
                    if "result" in data and data["result"] is not None:
                        await self._process_rpc_method_result(result, method, data["result"])
                        result.rpc_methods_available.append(method)
                        successful_methods += 1
                        self.logger.info(f"SUCCESS: RPC method {method} successful")
                        
                    elif "error" in data:
                        error = data["error"]
                        error_code = error.get("code", "unknown")
                        error_message = error.get("message", "")
                        
                        # Log but continue - some errors are expected
                        if error_code in [-32602, -32601]:
                            self.logger.warning(f"ERROR: RPC method {method} error [{error_code}]: {error_message}")
                        else:
                            self.logger.debug(f"ERROR: RPC method {method} error [{error_code}]: {error_message}")
                            
            except Exception as e:
                self.logger.debug(f"RPC method {method} failed: {e}")
                continue
        
        # Store extraction statistics
        result.metrics_snapshot["rpc_extraction"] = {
            "successful_methods": successful_methods,
            "total_methods_attempted": total_methods,
            "success_rate": successful_methods / total_methods if total_methods > 0 else 0
        }
        
        self.logger.info(f"RPC EXTRACTION COMPLETE: {successful_methods}/{total_methods} methods successful")

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
                self.logger.info(f"Network: {result.network} (chain: {result.chain_identifier})")
                
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
                            from ..utils import calculate_gini_coefficient
                            result.voting_power_gini = calculate_gini_coefficient(stakes)
                        
                        # Check if this node is a validator
                        await self._identify_node_as_validator(result, active_validators)
                        
                        self.logger.info(f"ENHANCED MODE: Processed activeValidators array ({len(active_validators)} validators)")
                else:
                    # Get validator count without processing the full array
                    active_validators = data.get("activeValidators", [])
                    if active_validators:
                        result.validator_count = len(active_validators)
                    
                    # Remove activeValidators completely when not in enhanced mode
                    system_state_copy.pop('activeValidators', None)
                    self.logger.info(f"STANDARD MODE: Skipped activeValidators processing ({result.validator_count} validators, use --enhanced to include)")
                
                result.system_state.update(system_state_copy)
                
                # Extract comprehensive epoch information
                result.current_epoch = data.get("epoch")
                result.protocol_version = str(data.get("protocolVersion", ""))
                result.reference_gas_price = data.get("referenceGasPrice")
                
                self.logger.info(f"System state: epoch {result.current_epoch}, protocol {result.protocol_version}, {result.validator_count} validators")
                
            elif method == "sui_getLatestCheckpointSequenceNumber":
                try:
                    result.checkpoint_height = int(data)
                    self.logger.info(f"Checkpoint: {result.checkpoint_height}")
                except (ValueError, TypeError):
                    pass
                    
            elif method == "sui_getTotalTransactionBlocks":
                try:
                    result.total_transactions = int(data)
                    self.logger.info(f"BLOCKCHAIN DATA: Total transactions: {result.total_transactions}")
                except (ValueError, TypeError):
                    pass
                    
            elif method == "suix_getReferenceGasPrice":
                try:
                    result.reference_gas_price = int(data)
                    self.logger.info(f"Gas price: {result.reference_gas_price}")
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
                
                self.logger.info(f"Protocol: {result.protocol_version}")
                
            elif method == "suix_getCommitteeInfo" and isinstance(data, dict):
                committee_members = data.get("validators", [])
                
                # Only process committee_info if enhanced mode is enabled  
                if self.config.get('enhanced', False):
                    result.committee_info.update(data)
                    if committee_members and not result.validator_count:
                        result.validator_count = len(committee_members)
                    self.logger.info(f"ENHANCED MODE: Committee {len(committee_members)} members processed")
                else:
                    # Only get the count without storing the full committee data
                    if committee_members and not result.validator_count:
                        result.validator_count = len(committee_members)
                    self.logger.info(f"STANDARD MODE: Committee {len(committee_members)} members (data skipped, use --enhanced to include)")
                
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
                    self.logger.info(f"VALIDATOR METRICS: APYs for {len(apys)} validators, avg {result.metrics_snapshot['validator_apys']['avg_apy']:.2f}%")
                    
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
                    self.logger.info(f"RPC DISCOVERY: {len(available_methods)} methods available")
                    
            elif method == "suix_getCoinMetadata" and isinstance(data, dict):
                result.metrics_snapshot["sui_coin_metadata"] = {
                    "name": data.get("name"),
                    "symbol": data.get("symbol"),
                    "decimals": data.get("decimals"),
                    "description": data.get("description"),
                }
                self.logger.info(f"SUI coin metadata: {data.get('symbol')} ({data.get('decimals')} decimals)")
                
            # Store raw data for debugging (but limit size)
            if method in ["suix_getLatestSuiSystemState", "suix_getCommitteeInfo"] and not self.config.get('enhanced', False):
                # Store a smaller sample for debugging when not in enhanced mode
                sample_data = str(data)[:500] + "... [truncated]" if len(str(data)) > 500 else str(data)
                result.metrics_snapshot[f"rpc_{method}_sample"] = sample_data
            else:
                result.metrics_snapshot[f"rpc_{method}_sample"] = str(data)[:200] if isinstance(data, (dict, list)) else str(data)
            
        except Exception as e:
            result.extraction_errors.append(f"rpc_processing_error_{method}: {str(e)}")
            self.logger.error(f"Error processing RPC method {method}: {e}")

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
                    
                    # Enhanced validator data extraction from prompt.md
                    try:
                        result.validator_stake = int(validator.get("stakingPoolSuiBalance", 0))
                    except (ValueError, TypeError):
                        result.validator_stake = 0
                        
                    try:
                        result.validator_commission_rate = int(validator.get("commissionRate", 0))
                    except (ValueError, TypeError):
                        result.validator_commission_rate = 0
                        
                    try:
                        result.validator_next_epoch_stake = int(validator.get("nextEpochStake", 0))
                    except (ValueError, TypeError):
                        result.validator_next_epoch_stake = 0
                        
                    try:
                        # Extract validator rewards from validator info
                        result.validator_rewards = int(validator.get("rewardsPool", 0))
                    except (ValueError, TypeError):
                        result.validator_rewards = 0
                    
                    self.logger.info(f"VALIDATOR IDENTIFIED: {result.validator_name} - Stake: {result.validator_stake}, Commission: {result.validator_commission_rate}%")
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

    def calculate_batch_metrics(self, results: List[SuiDataResult]):
        """Calculate checkpoint lag and transaction throughput across the scan batch"""
        if not results:
            return
            
        # Find maximum checkpoint in the batch
        max_checkpoint = 0
        checkpoint_nodes = []
        
        for result in results:
            if result.checkpoint_height and result.checkpoint_height > max_checkpoint:
                max_checkpoint = result.checkpoint_height
            if result.checkpoint_height:
                checkpoint_nodes.append(result)
        
        if max_checkpoint > 0:
            # Calculate checkpoint lag for each node
            for result in results:
                if result.checkpoint_height:
                    result.checkpoint_lag = max_checkpoint - result.checkpoint_height
                    self.logger.info(f"Node {result.ip} checkpoint lag: {result.checkpoint_lag} blocks")
        
        # Calculate transaction throughput TPS from delta (if we had previous scan data)
        # For now, store the transaction counts for future delta calculations
        for result in results:
            if result.total_transactions:
                result.metrics_snapshot["transaction_count_for_delta"] = {
                    "count": result.total_transactions,
                    "timestamp": time.time(),
                    "checkpoint": result.checkpoint_height
                }
                
        self.logger.info(f"Batch analysis complete: max checkpoint {max_checkpoint}, {len(checkpoint_nodes)} nodes with checkpoint data")