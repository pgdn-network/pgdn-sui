#!/usr/bin/env python3
"""
Sui Client Extractor
Handles local Sui client and SDK extraction
"""

import json
import logging
import subprocess
from typing import Optional, List, Dict
from datetime import datetime
from ..models import SuiDataResult
from ..utils import calculate_gini_coefficient

logger = logging.getLogger(__name__)

# Sui SDK imports
try:
    from pysui import SuiConfig, SyncClient
    SUI_SDK_AVAILABLE = True
except ImportError:
    SUI_SDK_AVAILABLE = False


class SuiClientExtractor:
    """Handles local Sui client and SDK extraction from Sui nodes"""
    
    def __init__(self, timeout: int = 10, config: dict = None, sui_client=None):
        self.timeout = timeout
        self.config = config or {}
        self.use_local_sui = self.config.get('use_local_sui', True)
        self.sui_binary = self.config.get('sui_binary', 'sui')
        self.sui_client = sui_client
        self.logger = logger

    async def extract_local_sui_intelligence_async(self, result: SuiDataResult, ip: str) -> bool:
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
                self.logger.info(f"SUI CLIENT: Testing against RPC endpoint: {rpc_url}")
                
                # Test basic connectivity first
                if not await self._test_sui_client_connectivity(rpc_url):
                    continue
                
                # Extract comprehensive blockchain intelligence
                if await self._extract_comprehensive_sui_client_data(result, rpc_url):
                    intelligence_extracted = True
                    result.intelligence_sources.append(f"sui_client_{rpc_url}")
                    self.logger.info(f"SUCCESS: Sui client intelligence extracted from {rpc_url}")
                    break  # Found working endpoint
                else:
                    self.logger.debug(f"FAILED: Sui client failed to extract from {rpc_url}")
            
            return intelligence_extracted
            
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            self.logger.debug(f"Sui client not available: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Sui client intelligence extraction failed: {e}")
            return False

    async def _test_sui_client_connectivity(self, rpc_url: str) -> bool:
        """Test if Sui client can connect to the RPC URL with improved validation"""
        try:
            # First try a simple curl test to validate endpoint responsiveness
            curl_test = subprocess.run([
                "curl", "-s", "-m", "5", "-X", "POST", 
                "-H", "Content-Type: application/json",
                "-d", '{"jsonrpc":"2.0","method":"sui_getChainIdentifier","params":[],"id":1}',
                rpc_url
            ], capture_output=True, text=True, timeout=6)
            
            if curl_test.returncode == 0:
                response = curl_test.stdout.strip()
                
                # Check if we got HTML error page
                if response.startswith('<!DOCTYPE html') or response.startswith('<html'):
                    self.logger.debug(f"CONNECTIVITY: {rpc_url} returned HTML error page - not a valid RPC endpoint")
                    return False
                
                # Check if we got a JSON response (even if error)
                if response.startswith('{'):
                    try:
                        data = json.loads(response)
                        if "result" in data or "error" in data:
                            self.logger.debug(f"CONNECTIVITY: {rpc_url} returned valid JSON-RPC response")
                            return True
                    except json.JSONDecodeError:
                        pass
                
                # If we got some other response, it's not a valid RPC endpoint
                self.logger.debug(f"CONNECTIVITY: {rpc_url} returned non-JSON response: {response[:50]}")
                return False
            
            # If curl failed, try the original sui client approach as fallback
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
            self.logger.debug(f"CONNECTIVITY: Timeout testing {rpc_url}")
            return False
        except Exception as e:
            self.logger.debug(f"CONNECTIVITY: Error testing {rpc_url}: {e}")
            return False

    async def _extract_comprehensive_sui_client_data(self, result: SuiDataResult, rpc_url: str) -> bool:
        """Extract comprehensive blockchain data using Sui client commands"""
        
        # First, let's check the actual Sui CLI syntax
        help_result = subprocess.run([self.sui_binary, "client", "--help"], 
                                   capture_output=True, text=True, timeout=5)
        
        # Determine the correct RPC URL parameter
        rpc_param = "--client.rpc-url" if "--client.rpc-url" in help_result.stdout else "--rpc-url"
        
        env_params = [rpc_param, rpc_url]
        
        # Add direct curl-based RPC calls as primary approach
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
        ]
        
        intelligence_extracted = False
        successful_extractions = 0
        critical_failures = 0
        
        for command_info in curl_commands:
            try:
                self.logger.debug(f"EXECUTING: Command {command_info['extract']}")
                
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
                    self.logger.info(f"SUCCESS: Extracted {command_info['extract']} via command")
                else:
                    # Check if this is an HTML error response
                    stderr_output = cmd_result.stderr.strip()
                    if stderr_output.startswith('<!DOCTYPE html') or stderr_output.startswith('<html'):
                        error_msg = "HTML error page received instead of JSON response"
                        if command_info["critical"]:
                            critical_failures += 1
                            result.extraction_errors.append(f"sui_client_html_error_{command_info['extract']}: {error_msg}")
                            self.logger.warning(f"HTML ERROR: {command_info['extract']} returned HTML error page")
                    else:
                        # Log the failure
                        if command_info["critical"]:
                            critical_failures += 1
                            # Only store the first 50 chars of stderr to avoid noise
                            error_summary = stderr_output[:50] + "..." if len(stderr_output) > 50 else stderr_output
                            result.extraction_errors.append(f"sui_client_critical_failure_{command_info['extract']}: {error_summary}")
                            self.logger.warning(f"CRITICAL FAILURE: Command failed: {command_info['extract']} - {error_summary}")
                        else:
                            self.logger.debug(f"COMMAND ERROR: {command_info['extract']} - {stderr_output[:50]}")
                        
            except subprocess.TimeoutExpired:
                if command_info["critical"]:
                    critical_failures += 1
                    result.extraction_errors.append(f"sui_client_timeout_{command_info['extract']}")
                self.logger.debug(f"Command timeout: {command_info['extract']}")
            except Exception as e:
                if command_info["critical"]:
                    critical_failures += 1
                    result.extraction_errors.append(f"sui_client_error_{command_info['extract']}: {str(e)}")
                self.logger.debug(f"Command error {command_info['extract']}: {e}")
        
        # Update metrics
        result.metrics_snapshot["sui_client_extractions"] = successful_extractions
        result.metrics_snapshot["sui_client_critical_failures"] = critical_failures
        
        self.logger.info(f"SUI CLIENT EXTRACTION: {successful_extractions} successful, {critical_failures} critical failures")
        
        return intelligence_extracted

    async def _process_sui_client_result(self, result: SuiDataResult, extract_type: str, output: str, rpc_url: str):
        """Process Sui client command results for intelligence extraction"""
        try:
            output = output.strip()
            
            # Detect HTML error pages early
            if output.startswith('<!DOCTYPE html') or output.startswith('<html'):
                error_msg = f"HTML error page received instead of JSON response"
                result.extraction_errors.append(f"sui_client_html_error_{extract_type}: {error_msg}")
                self.logger.warning(f"HTML ERROR: {extract_type} returned HTML error page from {rpc_url}")
                return
            
            # Validate that we have actual content
            if not output or len(output) < 3:
                error_msg = f"Empty or minimal response received"
                result.extraction_errors.append(f"sui_client_empty_response_{extract_type}: {error_msg}")
                self.logger.warning(f"EMPTY RESPONSE: {extract_type} returned empty response from {rpc_url}")
                return
            
            if extract_type in ["chain_id_curl", "chain_id"]:
                # Extract chain identifier from JSON response or direct output
                if output.startswith('{'):
                    try:
                        data = json.loads(output)
                        if "result" in data:
                            result.chain_identifier = str(data["result"]).strip('"')
                        elif "error" in data:
                            error_info = data["error"]
                            error_msg = f"RPC error: {error_info.get('message', 'unknown error')}"
                            result.extraction_errors.append(f"sui_client_rpc_error_{extract_type}: {error_msg}")
                            self.logger.warning(f"RPC ERROR: {extract_type} returned RPC error: {error_info}")
                            return
                    except json.JSONDecodeError as e:
                        error_msg = f"Invalid JSON response: {str(e)}"
                        result.extraction_errors.append(f"sui_client_json_error_{extract_type}: {error_msg}")
                        self.logger.warning(f"JSON ERROR: {extract_type} failed to parse JSON from {rpc_url}: {str(e)}")
                        return
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
                    self.logger.info(f"NETWORK: Identified as {result.network} (chain: {result.chain_identifier})")
            
            elif extract_type in ["checkpoint_curl", "checkpoint"]:
                # Extract checkpoint number
                if output.startswith('{'):
                    try:
                        data = json.loads(output)
                        if "result" in data:
                            result.checkpoint_height = int(data["result"])
                            self.logger.info(f"BLOCKCHAIN DATA: Checkpoint height: {result.checkpoint_height}")
                        elif "error" in data:
                            error_info = data["error"]
                            error_msg = f"RPC error: {error_info.get('message', 'unknown error')}"
                            result.extraction_errors.append(f"sui_client_rpc_error_{extract_type}: {error_msg}")
                            self.logger.warning(f"RPC ERROR: {extract_type} returned RPC error: {error_info}")
                            return
                    except json.JSONDecodeError as e:
                        error_msg = f"Invalid JSON response: {str(e)}"
                        result.extraction_errors.append(f"sui_client_json_error_{extract_type}: {error_msg}")
                        self.logger.warning(f"JSON ERROR: {extract_type} failed to parse JSON from {rpc_url}: {str(e)}")
                        return
                    except (ValueError, TypeError) as e:
                        error_msg = f"Invalid checkpoint number in response: {str(e)}"
                        result.extraction_errors.append(f"sui_client_parse_error_{extract_type}: {error_msg}")
                        self.logger.warning(f"PARSE ERROR: {extract_type} invalid checkpoint number: {str(e)}")
                        return
                else:
                    try:
                        result.checkpoint_height = int(output.strip('"'))
                        self.logger.info(f"BLOCKCHAIN DATA: Checkpoint height: {result.checkpoint_height}")
                    except (ValueError, TypeError) as e:
                        error_msg = f"Invalid checkpoint format in raw response: {str(e)}"
                        result.extraction_errors.append(f"sui_client_parse_error_{extract_type}: {error_msg}")
                        self.logger.warning(f"PARSE ERROR: {extract_type} invalid raw checkpoint: {str(e)}")
                        return
            
            elif extract_type in ["system_state_curl", "epoch_state"]:
                # Parse system state JSON
                if output.startswith('{'):
                    try:
                        data = json.loads(output)
                        
                        if "result" in data and isinstance(data["result"], dict):
                            epoch_data = data["result"]
                            
                            # Extract epoch information
                            result.current_epoch = epoch_data.get("epoch")
                            result.protocol_version = str(epoch_data.get("protocolVersion", ""))
                            result.reference_gas_price = epoch_data.get("referenceGasPrice")
                            
                            # Only process validator information if enhanced mode is enabled
                            if self.config.get('enhanced', False):
                                # Enhanced mode: include full system state with activeValidators
                                result.system_state.update(epoch_data)
                                # Extract validator information
                                active_validators = epoch_data.get("activeValidators", [])
                                if active_validators:
                                    result.validator_count = len(active_validators)
                                    await self._analyze_validator_set(result, active_validators)
                            else:
                                # Non-enhanced mode: exclude activeValidators from system state
                                epoch_data_copy = epoch_data.copy()
                                epoch_data_copy.pop('activeValidators', None)
                                result.system_state.update(epoch_data_copy)
                                # Still get validator count without storing the full array
                                active_validators = epoch_data.get("activeValidators", [])
                                if active_validators:
                                    result.validator_count = len(active_validators)
                                
                            self.logger.info(f"BLOCKCHAIN STATE: Epoch {result.current_epoch}, {result.validator_count} validators")
                        
                        elif "error" in data:
                            error_info = data["error"]
                            error_msg = f"RPC error: {error_info.get('message', 'unknown error')}"
                            result.extraction_errors.append(f"sui_client_rpc_error_{extract_type}: {error_msg}")
                            self.logger.warning(f"RPC ERROR: {extract_type} returned RPC error: {error_info}")
                            return
                        else:
                            error_msg = f"Unexpected JSON response structure"
                            result.extraction_errors.append(f"sui_client_structure_error_{extract_type}: {error_msg}")
                            self.logger.warning(f"STRUCTURE ERROR: {extract_type} unexpected response structure from {rpc_url}")
                            return
                            
                    except json.JSONDecodeError as e:
                        error_msg = f"Invalid JSON response: {str(e)}"
                        result.extraction_errors.append(f"sui_client_json_error_{extract_type}: {error_msg}")
                        self.logger.warning(f"JSON ERROR: {extract_type} failed to parse JSON from {rpc_url}: {str(e)}")
                        return
                else:
                    error_msg = f"Non-JSON response received"
                    result.extraction_errors.append(f"sui_client_format_error_{extract_type}: {error_msg}")
                    self.logger.warning(f"FORMAT ERROR: {extract_type} expected JSON but got: {output[:100]}")
                    return
                        
            # Store the raw output for debugging
            result.metrics_snapshot[f"sui_client_{extract_type}_raw"] = output[:200] if len(output) > 200 else output
            
        except Exception as e:
            result.extraction_errors.append(f"sui_client_processing_error_{extract_type}: {str(e)}")
            self.logger.error(f"Error processing command result {extract_type}: {e}")

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
                result.voting_power_gini = calculate_gini_coefficient(stakes)
                
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
                ([self.sui_binary, "client", "gas", "--client.rpc-url", rpc_url], "gas_objects"),
                ([self.sui_binary, "client", "addresses", "--client.rpc-url", rpc_url], "addresses"),
                ([self.sui_binary, "client", "active-address", "--client.rpc-url", rpc_url], "active_address"),
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
                    self.logger.info(f"VERSION: Sui version from CLI: {result.sui_version}")
                    
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