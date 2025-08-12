#!/usr/bin/env python3
"""
Network Throughput Calculator
Calculates TPS (Transactions per second) and CPS (Checkpoints per second) 
from consecutive Sui node scans with state persistence.
"""

import json
import time
import logging
from pathlib import Path
from typing import Optional, Dict, Tuple, Any
from datetime import datetime
import tempfile
import os

logger = logging.getLogger(__name__)


class ThroughputCalculator:
    """
    Calculates TPS and CPS from consecutive Sui node observations
    with persistent state storage and guardrails.
    """
    
    def __init__(self, state_file_path: Optional[str] = None, ttl_hours: int = 24):
        """
        Initialize throughput calculator with state persistence.
        
        Args:
            state_file_path: Path to state file (default: temp file)
            ttl_hours: Time to live for state entries in hours (default: 24)
        """
        self.ttl_seconds = ttl_hours * 3600
        
        if state_file_path:
            self.state_file = Path(state_file_path)
        else:
            # Use temp file with consistent name for state persistence
            temp_dir = Path(tempfile.gettempdir())
            self.state_file = temp_dir / "pgdn_sui_throughput_state.json"
        
        self.state = self._load_state()
        logger.debug(f"ThroughputCalculator initialized with state file: {self.state_file}")
    
    def _load_state(self) -> Dict[str, Dict[str, Any]]:
        """Load state from file, cleaning up expired entries."""
        if not self.state_file.exists():
            logger.debug("No existing state file found, starting fresh")
            return {}
        
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)
            
            # Clean up expired entries
            current_time = time.time()
            expired_keys = []
            
            for key, data in state.items():
                if current_time - data.get('ts_s', 0) > self.ttl_seconds:
                    expired_keys.append(key)
            
            for key in expired_keys:
                del state[key]
                logger.debug(f"Expired state entry removed: {key}")
            
            logger.debug(f"Loaded state with {len(state)} entries")
            return state
            
        except (json.JSONDecodeError, FileNotFoundError, KeyError) as e:
            logger.warning(f"Failed to load state file: {e}, starting fresh")
            return {}
    
    def _save_state(self) -> None:
        """Save current state to file atomically."""
        try:
            # Write to temporary file first for atomic operation
            temp_file = self.state_file.with_suffix('.tmp')
            
            with open(temp_file, 'w') as f:
                json.dump(self.state, f, separators=(',', ':'))
            
            # Atomic move
            temp_file.replace(self.state_file)
            logger.debug(f"State saved with {len(self.state)} entries")
            
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
    
    def _create_key(self, network: str, node_type: str, ip: str, port: int) -> str:
        """Create unique key for node state."""
        return f"{network}|{node_type}|{ip}|{port}"
    
    def _convert_timestamp_to_epoch(self, timestamp: Any) -> float:
        """
        Convert timestamp to epoch seconds (float).
        Handles datetime objects, RFC3339 strings, and epoch seconds.
        """
        if isinstance(timestamp, datetime):
            return timestamp.timestamp()
        elif isinstance(timestamp, str):
            try:
                # Try parsing RFC3339 format
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                return dt.timestamp()
            except ValueError:
                # If parsing fails, log and return current time
                logger.warning(f"Failed to parse timestamp string: {timestamp}")
                return time.time()
        elif isinstance(timestamp, (int, float)):
            return float(timestamp)
        else:
            logger.warning(f"Unknown timestamp type: {type(timestamp)}, using current time")
            return time.time()
    
    def calculate_throughput(self, 
                           network: str, 
                           node_type: str, 
                           ip: str, 
                           port: int,
                           total_transactions: Optional[int],
                           checkpoint_height: Optional[int], 
                           timestamp: Any) -> Dict[str, Optional[float]]:
        """
        Calculate TPS and CPS for a node based on current and previous observations.
        
        Args:
            network: Network name (mainnet, testnet, devnet)
            node_type: Node type (validator, public_rpc, etc.)
            ip: Node IP address
            port: Node port
            total_transactions: Current total transaction count
            checkpoint_height: Current checkpoint height
            timestamp: Current timestamp (datetime, RFC3339 string, or epoch seconds)
        
        Returns:
            Dict with tps, cps, and calculation_window_seconds (may contain nulls)
        """
        key = self._create_key(network, node_type, ip, port)
        ts_now = self._convert_timestamp_to_epoch(timestamp)
        
        # Initialize result
        result = {
            "tps": None,
            "cps": None, 
            "calculation_window_seconds": None
        }
        
        # Get previous snapshot
        prev_snapshot = self.state.get(key)
        
        if not prev_snapshot:
            logger.debug(f"No previous snapshot for {key}, storing first observation")
            # Store first observation, return nulls
            if total_transactions is not None and checkpoint_height is not None:
                self.state[key] = {
                    "total_transactions": total_transactions,
                    "checkpoint_height": checkpoint_height,
                    "ts_s": ts_now
                }
                self._save_state()
            return result
        
        # Calculate deltas
        delta_t = ts_now - prev_snapshot.get('ts_s', ts_now)
        delta_tx = None
        delta_cp = None
        
        if total_transactions is not None and prev_snapshot.get('total_transactions') is not None:
            delta_tx = total_transactions - prev_snapshot['total_transactions']
            
        if checkpoint_height is not None and prev_snapshot.get('checkpoint_height') is not None:
            delta_cp = checkpoint_height - prev_snapshot['checkpoint_height']
        
        result["calculation_window_seconds"] = round(delta_t, 2)
        
        # Apply guardrails
        if delta_t < 1.0:
            logger.debug(f"Delta time too small ({delta_t:.2f}s), setting metrics to null")
            # Don't update state if time delta is too small
            return result
        
        # Calculate TPS
        if delta_tx is not None:
            if delta_tx < 0:
                logger.warning(f"Transaction counter reset detected for {key} (Δtx={delta_tx})")
                result["tps"] = None
            else:
                result["tps"] = round(delta_tx / delta_t, 2)
                
                # Quality check for TPS
                if result["tps"] < 0 or result["tps"] > 10_000:
                    logger.debug(f"TPS sanity check: unusual TPS value {result['tps']} for {key}")
        
        # Calculate CPS  
        if delta_cp is not None:
            if delta_cp < 0:
                logger.warning(f"Checkpoint counter reset detected for {key} (Δcp={delta_cp})")
                result["cps"] = None
            else:
                result["cps"] = round(delta_cp / delta_t, 2)
                
                # Quality check for CPS
                if result["cps"] > 10 or result["cps"] < 0:
                    logger.debug(f"CPS sanity check: unusual CPS value {result['cps']} for {key}")
        
        # Atomically update state after calculation
        if total_transactions is not None and checkpoint_height is not None:
            self.state[key] = {
                "total_transactions": total_transactions,
                "checkpoint_height": checkpoint_height, 
                "ts_s": ts_now
            }
            self._save_state()
            
            logger.info(f"Throughput calculated for {key}: TPS={result['tps']}, CPS={result['cps']}, window={result['calculation_window_seconds']}s")
        
        return result
    
    def get_state_info(self) -> Dict[str, Any]:
        """Get information about current state for debugging."""
        current_time = time.time()
        state_info = {
            "total_entries": len(self.state),
            "state_file": str(self.state_file),
            "ttl_hours": self.ttl_seconds / 3600,
            "entries": {}
        }
        
        for key, data in self.state.items():
            age_hours = (current_time - data.get('ts_s', current_time)) / 3600
            state_info["entries"][key] = {
                "age_hours": round(age_hours, 2),
                "total_transactions": data.get('total_transactions'),
                "checkpoint_height": data.get('checkpoint_height')
            }
        
        return state_info
    
    def cleanup_expired_entries(self) -> int:
        """Manually clean up expired entries and return count removed."""
        current_time = time.time()
        expired_keys = []
        
        for key, data in self.state.items():
            if current_time - data.get('ts_s', 0) > self.ttl_seconds:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.state[key]
        
        if expired_keys:
            self._save_state()
            logger.info(f"Cleaned up {len(expired_keys)} expired state entries")
        
        return len(expired_keys)
    
    def clear_state(self) -> None:
        """Clear all state (for testing/debugging)."""
        self.state.clear()
        self._save_state()
        logger.info("All throughput calculation state cleared")