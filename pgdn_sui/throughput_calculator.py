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
    
    def _create_key(self, network: str, hostname_or_ip: str, port: int) -> str:
        """
        Create STABLE delta key that ignores role changes.
        Format: <network>|<hostname_or_ip>|<port>
        Uses hostname if available for better stability, otherwise IP.
        Normalized to lowercase for consistency.
        """
        # Normalize hostname/IP to lowercase for consistent matching
        identifier = hostname_or_ip.lower().strip()
        return f"{network}|{identifier}|{port}"
    
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
                           ip: str, 
                           port: int,
                           total_transactions: Optional[int],
                           checkpoint_height: Optional[int], 
                           timestamp: Any,
                           hostname: Optional[str] = None) -> Dict[str, Optional[float]]:
        """
        Calculate TPS and CPS for a node based on current and previous observations.
        Uses STABLE delta key that ignores role changes.
        
        Args:
            network: Network name (mainnet, testnet, devnet)
            ip: Node IP address
            port: Node port
            total_transactions: Current total transaction count
            checkpoint_height: Current checkpoint height
            timestamp: Current timestamp (datetime, RFC3339 string, or epoch seconds)
            hostname: Optional hostname for more stable identification (preferred over IP)
        
        Returns:
            Dict with tps, cps, calculation_window_seconds, and reason (may contain nulls)
        """
        # Use hostname if available, otherwise fallback to IP
        hostname_or_ip = hostname if hostname and hostname != ip else ip
        key = self._create_key(network, hostname_or_ip, port)
        ts_now = self._convert_timestamp_to_epoch(timestamp)
        
        # Rule F: Initialize result with independent reasons
        result = {
            "tps": None,
            "cps": None, 
            "calculation_window_seconds": None,
            "reason": None  # Overall reason if both are null for same reason
        }
        
        # Get previous snapshot
        prev_snapshot = self.state.get(key)
        
        if not prev_snapshot:
            logger.debug(f"No previous snapshot for {key}, storing first observation")
            # Store first observation, return nulls with reason
            result["reason"] = "no_previous_data"
            if total_transactions is not None and checkpoint_height is not None:
                self.state[key] = {
                    "total_transactions": total_transactions,
                    "checkpoint_height": checkpoint_height,
                    "ts_s": ts_now
                }
                self._save_state()
            else:
                result["reason"] = "missing_counters"
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
        
        # Rule F: Apply guardrails - require Δt >= 1s as specified
        if delta_t < 1.0:
            logger.debug(f"Delta time too small ({delta_t:.2f}s < 1s), setting TPS/CPS to null")
            result["reason"] = "interval_too_short"
            # Don't update state if time delta is too small - preserve for next calculation
            return result
        
        # Track if we have any valid calculations
        has_calculations = False
        tps_reason = None
        cps_reason = None
        
        # Rule F: Calculate TPS with independent reason tracking
        if delta_tx is not None and total_transactions is not None and prev_snapshot.get('total_transactions') is not None:
            if delta_tx < 0:
                logger.warning(f"Transaction counter reset detected for {key} (Δtx={delta_tx})")
                result["tps"] = None
                tps_reason = "counter_reset"
            elif delta_tx == 0:
                # No new transactions - legitimate 0 TPS
                result["tps"] = 0.0
                has_calculations = True
            else:
                # Round to 2 decimal places as specified
                result["tps"] = round(delta_tx / delta_t, 2)
                has_calculations = True
                
                # Quality check for TPS (allow 0, flag unusual high values)
                if result["tps"] > 50_000:  # Very high TPS threshold
                    logger.warning(f"TPS sanity check: unusually high TPS value {result['tps']} for {key}")
        elif total_transactions is None:
            tps_reason = "missing_counters"
        
        # Rule F: Calculate CPS with independent reason tracking
        if delta_cp is not None and checkpoint_height is not None and prev_snapshot.get('checkpoint_height') is not None:
            if delta_cp < 0:
                logger.warning(f"Checkpoint counter reset detected for {key} (Δcp={delta_cp})")
                result["cps"] = None
                cps_reason = "counter_reset"
            elif delta_cp == 0:
                # No new checkpoints - legitimate 0 CPS
                result["cps"] = 0.0
                has_calculations = True
            else:
                # Round to 2 decimal places as specified
                result["cps"] = round(delta_cp / delta_t, 2)
                has_calculations = True
                
                # Quality check for CPS (checkpoints are typically slower)
                if result["cps"] > 50:  # Very high CPS threshold
                    logger.warning(f"CPS sanity check: unusually high CPS value {result['cps']} for {key}")
        elif checkpoint_height is None:
            cps_reason = "missing_counters"
        
        # Rule F: Set overall reason only if both metrics have same reason
        if not has_calculations:
            if tps_reason == cps_reason and tps_reason is not None:
                result["reason"] = tps_reason
            elif total_transactions is None and checkpoint_height is None:
                result["reason"] = "missing_counters"
            else:
                # Independent failures - let each metric have its own reason in the broader system
                result["reason"] = None
        
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