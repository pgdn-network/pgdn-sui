#!/usr/bin/env python3
"""
Throughput Bands Calculator
Computes network-relative throughput performance bands using batch percentiles
"""

import logging
import statistics
from typing import List, Dict, Optional, Tuple, Any
from .models import SuiDataResult

logger = logging.getLogger(__name__)


class ThroughputBandCalculator:
    """
    Calculates throughput performance bands relative to network batch performance
    using percentile analysis (p10, p50, p90, p95)
    """
    
    def __init__(self, min_nodes: int = 3):
        """
        Initialize throughput band calculator.
        
        Args:
            min_nodes: Minimum nodes required for percentile calculation (default: 3)
        """
        self.min_nodes = min_nodes
        self.logger = logger
    
    def calculate_batch_bands(self, results: List[SuiDataResult]) -> Dict[str, Any]:
        """
        Calculate throughput bands for a batch of scan results.
        
        Args:
            results: List of SuiDataResult objects from current batch
            
        Returns:
            Dict containing batch statistics and band assignments
        """
        if not results:
            return {"stats": None, "bands_assigned": 0}
        
        # Extract valid throughput values
        tps_values = []
        cps_values = []
        valid_nodes = []
        
        for result in results:
            if (result.network_throughput and 
                isinstance(result.network_throughput, dict)):
                
                tps = result.network_throughput.get('tps')
                cps = result.network_throughput.get('cps')
                
                # Collect valid TPS values (including 0.0)
                if tps is not None and isinstance(tps, (int, float)):
                    tps_values.append(tps)
                
                # Collect valid CPS values (including 0.0) 
                if cps is not None and isinstance(cps, (int, float)):
                    cps_values.append(cps)
                
                # Track nodes with any valid throughput data
                if tps is not None or cps is not None:
                    valid_nodes.append(result)
        
        # Calculate statistics
        tps_stats = self._calculate_percentiles(tps_values, "TPS")
        cps_stats = self._calculate_percentiles(cps_values, "CPS")
        
        # Assign bands to nodes
        bands_assigned = 0
        for result in valid_nodes:
            if result.network_throughput:
                tps = result.network_throughput.get('tps')
                cps = result.network_throughput.get('cps')
                
                # Assign TPS band
                if tps is not None and tps_stats:
                    result.tps_band = self._assign_band(tps, tps_stats)
                    if result.tps_band:
                        bands_assigned += 1
                
                # Assign CPS band
                if cps is not None and cps_stats:
                    result.cps_band = self._assign_band(cps, cps_stats)
                    if result.cps_band:
                        bands_assigned += 1
                
                # Store batch statistics for context
                result.batch_stats = {
                    "tps": tps_stats,
                    "cps": cps_stats,
                    "batch_size": len(results),
                    "valid_nodes": len(valid_nodes)
                }
        
        batch_summary = {
            "stats": {
                "tps": tps_stats,
                "cps": cps_stats,
                "batch_size": len(results),
                "valid_nodes": len(valid_nodes),
                "tps_samples": len(tps_values),
                "cps_samples": len(cps_values)
            },
            "bands_assigned": bands_assigned
        }
        
        self._log_batch_summary(batch_summary)
        return batch_summary
    
    def _calculate_percentiles(self, values: List[float], metric_name: str) -> Optional[Dict[str, float]]:
        """
        Calculate percentiles for a list of values using robust method for small datasets.
        
        Args:
            values: List of numeric values
            metric_name: Name of the metric for logging (TPS/CPS)
            
        Returns:
            Dict with p10, p50, p90, p95 or None if insufficient data
        """
        if len(values) < self.min_nodes:
            self.logger.debug(f"Insufficient {metric_name} data for percentiles: {len(values)} < {self.min_nodes}")
            return None
        
        # Sort values for percentile calculation
        sorted_values = sorted(values)
        
        try:
            # Use manual percentile calculation that's more appropriate for small datasets
            stats = {
                "p10": self._percentile(sorted_values, 0.10),
                "p50": statistics.median(sorted_values), 
                "p90": self._percentile(sorted_values, 0.90),
                "p95": self._percentile(sorted_values, 0.95),
                "count": len(values),
                "min": sorted_values[0],
                "max": sorted_values[-1]
            }
            
            self.logger.debug(f"{metric_name} percentiles: p10={stats['p10']:.2f}, p50={stats['p50']:.2f}, p90={stats['p90']:.2f}, p95={stats['p95']:.2f}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to calculate {metric_name} percentiles: {e}")
            return None
    
    def _percentile(self, sorted_values: List[float], p: float) -> float:
        """
        Calculate percentile using linear interpolation method suitable for small datasets.
        
        Args:
            sorted_values: Pre-sorted list of values
            p: Percentile as decimal (0.10 for 10th percentile)
            
        Returns:
            Percentile value
        """
        if not sorted_values:
            return 0.0
        
        if len(sorted_values) == 1:
            return sorted_values[0]
        
        # For small datasets, use nearest-rank method with interpolation
        index = p * (len(sorted_values) - 1)
        lower = int(index)
        upper = min(lower + 1, len(sorted_values) - 1)
        
        if lower == upper:
            return sorted_values[lower]
        
        # Linear interpolation between adjacent values
        weight = index - lower
        return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight
    
    def _assign_band(self, value: float, stats: Dict[str, float]) -> Optional[str]:
        """
        Assign performance band based on percentile thresholds.
        
        Args:
            value: The throughput value to classify
            stats: Dictionary with percentile statistics
            
        Returns:
            Band assignment: "low", "normal", "high", or None
        """
        if not stats:
            return None
        
        p10 = stats['p10']
        p50 = stats['p50']
        p95 = stats['p95']
        
        # Band assignment logic per requirements
        if value < p10:
            return "low"
        elif p50 * 0.8 <= value <= p50 * 1.2:
            return "normal"
        elif value > p95:
            return "high"
        else:
            # Between thresholds - no specific band
            return None
    
    def _log_batch_summary(self, summary: Dict[str, Any]) -> None:
        """Log batch throughput analysis summary."""
        stats = summary.get('stats', {})
        if not stats:
            self.logger.info("No throughput data available for band calculation")
            return
        
        tps_stats = stats.get('tps')
        cps_stats = stats.get('cps')
        
        log_msg = f"Throughput band analysis: {stats['valid_nodes']}/{stats['batch_size']} nodes with data"
        
        if tps_stats:
            log_msg += f", TPS: p50={tps_stats['p50']:.2f} (n={tps_stats['count']})"
        
        if cps_stats:
            log_msg += f", CPS: p50={cps_stats['p50']:.2f} (n={cps_stats['count']})"
        
        log_msg += f", {summary['bands_assigned']} bands assigned"
        
        self.logger.info(log_msg)


def get_band_summary_for_evidence(tps: Optional[float], cps: Optional[float], 
                                 tps_band: Optional[str], cps_band: Optional[str]) -> str:
    """
    Generate band summary string for evidence fields.
    
    Args:
        tps: TPS value
        cps: CPS value  
        tps_band: TPS performance band
        cps_band: CPS performance band
        
    Returns:
        Formatted band summary for evidence string
    """
    bands = []
    
    if tps is not None and tps_band:
        bands.append(f"tps={tps}({tps_band})")
    elif tps is not None:
        bands.append(f"tps={tps}")
        
    if cps is not None and cps_band:
        bands.append(f"cps={cps}({cps_band})")
    elif cps is not None:
        bands.append(f"cps={cps}")
    
    return "/".join(bands) if bands else "no_data"