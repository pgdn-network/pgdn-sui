#!/usr/bin/env python3
"""
Utility Functions
Common utility functions used across Sui data extraction modules
"""

from typing import List


def calculate_gini_coefficient(stakes: List[float]) -> float:
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


def get_primary_port(ports: List[int], node_type: str) -> int:
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