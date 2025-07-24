#!/usr/bin/env python3
"""
Sui Data Models
Data structures for Sui protocol data extraction
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional, Any


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