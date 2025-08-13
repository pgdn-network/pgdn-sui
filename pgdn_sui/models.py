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
    hostname: Optional[str] = None
    
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
    consensus_commit_latency_seconds: Optional[float] = None
    narwhal_primary_network_peers: Optional[int] = None
    consensus_proposals_in_queue: Optional[int] = None
    consensus_rejected_transactions_total: Optional[int] = None
    mempool_transactions: Optional[int] = None
    mempool_transactions_total: Optional[int] = None
    mempool_pending_transactions: Optional[int] = None
    
    # High-impact Prometheus metrics (from TDD requirements)
    uptime_seconds_total: Optional[int] = None
    build_info_version: Optional[str] = None
    build_info_git_commit: Optional[str] = None
    
    # Uptime classification and edge-aware policy
    uptime_status: Optional[str] = None  # "available", "unavailable_public_metrics", "gated", "closed"
    uptime_expected: Optional[bool] = None  # False for edge nodes
    uptime_source: Optional[str] = None  # "node_exporter" for node_boot_time_seconds (extend.md requirement)
    edge: Optional[bool] = None  # True for provider edge nodes
    uptime_evidence: Optional[str] = None  # Evidence string ≤128 chars
    
    # Public node classification
    is_public_node: Optional[bool] = None  # True for known public RPC providers
    public_node_provider: Optional[str] = None  # Provider name if detected (e.g., "onfinality", "chainstack")
    metrics_surface: Optional[Dict[str, Any]] = None  # HTTP status and access patterns
    
    # Enhanced network intelligence
    network_peers: Optional[int] = None 
    peer_info: List[Dict] = None
    sync_status: Optional[str] = None
    checkpoint_lag: Optional[int] = None
    transaction_throughput: Optional[float] = None
    transaction_throughput_tps: Optional[float] = None
    reference_gas_price: Optional[int] = None
    total_transactions: Optional[int] = None
    
    # Service availability intelligence
    rpc_exposed: bool = False
    rpc_authenticated: bool = False
    rpc_reachable: bool = False  # Rule 1: HTTP 200 JSON-RPC response OR HTTP 429/rate limit after TLS
    rpc_status: Optional[str] = None  # "reachable", "unreachable", "rate_limited" (extend.md requirement)
    rpc_methods_available: List[str] = None
    rpc_rate_limit_events: int = 0  # Rule C: Count of rate limit events in this run
    websocket_available: bool = False
    graphql_available: bool = False
    grpc_available: bool = False
    grpc_services: List[str] = None
    grpc_reflection_enabled: Optional[bool] = None
    grpc_services_list: List[str] = None
    grpc_health_status: Optional[str] = None
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
    
    # Network throughput metrics (TPS & CPS)
    network_throughput: Optional[Dict[str, Optional[float]]] = None
    
    # Open ports detected via TCP connect (for gRPC detection)
    open_ports: Optional[Dict[str, List[int]]] = None
    
    # Capability-driven node role classification
    node_role: Optional[str] = None  # validator, public_rpc, metrics, hybrid, unknown
    has_narwhal_metrics: bool = False
    narwhal_missing_reason: Optional[str] = None  # not_validator_like, metrics_closed, metrics_gated, missing_metrics_data
    
    # Evidence strings (≤128 chars) for specific conditions
    metrics_evidence: Optional[str] = None  # "metrics closed (timeout)", "rate_limited (429)"
    rpc_evidence: Optional[str] = None  # "reachable", "rate_limited (429)"
    grpc_evidence: Optional[str] = None  # "grpc ok (reflection)", "grpc blocked"
    throughput_evidence: Optional[str] = None  # "delta ok: cps=4.12/37930.4s"
    
    # Debug mode sample storage (full samples when debug enabled)
    debug_samples: Optional[Dict[str, str]] = None  # Full samples for debug mode
    max_sample_length: int = 200  # Default truncation length

    def __post_init__(self):
        if self.peer_info is None:
            self.peer_info = []
        if self.rpc_methods_available is None:
            self.rpc_methods_available = []
        if self.grpc_services is None:
            self.grpc_services = []
        if self.grpc_services_list is None:
            self.grpc_services_list = []
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
        if self.network_throughput is None:
            self.network_throughput = {}
        if self.metrics_surface is None:
            self.metrics_surface = {}
        if self.open_ports is None:
            self.open_ports = {}
        if self.debug_samples is None:
            self.debug_samples = {}
    
    def store_sample(self, key: str, sample_data: str, debug_mode: bool = False) -> str:
        """Store sample data with appropriate truncation based on debug mode"""
        if debug_mode:
            # Store full sample in debug mode
            if self.debug_samples is None:
                self.debug_samples = {}
            self.debug_samples[key] = sample_data
            return sample_data
        else:
            # Truncate for normal mode
            if len(sample_data) <= self.max_sample_length:
                return sample_data
            return sample_data[:self.max_sample_length] + " [truncated]"
    
    def set_evidence(self, evidence_type: str, evidence: str) -> None:
        """Set evidence string with length validation (≤128 chars)"""
        if len(evidence) > 128:
            evidence = evidence[:125] + "..."
        
        if evidence_type == "metrics":
            self.metrics_evidence = evidence
        elif evidence_type == "rpc":
            self.rpc_evidence = evidence
        elif evidence_type == "grpc":
            self.grpc_evidence = evidence
        elif evidence_type == "throughput":
            self.throughput_evidence = evidence
        elif evidence_type == "uptime":
            self.uptime_evidence = evidence