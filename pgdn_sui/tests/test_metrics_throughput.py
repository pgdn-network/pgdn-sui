"""
Tests for metrics-based TPS/CPS derivation implementation (stage7.md)
"""

import pytest
import time
from unittest.mock import Mock, patch
from datetime import datetime

from pgdn_sui.models import SuiDataResult
from pgdn_sui.extractors.metrics_extractor import MetricsThroughputDetector
from pgdn_sui.throughput_calculator import ThroughputCalculator


class TestMetricsThroughputDetector:
    """Test the MetricsThroughputDetector class"""
    
    @pytest.fixture
    def detector(self):
        return MetricsThroughputDetector()
    
    def test_tx_counter_regex_patterns(self, detector):
        """Test TX counter regex patterns match expected formats"""
        test_cases = [
            # Should match
            ("sui_executed_transactions_total", True),
            ("sui_transactions_total", True), 
            ("sui_total_transaction_blocks", True),
            ("sui_total_transaction_blocks_total", True),
            ("transactions_total", True),
            ("sui_tx_executed_total", True),
            ("sui_tx_committed_total", True),
            
            # Should not match
            ("sui_transaction_rate", False),
            ("other_transactions_total", False),
            ("sui_transactions_pending", False),
        ]
        
        for metric_name, should_match in test_cases:
            found = any(pattern.match(metric_name) for pattern in detector.tx_patterns)
            assert found == should_match, f"Metric '{metric_name}' match result should be {should_match}"
    
    def test_cp_counter_regex_patterns(self, detector):
        """Test CP counter regex patterns match expected formats"""
        test_cases = [
            # Should match
            ("sui_checkpoint_sequence_number", True),
            ("sui_checkpoints_sequence_number", True),
            ("sui_checkpoint_number", True),
            ("sui_checkpoint_index", True),
            ("sui_checkpoint_height", True),
            ("checkpoints_total", True),
            ("checkpoints_committed_total", True),
            ("sui_consensus_committed_checkpoints", True),
            ("sui_consensus_committed_checkpoints_total", True),
            
            # Should not match
            ("sui_checkpoint_lag", False),
            ("other_checkpoints_total", False),
            ("sui_checkpoint_processing", False),
        ]
        
        for metric_name, should_match in test_cases:
            found = any(pattern.match(metric_name) for pattern in detector.cp_patterns)
            assert found == should_match, f"Metric '{metric_name}' match result should be {should_match}"
    
    def test_detect_counter_families_success(self, detector):
        """Test successful detection of TX and CP counter families"""
        metrics_text = """
# HELP sui_executed_transactions_total Total executed transactions
# TYPE sui_executed_transactions_total counter
sui_executed_transactions_total{node="validator1"} 12345
sui_executed_transactions_total{node="validator2"} 23456

# HELP sui_checkpoint_sequence_number Current checkpoint sequence number
# TYPE sui_checkpoint_sequence_number gauge
sui_checkpoint_sequence_number 9876

# HELP other_metric Some other metric
other_metric 42
"""
        
        tx_counter, cp_counter = detector.detect_counter_families(metrics_text)
        
        assert tx_counter == "sui_executed_transactions_total"
        assert cp_counter == "sui_checkpoint_sequence_number"
    
    def test_detect_counter_families_first_match_wins(self, detector):
        """Test that first matching pattern wins for each family"""
        metrics_text = """
sui_executed_transactions_total 100
sui_total_transaction_blocks_total 200
sui_checkpoint_sequence_number 50
checkpoints_total 75
"""
        
        tx_counter, cp_counter = detector.detect_counter_families(metrics_text)
        
        # First TX pattern should win
        assert tx_counter == "sui_executed_transactions_total"
        # First CP pattern should win  
        assert cp_counter == "sui_checkpoint_sequence_number"
    
    def test_detect_counter_families_partial_match(self, detector):
        """Test detection when only one family is present"""
        metrics_text_tx_only = """
sui_executed_transactions_total 100
other_metric 42
"""
        
        tx_counter, cp_counter = detector.detect_counter_families(metrics_text_tx_only)
        assert tx_counter == "sui_executed_transactions_total"
        assert cp_counter is None
        
        metrics_text_cp_only = """
sui_checkpoint_sequence_number 50
other_metric 42
"""
        
        tx_counter, cp_counter = detector.detect_counter_families(metrics_text_cp_only)
        assert tx_counter is None
        assert cp_counter == "sui_checkpoint_sequence_number"
    
    def test_detect_counter_families_no_match(self, detector):
        """Test detection when no matching families are found"""
        metrics_text = """
other_metric 42
cpu_usage_percent 85.5
memory_total_bytes 8589934592
"""
        
        tx_counter, cp_counter = detector.detect_counter_families(metrics_text)
        assert tx_counter is None
        assert cp_counter is None
    
    def test_parse_counter_values_sum_across_series(self, detector):
        """Test parsing and summing counter values across all series"""
        metrics_text = """
sui_executed_transactions_total{node="validator1"} 1000
sui_executed_transactions_total{node="validator2"} 2000
sui_executed_transactions_total{node="validator3"} 3000

sui_checkpoint_sequence_number{region="us"} 100
sui_checkpoint_sequence_number{region="eu"} 150

other_metric 42
"""
        
        result = detector.parse_counter_values(
            metrics_text, 
            tx_counter="sui_executed_transactions_total",
            cp_counter="sui_checkpoint_sequence_number"
        )
        
        assert result['tx_total'] == 6000  # 1000 + 2000 + 3000
        assert result['cp_total'] == 250   # 100 + 150
        assert 'timestamp' in result
    
    def test_parse_counter_values_no_counters(self, detector):
        """Test parsing when specified counters are not found"""
        metrics_text = """
other_metric 42
cpu_usage_percent 85.5
"""
        
        result = detector.parse_counter_values(
            metrics_text,
            tx_counter="sui_executed_transactions_total", 
            cp_counter="sui_checkpoint_sequence_number"
        )
        
        assert result['tx_total'] is None
        assert result['cp_total'] is None
        assert 'timestamp' in result
    
    def test_detect_counter_resets(self, detector):
        """Test counter reset detection"""
        current_values = {'tx_total': 100, 'cp_total': 50}
        previous_values = {'tx_total': 200, 'cp_total': 75}
        
        tx_reset, cp_reset = detector.detect_counter_resets(current_values, previous_values)
        
        assert tx_reset is True   # 100 < 200
        assert cp_reset is True   # 50 < 75
        
        # Test no reset case
        current_values = {'tx_total': 300, 'cp_total': 100}
        previous_values = {'tx_total': 200, 'cp_total': 75}
        
        tx_reset, cp_reset = detector.detect_counter_resets(current_values, previous_values)
        
        assert tx_reset is False  # 300 > 200
        assert cp_reset is False  # 100 > 75


class TestThroughputCalculatorMetrics:
    """Test metrics throughput calculation in ThroughputCalculator"""
    
    @pytest.fixture
    def calculator(self):
        # Use in-memory state for testing
        return ThroughputCalculator(state_file_path=":memory:")
    
    @pytest.fixture
    def sample_metrics_data(self):
        return {
            'tx_total': 10000,
            'cp_total': 500,
            'timestamp': time.time(),
            'counter_names': {
                'tx': 'sui_executed_transactions_total',
                'cp': 'sui_checkpoint_sequence_number'
            }
        }
    
    def test_calculate_metrics_throughput_first_observation(self, calculator, sample_metrics_data):
        """Test first observation stores data and returns no_previous_data"""
        result = calculator.calculate_metrics_throughput(
            network="testnet",
            ip="127.0.0.1", 
            port=9184,
            metrics_data=sample_metrics_data,
            timestamp=datetime.now()
        )
        
        assert result['tps'] is None
        assert result['cps'] is None
        assert result['reason'] == 'no_previous_data'
        assert result['source'] == 'metrics'
    
    def test_calculate_metrics_throughput_success(self, calculator, sample_metrics_data):
        """Test successful TPS/CPS calculation with valid time delta"""
        # First observation
        calculator.calculate_metrics_throughput(
            network="testnet",
            ip="127.0.0.1",
            port=9184, 
            metrics_data=sample_metrics_data,
            timestamp=datetime.now()
        )
        
        # Second observation after 120 seconds
        second_metrics_data = {
            'tx_total': 10240,  # +240 transactions
            'cp_total': 504,    # +4 checkpoints
            'timestamp': time.time(),
            'counter_names': sample_metrics_data['counter_names']
        }
        
        # Mock timestamp to be 120 seconds later
        import time
        with patch('time.time', return_value=sample_metrics_data['timestamp'] + 120):
            result = calculator.calculate_metrics_throughput(
                network="testnet",
                ip="127.0.0.1",
                port=9184,
                metrics_data=second_metrics_data,
                timestamp=datetime.now()
            )
        
        assert result['tps'] == 2.0  # 240 tx / 120s = 2 TPS
        assert result['cps'] == 0.03  # 4 cp / 120s = 0.0333... ≈ 0.03 CPS
        assert result['calculation_window_seconds'] == 120.0
        assert result['source'] == 'metrics'
        assert result['reason'] is None
    
    def test_calculate_metrics_throughput_window_too_small(self, calculator, sample_metrics_data):
        """Test validation failure when time window < 60s"""
        # First observation
        calculator.calculate_metrics_throughput(
            network="testnet",
            ip="127.0.0.1",
            port=9184,
            metrics_data=sample_metrics_data,
            timestamp=datetime.now()
        )
        
        # Second observation after only 30 seconds (< 60s requirement)
        second_metrics_data = {
            'tx_total': 10100,
            'cp_total': 502,
            'timestamp': time.time(),
            'counter_names': sample_metrics_data['counter_names']
        }
        
        with patch('time.time', return_value=sample_metrics_data['timestamp'] + 30):
            result = calculator.calculate_metrics_throughput(
                network="testnet",
                ip="127.0.0.1",
                port=9184,
                metrics_data=second_metrics_data,
                timestamp=datetime.now()
            )
        
        assert result['tps'] is None
        assert result['cps'] is None
        assert result['reason'] == 'validation_failed'
        assert result['calculation_window_seconds'] == 30.0
    
    def test_calculate_metrics_throughput_counter_reset(self, calculator, sample_metrics_data):
        """Test reset detection when counters decrease"""
        # First observation
        calculator.calculate_metrics_throughput(
            network="testnet",
            ip="127.0.0.1",
            port=9184,
            metrics_data=sample_metrics_data,
            timestamp=datetime.now()
        )
        
        # Second observation with reset counters (lower values)
        reset_metrics_data = {
            'tx_total': 5000,   # Reset: lower than 10000
            'cp_total': 250,    # Reset: lower than 500
            'timestamp': time.time(),
            'counter_names': sample_metrics_data['counter_names']
        }
        
        with patch('time.time', return_value=sample_metrics_data['timestamp'] + 120):
            result = calculator.calculate_metrics_throughput(
                network="testnet", 
                ip="127.0.0.1",
                port=9184,
                metrics_data=reset_metrics_data,
                timestamp=datetime.now()
            )
        
        assert result['tps'] is None
        assert result['cps'] is None
        assert result['reason'] == 'reset_detected'
    
    def test_calculate_metrics_throughput_missing_counters(self, calculator):
        """Test handling when no counters are provided"""
        empty_metrics_data = {
            'tx_total': None,
            'cp_total': None,
            'timestamp': time.time()
        }
        
        result = calculator.calculate_metrics_throughput(
            network="testnet",
            ip="127.0.0.1",
            port=9184,
            metrics_data=empty_metrics_data,
            timestamp=datetime.now()
        )
        
        assert result['tps'] is None
        assert result['cps'] is None
        assert result['reason'] == 'missing_counters'
    
    def test_reconcile_rpc_metrics_throughput_agree(self, calculator):
        """Test reconciliation when RPC and metrics agree (≤25% difference)"""
        rpc_result = {
            'tps': 10.0,
            'cps': 2.0,
            'calculation_window_seconds': 120.0,
            'source': 'rpc',
            'rpc_status': 'reachable'
        }
        
        metrics_result = {
            'tps': 12.0,  # 20% difference from RPC
            'cps': 2.4,   # 20% difference from RPC  
            'calculation_window_seconds': 120.0,
            'source': 'metrics'
        }
        
        result = calculator.reconcile_rpc_metrics_throughput(rpc_result, metrics_result)
        
        assert result['tps'] == 11.0   # Average of 10.0 and 12.0
        assert result['cps'] == 2.2    # Average of 2.0 and 2.4
        assert result['source'] == 'metrics+rpc'
        assert result['reason'] == 'agree'
    
    def test_reconcile_rpc_metrics_throughput_disagree_prefer_metrics(self, calculator):
        """Test reconciliation when they disagree and metrics should be preferred"""
        rpc_result = {
            'tps': 10.0,
            'cps': 2.0,
            'calculation_window_seconds': 30.0,  # < 60s, unreliable
            'source': 'rpc',
            'rpc_status': 'rate_limited'
        }
        
        metrics_result = {
            'tps': 20.0,  # 100% difference - should prefer metrics due to rate limiting
            'cps': 4.0,   # 100% difference
            'calculation_window_seconds': 120.0,  # ≥ 60s, reliable
            'source': 'metrics'
        }
        
        result = calculator.reconcile_rpc_metrics_throughput(rpc_result, metrics_result)
        
        assert result['tps'] == 20.0  # Keep metrics value
        assert result['cps'] == 4.0   # Keep metrics value
        assert result['source'] == 'metrics'
        assert result['reason'] == 'rpc_disagree'
    
    def test_reconcile_rpc_metrics_throughput_disagree_prefer_rpc(self, calculator):
        """Test reconciliation when they disagree and RPC should be preferred"""
        rpc_result = {
            'tps': 10.0,
            'cps': 2.0,
            'calculation_window_seconds': 300.0,  # ≥ 60s, reliable
            'source': 'rpc',
            'rpc_status': 'reachable'
        }
        
        metrics_result = {
            'tps': 20.0,  # 100% difference - should prefer RPC due to good RPC status
            'cps': 4.0,   # 100% difference  
            'calculation_window_seconds': 30.0,  # < 60s, unreliable
            'source': 'metrics'
        }
        
        result = calculator.reconcile_rpc_metrics_throughput(rpc_result, metrics_result)
        
        assert result['tps'] == 10.0  # Use RPC value
        assert result['cps'] == 2.0   # Use RPC value
        assert result['source'] == 'rpc'
        assert result['reason'] == 'metrics_disagree'


class TestMetricsThroughputIntegration:
    """Integration tests for metrics throughput in the full pipeline"""
    
    @pytest.fixture
    def sample_metrics_response(self):
        """Sample Prometheus metrics response"""
        return """
# HELP sui_executed_transactions_total Total executed transactions
# TYPE sui_executed_transactions_total counter
sui_executed_transactions_total{validator="node1"} 50000
sui_executed_transactions_total{validator="node2"} 30000

# HELP sui_checkpoint_sequence_number Current checkpoint number
# TYPE sui_checkpoint_sequence_number gauge  
sui_checkpoint_sequence_number 2500

# HELP uptime_seconds_total Process uptime
uptime_seconds_total 86400

# HELP other_metric Some other metric
other_metric 42
"""
    
    @patch('requests.get')
    def test_extract_metrics_counters_success(self, mock_get, sample_metrics_response):
        """Test successful extraction of metrics counters"""
        from pgdn_sui.advanced import SuiDataExtractor
        
        # Mock successful metrics response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = sample_metrics_response
        mock_get.return_value = mock_response
        
        # Create result with metrics exposed
        result = SuiDataResult(
            ip="127.0.0.1",
            port=9184,
            timestamp=datetime.now(),
            node_type="validator",
            network="testnet"
        )
        result.metrics_exposed = True
        
        # Create extractor and test
        extractor = SuiDataExtractor()
        
        import asyncio
        metrics_data = asyncio.run(extractor._extract_metrics_counters(result, "127.0.0.1"))
        
        assert metrics_data is not None
        assert metrics_data['tx_total'] == 80000  # 50000 + 30000
        assert metrics_data['cp_total'] == 2500
        assert metrics_data['counter_names']['tx'] == 'sui_executed_transactions_total'
        assert metrics_data['counter_names']['cp'] == 'sui_checkpoint_sequence_number'
    
    @patch('requests.get')
    def test_extract_metrics_counters_no_metrics_exposed(self, mock_get):
        """Test extraction when metrics are not exposed"""
        from pgdn_sui.advanced import SuiDataExtractor
        
        # Create result with metrics NOT exposed
        result = SuiDataResult(
            ip="127.0.0.1",
            port=9184,
            timestamp=datetime.now(),
            node_type="validator", 
            network="testnet"
        )
        result.metrics_exposed = False
        
        # Create extractor and test
        extractor = SuiDataExtractor()
        
        import asyncio
        metrics_data = asyncio.run(extractor._extract_metrics_counters(result, "127.0.0.1"))
        
        assert metrics_data is None
        # Should not make any HTTP requests
        mock_get.assert_not_called()
    
    @patch('requests.get')
    def test_extract_metrics_counters_no_counters_found(self, mock_get):
        """Test extraction when no TX/CP counters are found"""
        from pgdn_sui.advanced import SuiDataExtractor
        
        # Mock response with no relevant counters
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = """
# HELP cpu_usage CPU usage percentage
cpu_usage 75.5

# HELP memory_usage Memory usage
memory_usage 8192000000
"""
        mock_get.return_value = mock_response
        
        # Create result with metrics exposed
        result = SuiDataResult(
            ip="127.0.0.1",
            port=9184,
            timestamp=datetime.now(),
            node_type="validator",
            network="testnet"
        )
        result.metrics_exposed = True
        
        # Create extractor and test
        extractor = SuiDataExtractor()
        
        import asyncio
        metrics_data = asyncio.run(extractor._extract_metrics_counters(result, "127.0.0.1"))
        
        assert metrics_data is None


class TestStage7AcceptanceCriteria:
    """Test the specific acceptance criteria from stage7.md"""
    
    @pytest.fixture
    def calculator(self):
        return ThroughputCalculator(state_file_path=":memory:")
    
    @pytest.fixture
    def detector(self):
        return MetricsThroughputDetector()
    
    def test_acceptance_synthetic_prom_body_two_scrapes(self, calculator, detector):
        """
        Acceptance: Given a node with valid Prom tx/cp counters and two scrapes ≥60s apart,
        network_throughput.tps/cps computed and non-null.
        """
        # First scrape
        first_metrics = """
sui_executed_transactions_total 100000
sui_checkpoint_sequence_number 5000
"""
        
        tx_counter, cp_counter = detector.detect_counter_families(first_metrics)
        first_values = detector.parse_counter_values(first_metrics, tx_counter, cp_counter)
        first_values['counter_names'] = {'tx': tx_counter, 'cp': cp_counter}
        
        # Store first observation
        calculator.calculate_metrics_throughput(
            network="testnet",
            ip="192.168.1.100",
            port=9184,
            metrics_data=first_values,
            timestamp=datetime.now()
        )
        
        # Second scrape 5 minutes (300s) later
        second_metrics = """
sui_executed_transactions_total 101200
sui_checkpoint_sequence_number 5020
"""
        
        second_values = detector.parse_counter_values(second_metrics, tx_counter, cp_counter)
        second_values['counter_names'] = {'tx': tx_counter, 'cp': cp_counter}
        
        # Mock time to be 300s later
        with patch('time.time', return_value=first_values['timestamp'] + 300):
            result = calculator.calculate_metrics_throughput(
                network="testnet",
                ip="192.168.1.100", 
                port=9184,
                metrics_data=second_values,
                timestamp=datetime.now()
            )
        
        # Verify non-null TPS/CPS computed
        assert result['tps'] is not None
        assert result['cps'] is not None
        assert result['tps'] == 4.0   # (101200-100000)/300 = 4 TPS
        assert result['cps'] == 0.07  # (5020-5000)/300 = 0.0667 ≈ 0.07 CPS
        assert result['calculation_window_seconds'] == 300.0
        assert result['source'] == 'metrics'
    
    def test_acceptance_rpc_only_fallback(self, calculator):
        """
        Acceptance: Given a node with only RPC deltas, values remain computed as today.
        """
        # Test that RPC-only calculation still works (existing behavior preserved)
        rpc_result = calculator.calculate_throughput(
            network="testnet",
            ip="192.168.1.101",
            port=9000,
            total_transactions=50000,
            checkpoint_height=2500,
            timestamp=datetime.now(),
            hostname=None
        )
        
        # First call should store data
        assert rpc_result['reason'] == 'no_previous_data'
        
        # Second call with new data
        with patch('time.time', return_value=time.time() + 120):
            rpc_result2 = calculator.calculate_throughput(
                network="testnet",
                ip="192.168.1.101", 
                port=9000,
                total_transactions=50240,
                checkpoint_height=2504,
                timestamp=datetime.now(),
                hostname=None
            )
        
        # Should calculate successfully
        assert rpc_result2['tps'] == 2.0  # 240/120
        assert rpc_result2['cps'] == 0.03  # 4/120
    
    def test_acceptance_neither_source_missing_counters(self, calculator):
        """
        Acceptance: Given a node with neither metrics nor RPC, 
        values are null with reason="missing_counters".
        """
        empty_metrics_data = {
            'tx_total': None,
            'cp_total': None,
            'timestamp': time.time()
        }
        
        result = calculator.calculate_metrics_throughput(
            network="testnet",
            ip="192.168.1.102",
            port=9184,
            metrics_data=empty_metrics_data,
            timestamp=datetime.now()
        )
        
        assert result['tps'] is None
        assert result['cps'] is None
        assert result['reason'] == 'missing_counters'
    
    def test_acceptance_reconciliation_selects_correct_path(self, calculator):
        """
        Acceptance: Reconciliation selects the path per rules and sets source correctly.
        """
        # Test agreement case (≤25% difference)
        rpc_result = {
            'tps': 10.0,
            'cps': 2.0,
            'calculation_window_seconds': 120.0,
            'source': 'rpc',
            'rpc_status': 'reachable'
        }
        
        metrics_result = {
            'tps': 11.0,  # 10% difference
            'cps': 2.2,   # 10% difference
            'calculation_window_seconds': 120.0,
            'source': 'metrics'
        }
        
        result = calculator.reconcile_rpc_metrics_throughput(rpc_result, metrics_result)
        
        assert result['source'] == 'metrics+rpc'
        assert result['reason'] == 'agree'
        assert result['tps'] == 10.5   # Average
        assert result['cps'] == 2.1    # Average
    
    def test_acceptance_counter_reset_interval_dropped(self, calculator, detector):
        """
        Acceptance: Counter reset: second scrape lower → interval dropped;
        if third scrape exists and monotonic, use it.
        """
        # First scrape
        first_metrics = """
sui_executed_transactions_total 100000
sui_checkpoint_sequence_number 5000
"""
        
        tx_counter, cp_counter = detector.detect_counter_families(first_metrics)
        first_values = detector.parse_counter_values(first_metrics, tx_counter, cp_counter)
        first_values['counter_names'] = {'tx': tx_counter, 'cp': cp_counter}
        
        # Store first observation
        calculator.calculate_metrics_throughput(
            network="testnet",
            ip="192.168.1.103",
            port=9184,
            metrics_data=first_values,
            timestamp=datetime.now()
        )
        
        # Second scrape with reset (lower values)
        reset_metrics = """
sui_executed_transactions_total 50000
sui_checkpoint_sequence_number 2500
"""
        
        reset_values = detector.parse_counter_values(reset_metrics, tx_counter, cp_counter)
        reset_values['counter_names'] = {'tx': tx_counter, 'cp': cp_counter}
        
        with patch('time.time', return_value=first_values['timestamp'] + 120):
            reset_result = calculator.calculate_metrics_throughput(
                network="testnet",
                ip="192.168.1.103",
                port=9184,
                metrics_data=reset_values,
                timestamp=datetime.now()
            )
        
        # Should detect reset and drop interval
        assert reset_result['reason'] == 'reset_detected'
        assert reset_result['tps'] is None
        assert reset_result['cps'] is None
        
        # Third scrape - should work with the reset values as baseline
        third_metrics = """
sui_executed_transactions_total 50600
sui_checkpoint_sequence_number 2520
"""
        
        third_values = detector.parse_counter_values(third_metrics, tx_counter, cp_counter)
        third_values['counter_names'] = {'tx': tx_counter, 'cp': cp_counter}
        
        with patch('time.time', return_value=reset_values['timestamp'] + 120):
            third_result = calculator.calculate_metrics_throughput(
                network="testnet", 
                ip="192.168.1.103",
                port=9184,
                metrics_data=third_values,
                timestamp=datetime.now()
            )
        
        # Should calculate successfully from reset baseline
        assert third_result['tps'] == 5.0   # (50600-50000)/120
        assert third_result['cps'] == 0.17  # (2520-2500)/120
        assert third_result['reason'] is None
    
    def test_acceptance_no_family_missing_counters(self, detector):
        """
        Acceptance: No family: Prom 200 but no matching names → nulls + missing_counters.
        """
        metrics_without_sui_counters = """
# HELP cpu_usage_percent CPU usage
cpu_usage_percent 75.5

# HELP memory_usage_bytes Memory usage
memory_usage_bytes 8192000000

# HELP disk_io_total Disk I/O operations
disk_io_total 12345
"""
        
        tx_counter, cp_counter = detector.detect_counter_families(metrics_without_sui_counters)
        
        assert tx_counter is None
        assert cp_counter is None
        
        # Parsing should return None values
        result = detector.parse_counter_values(metrics_without_sui_counters, tx_counter, cp_counter)
        
        assert result['tx_total'] is None
        assert result['cp_total'] is None