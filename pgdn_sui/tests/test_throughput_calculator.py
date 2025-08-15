"""
Tests for ThroughputCalculator implementation
Tests TPS/CPS calculation, state management, and integration
"""

import pytest
import json
import time
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch, Mock

from pgdn_sui.throughput_calculator import ThroughputCalculator


class TestThroughputCalculatorBasics:
    """Test basic functionality of ThroughputCalculator"""
    
    @pytest.fixture
    def temp_state_file(self):
        """Create temporary state file for testing"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            temp_path = f.name
        yield temp_path
        # Cleanup
        Path(temp_path).unlink(missing_ok=True)
    
    @pytest.fixture
    def calculator(self, temp_state_file):
        """Create ThroughputCalculator with temp state file"""
        return ThroughputCalculator(state_file_path=temp_state_file, ttl_hours=1)
    
    def test_calculator_initialization(self, calculator, temp_state_file):
        """Test calculator initializes correctly"""
        assert calculator.state_file == Path(temp_state_file)
        assert calculator.ttl_seconds == 3600  # 1 hour
        assert calculator.state == {}
        
    def test_create_key(self, calculator):
        """Test key creation for state storage"""
        key = calculator._create_key("mainnet", "192.168.1.100", 9184)
        assert key == "mainnet|192.168.1.100|9184"
    
    def test_timestamp_conversion(self, calculator):
        """Test timestamp conversion to epoch seconds"""
        
        # Test datetime object
        dt = datetime(2023, 10, 15, 12, 30, 45, tzinfo=timezone.utc)
        epoch = calculator._convert_timestamp_to_epoch(dt)
        assert isinstance(epoch, float)
        assert epoch == dt.timestamp()
        
        # Test RFC3339 string
        rfc3339 = "2023-10-15T12:30:45Z"
        epoch2 = calculator._convert_timestamp_to_epoch(rfc3339)
        assert isinstance(epoch2, float)
        
        # Test epoch seconds as int/float
        epoch_int = 1697372245
        result = calculator._convert_timestamp_to_epoch(epoch_int)
        assert result == float(epoch_int)
        
        epoch_float = 1697372245.123
        result2 = calculator._convert_timestamp_to_epoch(epoch_float)
        assert result2 == epoch_float
        
        # Test unknown type (should return current time)
        with patch('time.time', return_value=1697372245.0):
            result3 = calculator._convert_timestamp_to_epoch(None)
            assert result3 == 1697372245.0


class TestThroughputCalculation:
    """Test throughput calculation logic"""
    
    @pytest.fixture
    def calculator(self):
        """Create calculator with temp state file"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            temp_path = f.name
        calc = ThroughputCalculator(state_file_path=temp_path, ttl_hours=24)
        yield calc
        Path(temp_path).unlink(missing_ok=True)
    
    def test_first_observation_returns_nulls(self, calculator):
        """Test that first observation stores state but returns nulls"""
        result = calculator.calculate_throughput(
            network="mainnet",
            ip="192.168.1.100",
            port=9184,
            total_transactions=1000000,
            checkpoint_height=500000,
            timestamp=1697372245.0
        )
        
        # Should return nulls for first observation
        assert result["tps"] is None
        assert result["cps"] is None
        assert result["calculation_window_seconds"] is None
        
        # Should store state
        key = "mainnet|192.168.1.100|9184"
        assert key in calculator.state
        assert calculator.state[key]["total_transactions"] == 1000000
        assert calculator.state[key]["checkpoint_height"] == 500000
        assert calculator.state[key]["ts_s"] == 1697372245.0
    
    def test_successful_calculation(self, calculator):
        """Test successful TPS/CPS calculation"""
        # First observation
        calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1000000, checkpoint_height=500000, timestamp=1697372245.0
        )
        
        # Second observation after 2 hours (7200 seconds)
        result = calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1408948, checkpoint_height=529420, timestamp=1697379445.0  # +7200s
        )
        
        # Expected: Δtx = 408948, Δcp = 29420, Δt = 7200
        # TPS = 408948/7200 = 56.80 (rounded to 56.8)
        # CPS = 29420/7200 = 4.08
        assert result["tps"] == 56.8  # 408948/7200 rounded to 2 decimals
        assert result["cps"] == 4.08   # 29420/7200 rounded to 2 decimals  
        assert result["calculation_window_seconds"] == 7200.0
    
    def test_time_delta_too_small(self, calculator):
        """Test guardrail: Δt < 1.0 second"""
        # First observation
        calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1000000, checkpoint_height=500000, timestamp=1697372245.0
        )
        
        # Second observation only 0.5 seconds later
        result = calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184, 
            total_transactions=1000100, checkpoint_height=500001, timestamp=1697372245.5
        )
        
        # Should return nulls due to small time delta
        assert result["tps"] is None
        assert result["cps"] is None
        assert result["calculation_window_seconds"] == 0.5
    
    def test_transaction_counter_reset(self, calculator):
        """Test guardrail: negative Δtx (counter reset)"""
        # First observation
        calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1000000, checkpoint_height=500000, timestamp=1697372245.0
        )
        
        # Second observation with lower transaction count (reset)
        result = calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=999000, checkpoint_height=500100, timestamp=1697372305.0  # +60s
        )
        
        # TPS should be null due to counter reset, but CPS should be calculated
        assert result["tps"] is None
        assert result["cps"] == 1.67  # 100/60 rounded to 2 decimals
        assert result["calculation_window_seconds"] == 60.0
    
    def test_checkpoint_counter_reset(self, calculator):
        """Test guardrail: negative Δcp (counter reset)"""
        # First observation
        calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1000000, checkpoint_height=500000, timestamp=1697372245.0
        )
        
        # Second observation with lower checkpoint (reset)
        result = calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1003600, checkpoint_height=499500, timestamp=1697372305.0  # +60s
        )
        
        # CPS should be null due to counter reset, but TPS should be calculated
        assert result["tps"] == 60.0  # 3600/60
        assert result["cps"] is None
        assert result["calculation_window_seconds"] == 60.0
    
    def test_missing_data_handling(self, calculator):
        """Test handling of None/missing data"""
        # Test with None values
        result = calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=None, checkpoint_height=None, timestamp=1697372245.0
        )
        
        assert result["tps"] is None
        assert result["cps"] is None
        assert result["calculation_window_seconds"] is None
        
        # Should not store state with None values
        key = "mainnet|validator|192.168.1.100|9184"
        assert key not in calculator.state


class TestStateManagement:
    """Test state persistence and management"""
    
    @pytest.fixture
    def temp_state_file(self):
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            temp_path = f.name
        yield temp_path
        Path(temp_path).unlink(missing_ok=True)
    
    def test_state_persistence(self, temp_state_file):
        """Test that state persists across calculator instances"""
        # First calculator instance
        calc1 = ThroughputCalculator(state_file_path=temp_state_file)
        calc1.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1000000, checkpoint_height=500000, timestamp=1697372245.0
        )
        
        # Second calculator instance should load existing state
        calc2 = ThroughputCalculator(state_file_path=temp_state_file)
        result = calc2.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1003600, checkpoint_height=500100, timestamp=1697372305.0  # +60s
        )
        
        # Should calculate based on previous state
        assert result["tps"] == 60.0  # 3600/60
        assert result["cps"] == 1.67   # 100/60 rounded
    
    def test_state_file_creation(self):
        """Test state file is created when it doesn't exist"""
        with tempfile.TemporaryDirectory() as temp_dir:
            state_file = Path(temp_dir) / "new_state.json"
            assert not state_file.exists()
            
            calc = ThroughputCalculator(state_file_path=str(state_file))
            calc.calculate_throughput(
                network="mainnet", ip="192.168.1.100", port=9184,
                total_transactions=1000000, checkpoint_height=500000, timestamp=1697372245.0
            )
            
            assert state_file.exists()
            with open(state_file) as f:
                data = json.load(f)
                assert len(data) == 1
    
    def test_ttl_cleanup(self, temp_state_file):
        """Test TTL-based cleanup of expired entries"""
        calc = ThroughputCalculator(state_file_path=temp_state_file, ttl_hours=1)
        
        # Add entry with old timestamp (2 hours ago)
        old_time = time.time() - 7200  # 2 hours ago
        calc.state["test_key"] = {
            "total_transactions": 1000000,
            "checkpoint_height": 500000,
            "ts_s": old_time
        }
        calc._save_state()
        
        # Create new instance (should trigger cleanup)
        calc2 = ThroughputCalculator(state_file_path=temp_state_file, ttl_hours=1)
        
        # Old entry should be removed
        assert "test_key" not in calc2.state
    
    def test_manual_cleanup(self, temp_state_file):
        """Test manual cleanup of expired entries"""
        calc = ThroughputCalculator(state_file_path=temp_state_file, ttl_hours=1)
        
        # Add expired and fresh entries
        old_time = time.time() - 7200  # 2 hours ago  
        fresh_time = time.time() - 1800  # 30 minutes ago
        
        calc.state["expired_key"] = {"total_transactions": 1000, "checkpoint_height": 500, "ts_s": old_time}
        calc.state["fresh_key"] = {"total_transactions": 2000, "checkpoint_height": 600, "ts_s": fresh_time}
        
        # Manual cleanup
        removed_count = calc.cleanup_expired_entries()
        
        assert removed_count == 1
        assert "expired_key" not in calc.state
        assert "fresh_key" in calc.state
    
    def test_get_state_info(self, temp_state_file):
        """Test state info retrieval for debugging"""
        calc = ThroughputCalculator(state_file_path=temp_state_file)
        calc.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1000000, checkpoint_height=500000, timestamp=time.time()
        )
        
        info = calc.get_state_info()
        assert info["total_entries"] == 1
        assert info["ttl_hours"] == 24
        assert str(temp_state_file) in info["state_file"]
        assert len(info["entries"]) == 1
    
    def test_clear_state(self, temp_state_file):
        """Test clearing all state"""
        calc = ThroughputCalculator(state_file_path=temp_state_file)
        calc.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1000000, checkpoint_height=500000, timestamp=time.time()
        )
        
        assert len(calc.state) == 1
        
        calc.clear_state()
        assert len(calc.state) == 0
        
        # State file should be updated
        calc2 = ThroughputCalculator(state_file_path=temp_state_file)
        assert len(calc2.state) == 0


class TestIntegrationScenarios:
    """Test realistic integration scenarios"""
    
    @pytest.fixture
    def calculator(self):
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            temp_path = f.name
        calc = ThroughputCalculator(state_file_path=temp_path, ttl_hours=24)
        yield calc
        Path(temp_path).unlink(missing_ok=True)
    
    def test_example_a_from_tps_md(self, calculator):
        """Test Example A from tps.md (2-hour window)"""
        # prev: total_tx=3955902979, checkpoint=178064252, ts=10:34:13Z
        # now : total_tx=3956311927, checkpoint=178093672, ts=12:34:13Z
        # Δt = 7200s, Δtx = 408948, Δcp = 29420
        # tps = 408948/7200 = 56.80, cps = 29420/7200 = 4.08
        
        prev_time = 1697372053  # 10:34:13
        now_time = prev_time + 7200  # 12:34:13 (+2 hours)
        
        # First observation  
        calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=3955902979, checkpoint_height=178064252, timestamp=prev_time
        )
        
        # Second observation
        result = calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=3956311927, checkpoint_height=178093672, timestamp=now_time
        )
        
        assert result["tps"] == 56.8  # 408948/7200 = 56.799... -> 56.8
        assert result["cps"] == 4.08   # 29420/7200 = 4.086... -> 4.09
        assert result["calculation_window_seconds"] == 7200.0
    
    def test_example_b_noisy_short_window(self, calculator):
        """Test Example B from tps.md (noisy short window)"""
        # First observation
        calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1000000, checkpoint_height=500000, timestamp=1697372245.0
        )
        
        # Second observation with Δt = 0.6s (should trigger guardrail)
        result = calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1000100, checkpoint_height=500001, timestamp=1697372245.6
        )
        
        assert result["tps"] is None
        assert result["cps"] is None
        assert result["calculation_window_seconds"] == 0.6
    
    def test_example_c_counter_reset(self, calculator):
        """Test Example C from tps.md (counter reset)"""
        # First observation
        calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1000000, checkpoint_height=500000, timestamp=1697372245.0
        )
        
        # Δtx < 0 but Δcp > 0 -> tps=null, cps valid
        result = calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=999000, checkpoint_height=500100, timestamp=1697372305.0  # +60s
        )
        
        assert result["tps"] is None  # Counter reset
        assert result["cps"] == 1.67   # 100/60 = 1.666... -> 1.67
        assert result["calculation_window_seconds"] == 60.0
    
    def test_multiple_nodes_independent_state(self, calculator):
        """Test that multiple nodes maintain independent state"""
        base_time = time.time()
        
        # Node 1 first observation
        result1a = calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1000000, checkpoint_height=500000, timestamp=base_time
        )
        
        # Node 2 first observation  
        result2a = calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.101", port=9184,
            total_transactions=2000000, checkpoint_height=600000, timestamp=base_time
        )
        
        # Both should be null (first observations)
        assert result1a["tps"] is None
        assert result2a["tps"] is None
        
        # Node 1 second observation
        result1b = calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1003600, checkpoint_height=500100, timestamp=base_time + 60
        )
        
        # Should calculate for Node 1
        assert result1b["tps"] == 60.0  # 3600/60
        assert result1b["cps"] == 1.67   # 100/60
        
        # Node 2 second observation (different timing and values)
        result2b = calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.101", port=9184,
            total_transactions=2007200, checkpoint_height=600200, timestamp=base_time + 120
        )
        
        # Should calculate independently for Node 2
        assert result2b["tps"] == 60.0  # 7200/120
        assert result2b["cps"] == 1.67   # 200/120
    
    def test_network_type_isolation(self, calculator):
        """Test that different networks maintain separate state"""
        base_time = time.time()
        
        # Same IP/port but different networks
        # Mainnet observation
        calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1000000, checkpoint_height=500000, timestamp=base_time
        )
        
        # Testnet observation  
        calculator.calculate_throughput(
            network="testnet", ip="192.168.1.100", port=9184,
            total_transactions=2000000, checkpoint_height=600000, timestamp=base_time
        )
        
        # Second mainnet observation
        result_mainnet = calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1003600, checkpoint_height=500100, timestamp=base_time + 60
        )
        
        # Second testnet observation  
        result_testnet = calculator.calculate_throughput(
            network="testnet", ip="192.168.1.100", port=9184,
            total_transactions=2001800, checkpoint_height=600050, timestamp=base_time + 60
        )
        
        # Both should calculate independently
        assert result_mainnet["tps"] == 60.0
        assert result_testnet["tps"] == 30.0  # 1800/60
        assert result_mainnet["cps"] == 1.67
        assert result_testnet["cps"] == 0.83  # 50/60


class TestQualityChecks:
    """Test quality check logging (sanity checks)"""
    
    @pytest.fixture
    def calculator(self):
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            temp_path = f.name
        calc = ThroughputCalculator(state_file_path=temp_path, ttl_hours=24)
        yield calc
        Path(temp_path).unlink(missing_ok=True)
    
    @patch('pgdn_sui.throughput_calculator.logger')
    def test_unusual_tps_logging(self, mock_logger, calculator):
        """Test logging for unusual TPS values"""
        # First observation
        calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1000000, checkpoint_height=500000, timestamp=1697372245.0
        )
        
        # Second observation with very high TPS (>10,000)
        calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1720000, checkpoint_height=500100, timestamp=1697372305.0  # +60s
        )
        
        # Should log debug message for unusual TPS
        mock_logger.debug.assert_called()
        debug_calls = [call[0][0] for call in mock_logger.debug.call_args_list]
        tps_warnings = [call for call in debug_calls if "TPS sanity check" in call]
        assert len(tps_warnings) > 0
    
    @patch('pgdn_sui.throughput_calculator.logger')
    def test_unusual_cps_logging(self, mock_logger, calculator):
        """Test logging for unusual CPS values"""
        # First observation
        calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1000000, checkpoint_height=500000, timestamp=1697372245.0
        )
        
        # Second observation with very high CPS (>10)
        calculator.calculate_throughput(
            network="mainnet", ip="192.168.1.100", port=9184,
            total_transactions=1003600, checkpoint_height=500800, timestamp=1697372305.0  # +60s
        )
        
        # Should log debug message for unusual CPS (800/60 = 13.33)
        mock_logger.debug.assert_called()
        debug_calls = [call[0][0] for call in mock_logger.debug.call_args_list]
        cps_warnings = [call for call in debug_calls if "CPS sanity check" in call]
        assert len(cps_warnings) > 0