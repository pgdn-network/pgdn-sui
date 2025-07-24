#!/usr/bin/env python3
"""
Test suite for Sui Data Extraction Add-on
"""

import json
import pytest
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from pgdn_sui.advanced import SuiDataExtractor, SuiDataResult


class TestSuiDataResult:
    """Test SuiDataResult dataclass"""
    
    def test_initialization(self):
        """Test basic initialization"""
        result = SuiDataResult(
            ip="test.com",
            port=9100,
            timestamp="2023-07-01T00:00:00",
            node_type="validator",
            network="mainnet"
        )
        
        assert result.ip == "test.com"
        assert result.port == 9100
        assert result.node_type == "validator"
        assert result.network == "mainnet"
        assert result.rpc_methods_available == []
        assert result.metrics_data == {}
        assert result.system_state == {}
        assert result.extraction_errors == []


class TestSuiDataExtractor:
    """Test SuiDataExtractor class"""
    
    @pytest.fixture
    def sample_discovery_data(self):
        """Sample discovery data for testing"""
        return [
            {
                "ip": "sui.test.com",
                "type": "validator", 
                "network": "mainnet",
                "capabilities": ["json_rpc", "prometheus_metrics"],
                "accessible_ports": [9100, 9000],
                "working_endpoints": ["https://sui.test.com/"]
            },
            {
                "ip": "fullnode.test.com",
                "type": "public_rpc",
                "network": "mainnet", 
                "capabilities": ["json_rpc"],
                "accessible_ports": [443, 9000],
                "working_endpoints": ["https://fullnode.test.com/"]
            }
        ]
    
    @pytest.fixture
    def sample_discovery_json_file(self, sample_discovery_data, tmp_path):
        """Create temporary JSON file with discovery data"""
        json_file = tmp_path / "test_discovery.json"
        
        # Test different JSON formats
        data_formats = [
            # Direct array format
            sample_discovery_data,
            # Legacy nodes format
            {"nodes": sample_discovery_data},
            # New data/meta format
            {
                "data": [
                    {"type": "sui_node", "payload": node} 
                    for node in sample_discovery_data
                ],
                "meta": {"scan_time": "2023-07-01T00:00:00"}
            }
        ]
        
        # Use the new data/meta format for testing
        with open(json_file, 'w') as f:
            json.dump(data_formats[2], f)
            
        return str(json_file)
    
    def test_from_discovery_json(self, sample_discovery_json_file):
        """Test initializing from discovery JSON file"""
        extractor = SuiDataExtractor.from_discovery_json(sample_discovery_json_file)
        
        assert hasattr(extractor, 'discovery_data')
        assert len(extractor.discovery_data) == 2
        assert extractor.discovery_data[0]['ip'] == 'sui.test.com'
        assert extractor.discovery_data[1]['ip'] == 'fullnode.test.com'
    
    def test_load_discovery_data_file_not_found(self):
        """Test loading discovery data with non-existent file"""
        extractor = SuiDataExtractor()
        
        with pytest.raises(FileNotFoundError):
            extractor._load_discovery_data("nonexistent.json")
    
    def test_has_sui_data_potential(self):
        """Test node filtering for Sui data potential"""
        extractor = SuiDataExtractor()
        
        # Should detect validator nodes
        validator_node = {"type": "validator", "capabilities": [], "accessible_ports": []}
        assert extractor._has_sui_data_potential(validator_node)
        
        # Should detect nodes with json_rpc capability
        rpc_node = {"type": "unknown", "capabilities": ["json_rpc"], "accessible_ports": []}
        assert extractor._has_sui_data_potential(rpc_node)
        
        # Should detect nodes with Sui ports
        port_node = {"type": "unknown", "capabilities": [], "accessible_ports": [9100]}
        assert extractor._has_sui_data_potential(port_node)
        
        # Should reject nodes with no Sui indicators
        non_sui_node = {"type": "unknown", "capabilities": ["http"], "accessible_ports": [80]}
        assert not extractor._has_sui_data_potential(non_sui_node)
    
    def test_get_primary_data_port(self):
        """Test primary data port selection"""
        extractor = SuiDataExtractor()
        
        # Should prioritize metrics port
        ports = [443, 9100, 9000]
        capabilities = ["prometheus_metrics", "json_rpc"]
        assert extractor._get_primary_data_port(ports, capabilities) == 9100
        
        # Should fall back to RPC port
        ports = [443, 9000]
        capabilities = ["json_rpc"]
        assert extractor._get_primary_data_port(ports, capabilities) == 9000
        
        # Should fall back to HTTPS
        ports = [443, 80]
        capabilities = []
        assert extractor._get_primary_data_port(ports, capabilities) == 443
    
    @patch('requests.post')
    def test_extract_rpc_data_success(self, mock_post):
        """Test successful RPC data extraction"""
        extractor = SuiDataExtractor()
        result = SuiDataResult(
            ip="test.com",
            port=9000,
            timestamp="2023-07-01T00:00:00",
            node_type="validator",
            network="mainnet"
        )
        
        # Mock successful RPC response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "result": {
                "epoch": "123",
                "protocolVersion": "1.0"
            }
        }
        mock_post.return_value = mock_response
        
        success = extractor._extract_rpc_data(result, "https://test.com/")
        
        assert success
        assert len(result.rpc_methods_available) > 0
        assert mock_post.called
    
    @patch('requests.post')
    def test_extract_rpc_data_auth_required(self, mock_post):
        """Test RPC data extraction with auth required"""
        extractor = SuiDataExtractor()
        result = SuiDataResult(
            ip="test.com", 
            port=9000,
            timestamp="2023-07-01T00:00:00",
            node_type="validator",
            network="mainnet"
        )
        
        # Mock auth required response
        mock_response = Mock()
        mock_response.status_code = 401
        mock_post.return_value = mock_response
        
        success = extractor._extract_rpc_data(result, "https://test.com/")
        
        assert not success
        assert result.rpc_auth_required is True
    
    @patch('requests.get')
    def test_extract_metrics_data_success(self, mock_get):
        """Test successful metrics data extraction"""
        extractor = SuiDataExtractor()
        result = SuiDataResult(
            ip="test.com",
            port=9100,
            timestamp="2023-07-01T00:00:00", 
            node_type="validator",
            network="mainnet"
        )
        
        # Mock metrics response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '''
        # HELP build_info Build information
        build_info{version="1.0.0",git_commit="abc123"} 1
        sui_epoch 456
        consensus_round 789
        '''
        mock_get.return_value = mock_response
        
        success = extractor._extract_metrics_data(result, "test.com")
        
        assert success
        assert result.metrics_publicly_accessible is True
        assert "build_info_version" in result.metrics_data
        assert result.sui_version == "1.0.0"
    
    def test_parse_sui_metrics(self):
        """Test parsing Sui metrics from Prometheus format"""
        extractor = SuiDataExtractor()
        
        metrics_text = '''
        # HELP build_info Build information
        build_info{version="1.0.0",git_commit="abc123"} 1
        sui_epoch 456
        consensus_round 789
        sui_network_peers 25
        uptime_seconds_total 3600
        '''
        
        parsed = extractor._parse_sui_metrics(metrics_text)
        
        assert parsed["build_info_version"] == "1.0.0"
        assert parsed["build_info_git_commit"] == "abc123"
        assert parsed["sui_epoch"] == 456
        assert parsed["consensus_round"] == 789
        assert parsed["sui_network_peers"] == 25
        assert parsed["uptime_seconds"] == 3600
    
    def test_calculate_gini_coefficient(self):
        """Test Gini coefficient calculation"""
        extractor = SuiDataExtractor()
        
        # Perfect equality should give 0
        equal_stakes = [100.0, 100.0, 100.0, 100.0]
        gini = extractor._calculate_gini_coefficient(equal_stakes)
        assert abs(gini - 0.0) < 0.01
        
        # Perfect inequality should give high value
        unequal_stakes = [1000.0, 1.0, 1.0, 1.0]
        gini = extractor._calculate_gini_coefficient(unequal_stakes)
        assert gini > 0.5
        
        # Empty/single stakes should give 0
        assert extractor._calculate_gini_coefficient([]) == 0.0
        assert extractor._calculate_gini_coefficient([100.0]) == 0.0
    
    def test_export_data_structured_format(self):
        """Test exporting data in structured format"""
        extractor = SuiDataExtractor()
        
        results = [
            SuiDataResult(
                ip="test1.com",
                port=9100,
                timestamp="2023-07-01T00:00:00",
                node_type="validator",
                network="mainnet",
                sui_version="1.0.0",
                data_completeness=0.8
            ),
            SuiDataResult(
                ip="test2.com", 
                port=9100,
                timestamp="2023-07-01T00:00:00",
                node_type="public_rpc",
                network="mainnet",
                data_completeness=0.6
            )
        ]
        
        output = extractor.export_data(results, "structured")
        data = json.loads(output)
        
        assert "extraction_metadata" in data
        assert "node_data" in data
        assert data["extraction_metadata"]["total_nodes"] == 2
        assert data["extraction_metadata"]["successful_extractions"] == 2
        assert len(data["node_data"]) == 2
        assert data["node_data"][0]["ip"] == "test1.com"
    
    def test_export_data_flat_format(self):
        """Test exporting data in flat format"""
        extractor = SuiDataExtractor()
        
        results = [
            SuiDataResult(
                ip="test.com",
                port=9100, 
                timestamp="2023-07-01T00:00:00",
                node_type="validator",
                network="mainnet",
                sui_version="1.0.0"
            )
        ]
        
        output = extractor.export_data(results, "flat")
        data = json.loads(output)
        
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["node_id"] == "test.com:9100"
        assert data[0]["sui_version"] == "1.0.0"
    
    def test_export_data_metrics_only_format(self):
        """Test exporting data in metrics_only format"""
        extractor = SuiDataExtractor()
        
        results = [
            SuiDataResult(
                ip="test.com",
                port=9100,
                timestamp="2023-07-01T00:00:00",
                node_type="validator", 
                network="mainnet"
            )
        ]
        results[0].metrics_data = {"sui_epoch": 123, "consensus_round": 456}
        
        output = extractor.export_data(results, "metrics_only")
        data = json.loads(output)
        
        assert "raw_metrics" in data
        assert len(data["raw_metrics"]) == 1
        assert data["raw_metrics"][0]["metrics"]["sui_epoch"] == 123


class TestSuiDataExtractorIntegration:
    """Integration tests for complete extraction workflows"""
    
    @pytest.fixture
    def full_discovery_json_file(self, tmp_path):
        """Create realistic discovery JSON file"""
        discovery_data = {
            "nodes": [
                {
                    "ip": "fullnode.mainnet.sui.io",
                    "hostname": "fullnode.mainnet.sui.io", 
                    "discovered_at": "2025-07-14T22:10:31.308320",
                    "type": "hybrid",
                    "confidence": 0.9,
                    "capabilities": ["json_rpc", "public_api", "grpc", "consensus"],
                    "network": "mainnet",
                    "discovery_time": 5.19,
                    "total_scan_time": 5.19,
                    "accessible_ports": [22, 80, 443, 8080, 9000, 9100, 50051],
                    "working_endpoints": [
                        "https://fullnode.mainnet.sui.io/",
                        "grpc://fullnode.mainnet.sui.io:50051"
                    ],
                    "analysis_level": "discovery"
                }
            ]
        }
        
        json_file = tmp_path / "realistic_discovery.json"
        with open(json_file, 'w') as f:
            json.dump(discovery_data, f)
        
        return str(json_file)
    
    @patch('requests.post')
    @patch('requests.get')
    def test_full_extraction_workflow(self, mock_get, mock_post, full_discovery_json_file):
        """Test complete extraction workflow with mocked responses"""
        
        # Mock metrics response
        mock_metrics_response = Mock()
        mock_metrics_response.status_code = 200
        mock_metrics_response.text = '''
        build_info{version="1.0.0"} 1
        sui_epoch 123
        consensus_round 456
        '''
        mock_get.return_value = mock_metrics_response
        
        # Mock RPC response
        mock_rpc_response = Mock()
        mock_rpc_response.status_code = 200
        mock_rpc_response.json.return_value = {
            "result": {
                "epoch": "123",
                "protocolVersion": "1.0"
            }
        }
        mock_post.return_value = mock_rpc_response
        
        # Run extraction
        extractor = SuiDataExtractor.from_discovery_json(full_discovery_json_file)
        results = extractor.extract_all_nodes()
        
        assert len(results) == 1
        result = results[0]
        
        assert result.ip == "fullnode.mainnet.sui.io"
        assert result.node_type == "hybrid"
        assert result.network == "mainnet"
        assert result.data_completeness > 0
        
        # Verify structured export
        structured_output = extractor.export_data(results, "structured")
        structured_data = json.loads(structured_output)
        
        assert structured_data["extraction_metadata"]["total_nodes"] == 1
        assert len(structured_data["node_data"]) == 1