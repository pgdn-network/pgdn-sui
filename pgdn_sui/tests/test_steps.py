"""
Tests for step implementations
"""

import pytest
from pgdn_sui.steps.base import BaseStep
from pgdn_sui.steps.phase1.port_scan import PortScanStep


class TestBaseStep:
    def test_base_step_is_abstract(self):
        with pytest.raises(TypeError):
            BaseStep("test.example.com")


class TestPortScanStep:
    def test_port_scan_step_creation(self):
        step = PortScanStep("test.example.com")
        assert step.hostname == "test.example.com"
        assert step.step_number == 1
        assert step.step_name == "port_scan"
        assert step.dependencies == []

    def test_port_scan_dependencies(self):
        step = PortScanStep("test.example.com")
        assert step.validate_dependencies([]) == True
