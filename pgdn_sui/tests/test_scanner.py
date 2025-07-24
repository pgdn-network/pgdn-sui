"""
Tests for ValidatorScanner class
"""

import pytest
from pgdn_sui import ValidatorScanner
from pgdn_sui.exceptions import InvalidStepError


class TestValidatorScanner:
    def test_scanner_initialization(self):
        scanner = ValidatorScanner()
        assert scanner.timeout == 3

        scanner = ValidatorScanner(timeout=10)
        assert scanner.timeout == 10

    def test_get_plan_phase1(self):
        scanner = ValidatorScanner()
        plan = scanner.get_plan("test.example.com", phase=1)

        assert plan['phase'] == 1
        assert plan['hostname'] == "test.example.com"
        assert len(plan['steps']) == 6
        assert plan['steps'][0]['name'] == 'port_scan'

    def test_get_plan_phase2(self):
        scanner = ValidatorScanner()
        plan = scanner.get_plan("test.example.com", phase=2)

        assert plan['phase'] == 2
        assert plan['hostname'] == "test.example.com"
        assert len(plan['steps']) == 4

    def test_invalid_phase(self):
        scanner = ValidatorScanner()
        with pytest.raises(InvalidStepError):
            scanner.get_plan("test.example.com", phase=3)

    def test_invalid_step(self):
        scanner = ValidatorScanner()
        with pytest.raises(InvalidStepError):
            scanner.execute_step("test.example.com", 1, 10)
