"""
PGDN-SUI: Enhanced Sui network scanner with discovery and deep analysis modes
"""

from .scanner import EnhancedSuiScanner, SuiNodeResult, setup_logging
from .advanced import SuiDataExtractor
from .models import SuiDataResult
from .protobuf_manager import SuiProtobufManager
from .utils import calculate_gini_coefficient, get_primary_port
from .exceptions import *


__version__ = "2.5.0"
__author__ = "PGDN Team"

__all__ = [
    "EnhancedSuiScanner",
    "SuiNodeResult",
    "SuiDataExtractor",
    "SuiDataResult",
    "SuiProtobufManager",
    "calculate_gini_coefficient",
    "get_primary_port",
    "setup_logging",
    "PgdnSuiException",
    "ScannerError",
    "NetworkError",
    "ValidationError"
]
