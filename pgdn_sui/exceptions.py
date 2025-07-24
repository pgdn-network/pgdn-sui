"""
Custom exceptions for PGDN-SUI library
"""


class PgdnSuiException(Exception):
    """Base exception for pgdn-sui library"""
    pass


class ScannerError(PgdnSuiException):
    """Error during scanning operation"""
    
    def __init__(self, hostname: str, phase: str, message: str):
        self.hostname = hostname
        self.phase = phase
        super().__init__(f"Scanner error for {hostname} (Phase {phase}): {message}")


class NetworkError(PgdnSuiException):
    """Network-related error"""
    
    def __init__(self, hostname: str, operation: str, message: str):
        self.hostname = hostname
        self.operation = operation
        super().__init__(f"Network error for {hostname} during {operation}: {message}")


class ValidationError(PgdnSuiException):
    """Input validation error"""
    pass


# Legacy exception classes for backward compatibility
class StepExecutionError(PgdnSuiException):
    """Legacy exception for step execution errors"""
    pass


class DependencyValidationError(PgdnSuiException):
    """Legacy exception for dependency validation errors"""
    pass


class InvalidStepError(PgdnSuiException):
    """Legacy exception for invalid step errors"""
    pass