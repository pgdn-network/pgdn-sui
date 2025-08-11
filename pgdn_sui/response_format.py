#!/usr/bin/env python3
"""
Standard Response Format for PGDN-SUI Library
Simple, consistent JSON structure across all responses
"""

import json
import time
from datetime import datetime
from typing import Dict, List, Any


def standard_response(
    data: List[Dict[str, Any]], 
    operation: str = "scan",
    status: str = "success",
    execution_time_ms: int = 0,
    **meta_fields
) -> Dict[str, Any]:
    """
    Create standard response format used across the library
    
    Args:
        data: List of result objects 
        operation: Operation type (scan, extraction, etc.)
        status: success/error/partial
        execution_time_ms: Time taken for operation
        **meta_fields: Additional metadata fields
    
    Returns:
        Standardized response dictionary with data/meta structure
    """
    
    # Format data items consistently
    formatted_data = []
    for item in data:
        data_item = {
            "type": meta_fields.get('data_type', 'result'),
            "payload": item
        }
        formatted_data.append(data_item)
    
    # Build metadata
    meta = {
        "status": status,
        "timestamp": datetime.now().isoformat(),
        "operation": operation,
        "execution_time_ms": execution_time_ms,
        "total_items": len(formatted_data),
        "version": "2.6.2"
    }
    
    # Add any additional metadata
    meta.update(meta_fields)
    
    return {
        "data": formatted_data,
        "meta": meta
    }


def error_response(error_message: str, operation: str = "scan") -> Dict[str, Any]:
    """Create standard error response"""
    return {
        "data": [],
        "meta": {
            "status": "error", 
            "timestamp": datetime.now().isoformat(),
            "operation": operation,
            "error": error_message,
            "total_items": 0,
            "version": "2.6.2"
        }
    }


def format_json(response: Dict[str, Any], pretty: bool = False) -> str:
    """Format response as JSON string"""
    if pretty:
        return json.dumps(response, indent=2, default=str)
    return json.dumps(response, separators=(',', ':'), default=str)