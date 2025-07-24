#!/usr/bin/env python3
"""
WebSocket Extractor
Handles WebSocket connection testing and extraction from Sui nodes
"""

import socket
import logging
from typing import List
from ..models import SuiDataResult

logger = logging.getLogger(__name__)


class WebsocketExtractor:
    """Handles WebSocket connection testing from Sui nodes"""
    
    def __init__(self, timeout: int = 10, config: dict = None):
        self.timeout = timeout
        self.config = config or {}
        self.logger = logger

    async def extract_websocket_intelligence_async(self, result: SuiDataResult, ip: str) -> bool:
        """Extract intelligence via WebSocket connections"""
        ws_ports = [9000, 9184]
        
        for port in ws_ports:
            try:
                # Test WebSocket upgrade capability
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(3)
                if sock.connect_ex((ip, port)) == 0:
                    result.websocket_available = True
                    sock.close()
                    return True
                sock.close()
            except:
                continue
        
        return False