#!/usr/bin/env python3
"""
gRPC Extractor
Handles gRPC extraction from Sui nodes
"""

import logging
from typing import List
from ..models import SuiDataResult

logger = logging.getLogger(__name__)

# gRPC imports - will be available after protobuf compilation
try:
    import grpc
    from grpc_reflection.v1alpha import reflection_pb2_grpc, reflection_pb2
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False


class GrpcExtractor:
    """Handles gRPC extraction from Sui nodes"""
    
    def __init__(self, timeout: int = 10, config: dict = None, sui_grpc_stubs=None):
        self.timeout = timeout
        self.config = config or {}
        self.sui_grpc_stubs = sui_grpc_stubs
        self.logger = logger

    async def extract_grpc_intelligence_async(self, result: SuiDataResult, ip: str, ports: List[int]) -> bool:
        """Extract intelligence via gRPC with protobuf support"""
        if not GRPC_AVAILABLE:
            return False
        
        grpc_ports = [8080, 50051, 9090]
        
        for port in grpc_ports:
            if port not in ports:
                continue
                
            try:
                channel_address = f"{ip}:{port}"
                channel = grpc.insecure_channel(channel_address)
                
                # Test basic connectivity
                try:
                    grpc.channel_ready_future(channel).result(timeout=5)
                    result.grpc_available = True
                    
                    # Try reflection if available
                    if self._extract_grpc_reflection(result, channel):
                        result.intelligence_sources.append("grpc_reflection")
                    
                    # Try Sui-specific gRPC services if stubs are available
                    if self.sui_grpc_stubs:
                        await self._extract_sui_grpc_intelligence(result, channel)
                    
                    channel.close()
                    return True
                    
                except grpc.FutureTimeoutError:
                    channel.close()
                    continue
                    
            except Exception as e:
                self.logger.debug(f"gRPC port {port} failed: {e}")
                continue
        
        return False

    def _extract_grpc_reflection(self, result: SuiDataResult, channel) -> bool:
        """Extract gRPC service information via reflection"""
        try:
            stub = reflection_pb2_grpc.ServerReflectionStub(channel)
            request = reflection_pb2.ServerReflectionRequest()
            request.list_services = ""
            
            responses = stub.ServerReflectionInfo(iter([request]))
            
            for response in responses:
                if response.HasField('list_services_response'):
                    services = [s.name for s in response.list_services_response.service]
                    result.grpc_services = services
                    result.grpc_reflection_data["services"] = services
                    
                    # Identify Sui-specific services
                    sui_services = [s for s in services if 'sui' in s.lower() or 'consensus' in s.lower()]
                    if sui_services:
                        result.grpc_reflection_data["sui_services"] = sui_services
                    
                    return True
                    
        except Exception as e:
            self.logger.debug(f"gRPC reflection failed: {e}")
            return False

    async def _extract_sui_grpc_intelligence(self, result: SuiDataResult, channel):
        """Extract Sui-specific intelligence via gRPC stubs"""
        if not self.sui_grpc_stubs:
            return
        
        try:
            # This would use the compiled Sui protobuf stubs
            # Implementation depends on actual Sui gRPC service definitions
            
            for stub_name, stub_module in self.sui_grpc_stubs.items():
                try:
                    # Create service stub
                    service_stub = getattr(stub_module, f"{stub_name}Stub", None)
                    if service_stub:
                        stub_instance = service_stub(channel)
                        
                        # Call Sui-specific gRPC methods
                        # This would be implemented based on actual Sui proto definitions
                        result.grpc_reflection_data[f"{stub_name}_available"] = True
                        
                except Exception as e:
                    self.logger.debug(f"Sui gRPC stub {stub_name} failed: {e}")
                    
        except Exception as e:
            result.extraction_errors.append(f"sui_grpc_intelligence_error: {str(e)}")