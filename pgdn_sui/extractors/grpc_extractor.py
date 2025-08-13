#!/usr/bin/env python3
"""
gRPC Extractor
Handles gRPC extraction from Sui nodes
"""

import logging
import time
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

# gRPC Health Check imports
try:
    from grpc_health.v1 import health_pb2_grpc, health_pb2
    GRPC_HEALTH_AVAILABLE = True
except ImportError:
    GRPC_HEALTH_AVAILABLE = False


class GrpcExtractor:
    """Handles gRPC extraction from Sui nodes"""
    
    def __init__(self, timeout: int = 10, config: dict = None, sui_grpc_stubs=None):
        self.timeout = timeout
        self.config = config or {}
        self.debug_mode = self.config.get('debug', False)
        self.max_sample_length = self.config.get('max_sample_length', 200)
        self.sui_grpc_stubs = sui_grpc_stubs
        self.logger = logger

    async def extract_grpc_intelligence_async(self, result: SuiDataResult, ip: str, ports: List[int]) -> bool:
        """Extract intelligence via gRPC with best-effort TCP connect detection"""
        if not GRPC_AVAILABLE:
            # Even without gRPC lib, do TCP connect-only detection
            return await self._tcp_detect_grpc_presence(result, ip)
        
        # Primary gRPC ports for TCP connect-only detection: 50051 and 8080
        grpc_ports_to_detect = [50051, 8080]
        # Try full gRPC on discovered ports
        grpc_ports_to_try = [9000, 8080, 50051, 9090]
        
        for port in grpc_ports_to_try:
            if port not in ports:
                continue
                
            try:
                channel_address = f"{ip}:{port}"
                channel = grpc.insecure_channel(channel_address)
                
                # Test basic connectivity with shorter timeout (2s per prompt.md)
                try:
                    grpc.channel_ready_future(channel).result(timeout=2)
                    result.grpc_available = True
                    if hasattr(result, 'set_evidence'):
                        result.set_evidence("grpc", f"grpc ok (port {port})")
                    
                    # Enhanced gRPC intelligence extraction for port 9000
                    if port == 9000:
                        self.logger.info(f"Found gRPC on port 9000 - extracting enhanced intelligence")
                        
                        # Try reflection (grpc_reflection_enabled + grpc_services_list)
                        if self._extract_grpc_reflection_enhanced(result, channel):
                            result.intelligence_sources.append("grpc_reflection_port_9000")
                        
                        # Try health check (grpc_health_status) 
                        if await self._extract_grpc_health_status(result, channel):
                            result.intelligence_sources.append("grpc_health_port_9000")
                    else:
                        # Legacy reflection for other ports
                        if self._extract_grpc_reflection(result, channel):
                            result.intelligence_sources.append("grpc_reflection")
                    
                    # Try Sui-specific gRPC services if stubs are available
                    if self.sui_grpc_stubs:
                        await self._extract_sui_grpc_intelligence(result, channel)
                    
                    channel.close()
                    return True
                    
                except grpc.FutureTimeoutError:
                    self.logger.debug(f"gRPC port {port} timed out")
                    channel.close()
                    continue
                    
            except Exception as e:
                self.logger.debug(f"gRPC port {port} failed: {e}")
                continue
        
        # If gRPC lib is available but no ports worked, try TCP detection
        return await self._tcp_detect_grpc_presence(result, ip)
    
    async def _tcp_detect_grpc_presence(self, result: SuiDataResult, ip: str) -> bool:
        """TCP connect-only to 50051 and 8080 for best-effort gRPC detection"""
        import socket
        
        grpc_ports_to_check = [50051, 8080]
        open_grpc_ports = []
        
        for port in grpc_ports_to_check:
            try:
                # TCP connect with short timeout
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1.5)  # 1.5s connect timeout as per spec
                result_code = sock.connect_ex((ip, port))
                sock.close()
                
                if result_code == 0:  # Connection successful
                    open_grpc_ports.append(port)
                    self.logger.info(f"TCP connect successful to gRPC port {port}")
                    
            except Exception as e:
                self.logger.debug(f"TCP connect failed to port {port}: {e}")
                continue
        
        if open_grpc_ports:
            result.grpc_available = True
            if hasattr(result, 'set_evidence'):
                result.set_evidence("grpc", f"grpc ok (tcp {open_grpc_ports})")
            # Store detected open ports for evidence
            if not hasattr(result, 'open_ports'):
                result.open_ports = {}
            result.open_ports['grpc'] = open_grpc_ports
            self.logger.info(f"gRPC detected via TCP connect on ports: {open_grpc_ports}")
            return True
        else:
            if hasattr(result, 'set_evidence'):
                result.set_evidence("grpc", "grpc blocked")
            
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

    def _extract_grpc_reflection_enhanced(self, result: SuiDataResult, channel) -> bool:
        """Enhanced gRPC reflection extraction for prompt.md requirements"""
        try:
            stub = reflection_pb2_grpc.ServerReflectionStub(channel)
            request = reflection_pb2.ServerReflectionRequest()
            request.list_services = ""
            
            responses = stub.ServerReflectionInfo(iter([request]))
            
            for response in responses:
                if response.HasField('list_services_response'):
                    services = [s.name for s in response.list_services_response.service]
                    
                    # Set the new fields from prompt.md
                    result.grpc_reflection_enabled = True
                    result.grpc_services_list = services
                    
                    # Also keep legacy field
                    result.grpc_services = services
                    result.grpc_reflection_data["services"] = services
                    result.grpc_reflection_data["reflection_enabled"] = True
                    
                    # Identify Sui-specific services
                    sui_services = [s for s in services if 'sui' in s.lower() or 'consensus' in s.lower()]
                    if sui_services:
                        result.grpc_reflection_data["sui_services"] = sui_services
                    
                    self.logger.info(f"gRPC reflection enabled - found {len(services)} services")
                    return True
                    
        except Exception as e:
            self.logger.debug(f"gRPC reflection failed: {e}")
            result.grpc_reflection_enabled = False
            result.grpc_services_list = []
            return False

    async def _extract_grpc_health_status(self, result: SuiDataResult, channel) -> bool:
        """Extract gRPC health status using grpc.health.v1.Health/Check"""
        if not GRPC_HEALTH_AVAILABLE:
            self.logger.debug("gRPC health check not available - missing grpc-health dependency")
            return False
        
        try:
            # Create health check stub
            health_stub = health_pb2_grpc.HealthStub(channel)
            
            # Create health check request
            request = health_pb2.HealthCheckRequest()
            request.service = ""  # Empty service name checks overall server health
            
            # Make the health check call with timeout
            response = health_stub.Check(request, timeout=2)
            
            # Map health status to string
            status_map = {
                health_pb2.HealthCheckResponse.UNKNOWN: "UNKNOWN",
                health_pb2.HealthCheckResponse.SERVING: "SERVING", 
                health_pb2.HealthCheckResponse.NOT_SERVING: "NOT_SERVING",
                health_pb2.HealthCheckResponse.SERVICE_UNKNOWN: "SERVICE_UNKNOWN"
            }
            
            status_string = status_map.get(response.status, f"UNKNOWN_STATUS_{response.status}")
            result.grpc_health_status = status_string
            
            self.logger.info(f"gRPC health check: {status_string}")
            
            # Store additional health data
            result.grpc_reflection_data["health_check"] = {
                "status": status_string,
                "status_code": response.status,
                "timestamp": time.time()
            }
            
            return True
            
        except grpc.RpcError as e:
            # Handle gRPC-specific errors
            status_code = e.code()
            if status_code == grpc.StatusCode.UNIMPLEMENTED:
                self.logger.debug("gRPC health service not implemented")
                result.grpc_health_status = "NOT_IMPLEMENTED"
            elif status_code == grpc.StatusCode.UNAVAILABLE:
                result.grpc_health_status = "UNAVAILABLE"
            else:
                result.grpc_health_status = f"ERROR_{status_code.name}"
            
            result.grpc_reflection_data["health_check_error"] = str(e)
            return False
            
        except Exception as e:
            self.logger.debug(f"gRPC health check failed: {e}")
            result.grpc_health_status = "ERROR"
            result.grpc_reflection_data["health_check_error"] = str(e)
            return False