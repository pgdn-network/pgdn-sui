#!/usr/bin/env python3
"""
Enhanced Unified Sui Network Scanner - RICH ENDPOINT DATA VERSION
Captures comprehensive endpoint information including SSH, SSL, service versions, and banners
"""

import requests
import socket
import json
import logging
import concurrent.futures
import time
import re
import ssl
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
import urllib3
urllib3.disable_warnings()

def setup_logging(level=logging.INFO):
    """Set up logging configuration to stderr only"""
    logging.basicConfig(
        level=level, 
        format='%(asctime)s - %(levelname)s - %(message)s',
        stream=sys.stderr
    )

logger = logging.getLogger(__name__)

@dataclass
class SuiNodeResult:
    """Complete Sui node analysis result with rich endpoint data"""
    ip: str
    hostname: str
    discovered_at: str
    node_type: str
    confidence: float
    capabilities: List[str]
    network: str
    discovery_time: float
    classification_time: float
    total_scan_time: float
    accessible_ports: List[int]
    working_endpoints: List[str]
    version_info: Dict[str, str] = None
    health_status: Dict = None
    performance_metrics: Dict = None
    system_state: Dict = None
    validator_info: Dict = None
    network_metrics: Dict = None
    chain_state: Dict = None
    endpoint_details: Dict = None  # NEW: Rich endpoint scanning data
    service_versions: Dict = None  # NEW: Service version detection
    ssl_info: Dict = None          # NEW: SSL/TLS certificate details
    security_scan: Dict = None     # NEW: Security assessment
    analysis_level: str = "discovery"

class EnhancedSuiScanner:
    """Enhanced scanner with comprehensive endpoint analysis"""
    
    def __init__(self, timeout: int = 5, max_workers: int = 3):
        self.timeout = timeout
        self.max_workers = max_workers
        
        # Session setup
        self.session = requests.Session()
        self.session.verify = False
        self.session.timeout = timeout
        self.session.headers.update({
            'User-Agent': 'EnhancedSuiScanner/2.0',
            'Connection': 'keep-alive'
        })
        
        # Comprehensive port list for rich scanning including Docker and correct metrics port
        self.sui_ports = [22, 80, 443, 2375, 2376, 8080, 9000, 9100, 50051]
        self.rpc_test_method = {
            "jsonrpc": "2.0", "method": "sui_getChainIdentifier", "params": [], "id": 1
        }

    def scan_targets_discovery_mode(self, targets: List[str]) -> List[SuiNodeResult]:
        """Run discovery mode scan (Phase 1 only) - Node type detection"""
        logger.info(f"Starting Discovery Mode scan of {len(targets)} targets")
        return self._scan_targets_internal(targets, run_deep_analysis=False)

    def scan_targets_deep_mode(self, targets: List[str]) -> List[SuiNodeResult]:
        """Run deep mode scan (Phase 1 + Phase 2) - Full endpoint analysis"""
        logger.info(f"Starting Deep Mode scan of {len(targets)} targets")
        return self._scan_targets_internal(targets, run_deep_analysis=True)

    def _scan_targets_internal(self, targets: List[str], run_deep_analysis: bool = False) -> List[SuiNodeResult]:
        """Internal method for scanning targets with configurable depth"""
        analysis_type = "Deep Analysis (Node Detection + Endpoint Scanning)" if run_deep_analysis else "Discovery (Node Detection Only)"
        logger.info(f"Running {analysis_type} on {len(targets)} targets")
        
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_target = {
                executor.submit(self._scan_single_target, target, run_deep_analysis): target 
                for target in targets
            }
            
            for future in concurrent.futures.as_completed(future_to_target, timeout=600):
                target = future_to_target[future]
                try:
                    result = future.result(timeout=120)
                    if result:
                        results.append(result)
                        version_info = self._format_node_summary(result)
                        logger.info(f"SUCCESS {target}: {result.node_type} ({result.confidence:.2f}){version_info}")
                    else:
                        logger.info(f"SKIP {target}: Not a Sui node")
                except Exception as e:
                    logger.error(f"ERROR {target}: Scan failed - {e}")
        
        logger.info(f"Scan complete: {len(results)} Sui nodes found")
        return results

    def process_existing_results_deep_mode(self, existing_results: List[SuiNodeResult]) -> List[SuiNodeResult]:
        """Run deep analysis on existing discovery results"""
        logger.info(f"Running Deep Analysis on {len(existing_results)} existing results")
        
        enhanced_results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_result = {
                executor.submit(self._enhance_existing_result, result): result 
                for result in existing_results
            }
            
            for future in concurrent.futures.as_completed(future_to_result, timeout=900):
                original_result = future_to_result[future]
                try:
                    enhanced_result = future.result(timeout=180)
                    enhanced_results.append(enhanced_result)
                    
                    version_info = self._format_node_summary(enhanced_result)
                    logger.info(f"SUCCESS {enhanced_result.ip}: Deep analysis complete{version_info}")
                    
                except Exception as e:
                    logger.error(f"ERROR {original_result.ip}: Deep analysis failed - {e}")
                    enhanced_results.append(original_result)
        
        logger.info(f"Deep analysis complete: Enhanced {len(enhanced_results)} results")
        return enhanced_results

    def _format_node_summary(self, result: SuiNodeResult) -> str:
        """Format a concise summary of the node"""
        info_parts = []
        
        # Prioritize Sui version information
        if result.version_info:
            # Show Sui build version (most important)
            if result.version_info.get("build_info_version"):
                info_parts.append(f"Sui:{result.version_info['build_info_version']}")
            elif result.version_info.get("detected_version"):
                info_parts.append(f"Sui:{result.version_info['detected_version']}")
            
            # Show protocol version
            if result.version_info.get("protocol_version"):
                info_parts.append(f"Protocol:{result.version_info['protocol_version']}")
            elif result.version_info.get("sui_current_protocol_version"):
                info_parts.append(f"Protocol:{result.version_info['sui_current_protocol_version']}")
            
            # Show epoch
            if result.version_info.get("current_epoch"):
                info_parts.append(f"Epoch:{result.version_info['current_epoch']}")
            elif result.version_info.get("sui_epoch"):
                info_parts.append(f"Epoch:{result.version_info['sui_epoch']}")
            
            # Show git commit (first 8 chars)
            if result.version_info.get("build_info_git_commit"):
                commit = result.version_info['build_info_git_commit'][:8]
                info_parts.append(f"Commit:{commit}")
            
            # Show error info if metrics blocked
            if result.version_info.get("metrics_error"):
                error_type = result.version_info.get("metrics_error_type", "Unknown")
                info_parts.append(f"Metrics:{error_type}")
        
        # Service versions (SSH, HTTP) - These we CAN get even from secured validators
        if result.service_versions:
            if result.service_versions.get("ssh_software"):
                # Extract just the version from SSH banner
                ssh_software = result.service_versions['ssh_software']
                if "Ubuntu" in ssh_software:
                    # Extract Ubuntu version (e.g., "3ubuntu0.13")
                    ubuntu_match = re.search(r'(\d+ubuntu[\d.]+)', ssh_software)
                    if ubuntu_match:
                        info_parts.append(f"SSH:{ubuntu_match.group(1)}")
                    else:
                        info_parts.append(f"SSH:{ssh_software.split()[0]}")
                else:
                    info_parts.append(f"SSH:{ssh_software.split()[0]}")
        
        # SSL info
        if result.ssl_info and result.ssl_info.get("certificate_count"):
            info_parts.append(f"SSL:{result.ssl_info['certificate_count']} certs")
        
        # Show what we detected from endpoints
        if result.endpoint_details:
            services = result.endpoint_details.get("services", {})
            detected_services = [s for s in services.values() if s.get("detected")]
            if detected_services:
                info_parts.append(f"Services:{len(detected_services)}")
        
        return f" | {', '.join(info_parts)}" if info_parts else ""

    def _scan_single_target(self, target: str, run_deep_analysis: bool = False) -> Optional[SuiNodeResult]:
        """Scan single target with configurable analysis depth"""
        total_start = time.time()
        
        # PHASE 1: Node Type Detection (Discovery Mode)
        logger.debug(f"Discovery: Detecting node type for {target}")
        phase1_start = time.time()
        
        # Comprehensive port scan
        accessible_ports = self._comprehensive_port_scan(target)
        if not accessible_ports:
            return None
        
        # Quick protocol detection
        rpc_data = self._quick_rpc_test(target, accessible_ports)
        grpc_data = self._quick_grpc_test(target, accessible_ports)
        metrics_data = self._quick_metrics_test(target, accessible_ports)
        
        # Node type classification
        node_classification = self._classify_node(rpc_data, grpc_data, metrics_data, target)
        phase1_time = time.time() - phase1_start
        
        if not node_classification:
            return None
        
        # Create discovery result
        result = SuiNodeResult(
            ip=target,
            hostname=target,
            discovered_at=datetime.now().isoformat(),
            node_type=node_classification["type"],
            confidence=node_classification["confidence"],
            capabilities=node_classification["capabilities"],
            network=node_classification["network"],
            discovery_time=phase1_time,
            classification_time=0,
            total_scan_time=phase1_time,
            accessible_ports=accessible_ports,
            working_endpoints=rpc_data.get("endpoints", []) + grpc_data.get("endpoints", []),
            analysis_level="discovery"
        )
        
        # PHASE 2: Deep Analysis (if requested)
        if run_deep_analysis:
            result = self._perform_deep_analysis(result, rpc_data, grpc_data, metrics_data, total_start)
        
        return result

    def _perform_deep_analysis(self, result: SuiNodeResult, rpc_data: Dict, grpc_data: Dict, metrics_data: Dict, total_start: float) -> SuiNodeResult:
        """Perform deep analysis on a node"""
        target = result.ip
        logger.debug(f"Deep Analysis: Rich endpoint analysis for {target} ({result.node_type})")
        phase2_start = time.time()
        
        try:
            # Rich endpoint scanning
            result.endpoint_details = self._scan_all_endpoints(target, result.accessible_ports)
            result.service_versions = self._extract_service_versions(target, result.accessible_ports)
            result.ssl_info = self._analyze_ssl_certificates(target, result.accessible_ports)
            result.security_scan = self._security_assessment(target, result.accessible_ports)
            
            # Sui-specific data extraction
            if result.node_type in ["public_rpc", "hybrid"]:
                result.version_info = self._extract_rpc_versions(target, rpc_data)
                result.performance_metrics = self._benchmark_rpc(target, rpc_data)
                result.system_state = self._extract_system_state(target, rpc_data)
                result.validator_info = self._extract_validator_set_info(target, rpc_data)
                result.chain_state = self._extract_chain_state(target, rpc_data)
                
            if result.node_type in ["validator", "hybrid", "monitoring"]:
                validator_versions = self._extract_validator_versions(target, metrics_data)
                network_metrics = self._extract_network_metrics(target, metrics_data)
                
                if result.version_info:
                    result.version_info.update(validator_versions)
                else:
                    result.version_info = validator_versions
                
                result.network_metrics = network_metrics
            
            # Comprehensive health check
            result.health_status = self._comprehensive_health_check(
                result.node_type, rpc_data, grpc_data, metrics_data, result.endpoint_details
            )
            
            result.analysis_level = "deep"
            
        except Exception as e:
            logger.error(f"Deep analysis error for {target}: {e}")
            result.version_info = {"error": str(e)}
        
        phase2_time = time.time() - phase2_start
        result.total_scan_time = time.time() - total_start
        
        logger.debug(f"{target}: Deep analysis took {phase2_time:.2f}s")
        return result

    def _enhance_existing_result(self, existing_result: SuiNodeResult) -> SuiNodeResult:
        """Enhance an existing discovery result with deep analysis"""
        target = existing_result.ip
        
        logger.debug(f"Enhancing existing result for {target}")
        
        # Reconstruct protocol data for deep analysis
        rpc_data = self._reconstruct_rpc_data(existing_result)
        grpc_data = self._reconstruct_grpc_data(existing_result)
        metrics_data = self._reconstruct_metrics_data(existing_result)
        
        # Perform deep analysis
        enhanced_result = self._perform_deep_analysis(existing_result, rpc_data, grpc_data, metrics_data, time.time())
        
        return enhanced_result

    def _comprehensive_port_scan(self, target: str) -> List[int]:
        """Comprehensive port scan including service detection"""
        accessible_ports = []
        
        def test_port(port):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                result = sock.connect_ex((target, port)) == 0
                sock.close()
                return port if result else None
            except:
                return None
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.sui_ports)) as executor:
            futures = [executor.submit(test_port, port) for port in self.sui_ports]
            for future in concurrent.futures.as_completed(futures, timeout=10):
                try:
                    result = future.result()
                    if result:
                        accessible_ports.append(result)
                except:
                    pass
        
        return sorted(accessible_ports)

    def _scan_all_endpoints(self, target: str, ports: List[int]) -> Dict:
        """Comprehensive endpoint scanning with service detection"""
        endpoint_details = {
            "scanned_ports": ports,
            "services": {},
            "banners": {},
            "http_headers": {},
            "response_codes": {}
        }
        
        for port in ports:
            try:
                # Service banner grabbing
                banner = self._grab_banner(target, port)
                if banner:
                    endpoint_details["banners"][str(port)] = banner
                
                # HTTP-specific scanning
                if port in [80, 443, 8080, 9000]:
                    http_info = self._scan_http_endpoint(target, port)
                    if http_info:
                        endpoint_details["http_headers"][str(port)] = http_info
                
                # Docker API scanning
                if port in [2375, 2376]:
                    docker_info = self._scan_docker_endpoint(target, port)
                    if docker_info:
                        endpoint_details["docker_api"] = endpoint_details.get("docker_api", {})
                        endpoint_details["docker_api"][str(port)] = docker_info
                
                # Service identification
                service_info = self._identify_service(target, port, banner)
                if service_info:
                    endpoint_details["services"][str(port)] = service_info
                    
            except Exception as e:
                logger.debug(f"Endpoint scanning error for {target}:{port} - {e}")
                continue
        
        return endpoint_details

    def _grab_banner(self, target: str, port: int) -> Optional[str]:
        """Grab service banner from port"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            sock.connect((target, port))
            
            # Send appropriate probe based on port
            if port == 22:  # SSH
                banner = sock.recv(1024).decode('utf-8', errors='ignore').strip()
            elif port in [80, 443, 8080, 9000]:  # HTTP
                sock.send(b"GET / HTTP/1.1\r\nHost: " + target.encode() + b"\r\n\r\n")
                banner = sock.recv(2048).decode('utf-8', errors='ignore').strip()
            else:
                # Generic probe
                banner = sock.recv(1024).decode('utf-8', errors='ignore').strip()
            
            sock.close()
            return banner if banner else None
            
        except Exception as e:
            logger.debug(f"Banner grab failed for {target}:{port} - {e}")
            return None

    def _scan_http_endpoint(self, target: str, port: int) -> Dict:
        """Detailed HTTP endpoint scanning"""
        http_info = {}
        
        try:
            protocol = "https" if port == 443 else "http"
            url = f"{protocol}://{target}:{port}/" if port not in [80, 443] else f"{protocol}://{target}/"
            
            response = self.session.get(url, timeout=5)
            
            http_info["status_code"] = response.status_code
            http_info["headers"] = dict(response.headers)
            http_info["content_length"] = len(response.content)
            
            # Extract server information
            if "server" in response.headers:
                http_info["server"] = response.headers["server"]
            
            # Check for common frameworks/technologies
            if response.text:
                http_info["content_preview"] = response.text[:200]
                
                # Technology detection
                technologies = []
                if "nginx" in response.text.lower():
                    technologies.append("nginx")
                if "apache" in response.text.lower():
                    technologies.append("apache")
                if "sui" in response.text.lower():
                    technologies.append("sui-related")
                
                if technologies:
                    http_info["detected_technologies"] = technologies
            
        except Exception as e:
            logger.debug(f"HTTP scanning failed for {target}:{port} - {e}")
        
        return http_info

    def _identify_service(self, target: str, port: int, banner: str) -> Dict:
        """Identify service type and version from banner"""
        service_info = {"port": port, "detected": False}
        
        if not banner:
            return service_info
        
        banner_lower = banner.lower()
        
        # SSH Detection
        if port == 22 or "ssh" in banner_lower:
            ssh_match = re.search(r'ssh-[\d.]+-(\S+)', banner, re.IGNORECASE)
            if ssh_match:
                service_info.update({
                    "service": "ssh",
                    "version": ssh_match.group(1),
                    "full_banner": banner,
                    "detected": True
                })
        
        # HTTP Server Detection
        elif port in [80, 443, 8080, 9000]:
            server_match = re.search(r'server:\s*([^\r\n]+)', banner, re.IGNORECASE)
            if server_match:
                service_info.update({
                    "service": "http",
                    "server": server_match.group(1),
                    "full_banner": banner[:500],
                    "detected": True
                })
        
        # Docker API Detection
        elif port in [2375, 2376]:
            try:
                url = f"http://{target}:{port}/version"
                response = self.session.get(url, timeout=3)
                if response.status_code == 200:
                    service_info.update({
                        "service": "docker_api",
                        "accessible": True,
                        "port_type": "unencrypted" if port == 2375 else "encrypted",
                        "detected": True
                    })
                    try:
                        docker_data = response.json()
                        service_info["docker_version"] = docker_data.get("Version", "unknown")
                    except:
                        pass
            except:
                pass
        
        # Generic service detection
        else:
            service_info.update({
                "service": "unknown",
                "banner_preview": banner[:200],
                "detected": True
            })
        
        return service_info

    def _scan_docker_endpoint(self, target: str, port: int) -> Dict:
        """Scan Docker API endpoints"""
        docker_info = {"accessible": False, "version": None, "info": None}
        
        try:
            # Docker API typically runs on HTTP
            base_url = f"http://{target}:{port}"
            
            # Try to get Docker version with fast timeout
            version_response = self.session.get(f"{base_url}/version", timeout=3)
            if version_response.status_code == 200:
                docker_info["accessible"] = True
                try:
                    version_data = version_response.json()
                    docker_info["version"] = {
                        "version": version_data.get("Version"),
                        "api_version": version_data.get("ApiVersion"),
                        "git_commit": version_data.get("GitCommit"),
                        "go_version": version_data.get("GoVersion"),
                        "os": version_data.get("Os"),
                        "arch": version_data.get("Arch")
                    }
                except:
                    docker_info["version"] = {"raw_response": version_response.text[:200]}
            
            # Try to get Docker info (more detailed) with fast timeout
            if docker_info["accessible"]:
                info_response = self.session.get(f"{base_url}/info", timeout=3)
                if info_response.status_code == 200:
                    try:
                        info_data = info_response.json()
                        docker_info["info"] = {
                            "containers": info_data.get("Containers"),
                            "containers_running": info_data.get("ContainersRunning"),
                            "containers_paused": info_data.get("ContainersPaused"),
                            "containers_stopped": info_data.get("ContainersStopped"),
                            "images": info_data.get("Images"),
                            "server_version": info_data.get("ServerVersion"),
                            "operating_system": info_data.get("OperatingSystem"),
                            "total_memory": info_data.get("MemTotal"),
                            "cpus": info_data.get("NCPU")
                        }
                    except:
                        docker_info["info"] = {"raw_response": info_response.text[:200]}
        
        except Exception as e:
            docker_info["error"] = str(e)
            logger.debug(f"Docker API scan failed for {target}:{port} - {e}")
        
        return docker_info

    def _extract_service_versions(self, target: str, ports: List[int]) -> Dict:
        """Extract detailed service version information"""
        versions = {}
        
        # SSH Version Detection
        if 22 in ports:
            ssh_version = self._get_ssh_version(target)
            if ssh_version:
                versions.update(ssh_version)
        
        # HTTP Server Versions
        for port in [80, 443, 8080, 9000]:
            if port in ports:
                http_version = self._get_http_server_version(target, port)
                if http_version:
                    versions.update({f"http_{port}": http_version})
        
        # Use nmap if available for enhanced detection
        nmap_versions = self._run_nmap_version_scan(target, ports)
        if nmap_versions:
            versions["nmap_results"] = nmap_versions
        
        return versions

    def _get_ssh_version(self, target: str) -> Dict:
        """Get detailed SSH version information"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((target, 22))
            
            banner = sock.recv(1024).decode('utf-8', errors='ignore').strip()
            sock.close()
            
            if banner.startswith('SSH-'):
                parts = banner.split('-')
                if len(parts) >= 3:
                    return {
                        "ssh_version": banner,
                        "ssh_protocol_version": parts[1],
                        "ssh_software": '-'.join(parts[2:]),
                        "ssh_banner_full": banner
                    }
        except Exception as e:
            logger.debug(f"SSH version detection failed for {target} - {e}")
        
        return {}

    def _get_http_server_version(self, target: str, port: int) -> Dict:
        """Get HTTP server version information"""
        try:
            protocol = "https" if port == 443 else "http"
            url = f"{protocol}://{target}:{port}/" if port not in [80, 443] else f"{protocol}://{target}/"
            
            response = self.session.head(url, timeout=5)
            
            server_info = {}
            if "server" in response.headers:
                server_info["http_server"] = response.headers["server"]
            
            # Additional headers of interest
            interesting_headers = ["x-powered-by", "x-frame-options", "x-content-type-options", "strict-transport-security"]
            for header in interesting_headers:
                if header in response.headers:
                    server_info[header.replace("-", "_")] = response.headers[header]
            
            return server_info
            
        except Exception as e:
            logger.debug(f"HTTP server detection failed for {target}:{port} - {e}")
            return {}

    def _run_nmap_version_scan(self, target: str, ports: List[int]) -> Dict:
        """Run nmap version scan if available"""
        try:
            # Check if nmap is available
            result = subprocess.run(['which', 'nmap'], capture_output=True, text=True, timeout=5)
            if result.returncode != 0:
                return {"error": "nmap not available"}
            
            # Run nmap version scan
            port_list = ','.join(map(str, ports))
            cmd = ['nmap', '-sV', '-p', port_list, '--version-intensity', '5', target]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                return {
                    "nmap_output": result.stdout,
                    "scan_time": datetime.now().isoformat()
                }
            else:
                return {"error": f"nmap failed: {result.stderr}"}
                
        except Exception as e:
            logger.debug(f"Nmap scan failed for {target} - {e}")
            return {"error": str(e)}

    def _analyze_ssl_certificates(self, target: str, ports: List[int]) -> Dict:
        """Analyze SSL/TLS certificates"""
        ssl_info = {"certificates": {}, "certificate_count": 0}
        
        ssl_ports = [port for port in ports if port in [443, 8080, 9000]]
        
        for port in ssl_ports:
            try:
                # Get certificate information
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                
                with socket.create_connection((target, port), timeout=5) as sock:
                    with context.wrap_socket(sock, server_hostname=target) as ssock:
                        cert = ssock.getpeercert()
                        cert_der = ssock.getpeercert(binary_form=True)
                        
                        cert_info = {
                            "subject": dict(x[0] for x in cert.get('subject', [])),
                            "issuer": dict(x[0] for x in cert.get('issuer', [])),
                            "version": cert.get('version'),
                            "serial_number": cert.get('serialNumber'),
                            "not_before": cert.get('notBefore'),
                            "not_after": cert.get('notAfter'),
                            "signature_algorithm": cert.get('signatureAlgorithm')
                        }
                        
                        # Subject Alternative Names
                        if 'subjectAltName' in cert:
                            cert_info['subject_alt_names'] = [name[1] for name in cert['subjectAltName']]
                        
                        ssl_info["certificates"][str(port)] = cert_info
                        ssl_info["certificate_count"] += 1
                        
            except Exception as e:
                logger.debug(f"SSL analysis failed for {target}:{port} - {e}")
                continue
        
        return ssl_info

    def _security_assessment(self, target: str, ports: List[int]) -> Dict:
        """Basic port and service information without scoring"""
        security = {
            "open_ports_count": len(ports),
            "ssl_ports": [],
            "unencrypted_ports": [],
            "standard_ports": [],
            "non_standard_ports": []
        }
        
        # Categorize ports by type (informational only)
        ssl_ports = [443]
        unencrypted_ports = [80, 8080, 9000, 9100]
        standard_ports = [22, 80, 443]
        
        for port in ports:
            if port in ssl_ports:
                security["ssl_ports"].append(port)
            if port in unencrypted_ports:
                security["unencrypted_ports"].append(port)
            if port in standard_ports:
                security["standard_ports"].append(port)
            else:
                security["non_standard_ports"].append(port)
        
        return security

    def _quick_rpc_test(self, target: str, ports: List[int]) -> Dict:
        """Enhanced RPC capability test"""
        rpc_data = {"working": False, "endpoints": [], "chain_id": None, "detected_endpoints": []}
        
        test_configs = [
            (443, "https", "/"),
            (443, "https", "/json-rpc"),
            (443, "https", "/rpc"),
            (9000, "http", "/"),
            (9000, "http", "/rpc"),
            (8080, "http", "/"),
        ]
        
        for port, protocol, path in test_configs:
            if port not in ports:
                continue
                
            url = f"{protocol}://{target}:{port}{path}" if port not in [80, 443] else f"{protocol}://{target}{path}"
            rpc_data["detected_endpoints"].append(url)
            
            try:
                response = self.session.post(url, json=self.rpc_test_method, timeout=self.timeout)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if "result" in data:
                            rpc_data["working"] = True
                            rpc_data["endpoints"].append(url)
                            rpc_data["chain_id"] = data["result"]
                            break
                    except:
                        pass
            except:
                continue
        
        return rpc_data

    def _quick_grpc_test(self, target: str, ports: List[int]) -> Dict:
        """Enhanced gRPC capability test"""
        grpc_data = {"detected": False, "ports": [], "endpoints": []}
        
        grpc_ports = [8080, 50051]
        
        for port in grpc_ports:
            if port not in ports:
                continue
            
            try:
                if port == 50051:
                    # Socket test for standard gRPC port
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(1)
                    if sock.connect_ex((target, port)) == 0:
                        grpc_data["detected"] = True
                        grpc_data["ports"].append(port)
                        grpc_data["endpoints"].append(f"grpc://{target}:{port}")
                    sock.close()
                else:
                    # HTTP probe for gRPC
                    response = self.session.get(f"http://{target}:{port}/", timeout=2)
                    if ('grpc' in response.headers.get('content-type', '').lower() or 
                        response.headers.get('grpc-status') is not None):
                        grpc_data["detected"] = True
                        grpc_data["ports"].append(port)
                        grpc_data["endpoints"].append(f"grpc://{target}:{port}")
            except:
                continue
        
        return grpc_data

    def _quick_metrics_test(self, target: str, ports: List[int]) -> Dict:
        """Enhanced metrics endpoint test with fast timeout"""
        metrics_data = {"available": False, "url": None, "sui_metrics": 0, "content_preview": ""}
        
        if 9100 in ports:
            try:
                url = f"http://{target}:9100/metrics"
                response = self.session.get(url, timeout=2)
                if response.status_code == 200 and "# HELP" in response.text:
                    sui_count = response.text.count('sui_')
                    if sui_count > 0:
                        metrics_data["available"] = True
                        metrics_data["url"] = url
                        metrics_data["sui_metrics"] = sui_count
                        # Store first 500 chars for quick analysis
                        metrics_data["content_preview"] = response.text[:500]
                    else:
                        # Even if no Sui metrics, still check for node_uname_info for network detection
                        if "node_uname_info" in response.text:
                            metrics_data["available"] = True
                            metrics_data["url"] = url
                            metrics_data["sui_metrics"] = 0
                            # Store relevant section containing node_uname_info
                            uname_start = response.text.find("node_uname_info")
                            if uname_start >= 0:
                                uname_section = response.text[uname_start:uname_start+500]
                                metrics_data["content_preview"] = uname_section
            except:
                pass
        
        return metrics_data

    def _classify_node(self, rpc_data: Dict, grpc_data: Dict, metrics_data: Dict, target: str = None) -> Optional[Dict]:
        """Enhanced node classification"""
        has_rpc = rpc_data["working"]
        has_grpc = grpc_data["detected"]
        has_metrics = metrics_data["available"]
        
        if not (has_rpc or has_grpc or has_metrics):
            return None
        
        # Determine network from chain ID
        network = "unknown"
        chain_id = rpc_data.get("chain_id", "")
        if chain_id == "35834a8a":
            network = "mainnet"
        elif "testnet" in str(chain_id).lower():
            network = "testnet"
        elif "devnet" in str(chain_id).lower():
            network = "devnet"
        
        # Fallback 1: Check metrics data for network information
        if network == "unknown" and metrics_data.get("available") and metrics_data.get("content_preview"):
            metrics_content = metrics_data["content_preview"].lower()
            if "mainnet" in metrics_content:
                network = "mainnet"
            elif "testnet" in metrics_content:
                network = "testnet"
            elif "devnet" in metrics_content:
                network = "devnet"
        
        # Fallback 2: Determine network from hostname if chain ID detection failed
        if network == "unknown" and target:
            target_lower = target.lower()
            if "mainnet" in target_lower:
                network = "mainnet"
            elif "testnet" in target_lower:
                network = "testnet"
            elif "devnet" in target_lower:
                network = "devnet"
        
        # Enhanced classification with capability detection
        capabilities = []
        
        if has_rpc:
            capabilities.extend(["json_rpc", "public_api"])
        if has_grpc:
            capabilities.extend(["grpc", "consensus"])
        if has_metrics:
            capabilities.append("prometheus_metrics")
        
        # Classify node type
        if has_rpc and not has_grpc:
            return {
                "type": "public_rpc",
                "confidence": 0.95,
                "capabilities": capabilities,
                "network": network
            }
        elif has_grpc and not has_rpc:
            return {
                "type": "validator", 
                "confidence": 0.95,
                "capabilities": capabilities,
                "network": network
            }
        elif has_rpc and has_grpc:
            capabilities.append("dual_protocol")
            return {
                "type": "hybrid",
                "confidence": 0.90,
                "capabilities": capabilities,
                "network": network
            }
        else:
            return {
                "type": "monitoring",
                "confidence": 0.70,
                "capabilities": capabilities,
                "network": network
            }

    def _extract_rpc_versions(self, target: str, rpc_data: Dict) -> Dict[str, str]:
        """Extract comprehensive version info from RPC endpoints"""
        version_info = {}
        
        if not rpc_data.get("endpoints"):
            return version_info
        
        endpoint = rpc_data["endpoints"][0]
        
        # Essential RPC methods for version extraction
        version_methods = [
            ("sui_getChainIdentifier", [], "chain_info"),
            ("sui_getProtocolConfig", [], "protocol_config"),
            ("sui_getRpcApiVersion", [], "api_version"),
            ("sui_getLatestSuiSystemState", [], "system_state"),
            ("sui_getTotalTransactionBlocks", [], "transaction_stats"),
            ("sui_getLatestCheckpointSequenceNumber", [], "checkpoint_info"),
        ]
        
        for method, params, info_type in version_methods:
            try:
                response = self.session.post(endpoint, json={
                    "jsonrpc": "2.0", "method": method, "params": params, "id": hash(method) % 1000
                }, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if "result" in data:
                        result = data["result"]
                        
                        if info_type == "protocol_config" and isinstance(result, dict):
                            version_info["protocol_version"] = str(result.get("protocolVersion", ""))
                            version_info["max_supported_protocol_version"] = str(result.get("maxSupportedProtocolVersion", ""))
                            version_info["min_supported_protocol_version"] = str(result.get("minSupportedProtocolVersion", ""))
                        
                        elif info_type == "system_state" and isinstance(result, dict):
                            version_info["current_epoch"] = str(result.get("epoch", ""))
                            version_info["system_state_version"] = str(result.get("systemStateVersion", ""))
                        
                        elif info_type == "api_version":
                            version_info["rpc_api_version"] = str(result)
                        
                        elif info_type == "chain_info":
                            version_info["chain_identifier"] = str(result)
                        
                        elif info_type == "transaction_stats":
                            version_info["total_transactions"] = str(result)
                        
                        elif info_type == "checkpoint_info":
                            version_info["latest_checkpoint_sequence"] = str(result)
                
            except Exception as e:
                logger.debug(f"RPC method {method} failed: {e}")
                continue
        
        return version_info

    def _extract_validator_versions(self, target: str, metrics_data: Dict) -> Dict[str, str]:
        """Extract comprehensive version info from validator metrics with fallback methods"""
        version_info = {}
        
        if not metrics_data.get("available"):
            logger.debug(f"Metrics not available for {target}")
            return version_info
        
        # Try multiple approaches to get Sui version data
        
        # Method 1: Standard metrics endpoint with fast timeout
        try:
            logger.debug(f"Fetching metrics from {metrics_data['url']}")
            response = self.session.get(metrics_data["url"], timeout=3)
            if response.status_code == 200:
                content = response.text
                logger.debug(f"Got {len(content)} bytes of metrics data")
                
                # Comprehensive metrics patterns for Sui validators
                patterns = {
                    # Core Sui version info
                    "sui_current_protocol_version": r'sui_current_protocol_version\s+(\d+)',
                    "sui_binary_max_protocol_version": r'sui_binary_max_protocol_version\s+(\d+)',
                    "sui_epoch": r'sui_epoch\s+(\d+)',
                    "sui_checkpoint_sequence_number": r'sui_checkpoint_sequence_number\s+(\d+)',
                    
                    # Build information (critical for Sui version)
                    "build_info_version": r'build_info\{[^}]*version="([^"]+)"',
                    "build_info_git_commit": r'build_info\{[^}]*git_commit="([^"]+)"',
                    "build_info_rust_version": r'build_info\{[^}]*rust_version="([^"]+)"',
                    "build_info_target": r'build_info\{[^}]*target="([^"]+)"',
                    
                    # Network detection from system metrics
                    "node_hostname": r'node_uname_info\{[^}]*nodename="([^"]+)"',
                    
                    # Consensus and network info
                    "consensus_round": r'consensus_round\s+(\d+)',
                    "narwhal_primary_current_round": r'narwhal_primary_current_round\s+(\d+)',
                    "narwhal_primary_network_peers": r'narwhal_primary_network_peers\s+(\d+)',
                    "sui_network_peers": r'sui_network_peers\s+(\d+)',
                    
                    # Database and state info
                    "sui_authority_state_total_effects": r'sui_authority_state_total_effects\s+(\d+)',
                    "sui_authority_state_total_certs": r'sui_authority_state_total_certs\s+(\d+)',
                    "sui_db_checkpoint_db_total_transactions": r'sui_db_checkpoint_db_total_transactions\s+(\d+)',
                    
                    # Performance metrics
                    "uptime_seconds": r'uptime_seconds_total\s+(\d+)',
                    "sui_validator_signature_count": r'sui_validator_signature_count\s+(\d+)',
                    
                    # Memory and resource usage
                    "memory_usage_bytes": r'memory_usage_bytes\s+(\d+)',
                    "sui_authority_state_db_size_bytes": r'sui_authority_state_db_size_bytes\s+(\d+)'
                }
                
                for key, pattern in patterns.items():
                    match = re.search(pattern, content)
                    if match:
                        version_info[key] = match.group(1)
                        logger.debug(f"Found {key}: {match.group(1)}")
                
                # Count different types of metrics for debugging
                sui_metrics_count = content.count('sui_')
                consensus_metrics_count = len([line for line in content.split('\n') if 'consensus' in line.lower() and not line.startswith('#')])
                narwhal_metrics_count = len([line for line in content.split('\n') if 'narwhal' in line.lower() and not line.startswith('#')])
                
                version_info["sui_metrics_total"] = str(sui_metrics_count)
                version_info["consensus_metrics_count"] = str(consensus_metrics_count)
                version_info["narwhal_metrics_count"] = str(narwhal_metrics_count)
                
                # Calculate uptime in human-readable format
                if "uptime_seconds" in version_info:
                    uptime_seconds = int(version_info["uptime_seconds"])
                    days = uptime_seconds // 86400
                    hours = (uptime_seconds % 86400) // 3600
                    version_info["uptime_human"] = f"{days}d {hours}h"
                
                logger.info(f"Extracted {len(version_info)} metrics from validator {target}")
                return version_info
                
            else:
                logger.warning(f"Metrics request failed with status {response.status_code}")
                
        except Exception as e:
            error_msg = str(e)
            logger.warning(f"Standard metrics extraction failed for {target}: {error_msg}")
            version_info["metrics_error"] = error_msg
            version_info["metrics_error_type"] = type(e).__name__
        
        # Method 2: Try alternative metrics paths with fast timeout
        alternative_paths = ["/metrics", "/stats", "/prometheus", "/monitoring/metrics"]
        for path in alternative_paths:
            if version_info:  # Already got data
                break
            try:
                alt_url = f"http://{target}:9100{path}"
                response = self.session.get(alt_url, timeout=2)
                if response.status_code == 200 and "sui_" in response.text:
                    logger.info(f"Found metrics at alternative path: {path}")
                    # Extract basic version info
                    version_match = re.search(r'build_info\{[^}]*version="([^"]+)"', response.text)
                    if version_match:
                        version_info["build_info_version"] = version_match.group(1)
                        version_info["metrics_source"] = f"alternative_path_{path}"
                        break
            except Exception as e:
                logger.debug(f"Alternative path {path} failed: {e}")
                continue
        
        # Method 3: Try gRPC reflection or status endpoints with fast timeout
        if not version_info:
            version_info.update(self._try_grpc_version_info(target))
        
        # Method 4: Use external tools if available with fast timeout
        if not version_info:
            version_info.update(self._try_external_sui_tools(target))
        
        # Add fallback information from other sources
        if not version_info or (len(version_info) <= 2 and "metrics_error" in version_info):
            fallback_info = {
                "metrics_status": "blocked_or_unavailable"
            }
            version_info.update(fallback_info)
        
        return version_info

    def _try_grpc_version_info(self, target: str) -> Dict[str, str]:
        """Try to get version info via gRPC endpoints"""
        version_info = {}
        
        # Try common gRPC status/health endpoints
        grpc_ports = [8080, 50051]
        
        for port in grpc_ports:
            try:
                # Try HTTP/1.1 to gRPC endpoint (some expose HTTP interfaces)
                url = f"http://{target}:{port}/health"
                response = self.session.get(url, timeout=2)
                if response.status_code == 200:
                    version_info["grpc_health_available"] = "true"
                    version_info["grpc_health_port"] = str(port)
                    break
            except Exception as e:
                logger.debug(f"gRPC health check failed on port {port}: {e}")
                continue
        
        return version_info

    def _try_external_sui_tools(self, target: str) -> Dict[str, str]:
        """Try to get Sui version using external tools"""
        version_info = {}
        
        try:
            # Try to use curl with specific Sui endpoints
            endpoints_to_try = [
                f"http://{target}:9000/",
                f"https://{target}/",
                f"http://{target}:8080/"
            ]
            
            for endpoint in endpoints_to_try:
                try:
                    response = self.session.get(endpoint, timeout=2)
                    if response.status_code == 200:
                        # Look for Sui version info in headers or response
                        if "sui" in response.headers.get("server", "").lower():
                            version_info["sui_server_header"] = response.headers["server"]
                        
                        # Check response content for version strings
                        if response.text and len(response.text) < 10000:  # Avoid huge responses
                            version_patterns = [
                                r'sui[_-]version["\s:]+([0-9]+\.[0-9]+\.[0-9]+)',
                                r'version["\s:]+([0-9]+\.[0-9]+\.[0-9]+)',
                                r'build["\s:]+([a-f0-9]{7,})'
                            ]
                            
                            for pattern in version_patterns:
                                match = re.search(pattern, response.text, re.IGNORECASE)
                                if match:
                                    version_info["detected_version"] = match.group(1)
                                    version_info["detection_source"] = endpoint
                                    break
                        
                        if version_info:
                            break
                            
                except Exception as e:
                    logger.debug(f"External tool check failed for {endpoint}: {e}")
                    continue
                    
        except Exception as e:
            logger.debug(f"External tools check failed: {e}")
        
        return version_info

    def _extract_system_state(self, target: str, rpc_data: Dict) -> Dict:
        """Extract detailed system state information"""
        return {}

    def _extract_validator_set_info(self, target: str, rpc_data: Dict) -> Dict:
        """Extract detailed validator set information"""  
        return {}

    def _extract_chain_state(self, target: str, rpc_data: Dict) -> Dict:
        """Extract chain state information"""
        return {}

    def _extract_network_metrics(self, target: str, metrics_data: Dict) -> Dict:
        """Extract detailed network and performance metrics"""
        return {}

    def _benchmark_rpc(self, target: str, rpc_data: Dict) -> Dict:
        """Enhanced RPC performance benchmarking"""
        return {}

    def _comprehensive_health_check(self, node_type: str, rpc_data: Dict, grpc_data: Dict, metrics_data: Dict, endpoint_details: Dict) -> Dict:
        """Comprehensive health assessment including endpoint analysis"""
        health = {
            "overall_status": "healthy",
            "issues": [],
            "capabilities_working": [],
            "endpoint_summary": {}
        }
        
        # Add endpoint analysis to health check
        if endpoint_details:
            health["endpoint_summary"] = {
                "total_services_detected": len(endpoint_details.get("services", {})),
                "ports_with_banners": len(endpoint_details.get("banners", {})),
                "http_endpoints": len(endpoint_details.get("http_headers", {}))
            }
        
        # Check each capability
        if rpc_data.get("working"):
            health["capabilities_working"].append("json_rpc")
            health["rpc_available"] = "True"
        else:
            health["rpc_available"] = "False"
            if node_type in ["public_rpc", "hybrid"]:
                health["issues"].append("RPC not responding")
        
        if grpc_data.get("detected"):
            health["capabilities_working"].append("grpc")
            health["grpc_available"] = "True"
        else:
            health["grpc_available"] = "False"
            if node_type in ["validator", "hybrid"]:
                health["issues"].append("gRPC not accessible")
        
        if metrics_data.get("available"):
            health["capabilities_working"].append("prometheus_metrics")
            health["metrics_available"] = "True"
        else:
            health["metrics_available"] = "False"
        
        # Determine overall status
        if health["issues"]:
            health["overall_status"] = "degraded" if len(health["issues"]) >= 2 else "partial"
        
        return health

    def _reconstruct_rpc_data(self, phase1_result: SuiNodeResult) -> Dict:
        """Reconstruct RPC data from Phase 1 results"""
        rpc_data = {"working": False, "endpoints": []}
        
        if "json_rpc" in phase1_result.capabilities:
            rpc_data["working"] = True
            # Find RPC endpoints from working_endpoints
            for endpoint in phase1_result.working_endpoints:
                if endpoint.startswith(("http://", "https://")) and not endpoint.startswith("grpc://"):
                    rpc_data["endpoints"].append(endpoint)
        
        return rpc_data

    def _reconstruct_grpc_data(self, phase1_result: SuiNodeResult) -> Dict:
        """Reconstruct gRPC data from Phase 1 results"""
        grpc_data = {"detected": False, "ports": []}
        
        if "grpc" in phase1_result.capabilities:
            grpc_data["detected"] = True
            # Find gRPC ports
            for port in [8080, 50051]:
                if port in phase1_result.accessible_ports:
                    grpc_data["ports"].append(port)
        
        return grpc_data

    def _reconstruct_metrics_data(self, phase1_result: SuiNodeResult) -> Dict:
        """Reconstruct metrics data from Phase 1 results"""
        metrics_data = {"available": False, "url": None}
        
        # Check for metrics capability OR if port 9100 is open (correct Prometheus port)
        has_metrics_capability = "prometheus_metrics" in phase1_result.capabilities
        has_metrics_port = 9100 in phase1_result.accessible_ports
        
        if has_metrics_capability or has_metrics_port:
            metrics_data["available"] = True
            metrics_data["url"] = f"http://{phase1_result.ip}:9100/metrics"
            logger.debug(f"Reconstructed metrics data for {phase1_result.ip}: {metrics_data}")
        else:
            logger.debug(f"No metrics available for {phase1_result.ip} - capabilities: {phase1_result.capabilities}, ports: {phase1_result.accessible_ports}")
        
        return metrics_data

    @classmethod
    def load_results_from_file(cls, file_path: str) -> List[SuiNodeResult]:
        """Load discovery results from JSON file"""
        if not Path(file_path).exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        results = []
        
        # Handle new data/meta format and legacy formats
        if isinstance(data, dict) and "data" in data:
            # New data/meta format
            nodes_data = []
            for item in data["data"]:
                if isinstance(item, dict) and "payload" in item:
                    nodes_data.append(item["payload"])
                else:
                    nodes_data.append(item)
        elif isinstance(data, dict) and "nodes" in data:
            # Legacy format
            nodes_data = data["nodes"]
        elif isinstance(data, list):
            # Direct array format
            nodes_data = data
        else:
            raise ValueError("Invalid data format")
        
        for node_data in nodes_data:
            result = SuiNodeResult(
                ip=node_data["ip"],
                hostname=node_data.get("hostname", node_data["ip"]),
                discovered_at=node_data.get("discovered_at", ""),
                node_type=node_data.get("type", node_data.get("node_type", "unknown")),
                confidence=node_data.get("confidence", 0.0),
                capabilities=node_data.get("capabilities", []),
                network=node_data.get("network", "unknown"),
                discovery_time=node_data.get("discovery_time", 0.0),
                classification_time=node_data.get("classification_time", 0.0),
                total_scan_time=node_data.get("scan_time", node_data.get("total_scan_time", 0.0)),
                accessible_ports=node_data.get("accessible_ports", []),
                working_endpoints=node_data.get("working_endpoints", []),
                analysis_level=node_data.get("analysis_level", "discovery")
            )
            results.append(result)
        
        logger.info(f"Loaded {len(results)} discovery results for deep analysis")
        return results

    def export_results(self, results: List[SuiNodeResult], format: str = "json", pretty: bool = False) -> str:
        """Export scan results with analysis level awareness - returns JSON by default"""
        # Always return JSON format regardless of format parameter
        # The library should only output JSON
        
        # Build the structured output with data/meta format
        analysis_levels = set(r.analysis_level for r in results) if results else set()
        has_deep_analysis = any(level == "deep" for level in analysis_levels)
        
        scanner_version = "Enhanced Rich Endpoint v2.0"
        if has_deep_analysis:
            scanner_version += " (Deep Analysis)"
        else:
            scanner_version += " (Discovery Mode)"
        
        # Calculate distributions for meta
        by_type = {}
        by_network = {}
        analysis_level_counts = {}
        
        for result in results:
            by_type[result.node_type] = by_type.get(result.node_type, 0) + 1
            by_network[result.network] = by_network.get(result.network, 0) + 1
            analysis_level_counts[result.analysis_level] = analysis_level_counts.get(result.analysis_level, 0) + 1
        
        # Build data array - each result becomes a data item with type and payload
        data = []
        for result in results:
            data_item = {
                "type": "sui_node",
                "payload": asdict(result)
            }
            data.append(data_item)
        
        # Build meta object with scan information
        scan_start_time = int(time.time() - (results[0].total_scan_time if results else 0))
        scan_end_time = int(time.time())
        
        output = {
            "data": data,
            "meta": {
                "operation": "target_scan",
                "stage": "scan",
                "scan_level": "deep" if has_deep_analysis else "discovery",
                "scan_duration": int(sum(r.total_scan_time for r in results) / len(results) if results else 0),
                "scanners_used": ["sui_scanner"],
                "tools_used": ["enhanced_sui_scanner"],
                "total_scan_duration": int(sum(r.total_scan_time for r in results) if results else 0),
                "targets_scanned": len(results),
                "scanner_version": scanner_version,
                "scan_start": scan_start_time,
                "scan_end": scan_end_time,
                "total_nodes": len(results),
                "by_type": by_type,
                "by_network": by_network,
                "analysis_levels": analysis_level_counts
            }
        }
        
        if pretty:
            return json.dumps(output, indent=2)
        return json.dumps(output, separators=(',', ':'))
        
        # Legacy summary format logic (now unused but kept for reference)
        if False:  # format == "summary":
            # Determine analysis types present
            analysis_levels = set(r.analysis_level for r in results)
            has_deep_analysis = any(level == "deep" for level in analysis_levels)
            
            scanner_version = "Enhanced Rich Endpoint v2.0"
            if has_deep_analysis:
                scanner_version += " (Deep Analysis)"
            else:
                scanner_version += " (Discovery Mode)"
            
            summary = {
                "scan_summary": {
                    "total_nodes": len(results),
                    "scan_timestamp": datetime.now().isoformat(),
                    "scanner_version": scanner_version,
                    "by_type": {},
                    "by_network": {},
                    "average_scan_time": sum(r.total_scan_time for r in results) / len(results) if results else 0,
                    "capabilities_distribution": {},
                    "analysis_levels": {},
                    "endpoint_statistics": {}
                },
                "nodes": []
            }
            
            # Calculate distributions and statistics
            total_services = 0
            total_ssl_certs = 0
            
            for result in results:
                # Type and network distribution
                summary["scan_summary"]["by_type"][result.node_type] = summary["scan_summary"]["by_type"].get(result.node_type, 0) + 1
                summary["scan_summary"]["by_network"][result.network] = summary["scan_summary"]["by_network"].get(result.network, 0) + 1
                
                # Capabilities distribution
                for cap in result.capabilities:
                    summary["scan_summary"]["capabilities_distribution"][cap] = summary["scan_summary"]["capabilities_distribution"].get(cap, 0) + 1
                
                # Analysis level distribution
                summary["scan_summary"]["analysis_levels"][result.analysis_level] = summary["scan_summary"]["analysis_levels"].get(result.analysis_level, 0) + 1
                
                # Endpoint statistics (only for deep analysis)
                if result.endpoint_details:
                    total_services += len(result.endpoint_details.get("services", {}))
                if result.ssl_info:
                    total_ssl_certs += result.ssl_info.get("certificate_count", 0)
                
                # Create comprehensive node summary
                node_summary = {
                    "ip": result.ip,
                    "hostname": result.hostname,
                    "type": result.node_type,
                    "network": result.network,
                    "confidence": result.confidence,
                    "capabilities": result.capabilities,
                    "scan_time": result.total_scan_time,
                    "endpoints": len(result.working_endpoints),
                    "accessible_ports": result.accessible_ports,
                    "working_endpoints": result.working_endpoints,
                    "discovered_at": result.discovered_at,
                    "analysis_level": result.analysis_level
                }
                
                # Add deep analysis data only if available
                if result.analysis_level == "deep":
                    if result.endpoint_details:
                        node_summary["port_analysis"] = result.endpoint_details
                    
                    if result.service_versions:
                        node_summary["service_versions"] = result.service_versions
                    
                    if result.ssl_info:
                        node_summary["ssl_info"] = result.ssl_info
                    
                    if result.security_scan:
                        node_summary["port_categorization"] = result.security_scan
                
                # Add Sui-specific data (available in both modes, but richer in deep mode)
                if result.version_info:
                    node_summary["version"] = result.version_info
                    
                if result.health_status:
                    node_summary["health"] = result.health_status
                
                if result.performance_metrics:
                    node_summary["performance"] = result.performance_metrics
                
                summary["nodes"].append(node_summary)
            
            # Add endpoint statistics
            summary["scan_summary"]["endpoint_statistics"] = {
                "total_services_detected": total_services,
                "total_ssl_certificates": total_ssl_certs,
                "nodes_with_deep_analysis": len([r for r in results if r.analysis_level == "deep"])
            }
            
            if pretty:
                return json.dumps(summary, indent=2)
            return json.dumps(summary, separators=(',', ':'))
        
        return str(results)

