#!/usr/bin/env python3
"""
Sui Protobuf Manager
Manages Sui protobuf compilation and gRPC stub generation
"""

import os
import sys
import logging
from pathlib import Path
from typing import Optional, Dict

logger = logging.getLogger(__name__)


class SuiProtobufManager:
    """
    Manages Sui protobuf compilation and gRPC stub generation
    """
    
    def __init__(self, sui_repo_path: Optional[str] = None):
        self.sui_repo_path = sui_repo_path or self._find_sui_repo()
        self.proto_dir = None
        self.generated_dir = Path("./generated_protos")
        self.stubs_ready = False
        
    def _find_sui_repo(self) -> Optional[str]:
        """Find Sui repository in common locations"""
        # First check environment variable
        if os.environ.get("SUI_REPO_PATH"):
            sui_path = Path(os.environ["SUI_REPO_PATH"]).expanduser()
            if sui_path.exists() and (sui_path / "crates").exists():
                logger.info(f"Found Sui repository via SUI_REPO_PATH: {sui_path}")
                return str(sui_path)
            else:
                logger.warning(f"SUI_REPO_PATH points to invalid location: {sui_path}")
        
        # Common repository locations
        common_paths = [
            "./sui",
            "../sui", 
            "~/sui",
            "~/code/sui",
            "~/Code/sui", 
            "~/src/sui",
            "~/Developer/sui",
            "/opt/sui",
            "/usr/local/src/sui"
        ]
        
        for path in common_paths:
            expanded_path = Path(path).expanduser()
            if expanded_path.exists() and (expanded_path / "crates").exists():
                logger.info(f"Found Sui repository at: {expanded_path}")
                return str(expanded_path)
        
        # Check if we can find it via git or other methods
        sui_from_git = self._find_sui_via_git()
        if sui_from_git:
            return sui_from_git
        
        return None
    
    def _find_sui_via_git(self) -> Optional[str]:
        """Try to find Sui repository using git or other methods"""
        try:
            # Look for recently used git repositories that might be Sui
            import subprocess
            
            # Check if we're in a Sui repo
            try:
                result = subprocess.run(['git', 'remote', 'get-url', 'origin'], 
                                      capture_output=True, text=True, timeout=5)
                if result.returncode == 0 and 'sui' in result.stdout.lower():
                    # We might be in or near a Sui repo
                    potential_paths = ['.', '..', '../sui']
                    for path in potential_paths:
                        expanded_path = Path(path).expanduser().resolve()
                        if (expanded_path / "crates").exists():
                            logger.info(f"Found Sui repository via git detection: {expanded_path}")
                            return str(expanded_path)
            except Exception:
                pass
                
        except Exception as e:
            logger.debug(f"Git-based Sui detection failed: {e}")
        
        return None
    
    def _check_sui_binary(self) -> Dict[str, any]:
        """Check if Sui binary is available and get version info"""
        try:
            import subprocess
            result = subprocess.run(['sui', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                version_info = result.stdout.strip()
                logger.info(f"Sui binary found: {version_info}")
                return {
                    "available": True,
                    "version": version_info,
                    "binary_path": "sui"  # Could be expanded to find full path
                }
        except Exception as e:
            logger.debug(f"Sui binary check failed: {e}")
        
        return {"available": False}
    
    def setup_protobufs(self) -> bool:
        """Setup and compile Sui protobufs for gRPC"""
        if not self.sui_repo_path:
            # Check if Sui binary is available
            sui_binary_info = self._check_sui_binary()
            
            if sui_binary_info["available"]:
                logger.info("Sui source repository not found, but Sui binary is available.")
                logger.info(f"Binary version: {sui_binary_info['version']}")
                logger.info("Note: gRPC functionality requires Sui source repository with .proto files")
                logger.info("gRPC enables: validator consensus data, real-time transaction streaming, advanced node introspection")
            else:
                logger.info("Neither Sui source repository nor Sui binary found.")
                
            logger.info("To enable full gRPC functionality (optional for most use cases):")
            logger.info("  1. Set SUI_REPO_PATH environment variable to your Sui source directory")
            logger.info("  2. Or clone Sui repository: git clone https://github.com/MystenLabs/sui.git")
            logger.debug("Checked locations: ~/sui, ~/Code/sui, ~/code/sui, ~/src/sui, ~/Developer/sui")
            return False
        
        try:
            # Find proto files in Sui repository
            sui_path = Path(self.sui_repo_path)
            proto_files = list(sui_path.rglob("*.proto"))
            
            if not proto_files:
                logger.warning(f"No .proto files found in {sui_path}")
                return False
            
            logger.info(f"Found {len(proto_files)} proto files")
            
            # Create output directory
            self.generated_dir.mkdir(exist_ok=True)
            (self.generated_dir / "__init__.py").touch()
            
            # Compile proto files
            for proto_file in proto_files:
                self._compile_proto(proto_file)
            
            # Add generated directory to Python path
            if str(self.generated_dir) not in sys.path:
                sys.path.insert(0, str(self.generated_dir))
            
            self.stubs_ready = True
            logger.info("Sui protobufs compiled successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup protobufs: {e}")
            return False
    
    def _compile_proto(self, proto_file: Path):
        """Compile a single proto file"""
        try:
            # Determine the relative path and package structure
            proto_dir = proto_file.parent
            relative_path = proto_file.relative_to(self.sui_repo_path)
            
            # Use grpcio-tools to compile
            from grpc_tools import protoc
            
            protoc_args = [
                "grpc_tools.protoc",
                f"--python_out={self.generated_dir}",
                f"--grpc_python_out={self.generated_dir}",
                f"--proto_path={self.sui_repo_path}",
                str(proto_file)
            ]
            
            result = protoc.main(protoc_args)
            if result == 0:
                logger.debug(f"Compiled: {relative_path}")
            else:
                logger.warning(f"Failed to compile: {relative_path}")
                
        except Exception as e:
            logger.debug(f"Error compiling {proto_file}: {e}")
    
    def get_sui_grpc_stubs(self):
        """Get compiled Sui gRPC stubs"""
        if not self.stubs_ready:
            return None
        
        try:
            # Try to import generated stubs
            # This would depend on the actual Sui proto structure
            stubs = {}
            
            # Look for common Sui gRPC services
            for proto_file in self.generated_dir.glob("*_pb2_grpc.py"):
                module_name = proto_file.stem.replace("_pb2_grpc", "")
                try:
                    # Dynamic import of generated stubs
                    import importlib.util
                    spec = importlib.util.spec_from_file_location(
                        f"{module_name}_grpc", proto_file
                    )
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    stubs[module_name] = module
                except Exception as e:
                    logger.debug(f"Failed to import {module_name}: {e}")
            
            return stubs if stubs else None
            
        except Exception as e:
            logger.error(f"Failed to get gRPC stubs: {e}")
            return None