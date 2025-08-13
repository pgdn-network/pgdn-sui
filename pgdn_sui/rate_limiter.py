#!/usr/bin/env python3
"""
Token Bucket Rate Limiter
Implements per-hostname rate limiting with 1 request/second and burst capacity
"""

import time
import threading
from typing import Dict, Optional
from urllib.parse import urlparse
import logging

logger = logging.getLogger(__name__)


class TokenBucket:
    """
    Token bucket rate limiter implementation
    - Allows burst capacity up to max_tokens
    - Refills at specified rate (tokens per second)
    """
    
    def __init__(self, max_tokens: int = 3, refill_rate: float = 1.0):
        """
        Initialize token bucket
        
        Args:
            max_tokens: Maximum tokens in bucket (burst capacity)
            refill_rate: Rate at which tokens are added (tokens per second)
        """
        self.max_tokens = max_tokens
        self.refill_rate = refill_rate
        self.tokens = max_tokens
        self.last_refill = time.time()
        self.lock = threading.Lock()
        
    def consume(self, tokens: int = 1) -> bool:
        """
        Try to consume tokens from the bucket
        
        Args:
            tokens: Number of tokens to consume
            
        Returns:
            True if tokens were consumed, False if not enough tokens available
        """
        with self.lock:
            self._refill()
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False
    
    def _refill(self):
        """Refill tokens based on time elapsed"""
        now = time.time()
        time_elapsed = now - self.last_refill
        
        # Add tokens based on elapsed time and refill rate
        tokens_to_add = time_elapsed * self.refill_rate
        self.tokens = min(self.max_tokens, self.tokens + tokens_to_add)
        self.last_refill = now
    
    def available_tokens(self) -> float:
        """Get current number of available tokens"""
        with self.lock:
            self._refill()
            return self.tokens
    
    def time_until_token(self) -> float:
        """Get time in seconds until next token is available"""
        with self.lock:
            self._refill()
            if self.tokens >= 1:
                return 0.0
            
            # Calculate time needed for one token
            tokens_needed = 1.0 - self.tokens
            return tokens_needed / self.refill_rate


class HostRateLimiter:
    """
    Per-hostname rate limiter using token buckets
    - 1 request per second base rate
    - Burst up to 3 requests
    """
    
    def __init__(self, max_tokens: int = 3, refill_rate: float = 1.0):
        """
        Initialize host rate limiter
        
        Args:
            max_tokens: Maximum burst tokens per host
            refill_rate: Token refill rate per second
        """
        self.max_tokens = max_tokens
        self.refill_rate = refill_rate
        self.host_buckets: Dict[str, TokenBucket] = {}
        self.lock = threading.Lock()
        
    def extract_hostname(self, url_or_ip: str) -> str:
        """
        Extract hostname/IP from URL or return IP as-is
        
        Args:
            url_or_ip: URL or IP address
            
        Returns:
            Hostname or IP
        """
        if "://" in url_or_ip:
            # Parse URL to extract hostname
            parsed = urlparse(url_or_ip)
            return parsed.hostname or parsed.netloc.split(':')[0]
        else:
            # Assume it's already a hostname/IP
            return url_or_ip
    
    def get_bucket(self, hostname: str) -> TokenBucket:
        """
        Get or create token bucket for hostname
        
        Args:
            hostname: Hostname or IP address
            
        Returns:
            TokenBucket instance for the hostname
        """
        with self.lock:
            if hostname not in self.host_buckets:
                self.host_buckets[hostname] = TokenBucket(
                    max_tokens=self.max_tokens,
                    refill_rate=self.refill_rate
                )
                logger.debug(f"Created token bucket for host: {hostname}")
            return self.host_buckets[hostname]
    
    def can_make_request(self, url_or_hostname: str) -> bool:
        """
        Check if request can be made to hostname
        
        Args:
            url_or_hostname: URL or hostname to check
            
        Returns:
            True if request is allowed, False if rate limited
        """
        hostname = self.extract_hostname(url_or_hostname)
        bucket = self.get_bucket(hostname)
        
        can_proceed = bucket.consume(1)
        
        if can_proceed:
            logger.debug(f"Request allowed for {hostname} (tokens remaining: {bucket.available_tokens():.2f})")
        else:
            wait_time = bucket.time_until_token()
            logger.info(f"Rate limited {hostname} - next token in {wait_time:.2f}s")
            
        return can_proceed
    
    def wait_for_token(self, url_or_hostname: str, timeout: float = 5.0) -> bool:
        """
        Wait for token to become available
        
        Args:
            url_or_hostname: URL or hostname
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if token obtained, False if timed out
        """
        hostname = self.extract_hostname(url_or_hostname)
        bucket = self.get_bucket(hostname)
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if bucket.consume(1):
                logger.debug(f"Token obtained for {hostname} after waiting")
                return True
            
            # Wait a short time before retrying
            wait_time = min(bucket.time_until_token(), 0.1)
            if wait_time > 0:
                time.sleep(wait_time)
        
        logger.warning(f"Timeout waiting for token for {hostname}")
        return False
    
    def get_bucket_status(self, url_or_hostname: str) -> Dict:
        """
        Get bucket status for hostname
        
        Args:
            url_or_hostname: URL or hostname
            
        Returns:
            Dictionary with bucket status information
        """
        hostname = self.extract_hostname(url_or_hostname)
        bucket = self.get_bucket(hostname)
        
        return {
            "hostname": hostname,
            "available_tokens": bucket.available_tokens(),
            "max_tokens": bucket.max_tokens,
            "refill_rate": bucket.refill_rate,
            "time_until_next_token": bucket.time_until_token()
        }
    
    def reset_bucket(self, url_or_hostname: str):
        """
        Reset bucket for hostname (useful for testing)
        
        Args:
            url_or_hostname: URL or hostname
        """
        hostname = self.extract_hostname(url_or_hostname)
        with self.lock:
            if hostname in self.host_buckets:
                del self.host_buckets[hostname]
                logger.debug(f"Reset bucket for {hostname}")
    
    def get_all_buckets_status(self) -> Dict[str, Dict]:
        """
        Get status of all active buckets
        
        Returns:
            Dictionary mapping hostnames to bucket status
        """
        status = {}
        with self.lock:
            for hostname in list(self.host_buckets.keys()):
                bucket = self.host_buckets[hostname]
                status[hostname] = {
                    "available_tokens": bucket.available_tokens(),
                    "max_tokens": bucket.max_tokens,
                    "refill_rate": bucket.refill_rate,
                    "time_until_next_token": bucket.time_until_token()
                }
        return status


# Global rate limiter instance
# Requirements: 1 request/second with burst up to 3 requests
_global_rate_limiter = HostRateLimiter(max_tokens=3, refill_rate=1.0)


def get_rate_limiter() -> HostRateLimiter:
    """Get the global rate limiter instance"""
    return _global_rate_limiter


def can_make_request(url_or_hostname: str) -> bool:
    """
    Convenience function to check if request can be made
    
    Args:
        url_or_hostname: URL or hostname to check
        
    Returns:
        True if request is allowed, False if rate limited
    """
    return _global_rate_limiter.can_make_request(url_or_hostname)


def wait_for_token(url_or_hostname: str, timeout: float = 5.0) -> bool:
    """
    Convenience function to wait for token
    
    Args:
        url_or_hostname: URL or hostname
        timeout: Maximum wait time in seconds
        
    Returns:
        True if token obtained, False if timed out
    """
    return _global_rate_limiter.wait_for_token(url_or_hostname, timeout)


def get_bucket_status(url_or_hostname: str) -> Dict:
    """
    Convenience function to get bucket status
    
    Args:
        url_or_hostname: URL or hostname
        
    Returns:
        Bucket status dictionary
    """
    return _global_rate_limiter.get_bucket_status(url_or_hostname)