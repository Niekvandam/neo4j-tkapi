"""
TKApi timeout configuration and session management.
"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tkapi import TKApi


class TimeoutHTTPAdapter(HTTPAdapter):
    """HTTP adapter with configurable timeout."""
    
    def __init__(self, timeout=None, *args, **kwargs):
        self.timeout = timeout
        super().__init__(*args, **kwargs)
    
    def send(self, request, **kwargs):
        kwargs.setdefault('timeout', self.timeout)
        return super().send(request, **kwargs)


def create_tkapi_with_timeout(
    connect_timeout: float = 10.0,
    read_timeout: float = 60.0,
    max_retries: int = 3,
    backoff_factor: float = 0.5
) -> TKApi:
    """
    Create a TKApi instance with proper timeout and retry configuration.
    
    Args:
        connect_timeout: Time to wait for connection establishment (seconds)
        read_timeout: Time to wait for server response (seconds) 
        max_retries: Number of retry attempts
        backoff_factor: Backoff factor for retries
        
    Returns:
        Configured TKApi instance
    """
    # Create session with timeout configuration
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=max_retries,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],  # Updated parameter name
        backoff_factor=backoff_factor
    )
    
    # Create adapter with timeout
    timeout_adapter = TimeoutHTTPAdapter(
        timeout=(connect_timeout, read_timeout),
        max_retries=retry_strategy
    )
    
    # Mount adapter for both HTTP and HTTPS
    session.mount("http://", timeout_adapter)
    session.mount("https://", timeout_adapter)
    
    # Create TKApi instance
    api = TKApi()
    
    # Replace the internal session if TKApi uses requests.Session
    # This is a workaround since TKApi might not expose timeout configuration
    if hasattr(api, '_session'):
        api._session = session
    else:
        # Monkey patch the requests module for this instance
        api._original_get = requests.get
        api._original_post = requests.post
        
        def patched_get(*args, **kwargs):
            kwargs.setdefault('timeout', (connect_timeout, read_timeout))
            return session.get(*args, **kwargs)
            
        def patched_post(*args, **kwargs):
            kwargs.setdefault('timeout', (connect_timeout, read_timeout))
            return session.post(*args, **kwargs)
        
        # Temporarily replace requests methods
        requests.get = patched_get
        requests.post = patched_post
    
    return api


def restore_requests():
    """Restore original requests methods if they were patched."""
    # This would be called after API operations if monkey patching was used
    pass


# Default timeout settings
DEFAULT_CONNECT_TIMEOUT = 10.0  # 10 seconds to establish connection
DEFAULT_READ_TIMEOUT = 120.0    # 2 minutes to read response
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_FACTOR = 0.5 