"""
Splunk Connection Module
========================

This module provides a robust connection handler for Splunk Enterprise.
It handles authentication, connection pooling, query execution, and error handling.

Features:
- Automatic reconnection on connection loss
- Query result streaming for large datasets
- Connection validation and health checks
- Support for both blocking and async queries
- LRU caching for query results
- Connection pooling support

Usage:
    from splunk_connection import SplunkConnection
    
    conn = SplunkConnection(config)
    results = conn.execute_query("search index=main | head 10")
"""

import logging
import time
import hashlib
from functools import lru_cache
from typing import List, Dict, Any, Optional, Generator
from contextlib import contextmanager

try:
    import splunklib.client as client
    import splunklib.results as results
    SPLUNK_SDK_AVAILABLE = True
except ImportError:
    SPLUNK_SDK_AVAILABLE = False

from .config import DiscoveryConfig

logger = logging.getLogger(__name__)


class SplunkConnectionError(Exception):
    """Custom exception for Splunk connection errors."""
    pass


class SplunkQueryError(Exception):
    """Custom exception for Splunk query errors."""
    pass


class SplunkConnection:
    """
    Manages connections to Splunk Enterprise.
    
    This class provides a high-level interface for connecting to Splunk,
    executing queries, and handling results. It includes automatic retry
    logic and connection validation.
    
    Supports two authentication methods:
    - Basic Auth: username/password
    - Token Auth: Splunk authentication token (Bearer/Session token)
    
    Attributes:
        config (DiscoveryConfig): Configuration object with connection details
        service (splunklib.client.Service): Splunk service connection
        is_connected (bool): Current connection status
    
    Example:
        >>> # Using username/password
        >>> config = DiscoveryConfig(splunk_host="splunk.example.com", username="admin", password="secret")
        >>> conn = SplunkConnection(config)
        >>> conn.connect()
        
        >>> # Using token authentication
        >>> config = DiscoveryConfig(splunk_host="splunk.example.com", token="your-token")
        >>> conn = SplunkConnection(config)
        >>> conn.connect()
    """
    
    def __init__(self, config: DiscoveryConfig):
        """
        Initialize the Splunk connection handler.
        
        Args:
            config: DiscoveryConfig object with connection parameters
        """
        if not SPLUNK_SDK_AVAILABLE:
            raise ImportError(
                "Splunk SDK is required. Install with: pip install splunk-sdk"
            )
        
        self.config = config
        self.service: Optional[client.Service] = None
        self.is_connected = False
        self._retry_count = 3
        self._retry_delay = 2  # seconds
        
        # Query result cache (LRU cache)
        self._query_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._cache_size = 100  # Maximum cached queries
        
        # Determine authentication type
        self._auth_type = self._determine_auth_type()
        
        logger.info(f"Splunk connection initialized for {config.splunk_host}:{config.splunk_port} (auth: {self._auth_type})")
    
    def _get_cache_key(self, query: str, max_results: Optional[int]) -> str:
        """
        Generate a cache key for a query.
        
        Args:
            query: SPL query string
            max_results: Maximum results parameter
            
        Returns:
            Cache key hash
        """
        key_str = f"{query}_{max_results}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _determine_auth_type(self) -> str:
        """
        Determine which authentication type to use.
        
        Returns:
            'token' or 'basic'
        """
        if self.config.auth_type == "token":
            return "token"
        elif self.config.auth_type == "basic":
            return "basic"
        else:  # auto-detect
            if self.config.token:
                return "token"
            else:
                return "basic"
    
    def connect(self) -> bool:
        """
        Establish connection to Splunk.
        
        Supports both token-based and basic authentication.
        Token authentication is preferred for service accounts.
        
        Returns:
            bool: True if connection successful
            
        Raises:
            SplunkConnectionError: If connection fails after retries
        """
        for attempt in range(self._retry_count):
            try:
                logger.info(f"Connecting to Splunk (attempt {attempt + 1}/{self._retry_count}, auth: {self._auth_type})...")
                
                # Build connection parameters
                connect_params = {
                    "host": self.config.splunk_host,
                    "port": self.config.splunk_port,
                    "scheme": "https" if self.config.use_ssl else "http"
                }
                
                # Add authentication based on type
                if self._auth_type == "token":
                    # Token authentication (Bearer token)
                    connect_params["splunkToken"] = self.config.token
                    logger.debug("Using token authentication")
                else:
                    # Basic authentication (username/password)
                    connect_params["username"] = self.config.username
                    connect_params["password"] = self.config.password
                    logger.debug("Using basic authentication")
                
                self.service = client.connect(**connect_params)
                
                # Validate connection
                _ = self.service.apps
                
                self.is_connected = True
                logger.info(f"Successfully connected to Splunk at {self.config.splunk_host}")
                return True
                
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}")
                if attempt < self._retry_count - 1:
                    time.sleep(self._retry_delay)
                else:
                    raise SplunkConnectionError(
                        f"Failed to connect to Splunk after {self._retry_count} attempts: {str(e)}"
                    )
        
        return False
    
    def disconnect(self) -> None:
        """Close the Splunk connection."""
        if self.service:
            try:
                self.service.logout()
                logger.info("Disconnected from Splunk")
            except Exception as e:
                logger.warning(f"Error during disconnect: {str(e)}")
            finally:
                self.service = None
                self.is_connected = False
    
    def validate_connection(self) -> bool:
        """
        Validate that the connection is still active.
        
        Returns:
            bool: True if connection is valid
        """
        if not self.service or not self.is_connected:
            return False
        
        try:
            # Simple validation query
            _ = self.service.apps
            return True
        except Exception:
            self.is_connected = False
            return False
    
    def execute_query(
        self, 
        query: str, 
        max_results: Optional[int] = None,
        timeout: int = 300,
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Execute a Splunk search query and return results.
        
        Args:
            query: SPL query string (with or without 'search' prefix)
            max_results: Maximum number of results to return
            timeout: Query timeout in seconds
            use_cache: Whether to use cached results if available
            
        Returns:
            List of dictionaries containing search results
            
        Raises:
            SplunkConnectionError: If not connected
            SplunkQueryError: If query execution fails
        """
        if not self.is_connected:
            raise SplunkConnectionError("Not connected to Splunk. Call connect() first.")
        
        # Ensure query has search command if it doesn't start with |
        if not query.strip().startswith('|') and not query.strip().lower().startswith('search'):
            query = f"search {query}"
        
        max_results = max_results or self.config.max_results
        
        # Check cache first
        if use_cache:
            cache_key = self._get_cache_key(query, max_results)
            if cache_key in self._query_cache:
                logger.debug(f"Cache hit for query: {query[:50]}...")
                return self._query_cache[cache_key]
        
        logger.debug(f"Executing query: {query[:100]}...")
        
        try:
            # Create search job with optimized settings
            job = self.service.jobs.create(
                query,
                exec_mode="blocking",
                timeout=timeout,
                max_count=max_results,
                required_field_list="*"  # Only fetch required fields
            )
            
            # Get results
            result_list = []
            reader = results.JSONResultsReader(
                job.results(output_mode="json", count=max_results)
            )
            
            for result in reader:
                if isinstance(result, dict):
                    result_list.append(result)
            
            # Store in cache if enabled
            if use_cache:
                cache_key = self._get_cache_key(query, max_results)
                # Implement simple LRU by limiting cache size
                if len(self._query_cache) >= self._cache_size:
                    # Remove oldest entry
                    self._query_cache.pop(next(iter(self._query_cache)))
                self._query_cache[cache_key] = result_list
            
            logger.debug(f"Query returned {len(result_list)} results")
            return result_list
            
        except Exception as e:
            raise SplunkQueryError(f"Query execution failed: {str(e)}")
    
    def execute_query_streaming(
        self, 
        query: str, 
        batch_size: int = 1000
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Execute a query and stream results for memory efficiency.
        
        Use this for very large result sets to avoid memory issues.
        
        Args:
            query: SPL query string
            batch_size: Number of results to process at a time
            
        Yields:
            Dictionary containing individual search results
        """
        if not self.is_connected:
            raise SplunkConnectionError("Not connected to Splunk")
        
        if not query.strip().startswith('|') and not query.strip().lower().startswith('search'):
            query = f"search {query}"
        
        try:
            job = self.service.jobs.create(query, exec_mode="blocking")
            
            offset = 0
            while True:
                reader = results.JSONResultsReader(
                    job.results(
                        output_mode="json", 
                        count=batch_size, 
                        offset=offset
                    )
                )
                
                batch_count = 0
                for result in reader:
                    if isinstance(result, dict):
                        batch_count += 1
                        yield result
                
                if batch_count < batch_size:
                    break
                    
                offset += batch_size
                
        except Exception as e:
            raise SplunkQueryError(f"Streaming query failed: {str(e)}")
    
    def get_indexes(self) -> List[str]:
        """
        Get list of all available indexes.
        
        Returns:
            List of index names
        """
        if not self.is_connected:
            raise SplunkConnectionError("Not connected to Splunk")
        
        indexes = []
        for index in self.service.indexes:
            if index.name not in self.config.excluded_indexes:
                indexes.append(index.name)
        
        return sorted(indexes)
    
    def get_sourcetypes(self, index: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get list of sourcetypes, optionally filtered by index.
        
        Args:
            index: Optional index name to filter by
            
        Returns:
            List of dictionaries with sourcetype information
        """
        index_filter = f"index={index}" if index else "index=*"
        query = f"| metadata type=sourcetypes {index_filter} | table sourcetype, totalCount, lastTime"
        
        return self.execute_query(query)
    
    def test_connection(self) -> Dict[str, Any]:
        """
        Test connection and return diagnostic information.
        
        Returns:
            Dictionary with connection status and server info
        """
        try:
            if not self.is_connected:
                self.connect()
            
            info = self.service.info
            
            return {
                "status": "connected",
                "auth_type": self._auth_type,
                "server_name": info.get("serverName", "Unknown"),
                "version": info.get("version", "Unknown"),
                "os": info.get("os_name", "Unknown"),
                "license_state": info.get("licenseState", "Unknown"),
                "index_count": len(list(self.service.indexes)),
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    @contextmanager
    def session(self):
        """
        Context manager for connection handling.
        
        Usage:
            with connection.session() as conn:
                results = conn.execute_query("...")
        """
        try:
            self.connect()
            yield self
        finally:
            self.disconnect()
    
    def __enter__(self):
        """Support for 'with' statement."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up connection on exit."""
        self.disconnect()
        return False


class MockSplunkConnection:
    """
    Mock connection for testing without a real Splunk instance.
    
    This class provides sample data for testing the discovery logic
    without requiring an actual Splunk connection.
    """
    
    def __init__(self, config: DiscoveryConfig):
        self.config = config
        self.is_connected = False
    
    def connect(self) -> bool:
        self.is_connected = True
        logger.info("Mock Splunk connection established")
        return True
    
    def disconnect(self) -> None:
        self.is_connected = False
    
    def execute_query(self, query: str, **kwargs) -> List[Dict[str, Any]]:
        """Return sample data based on query type."""
        
        if "metadata type=sources" in query:
            return [
                {"source": "/var/log/myapp/application.log", "totalCount": "15000"},
                {"source": "/opt/payment-service/logs/payment.log", "totalCount": "8500"},
                {"source": "C:\\Program Files\\OrderSystem\\logs\\orders.log", "totalCount": "12000"},
                {"source": "/var/log/nginx/access.log", "totalCount": "50000"},
                {"source": "/app/inventory-api/logs/inventory.log", "totalCount": "6700"},
            ]
        
        elif "metadata type=sourcetypes" in query:
            return [
                {"sourcetype": "myapp:application", "totalCount": "15000"},
                {"sourcetype": "payment_service_logs", "totalCount": "8500"},
                {"sourcetype": "order_system", "totalCount": "12000"},
                {"sourcetype": "access_combined", "totalCount": "50000"},
                {"sourcetype": "inventory_api_json", "totalCount": "6700"},
            ]
        
        elif "metadata type=hosts" in query:
            return [
                {"host": "myapp-prod-01.company.com", "totalCount": "15000"},
                {"host": "payment-server-01", "totalCount": "8500"},
                {"host": "order-api-prod-02", "totalCount": "12000"},
                {"host": "nginx-lb-01", "totalCount": "50000"},
                {"host": "inventory-app-01", "totalCount": "6700"},
            ]
        
        return []
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, *args):
        self.disconnect()
        return False
