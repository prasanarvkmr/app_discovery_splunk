"""
Configuration Module for Splunk Application Discovery
======================================================

This module handles all configuration settings for the discovery tool including:
- Splunk connection parameters
- Discovery timeframes
- Pattern matching rules
- Filtering and exclusion rules

Configuration can be loaded from:
- Environment variables
- Configuration files (YAML/JSON)
- Direct initialization

Usage:
    # From environment variables
    config = DiscoveryConfig.from_environment()
    
    # From configuration file
    config = DiscoveryConfig.from_file("config.yaml")
    
    # Direct initialization
    config = DiscoveryConfig(
        splunk_host="splunk.company.com",
        splunk_port=8089,
        username="admin",
        password="secret"
    )
"""

import os
import json
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class DiscoveryConfig:
    """
    Configuration class for Splunk Application Discovery.
    
    Attributes:
        splunk_host (str): Splunk server hostname or IP address
        splunk_port (int): Splunk management port (default: 8089)
        username (str): Splunk username for authentication (optional if using token)
        password (str): Splunk password for authentication (optional if using token)
        token (str): Splunk authentication token (Bearer/Session token)
        auth_type (str): Authentication type: 'basic' or 'token' (default: auto-detect)
        use_ssl (bool): Whether to use SSL for connection (default: True)
        verify_ssl (bool): Whether to verify SSL certificates (default: False)
        discovery_timeframe (str): Time range for discovery queries (default: "-7d")
        max_results (int): Maximum results per query (default: 50000)
        indexes (List[str]): Specific indexes to search (empty = all)
        excluded_indexes (List[str]): Indexes to exclude from search
        excluded_sourcetypes (List[str]): Sourcetypes to exclude
        excluded_apps (List[str]): Known non-app names to filter out
        min_event_count (int): Minimum events to consider an app valid
        confidence_threshold (int): Minimum confidence score (0-100)
        output_format (str): Output format: 'csv', 'json', 'excel'
        output_path (str): Path for output files
    """
    
    # Splunk Connection Settings
    splunk_host: str = "localhost"
    splunk_port: int = 8089
    username: str = ""
    password: str = ""
    token: str = ""  # Splunk authentication token (Bearer/Session token)
    auth_type: str = "auto"  # 'basic', 'token', or 'auto' (auto-detect)
    use_ssl: bool = True
    verify_ssl: bool = False
    
    # Discovery Settings
    discovery_timeframe: str = "-7d"
    max_results: int = 50000
    indexes: List[str] = field(default_factory=list)
    
    # Exclusion Filters
    excluded_indexes: List[str] = field(default_factory=lambda: [
        "_internal", "_audit", "_introspection", "_telemetry",
        "splunklogger", "summary", "history"
    ])
    
    excluded_sourcetypes: List[str] = field(default_factory=lambda: [
        "splunkd", "splunk_*", "stash", "mongod"
    ])
    
    excluded_apps: List[str] = field(default_factory=lambda: [
        # Generic terms that aren't app names
        "log", "logs", "tmp", "temp", "var", "etc", "opt", "usr",
        "debug", "info", "warn", "error", "fatal", "trace",
        "null", "true", "false", "none", "undefined",
        "server", "client", "host", "node", "pod", "container",
        "test", "dev", "prod", "uat", "staging",
        # Common non-app sourcetypes
        "syslog", "access", "audit", "security", "windows"
    ])
    
    # Validation Settings
    min_event_count: int = 100
    confidence_threshold: int = 30
    
    # Output Settings
    output_format: str = "csv"
    output_path: str = "./output"
    
    # Pattern Definitions for Discovery
    source_patterns: List[str] = field(default_factory=lambda: [
        r'[/\\]([a-zA-Z][\w-]+)(?:\.log|\.txt|_\d)',      # filename before extension
        r'[/\\]logs?[/\\]([a-zA-Z][\w-]+)',                # folder after /logs/
        r'Program Files[/\\]([^/\\]+)',                    # Windows program files
        r'/opt/([^/]+)',                                    # Linux /opt/
        r'/var/log/([^/]+)',                               # Linux /var/log/
        r'([a-zA-Z][\w-]+)-\d+\.\d+',                      # app-version pattern
        r'/home/[^/]+/([^/]+)',                            # User home directories
        r'/srv/([^/]+)',                                    # Linux /srv/
        r'/app/([^/]+)',                                    # Container /app/
    ])
    
    content_patterns: List[str] = field(default_factory=lambda: [
        r'(?i)(?:application|app|service|component|module)[=:\s"\']+(?P<app>[a-zA-Z][\w-]{2,30})',
        r'\[(?P<app>[A-Z][a-zA-Z]{2,20})\]',               # [AppName] format
        r'(?P<app>[a-z]+\.[a-z]+\.[A-Z][a-zA-Z]+)',        # Java namespace
        r'(?i)logger[=:\s"\']+(?P<app>[^\s"\']+)',         # Logger name
    ])
    
    hostname_patterns: List[str] = field(default_factory=lambda: [
        r'^([a-zA-Z]+)-',                                   # prefix: app-server01
        r'-([a-zA-Z]+)\d*$',                               # suffix: server-app01
        r'^[a-z]+\d*([a-z]{3,})',                          # embedded name
    ])
    
    # JSON fields commonly containing app names
    json_app_fields: List[str] = field(default_factory=lambda: [
        "application", "app", "appName", "app_name", 
        "service", "serviceName", "service_name",
        "component", "module", "logger", "logger_name",
        "project", "system", "program", "process_name"
    ])
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.splunk_host:
            raise ValueError("splunk_host is required")
        
        # Ensure output path exists
        Path(self.output_path).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Configuration initialized for Splunk host: {self.splunk_host}")
    
    @classmethod
    def from_environment(cls) -> "DiscoveryConfig":
        """
        Create configuration from environment variables.
        
        Environment variables:
            SPLUNK_HOST: Splunk server hostname
            SPLUNK_PORT: Splunk management port
            SPLUNK_USERNAME: Splunk username (optional if using token)
            SPLUNK_PASSWORD: Splunk password (optional if using token)
            SPLUNK_TOKEN: Splunk authentication token (Bearer/Session token)
            SPLUNK_AUTH_TYPE: Authentication type ('basic', 'token', or 'auto')
            SPLUNK_USE_SSL: Use SSL (true/false)
            SPLUNK_VERIFY_SSL: Verify SSL certificates (true/false)
            DISCOVERY_TIMEFRAME: Time range for queries
            DISCOVERY_OUTPUT_PATH: Output directory path
        
        Returns:
            DiscoveryConfig: Configuration instance
        """
        return cls(
            splunk_host=os.getenv("SPLUNK_HOST", "localhost"),
            splunk_port=int(os.getenv("SPLUNK_PORT", "8089")),
            username=os.getenv("SPLUNK_USERNAME", ""),
            password=os.getenv("SPLUNK_PASSWORD", ""),
            token=os.getenv("SPLUNK_TOKEN", ""),
            auth_type=os.getenv("SPLUNK_AUTH_TYPE", "auto"),
            use_ssl=os.getenv("SPLUNK_USE_SSL", "true").lower() == "true",
            verify_ssl=os.getenv("SPLUNK_VERIFY_SSL", "false").lower() == "true",
            discovery_timeframe=os.getenv("DISCOVERY_TIMEFRAME", "-7d"),
            output_path=os.getenv("DISCOVERY_OUTPUT_PATH", "./output")
        )
    
    @classmethod
    def from_file(cls, file_path: str) -> "DiscoveryConfig":
        """
        Load configuration from a YAML or JSON file.
        
        Args:
            file_path: Path to configuration file
            
        Returns:
            DiscoveryConfig: Configuration instance
        """
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {file_path}")
        
        with open(path, 'r') as f:
            if path.suffix in ['.yaml', '.yml']:
                try:
                    import yaml
                    config_data = yaml.safe_load(f)
                except ImportError:
                    raise ImportError("PyYAML is required for YAML config files. Install with: pip install pyyaml")
            else:
                config_data = json.load(f)
        
        return cls(**config_data)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.
        
        Returns:
            Dict containing all configuration values
        """
        return {
            "splunk_host": self.splunk_host,
            "splunk_port": self.splunk_port,
            "username": self.username,
            "password": "***HIDDEN***" if self.password else "",
            "token": "***HIDDEN***" if self.token else "",
            "auth_type": self.auth_type,
            "use_ssl": self.use_ssl,
            "verify_ssl": self.verify_ssl,
            "discovery_timeframe": self.discovery_timeframe,
            "max_results": self.max_results,
            "indexes": self.indexes,
            "excluded_indexes": self.excluded_indexes,
            "min_event_count": self.min_event_count,
            "confidence_threshold": self.confidence_threshold,
            "output_format": self.output_format,
            "output_path": self.output_path,
        }
    
    def save_to_file(self, file_path: str) -> None:
        """
        Save configuration to a file (password will be masked).
        
        Args:
            file_path: Path to save configuration
        """
        path = Path(file_path)
        config_data = self.to_dict()
        
        with open(path, 'w') as f:
            if path.suffix in ['.yaml', '.yml']:
                try:
                    import yaml
                    yaml.dump(config_data, f, default_flow_style=False)
                except ImportError:
                    raise ImportError("PyYAML required for YAML output")
            else:
                json.dump(config_data, f, indent=2)
        
        logger.info(f"Configuration saved to: {file_path}")
    
    def get_index_filter(self) -> str:
        """
        Generate Splunk index filter string.
        
        Returns:
            String for SPL query index filtering
        """
        if self.indexes:
            # Specific indexes requested
            return " OR ".join([f"index={idx}" for idx in self.indexes])
        else:
            # Exclude certain indexes
            exclusions = " ".join([f"index!={idx}" for idx in self.excluded_indexes])
            return f"index=* {exclusions}"


# Example configuration template
EXAMPLE_CONFIG = """
# Splunk Application Discovery Configuration
# ==========================================

# Splunk Connection
splunk_host: "splunk.company.com"
splunk_port: 8089

# Authentication Option 1: Username/Password
username: "discovery_user"
password: "your_password_here"

# Authentication Option 2: Token (recommended for service accounts)
# token: "your-splunk-token-here"
# auth_type: "token"  # Options: 'basic', 'token', or 'auto'

use_ssl: true
verify_ssl: false

# Discovery Settings
discovery_timeframe: "-7d"
max_results: 50000

# Indexes to search (empty = all non-excluded)
indexes: []

# Indexes to exclude
excluded_indexes:
  - "_internal"
  - "_audit"
  - "_introspection"

# Minimum events for valid app
min_event_count: 100

# Confidence threshold (0-100)
confidence_threshold: 30

# Output settings
output_format: "csv"  # csv, json, or excel
output_path: "./output"
"""


if __name__ == "__main__":
    # Print example configuration
    print(EXAMPLE_CONFIG)
