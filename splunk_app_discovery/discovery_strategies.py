"""
Discovery Strategies Module
===========================

This module contains individual discovery strategies for identifying applications
logging to Splunk. Each strategy targets a different aspect of log data:

1. SourceDiscovery - Analyzes source file paths
2. ContentDiscovery - Parses log message content
3. HostDiscovery - Extracts apps from hostname patterns
4. JsonFieldDiscovery - Finds app names in JSON fields
5. SourcetypeDiscovery - Analyzes sourcetype naming patterns

Each strategy implements the BaseDiscovery interface and can be used
independently or combined through the main discovery orchestrator.
"""

import re
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Dict, Set, Any, Optional
from collections import defaultdict

from .config import DiscoveryConfig
from .splunk_connection import SplunkConnection

logger = logging.getLogger(__name__)


@dataclass
class DiscoveredApp:
    """
    Represents a discovered application.
    
    Attributes:
        name: Application name (normalized to lowercase)
        sources: Set of source paths where this app was found
        hosts: Set of hosts logging this application
        sourcetypes: Set of sourcetypes associated with this app
        event_count: Total number of log events
        evidence: List of evidence strings explaining how app was discovered
        discovery_methods: Set of methods that found this app
        confidence_score: Calculated confidence (0-100)
        first_seen: Earliest timestamp found
        last_seen: Most recent timestamp found
        sample_events: Sample log entries
    """
    # Note: Using __slots__ with dataclass requires Python 3.10+ with slots=True
    # For compatibility, we use regular dataclass but optimize field access
    
    name: str
    sources: Set[str] = field(default_factory=set)
    hosts: Set[str] = field(default_factory=set)
    sourcetypes: Set[str] = field(default_factory=set)
    event_count: int = 0
    evidence: List[str] = field(default_factory=list)
    discovery_methods: Set[str] = field(default_factory=set)
    confidence_score: int = 0
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None
    sample_events: List[str] = field(default_factory=list)
    
    def add_evidence(self, method: str, detail: str) -> None:
        """Add discovery evidence."""
        self.evidence.append(f"[{method}] {detail}")
        self.discovery_methods.add(method)
    
    def merge_with(self, other: "DiscoveredApp") -> None:
        """Merge another discovered app into this one."""
        self.sources.update(other.sources)
        self.hosts.update(other.hosts)
        self.sourcetypes.update(other.sourcetypes)
        self.event_count += other.event_count
        self.evidence.extend(other.evidence)
        self.discovery_methods.update(other.discovery_methods)


class BaseDiscovery(ABC):
    """
    Abstract base class for discovery strategies.
    
    All discovery strategies must implement the discover() method
    which returns a dictionary of discovered applications.
    """
    
    # Class-level regex cache for better performance
    _regex_cache: Dict[str, re.Pattern] = {}
    
    def __init__(self, connection: SplunkConnection, config: DiscoveryConfig):
        """
        Initialize discovery strategy.
        
        Args:
            connection: Active Splunk connection
            config: Discovery configuration
        """
        self.connection = connection
        self.config = config
        self.discovered_apps: Dict[str, DiscoveredApp] = {}
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of this discovery strategy."""
        pass
    
    def _get_compiled_pattern(self, pattern: str, flags: int = re.IGNORECASE) -> re.Pattern:
        """
        Get or compile a regex pattern with caching.
        
        Args:
            pattern: Regex pattern string
            flags: Regex flags
            
        Returns:
            Compiled regex pattern
        """
        cache_key = f"{pattern}_{flags}"
        if cache_key not in self._regex_cache:
            self._regex_cache[cache_key] = re.compile(pattern, flags)
        return self._regex_cache[cache_key]
    
    @abstractmethod
    def discover(self) -> Dict[str, DiscoveredApp]:
        """
        Execute the discovery strategy.
        
        Returns:
            Dictionary mapping app names to DiscoveredApp objects
        """
        pass
    
    def _normalize_app_name(self, name: str) -> str:
        """
        Normalize application name for consistent matching.
        
        Args:
            name: Raw application name
            
        Returns:
            Normalized lowercase name
        """
        if not name:
            return ""
        
        # Remove common suffixes (use cached patterns)
        pattern1 = self._get_compiled_pattern(r'[-_]?(service|app|api|server|client|agent|daemon)$')
        name = pattern1.sub('', name)
        
        # Remove version numbers
        pattern2 = self._get_compiled_pattern(r'[-_]?v?\d+(\.\d+)*$')
        name = pattern2.sub('', name)
        
        # Convert to lowercase and strip
        return name.lower().strip()
    
    def _is_valid_app_name(self, name: str) -> bool:
        """
        Check if name is a valid application name.
        
        Args:
            name: Potential application name
            
        Returns:
            True if name appears to be a valid app name
        """
        if not name or len(name) < 3:
            return False
        
        # Check against exclusion list
        if name.lower() in self.config.excluded_apps:
            return False
        
        # Must contain at least one letter
        if not re.search(r'[a-zA-Z]', name):
            return False
        
        # Shouldn't be all numbers with optional prefix
        if re.match(r'^[a-z]?\d+$', name.lower()):
            return False
        
        return True
    
    def _add_discovered_app(
        self, 
        name: str, 
        evidence: str,
        source: Optional[str] = None,
        host: Optional[str] = None,
        sourcetype: Optional[str] = None,
        count: int = 0
    ) -> None:
        """
        Add or update a discovered application.
        
        Args:
            name: Application name
            evidence: Description of how it was discovered
            source: Source path if applicable
            host: Hostname if applicable
            sourcetype: Sourcetype if applicable
            count: Event count
        """
        normalized_name = self._normalize_app_name(name)
        
        if not self._is_valid_app_name(normalized_name):
            return
        
        if normalized_name not in self.discovered_apps:
            self.discovered_apps[normalized_name] = DiscoveredApp(name=normalized_name)
        
        app = self.discovered_apps[normalized_name]
        app.add_evidence(self.name, evidence)
        app.event_count += count
        
        if source:
            app.sources.add(source)
        if host:
            app.hosts.add(host)
        if sourcetype:
            app.sourcetypes.add(sourcetype)


class SourceDiscovery(BaseDiscovery):
    """
    Discovers applications by analyzing source file paths.
    
    This strategy extracts application names from log file paths,
    looking for patterns like:
    - /var/log/myapp/application.log -> myapp
    - /opt/payment-service/logs/... -> payment-service
    - C:\\Program Files\\OrderSystem\\... -> OrderSystem
    """
    
    @property
    def name(self) -> str:
        return "SourcePath"
    
    def discover(self) -> Dict[str, DiscoveredApp]:
        """
        Discover applications from source paths.
        Optimized with cached regex patterns.
        
        Returns:
            Dictionary of discovered applications
        """
        logger.info("Starting source path discovery...")
        
        # Pre-compile all patterns for performance
        compiled_patterns = [
            self._get_compiled_pattern(pattern) for pattern in self.config.source_patterns
        ]
        
        # Optimized query with field limiting
        query = f"""
        | metadata type=sources {self.config.get_index_filter()}
        | where totalCount >= {self.config.min_event_count}
        | table source, totalCount
        | sort -totalCount limit={self.config.max_results}
        """
        
        try:
            results = self.connection.execute_query(query, use_cache=True)
            logger.info(f"Found {len(results)} unique sources")
            
            for result in results:
                source = result.get('source', '')
                count = int(result.get('totalCount', 0))
                
                # Try each compiled pattern (faster)
                for pattern in compiled_patterns:
                    match = pattern.search(source)
                    if match:
                        app_name = match.group(1)
                        self._add_discovered_app(
                            name=app_name,
                            evidence=f"Source path: {source}",
                            source=source,
                            count=count
                        )
                        break  # Stop after first match
            
            logger.info(f"Source discovery found {len(self.discovered_apps)} applications")
            
        except Exception as e:
            logger.error(f"Source discovery failed: {str(e)}")
        
        return self.discovered_apps


class ContentDiscovery(BaseDiscovery):
    """
    Discovers applications by parsing log message content.
    
    This strategy looks for application identifiers within the actual
    log messages, such as:
    - application=myapp
    - [ServiceName]
    - com.company.MyApplication
    - logger: PaymentService
    """
    
    @property
    def name(self) -> str:
        return "LogContent"
    
    def discover(self) -> Dict[str, DiscoveredApp]:
        """
        Discover applications from log content patterns.
        
        Returns:
            Dictionary of discovered applications
        """
        logger.info("Starting log content discovery...")
        
        # Build regex extractions for SPL
        rex_commands = []
        for i, pattern in enumerate(self.config.content_patterns):
            # Convert Python regex to SPL rex format
            rex_commands.append(f'| rex "{pattern}"')
        
        query = f"""
        search {self.config.get_index_filter()} earliest={self.config.discovery_timeframe}
        | head {self.config.max_results}
        {' '.join(rex_commands)}
        | eval extracted_app=coalesce(app, app_field, bracket_app, namespace)
        | where isnotnull(extracted_app)
        | stats count by extracted_app, sourcetype
        | where count > {self.config.min_event_count // 10}
        | sort -count
        """
        
        try:
            results = self.connection.execute_query(query)
            logger.info(f"Content discovery found {len(results)} potential apps")
            
            for result in results:
                app_name = result.get('extracted_app', '')
                sourcetype = result.get('sourcetype', '')
                count = int(result.get('count', 0))
                
                # Handle namespace format (e.g., com.company.ServiceName)
                if '.' in app_name:
                    app_name = app_name.split('.')[-1]
                
                self._add_discovered_app(
                    name=app_name,
                    evidence=f"Found in log content with sourcetype: {sourcetype}",
                    sourcetype=sourcetype,
                    count=count
                )
            
            logger.info(f"Content discovery found {len(self.discovered_apps)} applications")
            
        except Exception as e:
            logger.error(f"Content discovery failed: {str(e)}")
        
        return self.discovered_apps


class HostDiscovery(BaseDiscovery):
    """
    Discovers applications from hostname patterns.
    
    This strategy extracts application hints from hostnames,
    looking for naming conventions like:
    - myapp-prod-01 -> myapp
    - payment-server-01 -> payment
    - web-orderapi-02 -> orderapi
    """
    
    @property
    def name(self) -> str:
        return "Hostname"
    
    def discover(self) -> Dict[str, DiscoveredApp]:
        """
        Discover applications from hostname patterns.
        
        Returns:
            Dictionary of discovered applications
        """
        logger.info("Starting hostname discovery...")
        
        query = f"""
        | metadata type=hosts {self.config.get_index_filter()}
        | table host, totalCount
        | sort -totalCount
        """
        
        try:
            results = self.connection.execute_query(query)
            logger.info(f"Found {len(results)} unique hosts")
            
            for result in results:
                host = result.get('host', '')
                count = int(result.get('totalCount', 0))
                
                # Clean hostname (remove domain)
                host_clean = re.sub(r'\.(com|net|org|local|internal|corp).*$', '', host.lower())
                
                for pattern in self.config.hostname_patterns:
                    match = re.search(pattern, host_clean, re.IGNORECASE)
                    if match:
                        app_name = match.group(1)
                        
                        # Skip environment/infrastructure names
                        if app_name.lower() not in ['prd', 'prod', 'dev', 'uat', 'stg', 'web', 'api', 'srv']:
                            self._add_discovered_app(
                                name=app_name,
                                evidence=f"Hostname pattern: {host}",
                                host=host,
                                count=count
                            )
                        break
            
            logger.info(f"Hostname discovery found {len(self.discovered_apps)} applications")
            
        except Exception as e:
            logger.error(f"Hostname discovery failed: {str(e)}")
        
        return self.discovered_apps


class JsonFieldDiscovery(BaseDiscovery):
    """
    Discovers applications from JSON log fields.
    
    This strategy searches for common application identifier fields
    in JSON-formatted logs, such as:
    - {"application": "myapp", ...}
    - {"service": "payment-service", ...}
    - {"component": "order-processor", ...}
    """
    
    @property
    def name(self) -> str:
        return "JsonField"
    
    def discover(self) -> Dict[str, DiscoveredApp]:
        """
        Discover applications from JSON fields.
        
        Returns:
            Dictionary of discovered applications
        """
        logger.info("Starting JSON field discovery...")
        
        # Build coalesce for all potential app fields
        field_list = ", ".join(self.config.json_app_fields)
        
        query = f"""
        search {self.config.get_index_filter()} earliest={self.config.discovery_timeframe} 
        sourcetype=*json* OR sourcetype=_json OR sourcetype=*:json
        | head {self.config.max_results}
        | spath
        | eval discovered_app=coalesce({field_list})
        | where isnotnull(discovered_app) AND discovered_app!=""
        | stats count values(sourcetype) as sourcetypes by discovered_app
        | where count > {self.config.min_event_count // 10}
        | sort -count
        """
        
        try:
            results = self.connection.execute_query(query)
            logger.info(f"JSON discovery found {len(results)} potential apps")
            
            for result in results:
                app_name = result.get('discovered_app', '')
                sourcetypes = result.get('sourcetypes', [])
                count = int(result.get('count', 0))
                
                if isinstance(sourcetypes, str):
                    sourcetypes = [sourcetypes]
                
                self._add_discovered_app(
                    name=app_name,
                    evidence=f"JSON field in sourcetypes: {', '.join(sourcetypes)}",
                    count=count
                )
                
                for st in sourcetypes:
                    self.discovered_apps.get(
                        self._normalize_app_name(app_name), 
                        DiscoveredApp(name="")
                    ).sourcetypes.add(st)
            
            logger.info(f"JSON discovery found {len(self.discovered_apps)} applications")
            
        except Exception as e:
            logger.error(f"JSON discovery failed: {str(e)}")
        
        return self.discovered_apps


class SourcetypeDiscovery(BaseDiscovery):
    """
    Discovers applications from sourcetype naming patterns.
    
    This strategy analyzes sourcetype names which often contain
    application identifiers:
    - myapp:application -> myapp
    - payment_service_logs -> payment_service
    - nginx:access -> nginx
    """
    
    @property
    def name(self) -> str:
        return "Sourcetype"
    
    def discover(self) -> Dict[str, DiscoveredApp]:
        """
        Discover applications from sourcetype names.
        
        Returns:
            Dictionary of discovered applications
        """
        logger.info("Starting sourcetype discovery...")
        
        query = f"""
        | metadata type=sourcetypes {self.config.get_index_filter()}
        | table sourcetype, totalCount, firstTime, lastTime
        | sort -totalCount
        """
        
        try:
            results = self.connection.execute_query(query)
            logger.info(f"Found {len(results)} unique sourcetypes")
            
            # Skip generic sourcetypes
            skip_patterns = [
                r'^(syslog|access|error|audit|security|generic|stash)$',
                r'^_',  # Internal sourcetypes
                r'^splunk',
            ]
            
            for result in results:
                sourcetype = result.get('sourcetype', '')
                count = int(result.get('totalCount', 0))
                
                if count < self.config.min_event_count:
                    continue
                
                # Check if should skip
                should_skip = False
                for skip_pat in skip_patterns:
                    if re.match(skip_pat, sourcetype, re.IGNORECASE):
                        should_skip = True
                        break
                
                if should_skip:
                    continue
                
                # Extract app name from sourcetype
                # Patterns: app:type, app_type, app-type
                parts = re.split(r'[_:\-]', sourcetype)
                
                # Take the first meaningful part
                for part in parts:
                    if len(part) > 3 and self._is_valid_app_name(part):
                        self._add_discovered_app(
                            name=part,
                            evidence=f"Sourcetype: {sourcetype}",
                            sourcetype=sourcetype,
                            count=count
                        )
                        break
            
            logger.info(f"Sourcetype discovery found {len(self.discovered_apps)} applications")
            
        except Exception as e:
            logger.error(f"Sourcetype discovery failed: {str(e)}")
        
        return self.discovered_apps


class ClusterDiscovery(BaseDiscovery):
    """
    Discovers applications by clustering similar log patterns.
    
    This strategy uses Splunk's cluster command to group similar
    log entries and identify distinct application patterns.
    """
    
    @property
    def name(self) -> str:
        return "Clustering"
    
    def discover(self) -> Dict[str, DiscoveredApp]:
        """
        Discover applications using log clustering.
        
        Returns:
            Dictionary of discovered applications
        """
        logger.info("Starting cluster-based discovery...")
        
        query = f"""
        search {self.config.get_index_filter()} earliest=-1h
        | head 10000
        | cluster showcount=true t=0.7
        | stats count values(sourcetype) as sourcetypes values(host) as hosts by cluster_label
        | where count > 50
        | sort -count
        | head 100
        """
        
        try:
            results = self.connection.execute_query(query)
            logger.info(f"Found {len(results)} clusters")
            
            # Cluster discovery provides less specific results
            # Used mainly to identify uncategorized logs
            for result in results:
                cluster_label = result.get('cluster_label', '')
                sourcetypes = result.get('sourcetypes', [])
                hosts = result.get('hosts', [])
                count = int(result.get('count', 0))
                
                # Try to extract app name from cluster
                for pattern in self.config.content_patterns:
                    match = re.search(pattern, cluster_label, re.IGNORECASE)
                    if match:
                        app_name = match.group(1) if match.lastindex else match.group(0)
                        self._add_discovered_app(
                            name=app_name,
                            evidence=f"Log cluster pattern: {cluster_label[:50]}...",
                            count=count
                        )
                        break
            
            logger.info(f"Cluster discovery found {len(self.discovered_apps)} applications")
            
        except Exception as e:
            logger.error(f"Cluster discovery failed: {str(e)}")
        
        return self.discovered_apps


# Export all discovery classes
__all__ = [
    'BaseDiscovery',
    'DiscoveredApp',
    'SourceDiscovery',
    'ContentDiscovery',
    'HostDiscovery',
    'JsonFieldDiscovery',
    'SourcetypeDiscovery',
    'ClusterDiscovery',
]
