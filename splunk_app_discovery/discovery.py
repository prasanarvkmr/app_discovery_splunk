"""
Main Discovery Orchestrator
===========================

This module coordinates all discovery strategies and aggregates results
to produce a comprehensive list of applications logging to Splunk.

The orchestrator:
1. Runs all enabled discovery strategies
2. Merges results from different strategies
3. Calculates confidence scores
4. Filters and ranks discovered applications
5. Produces final report

Usage:
    from discovery import SplunkAppDiscovery
    
    discovery = SplunkAppDiscovery(config)
    results = discovery.run_full_discovery()
    discovery.export_results("applications.csv")
"""

import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import asdict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from .config import DiscoveryConfig
from .splunk_connection import SplunkConnection, MockSplunkConnection
from .discovery_strategies import (
    BaseDiscovery,
    DiscoveredApp,
    SourceDiscovery,
    ContentDiscovery,
    HostDiscovery,
    JsonFieldDiscovery,
    SourcetypeDiscovery,
    ClusterDiscovery,
)

logger = logging.getLogger(__name__)


class SplunkAppDiscovery:
    """
    Main orchestrator for Splunk application discovery.
    
    This class coordinates multiple discovery strategies to identify
    applications logging to Splunk. It handles:
    - Strategy execution and coordination
    - Result aggregation and deduplication
    - Confidence score calculation
    - Result filtering and ranking
    
    Attributes:
        config (DiscoveryConfig): Configuration settings
        connection (SplunkConnection): Splunk connection handler
        discovered_apps (Dict[str, DiscoveredApp]): Aggregated results
        discovery_stats (Dict): Statistics about the discovery run
    
    Example:
        >>> config = DiscoveryConfig.from_file("config.yaml")
        >>> discovery = SplunkAppDiscovery(config)
        >>> results = discovery.run_full_discovery()
        >>> print(f"Found {len(results)} applications")
        >>> discovery.export_results("output/applications.csv")
    """
    
    def __init__(
        self, 
        config: DiscoveryConfig,
        use_mock: bool = False
    ):
        """
        Initialize the discovery orchestrator.
        
        Args:
            config: Discovery configuration object
            use_mock: If True, use mock connection for testing
        """
        self.config = config
        self.use_mock = use_mock
        
        # Initialize connection (lazy - connects when needed)
        if use_mock:
            self.connection = MockSplunkConnection(config)
        else:
            self.connection = SplunkConnection(config)
        
        # Results storage
        self.discovered_apps: Dict[str, DiscoveredApp] = {}
        self._results_lock = threading.Lock()  # Thread-safe result merging
        
        # Discovery statistics
        self.discovery_stats = {
            "start_time": None,
            "end_time": None,
            "strategies_run": [],
            "total_sources": 0,
            "total_hosts": 0,
            "total_sourcetypes": 0,
            "apps_before_filter": 0,
            "apps_after_filter": 0,
        }
        
        # Available strategies
        self._strategies: List[type] = [
            SourceDiscovery,
            SourcetypeDiscovery,
            HostDiscovery,
            JsonFieldDiscovery,
            ContentDiscovery,
            ClusterDiscovery,
        ]
        
        logger.info("SplunkAppDiscovery initialized")
    
    def run_full_discovery(
        self,
        strategies: Optional[List[str]] = None,
        parallel: bool = True,
        max_workers: int = 3
    ) -> List[DiscoveredApp]:
        """
        Execute all discovery strategies and return aggregated results.
        
        This method:
        1. Connects to Splunk
        2. Runs each enabled discovery strategy (optionally in parallel)
        3. Merges and deduplicates results
        4. Calculates confidence scores
        5. Filters by minimum thresholds
        6. Returns sorted results
        
        Args:
            strategies: List of strategy names to run (None = all)
                       Options: ['SourcePath', 'Sourcetype', 'Hostname', 
                                'JsonField', 'LogContent', 'Clustering']
            parallel: Whether to run strategies in parallel
            max_workers: Maximum number of parallel workers
        
        Returns:
            List of DiscoveredApp objects sorted by confidence score
        """
        self.discovery_stats["start_time"] = datetime.now().isoformat()
        
        logger.info("="*60)
        logger.info("Starting Splunk Application Discovery")
        logger.info("="*60)
        
        try:
            # Connect to Splunk
            logger.info("Connecting to Splunk...")
            self.connection.connect()
            
            if parallel and max_workers > 1:
                self._run_strategies_parallel(strategies, max_workers)
            else:
                self._run_strategies_sequential(strategies)
            
            # Calculate confidence scores
            logger.info("\nCalculating confidence scores...")
            self._calculate_all_confidence_scores()
            
            # Record stats before filtering
            self.discovery_stats["apps_before_filter"] = len(self.discovered_apps)
            
            # Filter by confidence threshold
            filtered_apps = self._filter_by_confidence()
            
            self.discovery_stats["apps_after_filter"] = len(filtered_apps)
            self.discovery_stats["end_time"] = datetime.now().isoformat()
            
            # Log summary
            self._log_discovery_summary()
            
            return filtered_apps
            
        except Exception as e:
            logger.error(f"Discovery failed: {str(e)}")
            raise
        
        finally:
            self.connection.disconnect()
    
    def _run_strategies_sequential(self, strategies: Optional[List[str]]) -> None:
        """Run strategies sequentially."""
        for strategy_class in self._strategies:
            strategy_name = strategy_class(self.connection, self.config).name
            
            # Check if this strategy should be run
            if strategies and strategy_name not in strategies:
                logger.info(f"Skipping {strategy_name} (not in selected strategies)")
                continue
            
            logger.info(f"\n{'='*40}")
            logger.info(f"Running strategy: {strategy_name}")
            logger.info(f"{'='*40}")
            
            try:
                strategy = strategy_class(self.connection, self.config)
                strategy_results = strategy.discover()
                
                # Merge results
                self._merge_results(strategy_results)
                
                self.discovery_stats["strategies_run"].append({
                    "name": strategy_name,
                    "apps_found": len(strategy_results),
                    "status": "success"
                })
                
                logger.info(f"{strategy_name} found {len(strategy_results)} applications")
                
            except Exception as e:
                logger.error(f"Strategy {strategy_name} failed: {str(e)}")
                self.discovery_stats["strategies_run"].append({
                    "name": strategy_name,
                    "apps_found": 0,
                    "status": f"error: {str(e)}"
                })
    
    def _run_strategies_parallel(self, strategies: Optional[List[str]], max_workers: int) -> None:
        """Run strategies in parallel using ThreadPoolExecutor."""
        logger.info(f"Running strategies in parallel (max workers: {max_workers})")
        
        # Filter strategies to run
        strategies_to_run = []
        for strategy_class in self._strategies:
            strategy_name = strategy_class(self.connection, self.config).name
            if not strategies or strategy_name in strategies:
                strategies_to_run.append((strategy_class, strategy_name))
        
        # Execute in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_strategy = {
                executor.submit(self._execute_strategy, strategy_class, strategy_name): strategy_name
                for strategy_class, strategy_name in strategies_to_run
            }
            
            for future in as_completed(future_to_strategy):
                strategy_name = future_to_strategy[future]
                try:
                    strategy_results, apps_found = future.result()
                    
                    # Thread-safe merge
                    with self._results_lock:
                        self._merge_results(strategy_results)
                        self.discovery_stats["strategies_run"].append({
                            "name": strategy_name,
                            "apps_found": apps_found,
                            "status": "success"
                        })
                    
                    logger.info(f"{strategy_name} completed: found {apps_found} applications")
                    
                except Exception as e:
                    logger.error(f"Strategy {strategy_name} failed: {str(e)}")
                    with self._results_lock:
                        self.discovery_stats["strategies_run"].append({
                            "name": strategy_name,
                            "apps_found": 0,
                            "status": f"error: {str(e)}"
                        })
    
    def _execute_strategy(self, strategy_class, strategy_name: str) -> Tuple[Dict[str, DiscoveredApp], int]:
        """Execute a single strategy and return results."""
        logger.info(f"Starting strategy: {strategy_name}")
        strategy = strategy_class(self.connection, self.config)
        strategy_results = strategy.discover()
        return strategy_results, len(strategy_results)
    
    def run_single_strategy(self, strategy_name: str) -> List[DiscoveredApp]:
        """
        Run a single discovery strategy.
        
        Args:
            strategy_name: Name of strategy to run
            
        Returns:
            List of discovered applications
        """
        return self.run_full_discovery(strategies=[strategy_name], parallel=False)
    
    def _merge_results(self, new_results: Dict[str, DiscoveredApp]) -> None:
        """
        Merge new results into the main results dictionary.
        
        Args:
            new_results: Results from a discovery strategy
        """
        for app_name, new_app in new_results.items():
            if app_name in self.discovered_apps:
                # Merge with existing
                self.discovered_apps[app_name].merge_with(new_app)
            else:
                # Add new
                self.discovered_apps[app_name] = new_app
    
    def _calculate_all_confidence_scores(self) -> None:
        """Calculate confidence scores for all discovered applications."""
        for app in self.discovered_apps.values():
            app.confidence_score = self._calculate_confidence(app)
    
    def _calculate_confidence(self, app: DiscoveredApp) -> int:
        """
        Calculate confidence score for a discovered application.
        
        The score is based on:
        - Number of discovery methods that found it (weight: 25)
        - Event count (weight: 30)
        - Number of distinct sources (weight: 15)
        - Number of distinct hosts (weight: 15)
        - Number of distinct sourcetypes (weight: 15)
        
        Args:
            app: DiscoveredApp to score
            
        Returns:
            Confidence score 0-100
        """
        score = 0
        
        # Multi-method bonus (25 points max)
        method_count = len(app.discovery_methods)
        score += min(method_count * 8, 25)
        
        # Event count (30 points max)
        if app.event_count >= 100000:
            score += 30
        elif app.event_count >= 10000:
            score += 25
        elif app.event_count >= 1000:
            score += 20
        elif app.event_count >= 100:
            score += 10
        elif app.event_count > 0:
            score += 5
        
        # Source diversity (15 points max)
        source_count = len(app.sources)
        if source_count >= 5:
            score += 15
        elif source_count >= 2:
            score += 10
        elif source_count >= 1:
            score += 5
        
        # Host diversity (15 points max)
        host_count = len(app.hosts)
        if host_count >= 10:
            score += 15
        elif host_count >= 5:
            score += 10
        elif host_count >= 1:
            score += 5
        
        # Sourcetype diversity (15 points max)
        st_count = len(app.sourcetypes)
        if st_count >= 3:
            score += 15
        elif st_count >= 2:
            score += 10
        elif st_count >= 1:
            score += 5
        
        return min(score, 100)
    
    def _filter_by_confidence(self) -> List[DiscoveredApp]:
        """
        Filter applications by confidence threshold.
        
        Returns:
            List of apps meeting the confidence threshold, sorted by score
        """
        filtered = [
            app for app in self.discovered_apps.values()
            if app.confidence_score >= self.config.confidence_threshold
        ]
        
        # Sort by confidence score (descending)
        filtered.sort(key=lambda x: (-x.confidence_score, -x.event_count))
        
        return filtered
    
    def _log_discovery_summary(self) -> None:
        """Log a summary of the discovery run."""
        stats = self.discovery_stats
        
        logger.info("\n" + "="*60)
        logger.info("DISCOVERY SUMMARY")
        logger.info("="*60)
        logger.info(f"Start Time: {stats['start_time']}")
        logger.info(f"End Time: {stats['end_time']}")
        logger.info(f"Strategies Run: {len(stats['strategies_run'])}")
        
        for strategy in stats['strategies_run']:
            status_icon = "✓" if strategy['status'] == 'success' else "✗"
            logger.info(f"  {status_icon} {strategy['name']}: {strategy['apps_found']} apps")
        
        logger.info(f"\nApplications Found (before filter): {stats['apps_before_filter']}")
        logger.info(f"Applications Found (after filter): {stats['apps_after_filter']}")
        logger.info(f"Confidence Threshold: {self.config.confidence_threshold}%")
        logger.info("="*60)
    
    def get_results_summary(self) -> Dict:
        """
        Get a summary of discovery results.
        
        Returns:
            Dictionary with summary statistics
        """
        apps = list(self.discovered_apps.values())
        
        return {
            "total_applications": len(apps),
            "high_confidence": len([a for a in apps if a.confidence_score >= 70]),
            "medium_confidence": len([a for a in apps if 40 <= a.confidence_score < 70]),
            "low_confidence": len([a for a in apps if a.confidence_score < 40]),
            "total_event_count": sum(a.event_count for a in apps),
            "discovery_methods_used": list(set(
                m for a in apps for m in a.discovery_methods
            )),
            "stats": self.discovery_stats,
        }
    
    def get_app_details(self, app_name: str) -> Optional[DiscoveredApp]:
        """
        Get detailed information about a specific application.
        
        Args:
            app_name: Name of application to look up
            
        Returns:
            DiscoveredApp object or None if not found
        """
        return self.discovered_apps.get(app_name.lower())
    
    def search_apps(self, query: str) -> List[DiscoveredApp]:
        """
        Search discovered applications by name.
        
        Args:
            query: Search string (partial match)
            
        Returns:
            List of matching applications
        """
        query_lower = query.lower()
        matches = [
            app for name, app in self.discovered_apps.items()
            if query_lower in name
        ]
        return sorted(matches, key=lambda x: -x.confidence_score)
    
    def to_dataframe(self):
        """
        Convert results to a pandas DataFrame.
        
        Returns:
            pandas.DataFrame with discovery results
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required. Install with: pip install pandas")
        
        data = []
        for app in self.discovered_apps.values():
            data.append({
                "application": app.name,
                "confidence_score": app.confidence_score,
                "event_count": app.event_count,
                "source_count": len(app.sources),
                "host_count": len(app.hosts),
                "sourcetype_count": len(app.sourcetypes),
                "discovery_methods": ", ".join(app.discovery_methods),
                "sample_sources": "; ".join(list(app.sources)[:3]),
                "sample_hosts": "; ".join(list(app.hosts)[:3]),
                "sourcetypes": "; ".join(app.sourcetypes),
                "evidence_count": len(app.evidence),
            })
        
        df = pd.DataFrame(data)
        return df.sort_values("confidence_score", ascending=False)


# Convenience function for quick discovery
def discover_applications(
    splunk_host: str,
    username: str,
    password: str,
    port: int = 8089,
    timeframe: str = "-7d",
    **kwargs
) -> List[DiscoveredApp]:
    """
    Quick function to discover applications with minimal setup.
    
    Args:
        splunk_host: Splunk server hostname
        username: Splunk username
        password: Splunk password
        port: Splunk management port
        timeframe: Discovery time range
        **kwargs: Additional config options
        
    Returns:
        List of discovered applications
        
    Example:
        >>> apps = discover_applications(
        ...     splunk_host="splunk.company.com",
        ...     username="admin",
        ...     password="secret"
        ... )
        >>> for app in apps[:10]:
        ...     print(f"{app.name}: {app.confidence_score}%")
    """
    config = DiscoveryConfig(
        splunk_host=splunk_host,
        splunk_port=port,
        username=username,
        password=password,
        discovery_timeframe=timeframe,
        **kwargs
    )
    
    discovery = SplunkAppDiscovery(config)
    return discovery.run_full_discovery()
