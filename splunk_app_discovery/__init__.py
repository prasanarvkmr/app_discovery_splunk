"""
Splunk Application Discovery Tool
=================================

A comprehensive Python solution for discovering applications logging to Splunk
in enterprise environments where no standardized tagging or naming conventions exist.

This tool uses multiple discovery strategies to identify applications:
- Source path analysis
- Log content parsing
- Hostname pattern matching
- JSON field extraction
- Sourcetype analysis
- Log clustering

Author: Enterprise Observability Team
Version: 1.0.0
Date: January 2026
"""

__version__ = "1.0.0"
__author__ = "Enterprise Observability Team"

from .discovery import SplunkAppDiscovery
from .config import DiscoveryConfig

__all__ = ["SplunkAppDiscovery", "DiscoveryConfig"]
