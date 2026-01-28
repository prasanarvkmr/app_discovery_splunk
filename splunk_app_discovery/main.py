#!/usr/bin/env python3
"""
Splunk Application Discovery Tool
==================================

Command-line interface for discovering applications logging to Splunk.

This tool identifies applications in enterprise environments where
no standardized tagging or naming conventions exist.

Usage Examples:
    # Basic usage with environment variables
    python main.py
    
    # Specify Splunk connection details
    python main.py --host splunk.company.com --user admin --password secret
    
    # Use a configuration file
    python main.py --config config.yaml
    
    # Run specific strategies only
    python main.py --strategies SourcePath,Sourcetype,Hostname
    
    # Change output format and location
    python main.py --format excel --output ./reports
    
    # Test mode with mock data
    python main.py --test
    
    # Verbose logging
    python main.py --verbose

For more information, see the README.md file.
"""

import argparse
import logging
import sys
import os
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from splunk_app_discovery.config import DiscoveryConfig, EXAMPLE_CONFIG
from splunk_app_discovery.discovery import SplunkAppDiscovery
from splunk_app_discovery.reporting import ReportGenerator, export_results


def setup_logging(verbose: bool = False, log_file: str = None) -> None:
    """
    Configure logging for the application.
    
    Args:
        verbose: Enable debug logging
        log_file: Optional log file path
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Create formatters
    console_format = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    file_format = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_format)
    root_logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_format)
        root_logger.addHandler(file_handler)


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Discover applications logging to Splunk Enterprise",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --host splunk.example.com --user admin
  %(prog)s --config config.yaml --format excel
  %(prog)s --strategies SourcePath,Hostname --timeframe "-3d"
  %(prog)s --test --verbose

For configuration file format, run: %(prog)s --show-config
        """
    )
    
    # Connection arguments
    conn_group = parser.add_argument_group('Splunk Connection')
    conn_group.add_argument(
        '--host', '-H',
        help='Splunk server hostname (or set SPLUNK_HOST env var)',
        default=os.getenv('SPLUNK_HOST', 'localhost')
    )
    conn_group.add_argument(
        '--port', '-P',
        type=int,
        help='Splunk management port (default: 8089)',
        default=int(os.getenv('SPLUNK_PORT', '8089'))
    )
    conn_group.add_argument(
        '--user', '-u',
        help='Splunk username (or set SPLUNK_USERNAME env var)',
        default=os.getenv('SPLUNK_USERNAME', '')
    )
    conn_group.add_argument(
        '--password', '-p',
        help='Splunk password (or set SPLUNK_PASSWORD env var)',
        default=os.getenv('SPLUNK_PASSWORD', '')
    )
    conn_group.add_argument(
        '--token', '-T',
        help='Splunk authentication token for service accounts (or set SPLUNK_TOKEN env var)',
        default=os.getenv('SPLUNK_TOKEN', '')
    )
    conn_group.add_argument(
        '--auth-type',
        choices=['basic', 'token', 'auto'],
        help='Authentication type: basic (user/pass), token, or auto-detect (default: auto)',
        default='auto'
    )
    conn_group.add_argument(
        '--no-ssl',
        action='store_true',
        help='Disable SSL (not recommended)'
    )
    
    # Discovery arguments
    disc_group = parser.add_argument_group('Discovery Options')
    disc_group.add_argument(
        '--config', '-c',
        help='Path to configuration file (YAML or JSON)'
    )
    disc_group.add_argument(
        '--timeframe', '-t',
        help='Discovery time range (default: -7d)',
        default='-7d'
    )
    disc_group.add_argument(
        '--strategies', '-s',
        help='Comma-separated list of strategies to run',
        default=None
    )
    disc_group.add_argument(
        '--confidence', 
        type=int,
        help='Minimum confidence threshold (0-100, default: 30)',
        default=30
    )
    disc_group.add_argument(
        '--min-events',
        type=int,
        help='Minimum event count for valid app (default: 100)',
        default=100
    )
    
    # Output arguments
    out_group = parser.add_argument_group('Output Options')
    out_group.add_argument(
        '--output', '-o',
        help='Output directory (default: ./output)',
        default='./output'
    )
    out_group.add_argument(
        '--format', '-f',
        choices=['csv', 'json', 'excel', 'html', 'all'],
        help='Output format (default: all)',
        default='all'
    )
    out_group.add_argument(
        '--filename',
        help='Custom output filename (without extension)'
    )
    
    # Other arguments
    other_group = parser.add_argument_group('Other Options')
    other_group.add_argument(
        '--test',
        action='store_true',
        help='Run with mock data (no Splunk connection required)'
    )
    other_group.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    other_group.add_argument(
        '--log-file',
        help='Log to file in addition to console'
    )
    other_group.add_argument(
        '--show-config',
        action='store_true',
        help='Show example configuration and exit'
    )
    other_group.add_argument(
        '--version',
        action='version',
        version='Splunk Application Discovery Tool v1.0.0'
    )
    
    return parser.parse_args()


def create_config(args: argparse.Namespace) -> DiscoveryConfig:
    """
    Create configuration from arguments.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        DiscoveryConfig instance
    """
    if args.config:
        # Load from configuration file
        config = DiscoveryConfig.from_file(args.config)
        
        # Override with command-line arguments if provided
        if args.host != 'localhost':
            config.splunk_host = args.host
        if args.user:
            config.username = args.user
        if args.password:
            config.password = args.password
        if args.token:
            config.token = args.token
        if args.auth_type != 'auto':
            config.auth_type = args.auth_type
    else:
        # Create from command-line arguments
        config = DiscoveryConfig(
            splunk_host=args.host,
            splunk_port=args.port,
            username=args.user,
            password=args.password,
            token=args.token,
            auth_type=args.auth_type,
            use_ssl=not args.no_ssl,
            discovery_timeframe=args.timeframe,
            min_event_count=args.min_events,
            confidence_threshold=args.confidence,
            output_path=args.output,
            output_format=args.format
        )
    
    return config


def validate_config(config: DiscoveryConfig, test_mode: bool) -> bool:
    """
    Validate configuration before running.
    
    Args:
        config: Configuration to validate
        test_mode: Whether running in test mode
        
    Returns:
        True if valid, False otherwise
    """
    if not test_mode:
        # Check if we have either token OR username/password
        has_token = bool(config.token)
        has_basic_auth = bool(config.username and config.password)
        
        if not has_token and not has_basic_auth:
            logging.error(
                "Authentication required. Provide either:\n"
                "  - Token: --token or set SPLUNK_TOKEN env var\n"
                "  - Basic auth: --user and --password (or SPLUNK_USERNAME/SPLUNK_PASSWORD env vars)"
            )
            return False
        
        if has_token:
            logging.info("Using token authentication")
        else:
            logging.info("Using basic authentication (username/password)")
    
    return True


def print_banner() -> None:
    """Print application banner."""
    banner = """
╔══════════════════════════════════════════════════════════════════╗
║       Splunk Application Discovery Tool                          ║
║       Enterprise Observability Suite                              ║
║       Version 1.0.0                                               ║
╚══════════════════════════════════════════════════════════════════╝
    """
    print(banner)


def print_results_summary(apps, stats) -> None:
    """Print a summary of discovery results."""
    print("\n" + "="*60)
    print("DISCOVERY RESULTS SUMMARY")
    print("="*60)
    print(f"Total Applications Discovered: {len(apps)}")
    print(f"  - High Confidence (≥70%):    {len([a for a in apps if a.confidence_score >= 70])}")
    print(f"  - Medium Confidence (40-69%): {len([a for a in apps if 40 <= a.confidence_score < 70])}")
    print(f"  - Low Confidence (<40%):      {len([a for a in apps if a.confidence_score < 40])}")
    print()
    
    if apps:
        print("Top 10 Applications by Confidence:")
        print("-"*60)
        print(f"{'Application':<30} {'Confidence':<12} {'Events':<15}")
        print("-"*60)
        for app in apps[:10]:
            print(f"{app.name:<30} {app.confidence_score:>3}%         {app.event_count:>12,}")
    print("="*60)


def main() -> int:
    """
    Main entry point for the application.
    
    Returns:
        Exit code (0 for success, 1 for error)
    """
    # Parse arguments
    args = parse_arguments()
    
    # Show configuration example and exit
    if args.show_config:
        print(EXAMPLE_CONFIG)
        return 0
    
    # Setup logging
    setup_logging(args.verbose, args.log_file)
    logger = logging.getLogger(__name__)
    
    # Print banner
    print_banner()
    
    try:
        # Create configuration
        config = create_config(args)
        
        # Validate configuration
        if not validate_config(config, args.test):
            return 1
        
        # Parse strategies if specified
        strategies = None
        if args.strategies:
            strategies = [s.strip() for s in args.strategies.split(',')]
            logger.info(f"Running specific strategies: {strategies}")
        
        # Create discovery instance
        logger.info(f"Connecting to Splunk at {config.splunk_host}:{config.splunk_port}")
        discovery = SplunkAppDiscovery(config, use_mock=args.test)
        
        # Run discovery
        logger.info("Starting application discovery...")
        apps = discovery.run_full_discovery(strategies=strategies)
        
        # Print summary
        print_results_summary(apps, discovery.discovery_stats)
        
        # Generate reports
        if apps:
            logger.info(f"Generating {args.format} report(s)...")
            reporter = ReportGenerator(apps, config, discovery.discovery_stats)
            
            if args.format == 'all':
                reports = reporter.generate_all_reports(args.filename)
                print("\nReports generated:")
                for fmt, path in reports.items():
                    print(f"  - {fmt.upper()}: {path}")
            else:
                if args.format == 'csv':
                    path = reporter.generate_csv(args.filename)
                elif args.format == 'json':
                    path = reporter.generate_json(args.filename)
                elif args.format == 'excel':
                    path = reporter.generate_excel(args.filename)
                elif args.format == 'html':
                    path = reporter.generate_html_report(args.filename)
                
                print(f"\nReport generated: {path}")
        else:
            logger.warning("No applications discovered. Check your Splunk connection and timeframe.")
        
        print("\n✓ Discovery completed successfully!")
        return 0
        
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        return 130
    except Exception as e:
        logger.error(f"Discovery failed: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
