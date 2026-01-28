# Splunk Application Discovery Tool

> **Discover applications logging to Splunk in enterprise environments where no standardized tagging or naming conventions exist.**

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Features](#-features)
- [Architecture](#-architecture)
- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Configuration](#-configuration)
- [Usage](#-usage)
- [Discovery Strategies](#-discovery-strategies)
- [Output Formats](#-output-formats)
- [API Reference](#-api-reference)
- [Examples](#-examples)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)

---

## 🎯 Overview

The **Splunk Application Discovery Tool** is an enterprise-grade solution for identifying applications that are logging to Splunk. It's designed for organizations that:

- Have grown organically without standardized logging conventions
- Need to inventory applications for observability initiatives
- Want to understand their Splunk data landscape
- Are planning logging standardization efforts

### The Challenge

In large enterprises, applications often log to Splunk without consistent:
- Tagging (no `application=xyz` field)
- Naming conventions (random sourcetypes)
- Documentation (no CMDB integration)

### The Solution

This tool uses **multiple discovery strategies** to forensically identify applications:

```
┌─────────────────────────────────────────────────────────────┐
│                    Splunk Data Lake                          │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐           │
│  │ App A   │ │ App B   │ │ App C   │ │ Unknown │           │
│  │ (tagged)│ │(partial)│ │(no tags)│ │  Logs   │           │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘           │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Discovery Strategies                            │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │
│  │ Source   │ │ Content  │ │ Hostname │ │ JSON     │       │
│  │ Paths    │ │ Parsing  │ │ Patterns │ │ Fields   │       │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘       │
│  ┌──────────┐ ┌──────────┐                                  │
│  │Sourcetype│ │ Log      │                                  │
│  │ Analysis │ │ Cluster  │                                  │
│  └──────────┘ └──────────┘                                  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Aggregation & Scoring                           │
│  • Merge results from all strategies                        │
│  • Calculate confidence scores                               │
│  • Filter by thresholds                                      │
│  • Generate comprehensive reports                            │
└─────────────────────────────────────────────────────────────┘
```

---

## ✨ Features

### Discovery Capabilities
- **6 Discovery Strategies** - Multiple approaches to identify applications
- **Confidence Scoring** - Each discovered app gets a 0-100 confidence score
- **Evidence Tracking** - Know *why* each application was identified
- **Configurable Thresholds** - Filter noise with customizable settings

### Output Options
- **CSV** - Simple spreadsheet export
- **JSON** - Machine-readable format
- **Excel** - Formatted workbook with multiple sheets
- **HTML** - Interactive report with search functionality

### Enterprise Ready
- **Secure** - Uses Splunk's official SDK
- **Scalable** - Streaming results for large datasets
- **Configurable** - YAML/JSON configuration files
- **Extensible** - Easy to add new strategies

---

## 🏗 Architecture

```
splunk_app_discovery/
├── __init__.py              # Package initialization
├── config.py                # Configuration management
├── splunk_connection.py     # Splunk SDK wrapper
├── discovery_strategies.py  # Individual discovery algorithms
├── discovery.py             # Main orchestrator
├── reporting.py             # Report generation
└── main.py                  # CLI entry point
```

### Component Responsibilities

| Component | Responsibility |
|-----------|---------------|
| `config.py` | Load, validate, and manage configuration |
| `splunk_connection.py` | Handle Splunk authentication and queries |
| `discovery_strategies.py` | Individual discovery algorithm implementations |
| `discovery.py` | Orchestrate strategies and aggregate results |
| `reporting.py` | Generate output reports in various formats |
| `main.py` | Command-line interface |

---

## 📦 Installation

### Prerequisites
- Python 3.8 or higher
- Access to a Splunk Enterprise instance
- Splunk user with search capabilities

### Steps

1. **Clone or download the project:**
```bash
cd d:\Prasana\Code\Splunk
```

2. **Create a virtual environment (recommended):**
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

4. **Verify installation:**
```bash
python -m splunk_app_discovery.main --version
```

---

## 🚀 Quick Start

### Option 1: Using Environment Variables

```bash
# Set environment variables
set SPLUNK_HOST=splunk.company.com
set SPLUNK_USERNAME=admin
set SPLUNK_PASSWORD=your_password

# Run discovery
python -m splunk_app_discovery.main
```

### Option 2: Using Command-Line Arguments

```bash
python -m splunk_app_discovery.main \
  --host splunk.company.com \
  --user admin \
  --password your_password \
  --format all
```

### Option 3: Using Configuration File

```bash
# Copy and edit the example config
copy config.example.yaml config.yaml
# Edit config.yaml with your settings

# Run with config file
python -m splunk_app_discovery.main --config config.yaml
```

### Option 4: Test Mode (No Splunk Required)

```bash
# Run with mock data to see how it works
python -m splunk_app_discovery.main --test --verbose
```

---

## ⚙️ Configuration

### Configuration File (config.yaml)

```yaml
# Splunk Connection
splunk_host: "splunk.company.com"
splunk_port: 8089
username: "discovery_user"
password: "secret"  # Better: use environment variable
use_ssl: true
verify_ssl: false

# Discovery Settings
discovery_timeframe: "-7d"    # Last 7 days
max_results: 50000
min_event_count: 100          # Ignore apps with fewer events
confidence_threshold: 30      # 0-100

# Output
output_format: "all"          # csv, json, excel, html, all
output_path: "./output"

# Exclusions
excluded_indexes:
  - "_internal"
  - "_audit"
  
excluded_apps:
  - "log"
  - "tmp"
  - "debug"
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SPLUNK_HOST` | Splunk server hostname | localhost |
| `SPLUNK_PORT` | Management port | 8089 |
| `SPLUNK_USERNAME` | Username | - |
| `SPLUNK_PASSWORD` | Password | - |
| `SPLUNK_USE_SSL` | Use HTTPS | true |
| `DISCOVERY_TIMEFRAME` | Search time range | -7d |
| `DISCOVERY_OUTPUT_PATH` | Output directory | ./output |

---

## 💻 Usage

### Command-Line Interface

```bash
python -m splunk_app_discovery.main [OPTIONS]
```

### Options

| Option | Short | Description |
|--------|-------|-------------|
| `--host` | `-H` | Splunk server hostname |
| `--port` | `-P` | Splunk management port |
| `--user` | `-u` | Splunk username |
| `--password` | `-p` | Splunk password |
| `--config` | `-c` | Path to config file |
| `--timeframe` | `-t` | Discovery time range (e.g., -7d) |
| `--strategies` | `-s` | Comma-separated strategy names |
| `--confidence` | | Minimum confidence threshold |
| `--output` | `-o` | Output directory |
| `--format` | `-f` | Output format (csv/json/excel/html/all) |
| `--test` | | Run with mock data |
| `--verbose` | `-v` | Enable debug logging |
| `--show-config` | | Show example configuration |

### Examples

```bash
# Basic discovery with default settings
python -m splunk_app_discovery.main --host splunk.company.com --user admin

# Discover only from source paths and hostnames
python -m splunk_app_discovery.main --strategies SourcePath,Hostname

# Generate only CSV report for last 24 hours
python -m splunk_app_discovery.main --timeframe "-24h" --format csv

# High confidence apps only
python -m splunk_app_discovery.main --confidence 70

# Verbose mode with log file
python -m splunk_app_discovery.main --verbose --log-file discovery.log
```

### Programmatic Usage

```python
from splunk_app_discovery import SplunkAppDiscovery, DiscoveryConfig

# Create configuration
config = DiscoveryConfig(
    splunk_host="splunk.company.com",
    splunk_port=8089,
    username="admin",
    password="secret",
    discovery_timeframe="-7d"
)

# Run discovery
discovery = SplunkAppDiscovery(config)
apps = discovery.run_full_discovery()

# Process results
for app in apps[:10]:
    print(f"{app.name}: {app.confidence_score}% ({app.event_count:,} events)")

# Export to DataFrame
df = discovery.to_dataframe()
print(df.head())
```

---

## 🔍 Discovery Strategies

### 1. Source Path Discovery (`SourcePath`)
Extracts application names from log file paths.

**What it finds:**
- `/var/log/myapp/application.log` → `myapp`
- `/opt/payment-service/logs/payment.log` → `payment-service`
- `C:\Program Files\OrderSystem\logs\orders.log` → `OrderSystem`

**Reliability:** ⭐⭐⭐⭐⭐ High

---

### 2. Sourcetype Discovery (`Sourcetype`)
Analyzes sourcetype naming patterns.

**What it finds:**
- `myapp:application` → `myapp`
- `payment_service_logs` → `payment_service`
- `order-api-json` → `order-api`

**Reliability:** ⭐⭐⭐⭐ Medium-High

---

### 3. Hostname Discovery (`Hostname`)
Extracts applications from server hostname patterns.

**What it finds:**
- `myapp-prod-01.company.com` → `myapp`
- `payment-server-01` → `payment`
- `web-orderapi-02` → `orderapi`

**Reliability:** ⭐⭐⭐ Medium

---

### 4. JSON Field Discovery (`JsonField`)
Finds app names in structured JSON logs.

**Fields searched:**
- `application`, `app`, `appName`
- `service`, `serviceName`
- `component`, `module`
- `logger`, `logger_name`

**Reliability:** ⭐⭐⭐⭐ Medium-High (for JSON logs)

---

### 5. Log Content Discovery (`LogContent`)
Parses actual log messages for app identifiers.

**Patterns matched:**
- `application=myapp` → `myapp`
- `[PaymentService]` → `PaymentService`
- `com.company.OrderProcessor` → `OrderProcessor`

**Reliability:** ⭐⭐ Low-Medium (requires tuning)

---

### 6. Cluster Discovery (`Clustering`)
Uses Splunk's clustering to group similar log patterns.

**Reliability:** ⭐⭐ Low (supplementary)

---

## 📊 Output Formats

### CSV
Simple spreadsheet format, easily imported into Excel or databases.

```csv
Application,Confidence Score,Event Count,Sources,Hosts,Discovery Methods
payment-service,85,125000,3,5,"SourcePath, Sourcetype, JsonField"
order-api,72,89000,2,3,"SourcePath, Hostname"
```

### JSON
Complete data with full evidence and metadata.

```json
{
  "generated_at": "2026-01-25T10:30:00",
  "summary": {
    "total_applications": 47,
    "high_confidence": 12,
    "medium_confidence": 23,
    "low_confidence": 12
  },
  "applications": [
    {
      "name": "payment-service",
      "confidence_score": 85,
      "event_count": 125000,
      "sources": ["/var/log/payment-service/..."],
      "evidence": ["[SourcePath] Source path: /var/log/..."]
    }
  ]
}
```

### Excel
Multi-sheet workbook with:
- **Summary** - Overview statistics
- **Applications** - Main results table (color-coded)
- **Evidence Details** - How each app was discovered
- **Sources** - All source paths by application

### HTML
Interactive report with:
- Summary statistics cards
- Searchable application table
- Color-coded confidence levels
- Responsive design

---

## 📖 API Reference

### DiscoveryConfig

```python
config = DiscoveryConfig(
    splunk_host="splunk.example.com",
    splunk_port=8089,
    username="admin",
    password="secret",
    discovery_timeframe="-7d",
    min_event_count=100,
    confidence_threshold=30,
    output_path="./output"
)

# Load from file
config = DiscoveryConfig.from_file("config.yaml")

# Load from environment
config = DiscoveryConfig.from_environment()
```

### SplunkAppDiscovery

```python
discovery = SplunkAppDiscovery(config)

# Run all strategies
apps = discovery.run_full_discovery()

# Run specific strategies
apps = discovery.run_full_discovery(strategies=["SourcePath", "Hostname"])

# Get summary
summary = discovery.get_results_summary()

# Search apps
matches = discovery.search_apps("payment")

# Convert to DataFrame
df = discovery.to_dataframe()
```

### ReportGenerator

```python
from splunk_app_discovery.reporting import ReportGenerator

reporter = ReportGenerator(apps, config, stats)

# Generate individual reports
reporter.generate_csv("apps.csv")
reporter.generate_json("apps.json")
reporter.generate_excel("apps.xlsx")
reporter.generate_html_report("report.html")

# Generate all formats
reporter.generate_all_reports("discovery_results")
```

---

## 📝 Examples

### Example 1: Quick Discovery Script

```python
"""quick_discovery.py - Simple discovery script"""

from splunk_app_discovery import SplunkAppDiscovery, DiscoveryConfig

# Configure
config = DiscoveryConfig(
    splunk_host="splunk.company.com",
    username="admin",
    password="secret"
)

# Discover
discovery = SplunkAppDiscovery(config)
apps = discovery.run_full_discovery()

# Print top 20 apps
print(f"\nDiscovered {len(apps)} applications:\n")
for app in apps[:20]:
    print(f"  {app.name:<30} {app.confidence_score:>3}% ({app.event_count:>10,} events)")
```

### Example 2: Databricks Integration

```python
"""databricks_discovery.py - Run discovery from Databricks"""

from splunk_app_discovery import SplunkAppDiscovery, DiscoveryConfig

# Get credentials from Databricks secrets
config = DiscoveryConfig(
    splunk_host="splunk.company.com",
    username=dbutils.secrets.get(scope="splunk", key="username"),
    password=dbutils.secrets.get(scope="splunk", key="password"),
    discovery_timeframe="-30d"
)

# Run discovery
discovery = SplunkAppDiscovery(config)
apps = discovery.run_full_discovery()

# Convert to Spark DataFrame
df = discovery.to_dataframe()
spark_df = spark.createDataFrame(df)

# Save to Delta table
spark_df.write.mode("overwrite").saveAsTable("observability.splunk_applications")

print(f"Saved {len(apps)} applications to Delta table")
```

### Example 3: Scheduled Discovery with Email Report

```python
"""scheduled_discovery.py - Weekly discovery with email"""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from splunk_app_discovery import SplunkAppDiscovery, DiscoveryConfig
from splunk_app_discovery.reporting import ReportGenerator

# Run discovery
config = DiscoveryConfig.from_file("config.yaml")
discovery = SplunkAppDiscovery(config)
apps = discovery.run_full_discovery()

# Generate Excel report
reporter = ReportGenerator(apps, config, discovery.discovery_stats)
report_path = reporter.generate_excel("weekly_discovery.xlsx")

# Email the report
msg = MIMEMultipart()
msg['Subject'] = f'Weekly Splunk Discovery: {len(apps)} Applications Found'
msg['From'] = 'observability@company.com'
msg['To'] = 'platform-team@company.com'

with open(report_path, 'rb') as f:
    attachment = MIMEBase('application', 'octet-stream')
    attachment.set_payload(f.read())
    encoders.encode_base64(attachment)
    attachment.add_header('Content-Disposition', f'attachment; filename=weekly_discovery.xlsx')
    msg.attach(attachment)

# Send email
with smtplib.SMTP('smtp.company.com') as server:
    server.send_message(msg)

print("Weekly discovery report sent!")
```

---

## 🔧 Troubleshooting

### Common Issues

#### Connection Failed
```
SplunkConnectionError: Failed to connect to Splunk after 3 attempts
```
**Solutions:**
- Verify hostname and port are correct
- Check firewall allows connection to port 8089
- Ensure username/password are valid
- Try with `--no-ssl` if SSL issues

#### No Applications Found
**Solutions:**
- Increase timeframe: `--timeframe "-30d"`
- Lower confidence threshold: `--confidence 20`
- Lower minimum events: `--min-events 10`
- Check if indexes are excluded

#### Missing splunk-sdk
```
ImportError: Splunk SDK is required
```
**Solution:**
```bash
pip install splunk-sdk
```

#### Memory Issues with Large Results
**Solution:**
- Reduce timeframe
- Reduce `max_results` in config
- Run strategies individually

### Debug Mode

```bash
python -m splunk_app_discovery.main --verbose --log-file debug.log
```

---

## 🤝 Contributing

Contributions are welcome! Areas for improvement:

1. **New Discovery Strategies**
   - Container/Kubernetes label extraction
   - AWS CloudWatch integration
   - Log4j MDC field parsing

2. **Improved Patterns**
   - More source path patterns
   - Better hostname parsing
   - Industry-specific patterns

3. **Integrations**
   - CMDB lookup and enrichment
   - ServiceNow integration
   - PagerDuty service mapping

---

## 📄 License

MIT License - See LICENSE file for details.

---

## 📞 Support

For issues or questions:
1. Check the [Troubleshooting](#-troubleshooting) section
2. Review existing issues
3. Create a new issue with:
   - Python version
   - Splunk version
   - Error message
   - Configuration (redact passwords)

---

**Made with ❤️ for Enterprise Observability**
