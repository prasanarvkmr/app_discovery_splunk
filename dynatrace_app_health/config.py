# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration — Dynatrace App Health & Coverage Report
# MAGIC
# MAGIC Connects CMDB application inventory to Dynatrace entity monitoring
# MAGIC via tag-based lookups (AppId / AppServiceId / AppServiceName).

class AppHealthConfig:
    """Central configuration for the Dynatrace app health & coverage pipeline."""

    # ── Unity Catalog ──
    CATALOG = "platform_analytics"
    GOLD_SCHEMA = "gold"

    # Output tables
    GOLD_APP_COVERAGE = f"{CATALOG}.{GOLD_SCHEMA}.app_dynatrace_coverage"
    GOLD_APP_HEALTH = f"{CATALOG}.{GOLD_SCHEMA}.app_dynatrace_health"

    # ── CMDB Source (Databricks table) ──
    CMDB_TABLE = "your_catalog.your_schema.cmdb_applications"  # Override per env
    CMDB_COLUMNS = {
        "app_id": "app_id",                    # e.g. 12345
        "app_service_id": "app_service_id",    # e.g. APPSVC0001
        "app_service_name": "app_service_name",
        "app_name": "app_name",                # display name
        "owner": "owner",
        "criticality": "criticality",          # P0, P1, P2, …
        "environment": "environment",          # PROD, UAT, DEV
    }
    CMDB_ENVIRONMENT_FILTER = "PROD"  # only scan PROD apps

    # ── Dynatrace PROD Connection ──
    DT_SECRET_SCOPE = "dynatrace"
    DT_URL_KEY = "prod_url"          # secret key for base URL
    DT_TOKEN_KEY = "prod_api_token"  # secret key for API token

    # SSL — override if your environment uses internal CAs
    DT_SSL_CONFIG = {"verify": False}  # set to CA path in production

    # ── Tag Matching ──
    # Precedence order: try each tag key until entities are found
    TAG_KEYS = [
        {"cmdb_col": "app_id",           "dt_tag": "AppId"},
        {"cmdb_col": "app_service_id",   "dt_tag": "AppServiceId"},
        {"cmdb_col": "app_service_name", "dt_tag": "AppServiceName"},
    ]

    # Entity types to scan
    ENTITY_TYPES = ["HOST", "SERVICE", "APPLICATION", "PROCESS_GROUP"]

    # ── API Tuning ──
    PAGE_SIZE = 500
    API_DELAY_SECONDS = 0.5   # delay between batches to avoid rate limits
    MAX_RETRIES = 3
    TIMEOUT_SECONDS = 60

    # ── Metrics ──
    # Host metrics (reuse from dt_host.py)
    HOST_METRICS = {
        "cpu_usage": "builtin:host.cpu.usage",
        "memory_usage": "builtin:host.mem.usage",
        "disk_usage": "builtin:host.disk.usedPct",
        "host_availability": "builtin:host.availability",
    }

    # Service metrics
    SERVICE_METRICS = {
        "response_time": "builtin:service.response.time",
        "error_count": "builtin:service.errors.total.count",
        "request_count": "builtin:service.requestCount.total",
        "failure_rate": "builtin:service.errors.total.rate",
    }

    # Web application metrics
    APPLICATION_METRICS = {
        "action_count": "builtin:apps.web.actionCount",
        "error_count": "builtin:apps.web.errors.httpErrorsCount",
        "load_action_duration": "builtin:apps.web.action.duration",
    }

    METRICS_TIME_RANGE = "now-1h"
    METRICS_RESOLUTION = "1h"

    # ── Coverage Classification ──
    # FULL   = at least hosts + services found
    # PARTIAL = some entity types found but not both hosts & services
    # HOSTS_ONLY = only hosts
    # NOT_MONITORED = zero entities
    FULL_COVERAGE_REQUIRES = {"HOST", "SERVICE"}

    # ── Report Output ──
    REPORT_OUTPUT_DIR = "/tmp/dynatrace_app_health"  # local/DBFS path
    REPORT_FORMATS = ["csv", "html"]
