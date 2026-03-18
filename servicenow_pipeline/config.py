# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration — ServiceNow Incident SLA Pipeline

class PipelineConfig:
    """Central configuration for the ServiceNow Incident SLA pipeline."""

    # ── Unity Catalog (shared with Genesys) ──
    CATALOG = "platform_analytics"
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    GOLD_SCHEMA = "gold"

    # ── Table Names ──
    # Bronze
    BRONZE_RAW = f"{CATALOG}.{BRONZE_SCHEMA}.raw_servicenow_incidents"
    BRONZE_CHECKPOINT = f"{CATALOG}.{BRONZE_SCHEMA}._splunk_extraction_checkpoint"

    # Silver
    SILVER_FACT_INCIDENTS = f"{CATALOG}.{SILVER_SCHEMA}.fact_incidents"
    SILVER_FACT_TIMELINE = f"{CATALOG}.{SILVER_SCHEMA}.fact_incident_timeline"
    SILVER_DIM_BUSINESS_SERVICES = f"{CATALOG}.{SILVER_SCHEMA}.dim_business_services"
    SILVER_DIM_ASSIGNMENT_GROUPS = f"{CATALOG}.{SILVER_SCHEMA}.dim_assignment_groups"
    SILVER_DIM_SLA_TARGETS = f"{CATALOG}.{SILVER_SCHEMA}.dim_sla_targets"

    # Gold
    GOLD_SLA_SUMMARY = f"{CATALOG}.{GOLD_SCHEMA}.agg_sla_summary"
    GOLD_PLATFORM_IMPACT = f"{CATALOG}.{GOLD_SCHEMA}.agg_platform_impact"
    GOLD_AGENT_IMPACT = f"{CATALOG}.{GOLD_SCHEMA}.agg_agent_impact"
    GOLD_VENDOR_SLA = f"{CATALOG}.{GOLD_SCHEMA}.agg_vendor_sla"
    GOLD_OPEN_INCIDENTS = f"{CATALOG}.{GOLD_SCHEMA}.fact_open_incidents"

    # ── Splunk Connection ──
    SPLUNK_SECRET_SCOPE = "splunk"
    SPLUNK_HOST_KEY = "host"
    SPLUNK_TOKEN_KEY = "token"
    SPLUNK_PORT = 8089
    SPLUNK_INDEX = "servicenow"
    SPLUNK_SOURCETYPE = "servicenow:incident"
    SPLUNK_USE_SSL = True
    SPLUNK_VERIFY_SSL = False

    # ── Polling ──
    POLL_OVERLAP_MINUTES = 3  # overlap window for zero data loss
    MAX_RESULTS_PER_QUERY = 50000

    # ── SLA Defaults (seeded into dim_sla_targets) ──
    SLA_TARGETS = {
        "P0": {"response": 15,   "resolution": 120,   "mode": "24X7"},
        "P1": {"response": 15,   "resolution": 240,   "mode": "24X7"},
        "P2": {"response": 30,   "resolution": 480,   "mode": "24X7"},
        "P3": {"response": 60,   "resolution": 1440,  "mode": "BUSINESS_HOURS"},
        "P4": {"response": 240,  "resolution": 2880,  "mode": "BUSINESS_HOURS"},
    }

    # ── Business Hours (for P3/P4 SLA calculation) ──
    BUSINESS_HOURS_START = "09:00"
    BUSINESS_HOURS_END = "18:00"
    BUSINESS_DAYS = "Mon,Tue,Wed,Thu,Fri"
    BUSINESS_HOURS_PER_DAY = 9  # 18:00 - 09:00

    # ── SLA Risk Threshold ──
    SLA_AT_RISK_PCT = 80  # >80% of SLA consumed → AT_RISK

    # ── Processing ──
    GOLD_RECOMPUTE_DAYS = 2

    # ── Splunk Query Fields ──
    SPLUNK_FIELDS = [
        "_time", "number", "sys_id", "short_description", "description",
        "priority", "urgency", "impact", "state",
        "category", "subcategory",
        "assignment_group", "assigned_to", "caller_id", "opened_by",
        "opened_at", "resolved_at", "closed_at",
        "response_time", "work_start",
        "close_code", "close_notes",
        "cmdb_ci", "business_service", "location", "company",
        "sys_created_on", "sys_updated_on"
    ]
