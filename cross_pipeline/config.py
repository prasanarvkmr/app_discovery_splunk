# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration — Cross-Pipeline Correlation
# MAGIC
# MAGIC References both the Genesys and ServiceNow catalogs to build
# MAGIC cross-reference views in the shared `platform_analytics` Gold layer.

class CorrelationConfig:
    """Configuration for Genesys ↔ ServiceNow correlation layer."""

    # ── Shared Output Catalog ──
    CATALOG = "platform_analytics"
    GOLD_SCHEMA = "gold"

    # ── Genesys Source Tables (cross-catalog read) ──
    GENESYS_CATALOG = "genesys_analytics"
    GENESYS_FACT_CONVERSATIONS = f"{GENESYS_CATALOG}.silver.fact_conversations"
    GENESYS_FACT_AGENT_LEGS = f"{GENESYS_CATALOG}.silver.fact_agent_legs"
    GENESYS_AGG_VENDOR_LOCATION = f"{GENESYS_CATALOG}.gold.agg_vendor_location"
    GENESYS_DIM_LOCATIONS = f"{GENESYS_CATALOG}.silver.dim_locations"

    # ── ServiceNow Source Tables (same catalog) ──
    SNOW_FACT_INCIDENTS = f"{CATALOG}.silver.fact_incidents"
    SNOW_DIM_BUSINESS_SERVICES = f"{CATALOG}.silver.dim_business_services"

    # ── Correlation Outputs ──
    GOLD_CORRELATION = f"{CATALOG}.{GOLD_SCHEMA}.agg_call_quality_incident_correlation"
    GOLD_VW_DETAIL = f"{CATALOG}.{GOLD_SCHEMA}.vw_call_quality_incident_detail"
    GOLD_VW_ROOT_CAUSE = f"{CATALOG}.{GOLD_SCHEMA}.vw_degradation_root_cause_summary"

    # ── Processing ──
    RECOMPUTE_DAYS = 3          # wider window than individual pipelines
    TIME_WINDOW_HOURS = 1       # ±1 hour overlap for event-level matching
