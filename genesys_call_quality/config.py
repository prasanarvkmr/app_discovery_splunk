# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration — Genesys Call Quality Pipeline

# Configuration constants for the pipeline
class PipelineConfig:
    """Central configuration for the Genesys Call Quality pipeline."""

    # ── Unity Catalog ──
    CATALOG = "genesys_analytics"
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    GOLD_SCHEMA = "gold"

    # ── Table Names ──
    # Bronze
    BRONZE_RAW = f"{CATALOG}.{BRONZE_SCHEMA}.raw_genesys_conversations"
    BRONZE_CHECKPOINT = f"{CATALOG}.{BRONZE_SCHEMA}._elk_pagination_checkpoint"

    # Silver
    SILVER_FACT_PARTICIPANTS = f"{CATALOG}.{SILVER_SCHEMA}.fact_conversation_participants"
    SILVER_FACT_CONVERSATIONS = f"{CATALOG}.{SILVER_SCHEMA}.fact_conversations"
    SILVER_FACT_AGENT_LEGS = f"{CATALOG}.{SILVER_SCHEMA}.fact_agent_legs"
    SILVER_DIM_AGENTS = f"{CATALOG}.{SILVER_SCHEMA}.dim_agents"
    SILVER_DIM_LOCATIONS = f"{CATALOG}.{SILVER_SCHEMA}.dim_locations"

    # Gold
    GOLD_CALL_QUALITY_SUMMARY = f"{CATALOG}.{GOLD_SCHEMA}.agg_call_quality_summary"
    GOLD_VENDOR_LOCATION = f"{CATALOG}.{GOLD_SCHEMA}.agg_vendor_location"
    GOLD_AGENT_FLAGGED = f"{CATALOG}.{GOLD_SCHEMA}.agg_agent_flagged"
    GOLD_AGENT_PERFORMANCE = f"{CATALOG}.{GOLD_SCHEMA}.agg_agent_performance_detail"

    # Gold Views
    GOLD_VW_VENDOR_CONTINENT = f"{CATALOG}.{GOLD_SCHEMA}.vw_vendor_by_continent"
    GOLD_VW_VENDOR_COUNTRY = f"{CATALOG}.{GOLD_SCHEMA}.vw_vendor_by_country"

    # ── ELK API ──
    ELK_BASE_URL = "https://your-elk-host:9200"  # Override via Databricks widget/secret
    ELK_INDEX = "genesys-conversations-*"
    ELK_SECRET_SCOPE = "elk"
    ELK_SECRET_KEY = "api_key"
    PAGE_SIZE = 5000
    PIT_KEEP_ALIVE = "5m"
    MAX_RETRIES = 3
    OVERLAP_MINUTES = 2  # overlap window for zero data loss

    # ── Call Quality Thresholds ──
    MOS_ERRORED_THRESHOLD = 2.0       # MOS < 2.0 → ERRORED
    MOS_DEGRADED_THRESHOLD = 3.5      # MOS >= 2.0 and < 3.5 → DEGRADED
    AGENT_ERRORED_FLAG_PCT = 10.0     # Flag agents with > 10% errored calls
    AGENT_DEGRADED_FLAG_PCT = 20.0    # Flag agents with > 20% degraded calls

    # ── Processing ──
    GOLD_RECOMPUTE_DAYS = 2  # Recompute last N days in Gold to catch late arrivals
