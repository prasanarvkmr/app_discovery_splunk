# Databricks notebook source
# MAGIC %md
# MAGIC # DDL — Correlation Table & Views
# MAGIC
# MAGIC Creates the cross-pipeline Gold table and views in `platform_analytics.gold`.
# MAGIC Run this notebook ONCE after both pipeline DDLs have been executed.

# COMMAND ----------

from config import CorrelationConfig as CC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Materialized Correlation Table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CC.GOLD_CORRELATION} (
        correlation_date            DATE,
        vendor                      STRING,
        continent                   STRING,
        country                     STRING,
        location                    STRING,

        -- Call quality metrics (Genesys)
        total_calls                 BIGINT,
        errored_calls               BIGINT,
        degraded_calls              BIGINT,
        errored_pct                 DOUBLE,
        degraded_pct                DOUBLE,

        -- Incident metrics (ServiceNow)
        active_incidents            BIGINT,
        p0_p1_incidents             BIGINT,
        p2_incidents                BIGINT,
        p3_p4_incidents             BIGINT,
        incident_numbers            ARRAY<STRING>,

        -- Correlation flags
        has_active_incident         BOOLEAN,
        has_critical_incident       BOOLEAN,
        degradation_with_incident   BOOLEAN,

        _refreshed_ts               TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Event-Level Detail View
# MAGIC
# MAGIC Individual degraded / errored calls matched to concurrent active incidents.
# MAGIC Uses a ±1 hour time window for temporal overlap.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {CC.GOLD_VW_DETAIL} AS
    SELECT
        gc.conversation_id,
        gc.call_date,
        gc.call_start_ts,
        gc.call_end_ts,
        gc.vendor                   AS genesys_vendor,
        gc.location                 AS genesys_location,
        COALESCE(gl.continent, 'Unknown') AS continent,
        COALESCE(gl.country, 'Unknown')   AS country,
        gc.overall_call_status,
        gc.agent_mos_score,
        gc.customer_mos_score,

        -- Matched incident
        si.incident_number,
        si.priority,
        si.state                    AS incident_state,
        si.short_description        AS incident_description,
        si.business_service,
        si.vendor_name              AS snow_vendor,
        si.opened_at                AS incident_opened_at,
        si.resolved_at              AS incident_resolved_at,
        si.response_sla_status,
        si.resolution_sla_status

    FROM {CC.GENESYS_FACT_CONVERSATIONS} gc

    -- Geographic enrichment
    LEFT JOIN {CC.GENESYS_DIM_LOCATIONS} gl
        ON gc.location = gl.location_key

    -- Match ServiceNow incidents: same vendor + incident active during call
    INNER JOIN {CC.SNOW_FACT_INCIDENTS} si
        ON UPPER(gc.vendor) = UPPER(si.vendor_name)
        AND si.opened_at <= gc.call_end_ts + INTERVAL {CC.TIME_WINDOW_HOURS} HOURS
        AND COALESCE(si.resolved_at, CURRENT_TIMESTAMP()) >= gc.call_start_ts - INTERVAL {CC.TIME_WINDOW_HOURS} HOURS

    WHERE gc.overall_call_status IN ('DEGRADED', 'ERRORED')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Root-Cause Summary View
# MAGIC
# MAGIC Dashboard-level metric: what % of degradation events have a concurrent incident?

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {CC.GOLD_VW_ROOT_CAUSE} AS
    WITH quality_events AS (
        SELECT
            gc.call_date,
            gc.vendor,
            gc.conversation_id,
            gc.overall_call_status,
            -- flag: was there at least one concurrent incident for this call?
            MAX(CASE WHEN si.incident_number IS NOT NULL THEN 1 ELSE 0 END) AS has_incident,
            MAX(CASE WHEN si.priority IN ('P0', 'P1') THEN 1 ELSE 0 END) AS has_critical_incident,
            COLLECT_SET(si.incident_number) AS matched_incidents
        FROM {CC.GENESYS_FACT_CONVERSATIONS} gc
        LEFT JOIN {CC.SNOW_FACT_INCIDENTS} si
            ON UPPER(gc.vendor) = UPPER(si.vendor_name)
            AND si.opened_at <= gc.call_end_ts + INTERVAL {CC.TIME_WINDOW_HOURS} HOURS
            AND COALESCE(si.resolved_at, CURRENT_TIMESTAMP()) >= gc.call_start_ts - INTERVAL {CC.TIME_WINDOW_HOURS} HOURS
        WHERE gc.overall_call_status IN ('DEGRADED', 'ERRORED')
        GROUP BY gc.call_date, gc.vendor, gc.conversation_id, gc.overall_call_status
    )
    SELECT
        call_date,
        vendor,
        overall_call_status,
        COUNT(*) AS total_degraded_calls,
        SUM(has_incident) AS calls_with_incident,
        SUM(has_critical_incident) AS calls_with_critical_incident,
        COUNT(*) - SUM(has_incident) AS calls_without_incident,
        ROUND(SUM(has_incident) * 100.0 / COUNT(*), 2) AS pct_explained_by_incident,
        ROUND(SUM(has_critical_incident) * 100.0 / COUNT(*), 2) AS pct_explained_by_critical
    FROM quality_events
    GROUP BY call_date, vendor, overall_call_status
""")

# COMMAND ----------

print("Correlation table and views created successfully.")
