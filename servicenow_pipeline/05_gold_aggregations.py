# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — SLA Aggregations & Operational Views
# MAGIC Silver → Gold: SLA compliance, platform impact, agent impact, vendor SLA, open incidents.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from config import PipelineConfig as C

# COMMAND ----------

class GoldAggregations:
    """Builds Gold layer aggregate tables from Silver fact_incidents."""

    def __init__(self, spark):
        self.spark = spark
        self.recompute_days = C.GOLD_RECOMPUTE_DAYS

    # ─────────────────────────────────────────────────
    # 1. SLA Summary — Overall compliance by priority
    # ─────────────────────────────────────────────────

    def agg_sla_summary(self):
        """Daily SLA compliance metrics by priority."""
        self.spark.sql(f"""
            MERGE INTO {C.GOLD_SLA_SUMMARY} AS t
            USING (
                SELECT
                    incident_date AS summary_date,
                    priority,
                    COUNT(*) AS total_incidents,

                    -- Response SLA
                    SUM(CASE WHEN response_sla_status = 'MET' THEN 1 ELSE 0 END) AS response_sla_met,
                    SUM(CASE WHEN response_sla_status = 'BREACHED' THEN 1 ELSE 0 END) AS response_sla_breached,
                    SUM(CASE WHEN response_sla_status IN ('IN_PROGRESS', 'AT_RISK') THEN 1 ELSE 0 END) AS response_sla_in_progress,

                    -- Resolution SLA
                    SUM(CASE WHEN resolution_sla_status = 'MET' THEN 1 ELSE 0 END) AS resolution_sla_met,
                    SUM(CASE WHEN resolution_sla_status = 'BREACHED' THEN 1 ELSE 0 END) AS resolution_sla_breached,
                    SUM(CASE WHEN resolution_sla_status IN ('IN_PROGRESS', 'AT_RISK') THEN 1 ELSE 0 END) AS resolution_sla_in_progress,

                    -- Compliance % (only count resolved incidents)
                    ROUND(
                        SUM(CASE WHEN response_sla_status = 'MET' THEN 1 ELSE 0 END) * 100.0
                        / NULLIF(SUM(CASE WHEN response_sla_status IN ('MET', 'BREACHED') THEN 1 ELSE 0 END), 0),
                    2) AS response_sla_compliance_pct,
                    ROUND(
                        SUM(CASE WHEN resolution_sla_status = 'MET' THEN 1 ELSE 0 END) * 100.0
                        / NULLIF(SUM(CASE WHEN resolution_sla_status IN ('MET', 'BREACHED') THEN 1 ELSE 0 END), 0),
                    2) AS resolution_sla_compliance_pct,

                    -- Averages
                    ROUND(AVG(response_time_minutes), 2) AS avg_response_time_minutes,
                    ROUND(AVG(resolution_time_minutes), 2) AS avg_resolution_time_minutes

                FROM {C.SILVER_FACT_INCIDENTS}
                WHERE incident_date >= CURRENT_DATE - INTERVAL {self.recompute_days} DAYS
                GROUP BY incident_date, priority
            ) AS s
            ON t.summary_date = s.summary_date AND t.priority = s.priority
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print("agg_sla_summary MERGE complete.")

    # ─────────────────────────────────────────────────
    # 2. Platform Impact — Apps impacting the platform
    # ─────────────────────────────────────────────────

    def agg_platform_impact(self):
        """SLA metrics for incidents impacting platform components."""
        self.spark.sql(f"""
            MERGE INTO {C.GOLD_PLATFORM_IMPACT} AS t
            USING (
                SELECT
                    incident_date AS summary_date,
                    business_service,
                    platform_component,
                    priority,
                    COUNT(*) AS total_incidents,
                    SUM(CASE WHEN is_open = true THEN 1 ELSE 0 END) AS open_incidents,

                    SUM(CASE WHEN response_sla_status = 'MET' THEN 1 ELSE 0 END) AS response_sla_met,
                    SUM(CASE WHEN response_sla_status = 'BREACHED' THEN 1 ELSE 0 END) AS response_sla_breached,
                    SUM(CASE WHEN resolution_sla_status = 'MET' THEN 1 ELSE 0 END) AS resolution_sla_met,
                    SUM(CASE WHEN resolution_sla_status = 'BREACHED' THEN 1 ELSE 0 END) AS resolution_sla_breached,

                    ROUND(
                        SUM(CASE WHEN response_sla_status = 'MET' THEN 1 ELSE 0 END) * 100.0
                        / NULLIF(SUM(CASE WHEN response_sla_status IN ('MET', 'BREACHED') THEN 1 ELSE 0 END), 0),
                    2) AS response_compliance_pct,
                    ROUND(
                        SUM(CASE WHEN resolution_sla_status = 'MET' THEN 1 ELSE 0 END) * 100.0
                        / NULLIF(SUM(CASE WHEN resolution_sla_status IN ('MET', 'BREACHED') THEN 1 ELSE 0 END), 0),
                    2) AS resolution_compliance_pct,

                    ROUND(AVG(response_time_minutes), 2) AS avg_response_minutes,
                    ROUND(AVG(resolution_time_minutes), 2) AS avg_resolution_minutes,
                    ROUND(AVG(CASE WHEN is_open = false THEN resolution_time_minutes END), 2) AS mttr_minutes

                FROM {C.SILVER_FACT_INCIDENTS}
                WHERE business_service_type = 'PLATFORM'
                  AND incident_date >= CURRENT_DATE - INTERVAL {self.recompute_days} DAYS
                GROUP BY incident_date, business_service, platform_component, priority
            ) AS s
            ON t.summary_date = s.summary_date
                AND t.business_service = s.business_service
                AND t.priority = s.priority
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print("agg_platform_impact MERGE complete.")

    # ─────────────────────────────────────────────────
    # 3. Agent Impact — Incidents impacting agents
    # ─────────────────────────────────────────────────

    def agg_agent_impact(self):
        """SLA metrics for incidents that impact contact center agents."""
        self.spark.sql(f"""
            MERGE INTO {C.GOLD_AGENT_IMPACT} AS t
            USING (
                SELECT
                    incident_date AS summary_date,
                    business_service,
                    priority,
                    COUNT(*) AS total_incidents,
                    SUM(CASE WHEN is_open = true THEN 1 ELSE 0 END) AS open_incidents,

                    SUM(CASE WHEN response_sla_status = 'MET' THEN 1 ELSE 0 END) AS response_sla_met,
                    SUM(CASE WHEN response_sla_status = 'BREACHED' THEN 1 ELSE 0 END) AS response_sla_breached,
                    SUM(CASE WHEN resolution_sla_status = 'MET' THEN 1 ELSE 0 END) AS resolution_sla_met,
                    SUM(CASE WHEN resolution_sla_status = 'BREACHED' THEN 1 ELSE 0 END) AS resolution_sla_breached,

                    ROUND(
                        SUM(CASE WHEN response_sla_status = 'MET' THEN 1 ELSE 0 END) * 100.0
                        / NULLIF(SUM(CASE WHEN response_sla_status IN ('MET', 'BREACHED') THEN 1 ELSE 0 END), 0),
                    2) AS response_compliance_pct,
                    ROUND(
                        SUM(CASE WHEN resolution_sla_status = 'MET' THEN 1 ELSE 0 END) * 100.0
                        / NULLIF(SUM(CASE WHEN resolution_sla_status IN ('MET', 'BREACHED') THEN 1 ELSE 0 END), 0),
                    2) AS resolution_compliance_pct,

                    ROUND(AVG(response_time_minutes), 2) AS avg_response_minutes,
                    ROUND(AVG(resolution_time_minutes), 2) AS avg_resolution_minutes,
                    ROUND(AVG(CASE WHEN is_open = false THEN resolution_time_minutes END), 2) AS mttr_minutes

                FROM {C.SILVER_FACT_INCIDENTS}
                WHERE is_agent_facing = true
                  AND incident_date >= CURRENT_DATE - INTERVAL {self.recompute_days} DAYS
                GROUP BY incident_date, business_service, priority
            ) AS s
            ON t.summary_date = s.summary_date
                AND t.business_service = s.business_service
                AND t.priority = s.priority
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print("agg_agent_impact MERGE complete.")

    # ─────────────────────────────────────────────────
    # 4. Vendor SLA — Response/Resolution by vendor
    # ─────────────────────────────────────────────────

    def agg_vendor_sla(self):
        """Vendor-level SLA compliance from business_service classification."""
        self.spark.sql(f"""
            MERGE INTO {C.GOLD_VENDOR_SLA} AS t
            USING (
                SELECT
                    incident_date AS summary_date,
                    vendor_name,
                    priority,
                    COUNT(*) AS total_incidents,

                    SUM(CASE WHEN response_sla_status = 'MET' THEN 1 ELSE 0 END) AS response_sla_met,
                    SUM(CASE WHEN response_sla_status = 'BREACHED' THEN 1 ELSE 0 END) AS response_sla_breached,
                    SUM(CASE WHEN resolution_sla_status = 'MET' THEN 1 ELSE 0 END) AS resolution_sla_met,
                    SUM(CASE WHEN resolution_sla_status = 'BREACHED' THEN 1 ELSE 0 END) AS resolution_sla_breached,

                    ROUND(
                        SUM(CASE WHEN response_sla_status = 'MET' THEN 1 ELSE 0 END) * 100.0
                        / NULLIF(SUM(CASE WHEN response_sla_status IN ('MET', 'BREACHED') THEN 1 ELSE 0 END), 0),
                    2) AS response_compliance_pct,
                    ROUND(
                        SUM(CASE WHEN resolution_sla_status = 'MET' THEN 1 ELSE 0 END) * 100.0
                        / NULLIF(SUM(CASE WHEN resolution_sla_status IN ('MET', 'BREACHED') THEN 1 ELSE 0 END), 0),
                    2) AS resolution_compliance_pct,

                    ROUND(AVG(response_time_minutes), 2) AS avg_response_minutes,
                    ROUND(AVG(resolution_time_minutes), 2) AS avg_resolution_minutes,
                    ROUND(MAX(resolution_time_minutes), 2) AS worst_resolution_minutes

                FROM {C.SILVER_FACT_INCIDENTS}
                WHERE business_service_type = 'VENDOR'
                  AND vendor_name IS NOT NULL
                  AND incident_date >= CURRENT_DATE - INTERVAL {self.recompute_days} DAYS
                GROUP BY incident_date, vendor_name, priority
            ) AS s
            ON t.summary_date = s.summary_date
                AND t.vendor_name = s.vendor_name
                AND t.priority = s.priority
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print("agg_vendor_sla MERGE complete.")

    # ─────────────────────────────────────────────────
    # 5. Open Incidents — Real-time operational view
    # ─────────────────────────────────────────────────

    def refresh_open_incidents(self):
        """
        Full replace of open incidents with live SLA remaining calculations.
        This is NOT a MERGE — it's a complete refresh every cycle.
        """
        self.spark.sql(f"TRUNCATE TABLE {C.GOLD_OPEN_INCIDENTS}")

        self.spark.sql(f"""
            INSERT INTO {C.GOLD_OPEN_INCIDENTS}
            SELECT
                incident_number,
                incident_sys_id,
                short_description,
                priority,
                state,
                assignment_group,
                assigned_to,
                business_service,
                business_service_type,
                vendor_name,
                platform_component,
                is_agent_facing,
                opened_at,
                first_response_at,
                response_time_minutes,
                response_sla_target_minutes,
                response_sla_status,
                resolution_sla_target_minutes,
                resolution_sla_status,

                -- Live calculated fields
                ROUND(
                    (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(opened_at)) / 60.0, 2
                ) AS time_open_minutes,

                ROUND(
                    GREATEST(0, response_sla_target_minutes -
                        COALESCE(response_time_minutes,
                            (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(opened_at)) / 60.0)
                    ), 2
                ) AS response_sla_remaining_minutes,

                ROUND(
                    GREATEST(0, resolution_sla_target_minutes -
                        (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(opened_at)) / 60.0
                    ), 2
                ) AS resolution_sla_remaining_minutes,

                CASE
                    WHEN resolution_sla_status = 'BREACHED' OR response_sla_status = 'BREACHED'
                        THEN 'BREACHED'
                    WHEN resolution_sla_status = 'AT_RISK' OR response_sla_status = 'AT_RISK'
                        THEN 'AT_RISK'
                    ELSE 'HEALTHY'
                END AS sla_risk_level,

                CURRENT_TIMESTAMP() AS _refreshed_ts

            FROM {C.SILVER_FACT_INCIDENTS}
            WHERE is_open = true
        """)

        count = self.spark.sql(f"SELECT COUNT(*) AS cnt FROM {C.GOLD_OPEN_INCIDENTS}").collect()[0]["cnt"]
        print(f"fact_open_incidents refreshed. {count} open incidents.")

    # ─────────────────────────────────────────────────
    # Run all Gold aggregations
    # ─────────────────────────────────────────────────

    def run_all(self):
        """Execute all Gold aggregations."""
        print("=" * 60)
        print("GOLD LAYER — Starting aggregations")
        print("=" * 60)

        self.agg_sla_summary()
        self.agg_platform_impact()
        self.agg_agent_impact()
        self.agg_vendor_sla()
        self.refresh_open_incidents()

        print("=" * 60)
        print("GOLD LAYER — All aggregations complete")
        print("=" * 60)
