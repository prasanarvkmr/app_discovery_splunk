# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Aggregations
# MAGIC Silver → Gold: Pre-computed aggregates for dashboards and reporting.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from config import PipelineConfig as C

# COMMAND ----------

class GoldAggregations:
    """Builds Gold layer aggregate tables from Silver facts."""

    def __init__(self, spark):
        self.spark = spark
        self.recompute_days = C.GOLD_RECOMPUTE_DAYS

    # ─────────────────────────────────────────────────
    # 1. Call Quality Summary
    # ─────────────────────────────────────────────────

    def agg_call_quality_summary(self):
        """
        Overall call quality metrics per day.
        Recomputes last N days to capture late arrivals.
        """
        self.spark.sql(f"""
            MERGE INTO {C.GOLD_CALL_QUALITY_SUMMARY} AS t
            USING (
                SELECT
                    call_date AS summary_date,
                    COUNT(*) AS total_calls,
                    SUM(CASE WHEN overall_call_status = 'ERRORED' THEN 1 ELSE 0 END) AS errored_calls,
                    SUM(CASE WHEN overall_call_status = 'DEGRADED' THEN 1 ELSE 0 END) AS degraded_calls,
                    SUM(CASE WHEN overall_call_status = 'NORMAL' THEN 1 ELSE 0 END) AS normal_calls,
                    ROUND(
                        SUM(CASE WHEN overall_call_status = 'ERRORED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
                    ) AS errored_pct,
                    ROUND(
                        SUM(CASE WHEN overall_call_status = 'DEGRADED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
                    ) AS degraded_pct,
                    ROUND(
                        (SUM(CASE WHEN overall_call_status IN ('ERRORED', 'DEGRADED') THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 2
                    ) AS call_quality_pct
                FROM {C.SILVER_FACT_CONVERSATIONS}
                WHERE call_date >= CURRENT_DATE - INTERVAL {self.recompute_days} DAYS
                GROUP BY call_date
            ) AS s
            ON t.summary_date = s.summary_date
            WHEN MATCHED THEN UPDATE SET
                t.total_calls = s.total_calls,
                t.errored_calls = s.errored_calls,
                t.degraded_calls = s.degraded_calls,
                t.normal_calls = s.normal_calls,
                t.errored_pct = s.errored_pct,
                t.degraded_pct = s.degraded_pct,
                t.call_quality_pct = s.call_quality_pct
            WHEN NOT MATCHED THEN INSERT (
                summary_date, total_calls, errored_calls, degraded_calls,
                normal_calls, errored_pct, degraded_pct, call_quality_pct
            ) VALUES (
                s.summary_date, s.total_calls, s.errored_calls, s.degraded_calls,
                s.normal_calls, s.errored_pct, s.degraded_pct, s.call_quality_pct
            )
        """)
        print("agg_call_quality_summary MERGE complete.")

    # ─────────────────────────────────────────────────
    # 2. Vendor × Location (with Continent/Country)
    # ─────────────────────────────────────────────────

    def agg_vendor_location(self):
        """
        Vendor performance across continent/country/site.
        Joins with dim_locations for geographic hierarchy.
        """
        self.spark.sql(f"""
            MERGE INTO {C.GOLD_VENDOR_LOCATION} AS t
            USING (
                SELECT
                    f.call_date AS summary_date,
                    f.vendor,
                    COALESCE(l.continent, 'Unknown') AS continent,
                    COALESCE(l.country, 'Unknown') AS country,
                    f.location,
                    COUNT(*) AS total_calls,
                    SUM(CASE WHEN f.overall_call_status = 'ERRORED' THEN 1 ELSE 0 END) AS errored_calls,
                    SUM(CASE WHEN f.overall_call_status = 'DEGRADED' THEN 1 ELSE 0 END) AS degraded_calls,
                    ROUND(
                        SUM(CASE WHEN f.overall_call_status = 'ERRORED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
                    ) AS errored_pct,
                    ROUND(
                        SUM(CASE WHEN f.overall_call_status = 'DEGRADED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
                    ) AS degraded_pct
                FROM {C.SILVER_FACT_CONVERSATIONS} f
                LEFT JOIN {C.SILVER_DIM_LOCATIONS} l
                    ON f.location = l.location_key
                WHERE f.call_date >= CURRENT_DATE - INTERVAL {self.recompute_days} DAYS
                GROUP BY f.call_date, f.vendor, l.continent, l.country, f.location
            ) AS s
            ON t.summary_date = s.summary_date
                AND t.vendor = s.vendor
                AND t.location = s.location
            WHEN MATCHED THEN UPDATE SET
                t.continent = s.continent,
                t.country = s.country,
                t.total_calls = s.total_calls,
                t.errored_calls = s.errored_calls,
                t.degraded_calls = s.degraded_calls,
                t.errored_pct = s.errored_pct,
                t.degraded_pct = s.degraded_pct
            WHEN NOT MATCHED THEN INSERT (
                summary_date, vendor, continent, country, location,
                total_calls, errored_calls, degraded_calls, errored_pct, degraded_pct
            ) VALUES (
                s.summary_date, s.vendor, s.continent, s.country, s.location,
                s.total_calls, s.errored_calls, s.degraded_calls, s.errored_pct, s.degraded_pct
            )
        """)
        print("agg_vendor_location MERGE complete.")

    # ─────────────────────────────────────────────────
    # 3. Flagged Agents (> threshold)
    # ─────────────────────────────────────────────────

    def agg_agent_flagged(self):
        """
        Agents exceeding errored (>10%) or degraded (>20%) thresholds.
        Uses fact_agent_legs for per-leg attribution.
        """
        errored_threshold = C.AGENT_ERRORED_FLAG_PCT
        degraded_threshold = C.AGENT_DEGRADED_FLAG_PCT

        self.spark.sql(f"""
            MERGE INTO {C.GOLD_AGENT_FLAGGED} AS t
            USING (
                SELECT
                    l.call_date AS summary_date,
                    l.agent_id,
                    l.email_id,
                    a.agent_name,
                    l.division,
                    l.location,
                    COUNT(*) AS total_calls,
                    SUM(CASE WHEN l.agent_call_status = 'ERRORED' THEN 1 ELSE 0 END) AS errored_calls,
                    SUM(CASE WHEN l.agent_call_status = 'DEGRADED' THEN 1 ELSE 0 END) AS degraded_calls,
                    ROUND(
                        SUM(CASE WHEN l.agent_call_status = 'ERRORED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
                    ) AS errored_pct,
                    ROUND(
                        SUM(CASE WHEN l.agent_call_status = 'DEGRADED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
                    ) AS degraded_pct,
                    CASE
                        WHEN SUM(CASE WHEN l.agent_call_status = 'ERRORED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > {errored_threshold}
                         AND SUM(CASE WHEN l.agent_call_status = 'DEGRADED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > {degraded_threshold}
                        THEN 'BOTH'
                        WHEN SUM(CASE WHEN l.agent_call_status = 'ERRORED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > {errored_threshold}
                        THEN 'ERRORED_HIGH'
                        ELSE 'DEGRADED_HIGH'
                    END AS flag_reason
                FROM {C.SILVER_FACT_AGENT_LEGS} l
                LEFT JOIN {C.SILVER_DIM_AGENTS} a
                    ON l.agent_id = a.agent_id AND a.is_current = true
                WHERE l.call_date >= CURRENT_DATE - INTERVAL {self.recompute_days} DAYS
                GROUP BY l.call_date, l.agent_id, l.email_id, a.agent_name, l.division, l.location
                HAVING
                    SUM(CASE WHEN l.agent_call_status = 'ERRORED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > {errored_threshold}
                    OR SUM(CASE WHEN l.agent_call_status = 'DEGRADED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > {degraded_threshold}
            ) AS s
            ON t.summary_date = s.summary_date AND t.agent_id = s.agent_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        # Clean up agents who no longer exceed thresholds after recomputation
        self.spark.sql(f"""
            DELETE FROM {C.GOLD_AGENT_FLAGGED}
            WHERE summary_date >= CURRENT_DATE - INTERVAL {self.recompute_days} DAYS
              AND (agent_id, summary_date) NOT IN (
                SELECT l.agent_id, l.call_date
                FROM {C.SILVER_FACT_AGENT_LEGS} l
                WHERE l.call_date >= CURRENT_DATE - INTERVAL {self.recompute_days} DAYS
                GROUP BY l.call_date, l.agent_id
                HAVING
                    SUM(CASE WHEN l.agent_call_status = 'ERRORED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > {errored_threshold}
                    OR SUM(CASE WHEN l.agent_call_status = 'DEGRADED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > {degraded_threshold}
              )
        """)

        print("agg_agent_flagged MERGE + cleanup complete.")

    # ─────────────────────────────────────────────────
    # 4. Agent Performance Detail (with drill-down)
    # ─────────────────────────────────────────────────

    def agg_agent_performance_detail(self):
        """
        Per-agent performance with nested conversation details for drill-down.
        Includes COLLECT_LIST of conversation-level detail for expand/collapse UI.
        """
        self.spark.sql(f"""
            MERGE INTO {C.GOLD_AGENT_PERFORMANCE} AS t
            USING (
                SELECT
                    l.call_date AS summary_date,
                    l.agent_id,
                    l.email_id,
                    a.agent_name,
                    l.division,
                    COUNT(*) AS total_calls,
                    SUM(CASE WHEN l.agent_call_status = 'ERRORED' THEN 1 ELSE 0 END) AS errored_calls,
                    SUM(CASE WHEN l.agent_call_status = 'DEGRADED' THEN 1 ELSE 0 END) AS degraded_calls,
                    ROUND(
                        SUM(CASE WHEN l.agent_call_status = 'ERRORED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
                    ) AS errored_pct,
                    ROUND(
                        SUM(CASE WHEN l.agent_call_status = 'DEGRADED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
                    ) AS degraded_pct,
                    COLLECT_LIST(
                        NAMED_STRUCT(
                            'conversation_id', l.conversation_id,
                            'agent_call_status', l.agent_call_status,
                            'agent_mos_score', l.agent_mos_score,
                            'leg_sequence', l.leg_sequence,
                            'is_transfer', l.is_transfer,
                            'leg_duration_seconds', l.leg_duration_seconds
                        )
                    ) AS conversation_details
                FROM {C.SILVER_FACT_AGENT_LEGS} l
                LEFT JOIN {C.SILVER_DIM_AGENTS} a
                    ON l.agent_id = a.agent_id AND a.is_current = true
                WHERE l.call_date >= CURRENT_DATE - INTERVAL {self.recompute_days} DAYS
                GROUP BY l.call_date, l.agent_id, l.email_id, a.agent_name, l.division
            ) AS s
            ON t.summary_date = s.summary_date AND t.agent_id = s.agent_id
            WHEN MATCHED THEN UPDATE SET
                t.email_id = s.email_id,
                t.agent_name = s.agent_name,
                t.division = s.division,
                t.total_calls = s.total_calls,
                t.errored_calls = s.errored_calls,
                t.degraded_calls = s.degraded_calls,
                t.errored_pct = s.errored_pct,
                t.degraded_pct = s.degraded_pct,
                t.conversation_details = s.conversation_details
            WHEN NOT MATCHED THEN INSERT *
        """)
        print("agg_agent_performance_detail MERGE complete.")

    # ─────────────────────────────────────────────────
    # Run all Gold aggregations
    # ─────────────────────────────────────────────────

    def run_all(self):
        """Execute all Gold aggregations."""
        print("=" * 60)
        print("GOLD LAYER — Starting aggregations")
        print("=" * 60)

        self.agg_call_quality_summary()
        self.agg_vendor_location()
        self.agg_agent_flagged()
        self.agg_agent_performance_detail()

        print("=" * 60)
        print("GOLD LAYER — All aggregations complete")
        print("=" * 60)
