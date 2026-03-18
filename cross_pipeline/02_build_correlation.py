# Databricks notebook source
# MAGIC %md
# MAGIC # Build — Call Quality ↔ Incident Correlation
# MAGIC
# MAGIC Materialises the daily correlation table by joining:
# MAGIC - **Genesys** `agg_vendor_location` (call quality per date × vendor × location)
# MAGIC - **ServiceNow** `fact_incidents` (active incidents per date × vendor)
# MAGIC
# MAGIC Schedule this notebook to run **after** both pipelines complete,
# MAGIC or attach it as a final task in either workflow.

# COMMAND ----------

from config import CorrelationConfig as CC

# COMMAND ----------

class CorrelationBuilder:
    """Builds the daily Genesys ↔ ServiceNow correlation Gold table."""

    def __init__(self, spark):
        self.spark = spark
        self.recompute_days = CC.RECOMPUTE_DAYS

    def build(self):
        """
        MERGE into agg_call_quality_incident_correlation.

        Logic:
        1. Left side: Genesys agg_vendor_location — one row per date × vendor × location.
        2. Right side: ServiceNow fact_incidents rolled up to date × vendor,
           counting active incidents (opened ≤ date AND (resolved ≥ date OR still open)).
        3. Correlation flags derived from both sides.
        """

        self.spark.sql(f"""
            MERGE INTO {CC.GOLD_CORRELATION} AS tgt
            USING (
                WITH genesys AS (
                    SELECT
                        summary_date,
                        vendor,
                        continent,
                        country,
                        location,
                        total_calls,
                        errored_calls,
                        degraded_calls,
                        errored_pct,
                        degraded_pct
                    FROM {CC.GENESYS_AGG_VENDOR_LOCATION}
                    WHERE summary_date >= CURRENT_DATE - INTERVAL {self.recompute_days} DAYS
                ),

                incidents_by_vendor_day AS (
                    SELECT
                        active_date,
                        vendor_name,
                        COUNT(*)                                                         AS active_incidents,
                        SUM(CASE WHEN priority IN ('P0', 'P1') THEN 1 ELSE 0 END)       AS p0_p1_incidents,
                        SUM(CASE WHEN priority = 'P2'          THEN 1 ELSE 0 END)        AS p2_incidents,
                        SUM(CASE WHEN priority IN ('P3', 'P4') THEN 1 ELSE 0 END)        AS p3_p4_incidents,
                        COLLECT_LIST(incident_number)                                     AS incident_numbers
                    FROM (
                        -- Explode each incident across all dates it was active
                        SELECT
                            inc.incident_number,
                            inc.priority,
                            inc.vendor_name,
                            d.active_date
                        FROM {CC.SNOW_FACT_INCIDENTS} inc
                        -- Generate date range: opened_at … COALESCE(resolved_at, today)
                        LATERAL VIEW EXPLODE(
                            SEQUENCE(
                                DATE(inc.opened_at),
                                COALESCE(DATE(inc.resolved_at), CURRENT_DATE()),
                                INTERVAL 1 DAY
                            )
                        ) d AS active_date
                        WHERE inc.vendor_name IS NOT NULL
                          AND DATE(inc.opened_at) >= CURRENT_DATE - INTERVAL {self.recompute_days + 30} DAYS
                    )
                    WHERE active_date >= CURRENT_DATE - INTERVAL {self.recompute_days} DAYS
                    GROUP BY active_date, vendor_name
                )

                SELECT
                    g.summary_date          AS correlation_date,
                    g.vendor,
                    g.continent,
                    g.country,
                    g.location,

                    g.total_calls,
                    g.errored_calls,
                    g.degraded_calls,
                    g.errored_pct,
                    g.degraded_pct,

                    COALESCE(i.active_incidents, 0)   AS active_incidents,
                    COALESCE(i.p0_p1_incidents, 0)    AS p0_p1_incidents,
                    COALESCE(i.p2_incidents, 0)        AS p2_incidents,
                    COALESCE(i.p3_p4_incidents, 0)     AS p3_p4_incidents,
                    COALESCE(i.incident_numbers, ARRAY()) AS incident_numbers,

                    -- Flags
                    COALESCE(i.active_incidents, 0) > 0                         AS has_active_incident,
                    COALESCE(i.p0_p1_incidents, 0) > 0                          AS has_critical_incident,
                    (g.errored_pct > 0 OR g.degraded_pct > 0)
                        AND COALESCE(i.active_incidents, 0) > 0                 AS degradation_with_incident,

                    CURRENT_TIMESTAMP() AS _refreshed_ts

                FROM genesys g
                LEFT JOIN incidents_by_vendor_day i
                    ON UPPER(g.vendor) = UPPER(i.vendor_name)
                    AND g.summary_date = i.active_date

            ) AS src
            ON  tgt.correlation_date = src.correlation_date
            AND tgt.vendor           = src.vendor
            AND tgt.location         = src.location

            WHEN MATCHED THEN UPDATE SET
                tgt.continent               = src.continent,
                tgt.country                 = src.country,
                tgt.total_calls             = src.total_calls,
                tgt.errored_calls           = src.errored_calls,
                tgt.degraded_calls          = src.degraded_calls,
                tgt.errored_pct             = src.errored_pct,
                tgt.degraded_pct            = src.degraded_pct,
                tgt.active_incidents        = src.active_incidents,
                tgt.p0_p1_incidents         = src.p0_p1_incidents,
                tgt.p2_incidents            = src.p2_incidents,
                tgt.p3_p4_incidents         = src.p3_p4_incidents,
                tgt.incident_numbers        = src.incident_numbers,
                tgt.has_active_incident     = src.has_active_incident,
                tgt.has_critical_incident   = src.has_critical_incident,
                tgt.degradation_with_incident = src.degradation_with_incident,
                tgt._refreshed_ts           = src._refreshed_ts

            WHEN NOT MATCHED THEN INSERT *
        """)

        # Report
        stats = self.spark.sql(f"""
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN degradation_with_incident THEN 1 ELSE 0 END) AS degradation_with_incident_rows,
                SUM(CASE WHEN has_critical_incident THEN 1 ELSE 0 END) AS critical_incident_rows
            FROM {CC.GOLD_CORRELATION}
            WHERE correlation_date >= CURRENT_DATE - INTERVAL {self.recompute_days} DAYS
        """).collect()[0]

        print(f"Correlation MERGE complete.")
        print(f"  Rows (last {self.recompute_days} days): {stats['total_rows']}")
        print(f"  Degradation + incident:  {stats['degradation_with_incident_rows']}")
        print(f"  With critical (P0/P1):   {stats['critical_incident_rows']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute

# COMMAND ----------

builder = CorrelationBuilder(spark)
builder.build()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Validation

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            correlation_date,
            vendor,
            location,
            total_calls,
            errored_pct,
            degraded_pct,
            active_incidents,
            p0_p1_incidents,
            incident_numbers,
            degradation_with_incident
        FROM {CC.GOLD_CORRELATION}
        WHERE degradation_with_incident = true
        ORDER BY correlation_date DESC, errored_pct DESC
        LIMIT 20
    """)
)
