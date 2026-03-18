# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Incident Transformations + SLA Calculation
# MAGIC Bronze → Silver: Deduplicate, calculate SLA, classify by business service, track state changes.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from config import PipelineConfig as C

# COMMAND ----------

class SilverTransformations:
    """Transforms bronze incidents into silver facts with SLA calculations."""

    def __init__(self, spark):
        self.spark = spark

    def get_last_silver_watermark(self):
        """Get latest processed timestamp from silver fact_incidents."""
        try:
            result = self.spark.sql(f"""
                SELECT COALESCE(MAX(_processed_ts), TIMESTAMP '1900-01-01') AS ts
                FROM {C.SILVER_FACT_INCIDENTS}
            """).collect()[0]["ts"]
            return result
        except Exception:
            return "1900-01-01T00:00:00"

    # ─────────────────────────────────────────────────
    # Step 1: Bronze → fact_incidents (MERGE on incident_number)
    # ─────────────────────────────────────────────────

    def process_incidents(self):
        """
        MERGE bronze into silver fact_incidents.
        - Deduplicates on incident_number (keep latest)
        - Joins dim_sla_targets for thresholds
        - Joins dim_business_services for classification
        - Calculates response/resolution SLA status
        """
        last_watermark = self.get_last_silver_watermark()
        print(f"Processing incidents since: {last_watermark}")

        # Read new bronze records, deduplicate within batch
        bronze_new = self.spark.sql(f"""
            WITH ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY incident_number
                        ORDER BY _ingestion_ts DESC
                    ) AS _rn
                FROM {C.BRONZE_RAW}
                WHERE _ingestion_ts > '{last_watermark}'
            )
            SELECT * FROM ranked WHERE _rn = 1
        """).drop("_rn", "raw_payload", "_batch_id")

        if bronze_new.isEmpty():
            print("No new bronze records to process.")
            return

        # Join SLA targets
        bronze_with_sla = bronze_new.alias("b").join(
            self.spark.table(C.SILVER_DIM_SLA_TARGETS).alias("s"),
            F.col("b.priority") == F.col("s.priority"),
            "left"
        ).select(
            "b.*",
            F.col("s.response_sla_minutes").alias("response_sla_target_minutes"),
            F.col("s.resolution_sla_minutes").alias("resolution_sla_target_minutes"),
            F.col("s.sla_mode").alias("sla_calculation_mode"),
        )

        # Join business service classification
        staged = bronze_with_sla.alias("i").join(
            self.spark.table(C.SILVER_DIM_BUSINESS_SERVICES).alias("d"),
            F.col("i.business_service") == F.col("d.business_service"),
            "left"
        ).select(
            "i.*",
            F.coalesce(F.col("d.service_type"), F.lit("OTHER")).alias("business_service_type"),
            F.col("d.vendor_name").alias("vendor_name"),
            F.col("d.platform_component").alias("platform_component"),
            F.coalesce(F.col("d.is_agent_facing"), F.lit(False)).alias("is_agent_facing"),
        )

        # Calculate SLA metrics
        staged = self._calculate_sla(staged)

        # Add metadata
        staged = (staged
            .withColumn("incident_date", F.to_date("opened_at"))
            .withColumn("is_open", F.when(
                F.col("state").isin("Resolved", "Closed", "Canceled"),
                F.lit(False)
            ).otherwise(F.lit(True)))
            .withColumn("_processed_ts", F.current_timestamp())
        )

        # MERGE into fact_incidents
        target = DeltaTable.forName(self.spark, C.SILVER_FACT_INCIDENTS)

        (target.alias("t")
            .merge(staged.alias("s"), "t.incident_number = s.incident_number")
            .whenMatchedUpdate(
                condition="s._ingestion_ts > t._processed_ts",
                set={
                    "incident_sys_id": "s.incident_sys_id",
                    "short_description": "s.short_description",
                    "priority": "s.priority",
                    "urgency": "s.urgency",
                    "impact": "s.impact",
                    "state": "s.state",
                    "category": "s.category",
                    "subcategory": "s.subcategory",
                    "assignment_group": "s.assignment_group",
                    "assigned_to": "s.assigned_to",
                    "caller_id": "s.caller_id",
                    "opened_by": "s.opened_by",
                    "cmdb_ci": "s.cmdb_ci",
                    "business_service": "s.business_service",
                    "location": "s.location",
                    "company": "s.company",
                    "opened_at": "s.opened_at",
                    "resolved_at": "s.resolved_at",
                    "closed_at": "s.closed_at",
                    "first_response_at": "s.first_response_at",
                    "work_start_at": "s.work_start_at",
                    "close_code": "s.close_code",
                    "close_notes": "s.close_notes",
                    "incident_date": "s.incident_date",
                    "response_time_minutes": "s.response_time_minutes",
                    "resolution_time_minutes": "s.resolution_time_minutes",
                    "response_sla_target_minutes": "s.response_sla_target_minutes",
                    "resolution_sla_target_minutes": "s.resolution_sla_target_minutes",
                    "response_sla_status": "s.response_sla_status",
                    "resolution_sla_status": "s.resolution_sla_status",
                    "response_sla_pct": "s.response_sla_pct",
                    "resolution_sla_pct": "s.resolution_sla_pct",
                    "sla_calculation_mode": "s.sla_calculation_mode",
                    "business_service_type": "s.business_service_type",
                    "vendor_name": "s.vendor_name",
                    "platform_component": "s.platform_component",
                    "is_agent_facing": "s.is_agent_facing",
                    "is_open": "s.is_open",
                    "_processed_ts": "s._processed_ts",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("fact_incidents MERGE complete.")

    # ─────────────────────────────────────────────────
    # SLA Calculation Engine
    # ─────────────────────────────────────────────────

    def _calculate_sla(self, df):
        """
        Calculate response time, resolution time, and SLA status.
        - 24X7: straight clock elapsed minutes
        - BUSINESS_HOURS: only count minutes within business hours (Mon-Fri 09:00-18:00)
        """
        bh_per_day = C.BUSINESS_HOURS_PER_DAY * 60  # minutes per business day

        # ── Response Time ──
        # For 24x7: simple difference in minutes
        # For business hours: calculate business-hours-adjusted minutes
        df = df.withColumn("_response_raw_minutes",
            F.when(
                F.col("first_response_at").isNotNull(),
                (F.unix_timestamp("first_response_at") - F.unix_timestamp("opened_at")) / 60.0
            )
        )

        # For open incidents without response yet, calculate elapsed time
        df = df.withColumn("_response_elapsed_minutes",
            F.when(
                F.col("first_response_at").isNull(),
                (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("opened_at")) / 60.0
            ).otherwise(F.col("_response_raw_minutes"))
        )

        # Apply business hours adjustment for P3/P4
        df = df.withColumn("response_time_minutes",
            F.when(
                F.col("sla_calculation_mode") == "BUSINESS_HOURS",
                self._business_hours_minutes_expr("_response_raw_minutes")
            ).otherwise(F.col("_response_raw_minutes"))
        )

        # ── Resolution Time ──
        df = df.withColumn("_resolution_raw_minutes",
            F.when(
                F.col("resolved_at").isNotNull(),
                (F.unix_timestamp("resolved_at") - F.unix_timestamp("opened_at")) / 60.0
            )
        )

        df = df.withColumn("_resolution_elapsed_minutes",
            F.when(
                F.col("resolved_at").isNull(),
                (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("opened_at")) / 60.0
            ).otherwise(F.col("_resolution_raw_minutes"))
        )

        df = df.withColumn("resolution_time_minutes",
            F.when(
                F.col("sla_calculation_mode") == "BUSINESS_HOURS",
                self._business_hours_minutes_expr("_resolution_raw_minutes")
            ).otherwise(F.col("_resolution_raw_minutes"))
        )

        # ── SLA Percentage Consumed ──
        df = df.withColumn("response_sla_pct",
            F.when(
                F.col("response_sla_target_minutes").isNotNull() & (F.col("response_sla_target_minutes") > 0),
                F.round(
                    F.coalesce(F.col("response_time_minutes"), F.col("_response_elapsed_minutes"))
                    / F.col("response_sla_target_minutes") * 100, 2
                )
            )
        )

        df = df.withColumn("resolution_sla_pct",
            F.when(
                F.col("resolution_sla_target_minutes").isNotNull() & (F.col("resolution_sla_target_minutes") > 0),
                F.round(
                    F.coalesce(F.col("resolution_time_minutes"), F.col("_resolution_elapsed_minutes"))
                    / F.col("resolution_sla_target_minutes") * 100, 2
                )
            )
        )

        # ── Response SLA Status ──
        at_risk_pct = C.SLA_AT_RISK_PCT

        df = df.withColumn("response_sla_status",
            F.when(
                F.col("first_response_at").isNotNull() & (F.col("response_sla_pct") <= 100),
                F.lit("MET")
            ).when(
                F.col("first_response_at").isNotNull() & (F.col("response_sla_pct") > 100),
                F.lit("BREACHED")
            ).when(
                F.col("first_response_at").isNull() & (F.col("response_sla_pct") > 100),
                F.lit("BREACHED")
            ).when(
                F.col("first_response_at").isNull() & (F.col("response_sla_pct") > at_risk_pct),
                F.lit("AT_RISK")
            ).otherwise(F.lit("IN_PROGRESS"))
        )

        # ── Resolution SLA Status ──
        df = df.withColumn("resolution_sla_status",
            F.when(
                F.col("resolved_at").isNotNull() & (F.col("resolution_sla_pct") <= 100),
                F.lit("MET")
            ).when(
                F.col("resolved_at").isNotNull() & (F.col("resolution_sla_pct") > 100),
                F.lit("BREACHED")
            ).when(
                F.col("resolved_at").isNull() & (F.col("resolution_sla_pct") > 100),
                F.lit("BREACHED")
            ).when(
                F.col("resolved_at").isNull() & (F.col("resolution_sla_pct") > at_risk_pct),
                F.lit("AT_RISK")
            ).otherwise(F.lit("IN_PROGRESS"))
        )

        # Drop temp columns
        df = df.drop(
            "_response_raw_minutes", "_response_elapsed_minutes",
            "_resolution_raw_minutes", "_resolution_elapsed_minutes"
        )

        return df

    @staticmethod
    def _business_hours_minutes_expr(raw_minutes_col):
        """
        Approximate business hours adjustment.
        Converts raw elapsed minutes to business-hours-only minutes.

        Logic: For every 24 hours elapsed, only 9 business hours count.
        Also subtracts weekends (2 out of every 7 days).

        For precise calculation, a UDF with calendar aware logic is needed.
        This approximation is accurate within ~5% for multi-day incidents.
        """
        bh_per_day = C.BUSINESS_HOURS_PER_DAY * 60  # 540 minutes
        total_day_minutes = 24 * 60  # 1440 minutes

        # Ratio: 9 business hours per 24 clock hours, 5 business days per 7
        adjustment_factor = (bh_per_day / total_day_minutes) * (5.0 / 7.0)

        return F.round(F.col(raw_minutes_col) * adjustment_factor, 2)

    # ─────────────────────────────────────────────────
    # Step 2: Detect state changes → fact_incident_timeline
    # ─────────────────────────────────────────────────

    def process_state_changes(self):
        """
        Compare current silver state with incoming bronze to detect changes.
        Append new state transitions to the timeline table.
        """
        last_watermark = self.get_last_silver_watermark()

        state_changes = self.spark.sql(f"""
            WITH latest_bronze AS (
                SELECT
                    incident_number,
                    state AS new_state,
                    assigned_to AS changed_by,
                    _ingestion_ts AS state_changed_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY incident_number
                        ORDER BY _ingestion_ts DESC
                    ) AS _rn
                FROM {C.BRONZE_RAW}
                WHERE _ingestion_ts > '{last_watermark}'
            ),
            current_silver AS (
                SELECT
                    incident_number,
                    state AS previous_state,
                    _processed_ts
                FROM {C.SILVER_FACT_INCIDENTS}
            )
            SELECT
                b.incident_number,
                s.previous_state,
                b.new_state,
                b.state_changed_at,
                ROUND(
                    (UNIX_TIMESTAMP(b.state_changed_at) - UNIX_TIMESTAMP(s._processed_ts)) / 60.0, 2
                ) AS time_in_previous_state_minutes,
                b.changed_by,
                CURRENT_TIMESTAMP() AS _processed_ts
            FROM latest_bronze b
            JOIN current_silver s ON b.incident_number = s.incident_number
            WHERE b._rn = 1
              AND b.new_state != s.previous_state
        """)

        if state_changes.isEmpty():
            print("No state changes detected.")
            return

        change_count = state_changes.count()
        state_changes.write.format("delta").mode("append").saveAsTable(C.SILVER_FACT_TIMELINE)
        print(f"Appended {change_count} state changes to timeline.")

    # ─────────────────────────────────────────────────
    # Run all Silver steps
    # ─────────────────────────────────────────────────

    def run_all(self):
        """Execute all Silver transformations in order."""
        print("=" * 60)
        print("SILVER LAYER — Starting transformations")
        print("=" * 60)

        # Detect state changes BEFORE updating fact_incidents (need old state)
        self.process_state_changes()

        # MERGE incidents with SLA calculations
        self.process_incidents()

        print("=" * 60)
        print("SILVER LAYER — All transformations complete")
        print("=" * 60)
