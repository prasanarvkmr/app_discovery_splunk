# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Transformations
# MAGIC Bronze → Silver: Classify call quality, deduplicate, build conversation/agent-leg facts.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
C = PipelineConfig

# COMMAND ----------

class SilverTransformations:
    """Transforms bronze data into silver fact and dimension tables."""

    def __init__(self, spark):
        self.spark = spark

    def get_last_silver_watermark(self):
        """Get the latest processed timestamp from silver participants."""
        try:
            result = self.spark.sql(f"""
                SELECT COALESCE(MAX(_processed_ts), TIMESTAMP '1900-01-01') AS ts
                FROM {C.SILVER_FACT_PARTICIPANTS}
            """).collect()[0]["ts"]
            return result
        except Exception:
            return "1900-01-01T00:00:00"

    # ─────────────────────────────────────────────────
    # Step 1: Bronze → fact_conversation_participants
    # ─────────────────────────────────────────────────

    def process_participants(self):
        """
        MERGE bronze records into silver fact_conversation_participants.
        Key: conversation_id + purpose (+ participant_id for multi-agent).
        Classifies call_status based on MOS thresholds.
        """
        last_watermark = self.get_last_silver_watermark()
        print(f"Processing participants since: {last_watermark}")

        # Read new bronze records
        bronze_new = self.spark.sql(f"""
            SELECT *
            FROM {C.BRONZE_RAW}
            WHERE _ingestion_ts > '{last_watermark}'
        """)

        if bronze_new.isEmpty():
            print("No new bronze records to process.")
            return

        # Classify call_status per participant
        staged = (bronze_new
            .withColumn("call_status",
                F.when(
                    F.col("error_code").isNotNull() | (F.col("mos_score") < C.MOS_ERRORED_THRESHOLD),
                    F.lit("ERRORED")
                ).when(
                    (F.col("mos_score") >= C.MOS_ERRORED_THRESHOLD) & (F.col("mos_score") < C.MOS_DEGRADED_THRESHOLD),
                    F.lit("DEGRADED")
                ).otherwise(F.lit("NORMAL"))
            )
            .withColumn("call_date", F.to_date("call_start_ts"))
            .withColumn("_processed_ts", F.current_timestamp())
            # Deduplicate within batch: keep latest per conversation_id + purpose + participant_id
            .withColumn("_row_num", F.row_number().over(
                Window.partitionBy("conversation_id", "purpose", "participant_id")
                .orderBy(F.col("_ingestion_ts").desc())
            ))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num", "raw_payload", "_batch_id",
                  "_extraction_window_start", "_extraction_window_end")
            .withColumnRenamed("_ingestion_ts", "_raw_ingestion_ts")
        )

        # MERGE into silver
        target = DeltaTable.forName(self.spark, C.SILVER_FACT_PARTICIPANTS)

        (target.alias("t")
            .merge(
                staged.alias("s"),
                """t.conversation_id = s.conversation_id
                   AND t.purpose = s.purpose
                   AND t.participant_id = s.participant_id"""
            )
            .whenMatchedUpdate(
                condition="s._raw_ingestion_ts > t._raw_ingestion_ts",
                set={
                    "agent_id": "s.agent_id",
                    "email_id": "s.email_id",
                    "queue_id": "s.queue_id",
                    "vendor": "s.vendor",
                    "location": "s.location",
                    "division": "s.division",
                    "media_type": "s.media_type",
                    "direction": "s.direction",
                    "mos_score": "s.mos_score",
                    "error_code": "s.error_code",
                    "call_status": "s.call_status",
                    "call_start_ts": "s.call_start_ts",
                    "call_end_ts": "s.call_end_ts",
                    "call_date": "s.call_date",
                    "duration_seconds": "s.duration_seconds",
                    "_raw_ingestion_ts": "s._raw_ingestion_ts",
                    "_processed_ts": "s._processed_ts"
                }
            )
            .whenNotMatchedInsert(
                values={
                    "conversation_id": "s.conversation_id",
                    "purpose": "s.purpose",
                    "participant_id": "s.participant_id",
                    "agent_id": "s.agent_id",
                    "email_id": "s.email_id",
                    "queue_id": "s.queue_id",
                    "vendor": "s.vendor",
                    "location": "s.location",
                    "division": "s.division",
                    "media_type": "s.media_type",
                    "direction": "s.direction",
                    "mos_score": "s.mos_score",
                    "error_code": "s.error_code",
                    "call_status": "s.call_status",
                    "call_start_ts": "s.call_start_ts",
                    "call_end_ts": "s.call_end_ts",
                    "call_date": "s.call_date",
                    "duration_seconds": "s.duration_seconds",
                    "_raw_ingestion_ts": "s._raw_ingestion_ts",
                    "_processed_ts": "s._processed_ts"
                }
            )
            .execute()
        )

        print("fact_conversation_participants MERGE complete.")

    # ─────────────────────────────────────────────────
    # Step 2a: Participants → fact_conversations
    # ─────────────────────────────────────────────────

    def process_conversations(self, watermark=None):
        """
        Pivot participant-level data into one row per conversation.
        Derives overall_call_status as the worst of agent + customer.
        """
        last_watermark = watermark or self.get_last_silver_watermark()

        # Rollup: one row per conversation with agent/customer metrics pivoted
        conv_rollup = self.spark.sql(f"""
            SELECT
                conversation_id,

                -- Agent-side
                MAX(CASE WHEN purpose = 'agent' THEN agent_id END) AS agent_id,
                MAX(CASE WHEN purpose = 'agent' THEN email_id END) AS email_id,
                MAX(CASE WHEN purpose = 'agent' THEN mos_score END) AS agent_mos_score,
                -- Worst-status wins: ERRORED > DEGRADED > NORMAL
                CASE
                    WHEN MAX(CASE WHEN purpose = 'agent' AND call_status = 'ERRORED' THEN 1 ELSE 0 END) = 1 THEN 'ERRORED'
                    WHEN MAX(CASE WHEN purpose = 'agent' AND call_status = 'DEGRADED' THEN 1 ELSE 0 END) = 1 THEN 'DEGRADED'
                    ELSE 'NORMAL'
                END AS agent_call_status,
                MAX(CASE WHEN purpose = 'agent' THEN error_code END) AS agent_error_code,

                -- Customer-side
                MAX(CASE WHEN purpose = 'customer' THEN mos_score END) AS customer_mos_score,
                CASE
                    WHEN MAX(CASE WHEN purpose = 'customer' AND call_status = 'ERRORED' THEN 1 ELSE 0 END) = 1 THEN 'ERRORED'
                    WHEN MAX(CASE WHEN purpose = 'customer' AND call_status = 'DEGRADED' THEN 1 ELSE 0 END) = 1 THEN 'DEGRADED'
                    ELSE 'NORMAL'
                END AS customer_call_status,
                MAX(CASE WHEN purpose = 'customer' THEN error_code END) AS customer_error_code,

                -- Shared fields
                MAX(queue_id) AS queue_id,
                MAX(vendor) AS vendor,
                MAX(location) AS location,
                MAX(division) AS division,
                MAX(media_type) AS media_type,
                MAX(direction) AS direction,
                MIN(call_start_ts) AS call_start_ts,
                MAX(call_end_ts) AS call_end_ts,
                MAX(call_date) AS call_date,
                MAX(duration_seconds) AS duration_seconds,
                COUNT(*) AS participant_count,

                current_timestamp() AS _processed_ts

            FROM {C.SILVER_FACT_PARTICIPANTS}
            WHERE _processed_ts > '{last_watermark}'
            GROUP BY conversation_id
        """)

        if conv_rollup.isEmpty():
            print("No new conversations to process.")
            return

        # Derive overall_call_status (worst of agent + customer)
        conv_rollup = conv_rollup.withColumn("overall_call_status",
            F.when(
                (F.col("agent_call_status") == "ERRORED") | (F.col("customer_call_status") == "ERRORED"),
                F.lit("ERRORED")
            ).when(
                (F.col("agent_call_status") == "DEGRADED") | (F.col("customer_call_status") == "DEGRADED"),
                F.lit("DEGRADED")
            ).otherwise(F.lit("NORMAL"))
        )

        # MERGE into fact_conversations
        target = DeltaTable.forName(self.spark, C.SILVER_FACT_CONVERSATIONS)

        (target.alias("t")
            .merge(conv_rollup.alias("s"), "t.conversation_id = s.conversation_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("fact_conversations MERGE complete.")

    # ─────────────────────────────────────────────────
    # Step 2b: Participants → fact_agent_legs
    # ─────────────────────────────────────────────────

    def process_agent_legs(self, watermark=None):
        """
        Extract agent-purpose records into per-agent-per-conversation legs.
        Handles multi-agent (transfer) scenarios with leg_sequence.
        """
        last_watermark = watermark or self.get_last_silver_watermark()

        agent_legs = self.spark.sql(f"""
            SELECT
                conversation_id,
                agent_id,
                email_id,
                queue_id,
                division,
                location,
                vendor,
                ROW_NUMBER() OVER (
                    PARTITION BY conversation_id
                    ORDER BY call_start_ts
                ) AS leg_sequence,
                mos_score AS agent_mos_score,
                error_code AS agent_error_code,
                call_status AS agent_call_status,
                call_start_ts AS leg_start_ts,
                call_end_ts AS leg_end_ts,
                duration_seconds AS leg_duration_seconds,
                call_date,
                CASE
                    WHEN ROW_NUMBER() OVER (
                        PARTITION BY conversation_id ORDER BY call_start_ts
                    ) > 1 THEN true
                    ELSE false
                END AS is_transfer,
                current_timestamp() AS _processed_ts
            FROM {C.SILVER_FACT_PARTICIPANTS}
            WHERE purpose = 'agent'
              AND agent_id IS NOT NULL
              AND _processed_ts > '{last_watermark}'
        """)

        if agent_legs.isEmpty():
            print("No new agent legs to process.")
            return

        # MERGE into fact_agent_legs
        target = DeltaTable.forName(self.spark, C.SILVER_FACT_AGENT_LEGS)

        (target.alias("t")
            .merge(
                agent_legs.alias("s"),
                "t.conversation_id = s.conversation_id AND t.agent_id = s.agent_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("fact_agent_legs MERGE complete.")

    # ─────────────────────────────────────────────────
    # Step 3: Upsert dim_agents (SCD Type 2)
    # ─────────────────────────────────────────────────

    def upsert_dim_agents(self):
        """
        SCD Type 2 merge for agents.
        Detects changes in division/location and creates new effective records.
        """
        # Get distinct agent profiles from latest participant data
        new_agents = self.spark.sql(f"""
            SELECT DISTINCT
                agent_id,
                email_id,
                division,
                location
            FROM {C.SILVER_FACT_PARTICIPANTS}
            WHERE purpose = 'agent'
              AND agent_id IS NOT NULL
        """)

        if new_agents.isEmpty():
            print("No agents to upsert.")
            return

        # Find agents whose attributes have changed
        changed = self.spark.sql(f"""
            SELECT n.agent_id, n.email_id, n.division, n.location
            FROM (
                SELECT DISTINCT agent_id, email_id, division, location
                FROM {C.SILVER_FACT_PARTICIPANTS}
                WHERE purpose = 'agent' AND agent_id IS NOT NULL
            ) n
            LEFT JOIN {C.SILVER_DIM_AGENTS} d
                ON n.agent_id = d.agent_id AND d.is_current = true
            WHERE d.agent_id IS NULL
               OR n.division != d.division
               OR n.location != d.location
               OR n.email_id != d.email_id
        """)

        if changed.isEmpty():
            print("No agent dimension changes detected.")
            return

        # Expire old records for changed agents
        target = DeltaTable.forName(self.spark, C.SILVER_DIM_AGENTS)
        changed_ids = changed.select("agent_id")

        (target.alias("t")
            .merge(
                changed_ids.alias("s"),
                "t.agent_id = s.agent_id AND t.is_current = true"
            )
            .whenMatchedUpdate(set={
                "is_current": F.lit(False),
                "effective_to": F.current_date()
            })
            .execute()
        )

        # Insert new current records
        new_records = (changed
            .withColumn("agent_name", F.lit(None).cast("string"))  # populate from HR feed if available
            .withColumn("is_current", F.lit(True))
            .withColumn("effective_from", F.current_date())
            .withColumn("effective_to", F.to_date(F.lit("9999-12-31")))
        )

        (new_records.write
            .format("delta")
            .mode("append")
            .saveAsTable(C.SILVER_DIM_AGENTS)
        )

        print(f"dim_agents SCD2 upsert complete. {new_records.count()} records updated/inserted.")

    # ─────────────────────────────────────────────────
    # Run all Silver steps
    # ─────────────────────────────────────────────────

    def run_all(self):
        """Execute all Silver transformations in order."""
        print("=" * 60)
        print("SILVER LAYER — Starting transformations")
        print("=" * 60)

        # Capture watermark BEFORE processing participants —
        # process_conversations / process_agent_legs need to find the rows
        # that process_participants is about to insert/update.
        pre_run_watermark = self.get_last_silver_watermark()

        # Step 1: Bronze → Participants (must run first)
        self.process_participants()

        # Step 2a & 2b: Participants → Conversations + Agent Legs
        # Use the pre-run watermark so the new rows (with _processed_ts > pre_run) are included
        self.process_conversations(pre_run_watermark)
        self.process_agent_legs(pre_run_watermark)

        # Step 3: Upsert agent dimension
        self.upsert_dim_agents()

        print("=" * 60)
        print("SILVER LAYER — All transformations complete")
        print("=" * 60)
