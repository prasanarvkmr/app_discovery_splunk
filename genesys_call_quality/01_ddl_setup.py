# Databricks notebook source
# MAGIC %md
# MAGIC # DDL — Create All Tables and Schemas in Unity Catalog
# MAGIC Run this notebook ONCE to initialize the catalog, schemas, and all tables.

# COMMAND ----------

from config import PipelineConfig as C

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog and Schemas

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {C.CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C.CATALOG}.{C.BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C.CATALOG}.{C.SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C.CATALOG}.{C.GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.BRONZE_RAW} (
        conversation_id       STRING,
        purpose               STRING,
        participant_id        STRING,
        agent_id              STRING,
        email_id              STRING,
        queue_id              STRING,
        division              STRING,
        vendor                STRING,
        location              STRING,
        media_type            STRING,
        direction             STRING,
        mos_score             DOUBLE,
        error_code            STRING,
        call_start_ts         TIMESTAMP,
        call_end_ts           TIMESTAMP,
        duration_seconds      LONG,
        raw_payload           STRING,
        _ingestion_ts         TIMESTAMP,
        _extraction_window_start STRING,
        _extraction_window_end   STRING,
        _batch_id             STRING
    )
    USING DELTA
    PARTITIONED BY (date(call_start_ts))
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.BRONZE_CHECKPOINT} (
        extraction_start_ts   STRING,
        extraction_end_ts     STRING,
        page_num              INT,
        records_fetched       BIGINT,
        search_after_cursor   STRING,
        status                STRING,
        checkpoint_ts         STRING
    )
    USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.SILVER_FACT_PARTICIPANTS} (
        conversation_id       STRING,
        purpose               STRING,
        participant_id        STRING,
        agent_id              STRING,
        email_id              STRING,
        queue_id              STRING,
        vendor                STRING,
        location              STRING,
        division              STRING,
        media_type            STRING,
        direction             STRING,
        mos_score             DOUBLE,
        error_code            STRING,
        call_status           STRING,
        call_start_ts         TIMESTAMP,
        call_end_ts           TIMESTAMP,
        call_date             DATE,
        duration_seconds      LONG,
        _raw_ingestion_ts     TIMESTAMP,
        _processed_ts         TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (call_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'delta.enableChangeDataFeed' = 'true'
    )
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.SILVER_FACT_CONVERSATIONS} (
        conversation_id       STRING,
        agent_id              STRING,
        email_id              STRING,
        queue_id              STRING,
        vendor                STRING,
        location              STRING,
        division              STRING,
        media_type            STRING,
        direction             STRING,
        agent_mos_score       DOUBLE,
        customer_mos_score    DOUBLE,
        agent_call_status     STRING,
        customer_call_status  STRING,
        overall_call_status   STRING,
        agent_error_code      STRING,
        customer_error_code   STRING,
        call_start_ts         TIMESTAMP,
        call_end_ts           TIMESTAMP,
        call_date             DATE,
        duration_seconds      LONG,
        participant_count     INT,
        _processed_ts         TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (call_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'delta.enableChangeDataFeed' = 'true'
    )
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.SILVER_FACT_AGENT_LEGS} (
        conversation_id       STRING,
        agent_id              STRING,
        email_id              STRING,
        queue_id              STRING,
        division              STRING,
        location              STRING,
        vendor                STRING,
        leg_sequence          INT,
        agent_mos_score       DOUBLE,
        agent_error_code      STRING,
        agent_call_status     STRING,
        leg_start_ts          TIMESTAMP,
        leg_end_ts            TIMESTAMP,
        leg_duration_seconds  LONG,
        call_date             DATE,
        is_transfer           BOOLEAN,
        _processed_ts         TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (call_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.SILVER_DIM_AGENTS} (
        agent_id              STRING,
        email_id              STRING,
        agent_name            STRING,
        division              STRING,
        location              STRING,
        is_current            BOOLEAN,
        effective_from        DATE,
        effective_to          DATE
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.SILVER_DIM_LOCATIONS} (
        location_key          STRING,
        site_name             STRING,
        city                  STRING,
        country               STRING,
        country_code          STRING,
        continent             STRING,
        region                STRING
    )
    USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Tables

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.GOLD_CALL_QUALITY_SUMMARY} (
        summary_date          DATE,
        total_calls           BIGINT,
        errored_calls         BIGINT,
        degraded_calls        BIGINT,
        normal_calls          BIGINT,
        errored_pct           DOUBLE,
        degraded_pct          DOUBLE,
        call_quality_pct      DOUBLE
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.GOLD_VENDOR_LOCATION} (
        summary_date          DATE,
        vendor                STRING,
        continent             STRING,
        country               STRING,
        location              STRING,
        total_calls           BIGINT,
        errored_calls         BIGINT,
        degraded_calls        BIGINT,
        errored_pct           DOUBLE,
        degraded_pct          DOUBLE
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.GOLD_AGENT_FLAGGED} (
        summary_date          DATE,
        agent_id              STRING,
        email_id              STRING,
        agent_name            STRING,
        division              STRING,
        location              STRING,
        total_calls           BIGINT,
        errored_calls         BIGINT,
        degraded_calls        BIGINT,
        errored_pct           DOUBLE,
        degraded_pct          DOUBLE,
        flag_reason           STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.GOLD_AGENT_PERFORMANCE} (
        summary_date          DATE,
        agent_id              STRING,
        email_id              STRING,
        agent_name            STRING,
        division              STRING,
        total_calls           BIGINT,
        errored_calls         BIGINT,
        degraded_calls        BIGINT,
        errored_pct           DOUBLE,
        degraded_pct          DOUBLE,
        conversation_details  ARRAY<STRUCT<
            conversation_id: STRING,
            agent_call_status: STRING,
            agent_mos_score: DOUBLE,
            leg_sequence: INT,
            is_transfer: BOOLEAN,
            leg_duration_seconds: LONG
        >>
    )
    USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Views

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {C.GOLD_VW_VENDOR_CONTINENT} AS
    SELECT
        summary_date,
        vendor,
        continent,
        SUM(total_calls) AS total_calls,
        SUM(errored_calls) AS errored_calls,
        SUM(degraded_calls) AS degraded_calls,
        ROUND(SUM(errored_calls) * 100.0 / SUM(total_calls), 2) AS errored_pct,
        ROUND(SUM(degraded_calls) * 100.0 / SUM(total_calls), 2) AS degraded_pct
    FROM {C.GOLD_VENDOR_LOCATION}
    GROUP BY summary_date, vendor, continent
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {C.GOLD_VW_VENDOR_COUNTRY} AS
    SELECT
        summary_date,
        vendor,
        continent,
        country,
        SUM(total_calls) AS total_calls,
        SUM(errored_calls) AS errored_calls,
        SUM(degraded_calls) AS degraded_calls,
        ROUND(SUM(errored_calls) * 100.0 / SUM(total_calls), 2) AS errored_pct,
        ROUND(SUM(degraded_calls) * 100.0 / SUM(total_calls), 2) AS degraded_pct
    FROM {C.GOLD_VENDOR_LOCATION}
    GROUP BY summary_date, vendor, continent, country
""")

# COMMAND ----------

print("All tables and views created successfully.")
