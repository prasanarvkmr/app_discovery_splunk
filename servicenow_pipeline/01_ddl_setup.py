# Databricks notebook source
# MAGIC %md
# MAGIC # DDL — Create All Tables and Schemas for ServiceNow Pipeline
# MAGIC Run this notebook ONCE to initialize the shared catalog, schemas, and all tables.

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
        incident_number         STRING,
        incident_sys_id         STRING,
        short_description       STRING,
        description             STRING,
        priority                STRING,
        urgency                 STRING,
        impact                  STRING,
        state                   STRING,
        category                STRING,
        subcategory             STRING,
        assignment_group        STRING,
        assigned_to             STRING,
        caller_id               STRING,
        opened_by               STRING,
        cmdb_ci                 STRING,
        business_service        STRING,
        location                STRING,
        company                 STRING,
        opened_at               TIMESTAMP,
        resolved_at             TIMESTAMP,
        closed_at               TIMESTAMP,
        first_response_at       TIMESTAMP,
        work_start_at           TIMESTAMP,
        sys_created_on          TIMESTAMP,
        sys_updated_on          TIMESTAMP,
        close_code              STRING,
        close_notes             STRING,
        raw_payload             STRING,
        _ingestion_ts           TIMESTAMP,
        _batch_id               STRING
    )
    USING DELTA
    PARTITIONED BY (date(opened_at))
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.BRONZE_CHECKPOINT} (
        extraction_start_ts     STRING,
        extraction_end_ts       STRING,
        records_fetched         BIGINT,
        status                  STRING,
        checkpoint_ts           STRING
    )
    USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Tables

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.SILVER_FACT_INCIDENTS} (
        incident_number             STRING,
        incident_sys_id             STRING,
        short_description           STRING,
        priority                    STRING,
        urgency                     STRING,
        impact                      STRING,
        state                       STRING,
        category                    STRING,
        subcategory                 STRING,
        assignment_group            STRING,
        assigned_to                 STRING,
        caller_id                   STRING,
        opened_by                   STRING,
        cmdb_ci                     STRING,
        business_service            STRING,
        location                    STRING,
        company                     STRING,
        opened_at                   TIMESTAMP,
        resolved_at                 TIMESTAMP,
        closed_at                   TIMESTAMP,
        first_response_at           TIMESTAMP,
        work_start_at               TIMESTAMP,
        close_code                  STRING,
        close_notes                 STRING,
        incident_date               DATE,

        -- SLA Calculations
        response_time_minutes       DOUBLE,
        resolution_time_minutes     DOUBLE,
        response_sla_target_minutes DOUBLE,
        resolution_sla_target_minutes DOUBLE,
        response_sla_status         STRING,
        resolution_sla_status       STRING,
        response_sla_pct            DOUBLE,
        resolution_sla_pct          DOUBLE,
        sla_calculation_mode        STRING,

        -- Classification (from dim_business_services)
        business_service_type       STRING,
        vendor_name                 STRING,
        platform_component          STRING,
        is_agent_facing             BOOLEAN,

        -- State tracking
        is_open                     BOOLEAN,
        _processed_ts               TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (incident_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'delta.enableChangeDataFeed' = 'true'
    )
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.SILVER_FACT_TIMELINE} (
        incident_number             STRING,
        previous_state              STRING,
        new_state                   STRING,
        state_changed_at            TIMESTAMP,
        time_in_previous_state_minutes DOUBLE,
        changed_by                  STRING,
        _processed_ts               TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.SILVER_DIM_BUSINESS_SERVICES} (
        business_service        STRING,
        service_display_name    STRING,
        service_type            STRING,
        vendor_name             STRING,
        platform_component      STRING,
        is_agent_facing         BOOLEAN,
        owner_team              STRING,
        criticality             STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.SILVER_DIM_ASSIGNMENT_GROUPS} (
        assignment_group        STRING,
        group_display_name      STRING,
        team_lead               STRING,
        department              STRING,
        is_vendor_team          BOOLEAN,
        vendor_name             STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.SILVER_DIM_SLA_TARGETS} (
        priority                    STRING,
        response_sla_minutes        DOUBLE,
        resolution_sla_minutes      DOUBLE,
        sla_mode                    STRING,
        business_hours_start        STRING,
        business_hours_end          STRING,
        business_days               STRING
    )
    USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Tables

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.GOLD_SLA_SUMMARY} (
        summary_date                    DATE,
        priority                        STRING,
        total_incidents                 BIGINT,
        response_sla_met                BIGINT,
        response_sla_breached           BIGINT,
        response_sla_in_progress        BIGINT,
        resolution_sla_met              BIGINT,
        resolution_sla_breached         BIGINT,
        resolution_sla_in_progress      BIGINT,
        response_sla_compliance_pct     DOUBLE,
        resolution_sla_compliance_pct   DOUBLE,
        avg_response_time_minutes       DOUBLE,
        avg_resolution_time_minutes     DOUBLE
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.GOLD_PLATFORM_IMPACT} (
        summary_date                    DATE,
        business_service                STRING,
        platform_component              STRING,
        priority                        STRING,
        total_incidents                 BIGINT,
        open_incidents                  BIGINT,
        response_sla_met                BIGINT,
        response_sla_breached           BIGINT,
        resolution_sla_met              BIGINT,
        resolution_sla_breached         BIGINT,
        response_compliance_pct         DOUBLE,
        resolution_compliance_pct       DOUBLE,
        avg_response_minutes            DOUBLE,
        avg_resolution_minutes          DOUBLE,
        mttr_minutes                    DOUBLE
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.GOLD_AGENT_IMPACT} (
        summary_date                    DATE,
        business_service                STRING,
        priority                        STRING,
        total_incidents                 BIGINT,
        open_incidents                  BIGINT,
        response_sla_met                BIGINT,
        response_sla_breached           BIGINT,
        resolution_sla_met              BIGINT,
        resolution_sla_breached         BIGINT,
        response_compliance_pct         DOUBLE,
        resolution_compliance_pct       DOUBLE,
        avg_response_minutes            DOUBLE,
        avg_resolution_minutes          DOUBLE,
        mttr_minutes                    DOUBLE
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.GOLD_VENDOR_SLA} (
        summary_date                    DATE,
        vendor_name                     STRING,
        priority                        STRING,
        total_incidents                 BIGINT,
        response_sla_met                BIGINT,
        response_sla_breached           BIGINT,
        resolution_sla_met              BIGINT,
        resolution_sla_breached         BIGINT,
        response_compliance_pct         DOUBLE,
        resolution_compliance_pct       DOUBLE,
        avg_response_minutes            DOUBLE,
        avg_resolution_minutes          DOUBLE,
        worst_resolution_minutes        DOUBLE
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {C.GOLD_OPEN_INCIDENTS} (
        incident_number             STRING,
        incident_sys_id             STRING,
        short_description           STRING,
        priority                    STRING,
        state                       STRING,
        assignment_group            STRING,
        assigned_to                 STRING,
        business_service            STRING,
        business_service_type       STRING,
        vendor_name                 STRING,
        platform_component          STRING,
        is_agent_facing             BOOLEAN,
        opened_at                   TIMESTAMP,
        first_response_at           TIMESTAMP,
        response_time_minutes       DOUBLE,
        response_sla_target_minutes DOUBLE,
        response_sla_status         STRING,
        resolution_sla_target_minutes DOUBLE,
        resolution_sla_status       STRING,
        time_open_minutes           DOUBLE,
        response_sla_remaining_minutes  DOUBLE,
        resolution_sla_remaining_minutes DOUBLE,
        sla_risk_level              STRING,
        _refreshed_ts               TIMESTAMP
    )
    USING DELTA
""")

# COMMAND ----------

print("All ServiceNow pipeline tables created successfully.")
