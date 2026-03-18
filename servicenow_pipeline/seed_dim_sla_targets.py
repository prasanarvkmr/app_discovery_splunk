# Databricks notebook source
# MAGIC %md
# MAGIC # Seed — dim_sla_targets
# MAGIC
# MAGIC Populates the SLA target dimension from `PipelineConfig.SLA_TARGETS`.
# MAGIC Run this notebook once, or whenever SLA definitions change.

# COMMAND ----------

from config import PipelineConfig as C

# COMMAND ----------

SEED_DATA = [
    (
        priority,
        float(targets["response"]),
        float(targets["resolution"]),
        targets["mode"],
        C.BUSINESS_HOURS_START if targets["mode"] == "BUSINESS_HOURS" else None,
        C.BUSINESS_HOURS_END   if targets["mode"] == "BUSINESS_HOURS" else None,
        C.BUSINESS_DAYS        if targets["mode"] == "BUSINESS_HOURS" else None,
    )
    for priority, targets in C.SLA_TARGETS.items()
]

COLUMNS = [
    "priority",
    "response_sla_minutes",
    "resolution_sla_minutes",
    "sla_mode",
    "business_hours_start",
    "business_hours_end",
    "business_days",
]

# COMMAND ----------

df_seed = spark.createDataFrame(SEED_DATA, schema=COLUMNS)

df_seed.createOrReplaceTempView("v_seed_sla_targets")

spark.sql(f"""
    MERGE INTO {C.SILVER_DIM_SLA_TARGETS} AS tgt
    USING v_seed_sla_targets AS src
    ON tgt.priority = src.priority
    WHEN MATCHED THEN UPDATE SET
        tgt.response_sla_minutes   = src.response_sla_minutes,
        tgt.resolution_sla_minutes = src.resolution_sla_minutes,
        tgt.sla_mode               = src.sla_mode,
        tgt.business_hours_start   = src.business_hours_start,
        tgt.business_hours_end     = src.business_hours_end,
        tgt.business_days          = src.business_days
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

display(spark.table(C.SILVER_DIM_SLA_TARGETS))
print(f"dim_sla_targets seeded — {spark.table(C.SILVER_DIM_SLA_TARGETS).count()} rows.")
