# Databricks notebook source
# MAGIC %md
# MAGIC # Genesys Call Quality Pipeline — Main Orchestrator
# MAGIC
# MAGIC **Schedule**: Every 10 minutes via Databricks Workflow
# MAGIC
# MAGIC **Flow**:
# MAGIC ```
# MAGIC ELK API → Bronze (append) → Silver (MERGE) → Gold (MERGE)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from datetime import datetime
C = PipelineConfig

print(f"Pipeline started at {datetime.utcnow().isoformat()}Z")
print(f"Catalog: {C.CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Extract from ELK API

# COMMAND ----------

# MAGIC %run ./02_elk_extraction

# COMMAND ----------

extractor = ELKExtractor(spark, dbutils)
records, total, start_ts, end_ts = extractor.extract()

print(f"Extracted {total} records for window {start_ts} → {end_ts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Write to Bronze

# COMMAND ----------

# MAGIC %run ./03_bronze_ingestion

# COMMAND ----------

bronze = BronzeIngestion(spark)
bronze_count = bronze.write_to_bronze(records, start_ts, end_ts)

# Mark extraction complete only after bronze write succeeds
extractor.mark_extraction_complete(start_ts, end_ts, total)
print(f"Bronze ingestion complete. {bronze_count} participant records written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Silver Transformations

# COMMAND ----------

# MAGIC %run ./04_silver_transformations

# COMMAND ----------

silver = SilverTransformations(spark)
silver.run_all()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Gold Aggregations

# COMMAND ----------

# MAGIC %run ./05_gold_aggregations

# COMMAND ----------

gold = GoldAggregations(spark)
gold.run_all()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete

# COMMAND ----------

print(f"Pipeline completed at {datetime.utcnow().isoformat()}Z")
print(f"Window: {start_ts} → {end_ts}")
print(f"Records extracted: {total}")
print(f"Bronze rows: {bronze_count}")
