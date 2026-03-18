# Databricks notebook source
# MAGIC %md
# MAGIC # ServiceNow Incident SLA Pipeline — Main Orchestrator
# MAGIC
# MAGIC **Schedule**: Every 2 minutes via Databricks Workflow
# MAGIC
# MAGIC **Flow**:
# MAGIC ```
# MAGIC Splunk API → Bronze (append) → Silver (MERGE + SLA calc) → Gold (MERGE + open incidents)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize

# COMMAND ----------

from config import PipelineConfig as C
from datetime import datetime

print(f"Pipeline started at {datetime.utcnow().isoformat()}Z")
print(f"Catalog: {C.CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Extract from Splunk

# COMMAND ----------

from splunk_extraction import SplunkExtractor

extractor = SplunkExtractor(spark, dbutils)
records, total, start_ts, end_ts = extractor.extract()

print(f"Extracted {total} records for window {start_ts} → {end_ts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Write to Bronze

# COMMAND ----------

from bronze_ingestion import BronzeIngestion

bronze = BronzeIngestion(spark)
bronze_count = bronze.write_to_bronze(records, start_ts, end_ts)

# Mark extraction complete only after bronze write succeeds
extractor.mark_extraction_complete(start_ts, end_ts, total)
print(f"Bronze ingestion complete. {bronze_count} records written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Silver Transformations (SLA Calculation)

# COMMAND ----------

from silver_transformations import SilverTransformations

silver = SilverTransformations(spark)
silver.run_all()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Gold Aggregations

# COMMAND ----------

from gold_aggregations import GoldAggregations

gold = GoldAggregations(spark)
gold.run_all()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Cross-Pipeline Correlation (Genesys ↔ ServiceNow)

# COMMAND ----------

try:
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(os.getcwd()), "cross_pipeline"))
    from cross_pipeline.config import CorrelationConfig  # noqa: F401
    from cross_pipeline.build_correlation import CorrelationBuilder

    correlation = CorrelationBuilder(spark)
    correlation.build()
except ImportError:
    print("Cross-pipeline correlation module not found — skipping. "
          "Deploy the cross_pipeline/ notebooks to enable.")
except Exception as e:
    # Correlation is additive; don't fail the core pipeline
    print(f"Correlation step failed (non-fatal): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete

# COMMAND ----------

print(f"Pipeline completed at {datetime.utcnow().isoformat()}Z")
print(f"Window: {start_ts} → {end_ts}")
print(f"Records extracted: {total}")
print(f"Bronze rows: {bronze_count}")
