# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw ServiceNow Incident Ingestion
# MAGIC Maps Splunk fields to bronze schema and appends raw records.

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)
from config import PipelineConfig as C

# COMMAND ----------

class BronzeIngestion:
    """Flattens Splunk ServiceNow records and writes to bronze layer."""

    # Field mapping: Splunk field → Bronze column
    FIELD_MAP = {
        "number": "incident_number",
        "sys_id": "incident_sys_id",
        "short_description": "short_description",
        "description": "description",
        "priority": "priority",
        "urgency": "urgency",
        "impact": "impact",
        "state": "state",
        "category": "category",
        "subcategory": "subcategory",
        "assignment_group": "assignment_group",
        "assigned_to": "assigned_to",
        "caller_id": "caller_id",
        "opened_by": "opened_by",
        "cmdb_ci": "cmdb_ci",
        "business_service": "business_service",
        "location": "location",
        "company": "company",
        "opened_at": "opened_at",
        "resolved_at": "resolved_at",
        "closed_at": "closed_at",
        "response_time": "first_response_at",
        "work_start": "work_start_at",
        "sys_created_on": "sys_created_on",
        "sys_updated_on": "sys_updated_on",
        "close_code": "close_code",
        "close_notes": "close_notes",
    }

    TIMESTAMP_FIELDS = [
        "opened_at", "resolved_at", "closed_at",
        "first_response_at", "work_start_at",
        "sys_created_on", "sys_updated_on"
    ]

    def __init__(self, spark):
        self.spark = spark

    def _map_record(self, record):
        """Map a single Splunk record to bronze schema."""
        mapped = {}
        for splunk_field, bronze_col in self.FIELD_MAP.items():
            mapped[bronze_col] = record.get(splunk_field)

        # Normalize priority: "1 - Critical" → "P1", or keep as-is if already "P0"-"P4"
        raw_priority = mapped.get("priority", "")
        if raw_priority and not raw_priority.startswith("P"):
            try:
                priority_num = str(raw_priority).strip()[0]
                mapped["priority"] = f"P{priority_num}"
            except (IndexError, ValueError):
                pass

        mapped["raw_payload"] = json.dumps(record)
        return mapped

    def write_to_bronze(self, records, start_ts, end_ts):
        """Write mapped records to bronze table."""
        if not records:
            print("No records to write to bronze.")
            return 0

        mapped_records = [self._map_record(r) for r in records]

        # Build schema — all strings initially, cast timestamps after
        string_fields = [
            StructField(col, StringType()) for col in self.FIELD_MAP.values()
        ]
        string_fields.append(StructField("raw_payload", StringType()))
        schema = StructType(string_fields)

        df = self.spark.createDataFrame(mapped_records, schema=schema)

        # Cast timestamp fields
        for ts_col in self.TIMESTAMP_FIELDS:
            df = df.withColumn(ts_col, F.to_timestamp(ts_col))

        batch_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")

        bronze_df = (df
            .withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("_batch_id", F.lit(batch_id))
        )

        bronze_df.write.format("delta").mode("append").saveAsTable(C.BRONZE_RAW)

        row_count = bronze_df.count()
        print(f"Wrote {row_count} records to bronze.")
        return row_count
