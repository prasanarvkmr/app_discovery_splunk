# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC Flattens ELK API response and appends to bronze raw table.
# MAGIC Each participant record becomes one row.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, LongType
)
C = PipelineConfig

# COMMAND ----------

class BronzeIngestion:
    """Flattens Genesys ELK API response and writes to bronze layer."""

    def __init__(self, spark):
        self.spark = spark

    def flatten_records(self, raw_records):
        """
        Flatten the nested ELK/Genesys JSON into one row per participant.

        Expected input structure per record:
        {
            "conversationId": "CONV-001",
            "conversationStart": "2026-03-17T10:00:00Z",
            "conversationEnd": "2026-03-17T10:05:00Z",
            "divisionIds": ["div-01"],
            "participants": [
                {
                    "purpose": "agent",
                    "participantId": "PART-001",
                    "userId": "agent@company.com",
                    "sessions": [{
                        "mediaType": "voice",
                        "direction": "inbound",
                        "edgeId": "edge-01",
                        "provider": "Vendor-A",
                        "remoteNameDisplayable": "Chennai",
                        "metrics": [
                            {"name": "nMos", "emitDate": "...", "value": 4.2}
                        ],
                        "segments": [...],
                        "errorInfo": {"code": "MEDIA_ERR", "message": "..."}
                    }]
                },
                ...
            ]
        }
        """
        flattened = []

        for record in raw_records:
            conversation_id = record.get("conversationId")
            conv_start = record.get("conversationStart")
            conv_end = record.get("conversationEnd")
            divisions = record.get("divisionIds", [])
            division = divisions[0] if divisions else None

            participants = record.get("participants", [])

            for participant in participants:
                purpose = participant.get("purpose")
                participant_id = participant.get("participantId")
                user_id = participant.get("userId")  # only for agents

                sessions = participant.get("sessions", [])
                for session in sessions:
                    media_type = session.get("mediaType")
                    direction = session.get("direction")
                    vendor = session.get("provider")
                    location = session.get("remoteNameDisplayable")

                    # Extract MOS score from metrics array
                    mos_score = None
                    metrics = session.get("metrics", [])
                    for metric in metrics:
                        if metric.get("name") == "nMos":
                            mos_score = metric.get("value")
                            break

                    # Extract error code
                    error_info = session.get("errorInfo")
                    error_code = error_info.get("code") if error_info else None

                    # Calculate duration
                    duration_seconds = None
                    if conv_start and conv_end:
                        try:
                            start_dt = datetime.fromisoformat(conv_start.replace("Z", "+00:00"))
                            end_dt = datetime.fromisoformat(conv_end.replace("Z", "+00:00"))
                            duration_seconds = int((end_dt - start_dt).total_seconds())
                        except (ValueError, TypeError):
                            duration_seconds = None

                    flattened.append({
                        "conversation_id": conversation_id,
                        "purpose": purpose,
                        "participant_id": participant_id,
                        "agent_id": user_id if purpose == "agent" else None,
                        "email_id": user_id if purpose == "agent" else None,
                        "queue_id": participant.get("queueId"),
                        "division": division,
                        "vendor": vendor,
                        "location": location,
                        "media_type": media_type,
                        "direction": direction,
                        "mos_score": float(mos_score) if mos_score is not None else None,
                        "error_code": error_code,
                        "call_start_ts": conv_start,
                        "call_end_ts": conv_end,
                        "duration_seconds": duration_seconds,
                        "raw_payload": json.dumps(record)
                    })

        return flattened

    def write_to_bronze(self, records, start_ts, end_ts):
        """Write flattened records to bronze table."""
        if not records:
            print("No records to write to bronze.")
            return 0

        flattened = self.flatten_records(records)

        if not flattened:
            print("No participant records after flattening.")
            return 0

        schema = StructType([
            StructField("conversation_id", StringType()),
            StructField("purpose", StringType()),
            StructField("participant_id", StringType()),
            StructField("agent_id", StringType()),
            StructField("email_id", StringType()),
            StructField("queue_id", StringType()),
            StructField("division", StringType()),
            StructField("vendor", StringType()),
            StructField("location", StringType()),
            StructField("media_type", StringType()),
            StructField("direction", StringType()),
            StructField("mos_score", DoubleType()),
            StructField("error_code", StringType()),
            StructField("call_start_ts", StringType()),
            StructField("call_end_ts", StringType()),
            StructField("duration_seconds", LongType()),
            StructField("raw_payload", StringType()),
        ])

        df = self.spark.createDataFrame(flattened, schema=schema)

        batch_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")

        bronze_df = (df
            .withColumn("call_start_ts", F.to_timestamp("call_start_ts"))
            .withColumn("call_end_ts", F.to_timestamp("call_end_ts"))
            .withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("_extraction_window_start", F.lit(start_ts))
            .withColumn("_extraction_window_end", F.lit(end_ts))
            .withColumn("_batch_id", F.lit(batch_id))
        )

        row_count = len(flattened)

        (bronze_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(C.BRONZE_RAW)
        )

        print(f"Wrote {row_count} participant records to bronze.")
        return row_count
