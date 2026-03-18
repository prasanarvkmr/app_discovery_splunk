# Databricks notebook source
# MAGIC %md
# MAGIC # Splunk Extraction — ServiceNow Incidents
# MAGIC Polls Splunk API every 2 minutes using Splunk SDK with overlap window.

# COMMAND ----------

import time
from datetime import datetime, timedelta
from config import PipelineConfig as C

# COMMAND ----------

class SplunkExtractor:
    """Extracts ServiceNow incident data from Splunk via SDK."""

    def __init__(self, spark, dbutils):
        self.spark = spark
        self.dbutils = dbutils
        self.service = None

    def _connect(self):
        """Establish Splunk SDK connection using token auth."""
        import splunklib.client as client

        host = self.dbutils.secrets.get(scope=C.SPLUNK_SECRET_SCOPE, key=C.SPLUNK_HOST_KEY)
        token = self.dbutils.secrets.get(scope=C.SPLUNK_SECRET_SCOPE, key=C.SPLUNK_TOKEN_KEY)

        self.service = client.connect(
            host=host,
            port=C.SPLUNK_PORT,
            splunkToken=token,
            scheme="https" if C.SPLUNK_USE_SSL else "http",
            autologin=True
        )
        print(f"Connected to Splunk: {host}:{C.SPLUNK_PORT}")

    # ── Time Window ──

    def get_last_successful_timestamp(self):
        """Get the end timestamp of the last COMPLETED extraction."""
        try:
            result = self.spark.sql(f"""
                SELECT MAX(extraction_end_ts) AS last_ts
                FROM {C.BRONZE_CHECKPOINT}
                WHERE status = 'COMPLETED'
            """).collect()[0]["last_ts"]
            return result if result else "2025-01-01T00:00:00Z"
        except Exception:
            return "2025-01-01T00:00:00Z"

    def get_extraction_window(self):
        """Calculate extraction window with overlap for zero data loss."""
        last_ts = self.get_last_successful_timestamp()
        adjusted_start = (
            datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
            - timedelta(minutes=C.POLL_OVERLAP_MINUTES)
        ).strftime("%Y-%m-%dT%H:%M:%S")
        end_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        return adjusted_start, end_ts

    # ── Query Execution ──

    def _build_query(self, start_ts, end_ts):
        """Build the Splunk SPL query for ServiceNow incidents."""
        fields = ", ".join(C.SPLUNK_FIELDS)
        return (
            f'search index={C.SPLUNK_INDEX} '
            f'sourcetype="{C.SPLUNK_SOURCETYPE}" '
            f'earliest="{start_ts}" '
            f'latest="{end_ts}" '
            f'| fields {fields} '
            f'| sort 0 _time'
        )

    def _execute_query(self, query):
        """Execute Splunk search with retry logic."""
        import splunklib.results as results

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                job = self.service.jobs.create(query, **{
                    "exec_mode": "normal",
                    "max_count": C.MAX_RESULTS_PER_QUERY,
                })

                # Wait for job to complete
                while not job.is_done():
                    time.sleep(2)
                    job.refresh()

                result_count = int(job["resultCount"])
                print(f"Splunk job complete. Results: {result_count}")

                # Stream results
                records = []
                offset = 0
                batch_size = 5000

                while offset < result_count:
                    rr = job.results(
                        output_mode="json",
                        count=batch_size,
                        offset=offset
                    )
                    reader = results.JSONResultsReader(rr)
                    for record in reader:
                        if isinstance(record, dict):
                            records.append(record)
                    offset += batch_size

                job.cancel()
                return records

            except Exception as e:
                if attempt == max_retries:
                    raise RuntimeError(
                        f"Splunk query failed after {max_retries} attempts: {e}"
                    ) from e
                wait_seconds = 2 ** attempt
                print(f"Attempt {attempt} failed: {e}. Retrying in {wait_seconds}s...")
                time.sleep(wait_seconds)

    # ── Checkpoint ──

    def mark_extraction_complete(self, start_ts, end_ts, total_records):
        """Record successful extraction in checkpoint table."""
        df = self.spark.createDataFrame([{
            "extraction_start_ts": start_ts,
            "extraction_end_ts": end_ts,
            "records_fetched": total_records,
            "status": "COMPLETED",
            "checkpoint_ts": datetime.utcnow().isoformat()
        }])
        df.write.format("delta").mode("append").saveAsTable(C.BRONZE_CHECKPOINT)

    # ── Main Entry ──

    def extract(self):
        """
        Main extraction flow.
        Returns: (records, total_count, start_ts, end_ts)
        """
        start_ts, end_ts = self.get_extraction_window()
        print(f"Extraction window: {start_ts} → {end_ts}")

        self._connect()

        query = self._build_query(start_ts, end_ts)
        print(f"Executing query...")

        records = self._execute_query(query)
        total = len(records)
        print(f"Extracted {total} records.")

        return records, total, start_ts, end_ts
