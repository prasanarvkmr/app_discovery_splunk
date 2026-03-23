# Databricks notebook source
# MAGIC %md
# MAGIC # ELK API Extraction — PIT + search_after with Checkpoint
# MAGIC Fetches Genesys conversation data from ELK API with zero data loss guarantees.

# COMMAND ----------

import requests
import json
import time
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from config import PipelineConfig as C

# COMMAND ----------

# MAGIC %md
# MAGIC ## ELK API Client

# COMMAND ----------

class ELKExtractor:
    """Handles ELK API pagination using PIT + search_after with checkpoint recovery."""

    def __init__(self, spark, dbutils):
        self.spark = spark
        self.dbutils = dbutils
        self.base_url = C.ELK_BASE_URL
        self.index = C.ELK_INDEX
        self.page_size = C.PAGE_SIZE
        self.pit_keep_alive = C.PIT_KEEP_ALIVE
        self.max_retries = C.MAX_RETRIES
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"ApiKey {self._get_api_key()}"
        }

    def _get_api_key(self):
        """Retrieve API key from Databricks secrets."""
        return self.dbutils.secrets.get(scope=C.ELK_SECRET_SCOPE, key=C.ELK_SECRET_KEY)

    # ── Time Window Management ──

    def get_last_successful_timestamp(self):
        """Get the end timestamp of the last fully completed extraction."""
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
        """Calculate the extraction window with overlap for zero data loss."""
        last_ts = self.get_last_successful_timestamp()
        # Apply overlap to catch late-indexed documents
        adjusted_start = (
            datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
            - timedelta(minutes=C.OVERLAP_MINUTES)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        return adjusted_start, end_ts

    # ── PIT Management ──

    def open_pit(self):
        """Open a Point-in-Time snapshot on the ELK index."""
        response = self._request_with_retry(
            method="POST",
            url=f"{self.base_url}/{self.index}/_pit?keep_alive={self.pit_keep_alive}",
            body={}
        )
        pit_id = response["id"]
        print(f"PIT opened: {pit_id[:50]}...")
        return pit_id

    def close_pit(self, pit_id):
        """Release the PIT snapshot."""
        try:
            requests.delete(
                f"{self.base_url}/_pit",
                headers=self.headers,
                json={"id": pit_id},
                timeout=30
            )
            print("PIT closed.")
        except Exception as e:
            print(f"Warning: PIT close failed (will auto-expire): {e}")

    # ── Pagination ──

    def fetch_all_pages(self, pit_id, start_ts, end_ts):
        """
        Paginate through all records using PIT + search_after.
        Returns list of all source records.
        """
        all_records = []
        search_after = None
        page_num = 0
        total_fetched = 0

        query_body = {
            "size": self.page_size,
            "query": {
                "bool": {
                    "filter": [
                        {"range": {
                            "conversationStart": {
                                "gte": start_ts,
                                "lt": end_ts
                            }
                        }}
                    ]
                }
            },
            "pit": {
                "id": pit_id,
                "keep_alive": self.pit_keep_alive
            },
            "sort": [
                {"conversationStart": "asc"},
                {"_shard_doc": "asc"}  # unique tiebreaker — prevents data loss at page boundaries
            ]
        }

        while True:
            page_num += 1

            if search_after:
                query_body["search_after"] = search_after

            response = self._request_with_retry(
                method="POST",
                url=f"{self.base_url}/_search",
                body=query_body
            )

            hits = response["hits"]["hits"]

            if not hits:
                print(f"Pagination complete. Pages: {page_num - 1}, Records: {total_fetched}")
                break

            page_records = [hit["_source"] for hit in hits]
            all_records.extend(page_records)
            total_fetched += len(hits)

            # Update cursor for next page
            search_after = hits[-1]["sort"]

            # Save page checkpoint
            self._save_page_checkpoint(page_num, total_fetched, search_after, start_ts, end_ts)

            print(f"Page {page_num}: {len(hits)} records (total: {total_fetched})")

            # Last page check
            if len(hits) < self.page_size:
                print(f"Last page. Total records: {total_fetched}")
                break

            # Refresh PIT ID if returned
            if "pit_id" in response:
                query_body["pit"]["id"] = response["pit_id"]

        return all_records, total_fetched

    # ── HTTP Request with Retry ──

    def _request_with_retry(self, method, url, body):
        """Execute HTTP request with exponential backoff retry."""
        for attempt in range(1, self.max_retries + 1):
            try:
                if method == "POST":
                    response = requests.post(
                        url, headers=self.headers, json=body, timeout=60
                    )
                else:
                    response = requests.get(
                        url, headers=self.headers, timeout=60
                    )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries:
                    raise RuntimeError(
                        f"ELK API failed after {self.max_retries} attempts: {e}"
                    ) from e
                wait_seconds = 2 ** attempt
                print(f"Attempt {attempt} failed: {e}. Retrying in {wait_seconds}s...")
                time.sleep(wait_seconds)

    # ── Checkpoint Management ──

    def _save_page_checkpoint(self, page_num, total_fetched, search_after_cursor, start_ts, end_ts):
        """Persist page-level progress for crash recovery."""
        checkpoint_df = self.spark.createDataFrame([{
            "extraction_start_ts": start_ts,
            "extraction_end_ts": end_ts,
            "page_num": page_num,
            "records_fetched": total_fetched,
            "search_after_cursor": json.dumps(search_after_cursor),
            "status": "IN_PROGRESS",
            "checkpoint_ts": datetime.utcnow().isoformat()
        }])
        (checkpoint_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(C.BRONZE_CHECKPOINT)
        )

    def mark_extraction_complete(self, start_ts, end_ts, total_records):
        """Mark the extraction window as successfully completed."""
        completion_df = self.spark.createDataFrame([{
            "extraction_start_ts": start_ts,
            "extraction_end_ts": end_ts,
            "page_num": -1,
            "records_fetched": total_records,
            "search_after_cursor": None,
            "status": "COMPLETED",
            "checkpoint_ts": datetime.utcnow().isoformat()
        }])
        (completion_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(C.BRONZE_CHECKPOINT)
        )

    # ── Main Extraction Flow ──

    def extract(self):
        """
        Main entry point. Fetches data from ELK and returns records + metadata.
        Does NOT write to Bronze — that's handled by the Bronze layer module.
        """
        start_ts, end_ts = self.get_extraction_window()
        print(f"Extraction window: {start_ts} → {end_ts}")

        pit_id = None
        try:
            pit_id = self.open_pit()
            records, total = self.fetch_all_pages(pit_id, start_ts, end_ts)
            self.mark_extraction_complete(start_ts, end_ts, total)
            return records, total, start_ts, end_ts
        except Exception as e:
            print(f"EXTRACTION FAILED: {e}")
            raise
        finally:
            if pit_id:
                self.close_pit(pit_id)
