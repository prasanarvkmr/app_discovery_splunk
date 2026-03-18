# Databricks notebook source
# MAGIC %md
# MAGIC # Report Generator
# MAGIC
# MAGIC Produces coverage and health reports as:
# MAGIC - CSV files (coverage + health)
# MAGIC - HTML summary report
# MAGIC - Unity Catalog Delta tables (MERGE into gold layer)

# COMMAND ----------

import csv
import json
import os
from datetime import datetime
from typing import List, Dict, Any
from config import AppHealthConfig as C

# COMMAND ----------

class ReportGenerator:
    """
    Generates Dynatrace app health & coverage reports.
    Outputs to CSV, HTML, and Unity Catalog gold tables.
    """

    def __init__(self, spark=None):
        self.spark = spark
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(C.REPORT_OUTPUT_DIR, exist_ok=True)

    # ──────────────────────────────────────────────
    # CSV Reports
    # ──────────────────────────────────────────────

    def generate_coverage_csv(self, coverage_results: List[Dict[str, Any]]) -> str:
        filepath = os.path.join(C.REPORT_OUTPUT_DIR, f"coverage_{self.timestamp}.csv")
        headers = [
            "app_id", "app_service_id", "app_service_name", "app_name",
            "owner", "criticality", "matched_tag_key", "matched_tag_value",
            "coverage_classification", "total_entities",
            "host_count", "service_count", "application_count", "process_group_count",
        ]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
            writer.writeheader()
            for r in coverage_results:
                row = {**r}
                counts = r.get("entity_counts", {})
                row["host_count"] = counts.get("HOST", 0)
                row["service_count"] = counts.get("SERVICE", 0)
                row["application_count"] = counts.get("APPLICATION", 0)
                row["process_group_count"] = counts.get("PROCESS_GROUP", 0)
                writer.writerow(row)

        print(f"Coverage CSV: {filepath}")
        return filepath

    def generate_health_csv(self, health_results: List[Dict[str, Any]]) -> str:
        filepath = os.path.join(C.REPORT_OUTPUT_DIR, f"health_{self.timestamp}.csv")
        headers = [
            "app_id", "app_service_id", "app_service_name", "app_name",
            "owner", "criticality", "coverage_classification", "health_status",
            "open_problems_count",
            "avg_cpu_usage", "avg_memory_usage", "avg_disk_usage", "avg_host_availability",
            "avg_response_time", "total_error_count", "total_request_count", "avg_failure_rate",
            "total_action_count", "total_app_error_count", "avg_load_action_duration",
            "host_count", "service_count", "application_count", "process_group_count",
        ]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
            writer.writeheader()
            for r in health_results:
                writer.writerow(r)

        print(f"Health CSV: {filepath}")
        return filepath

    # ──────────────────────────────────────────────
    # HTML Report
    # ──────────────────────────────────────────────

    def generate_html(
        self,
        coverage_results: List[Dict[str, Any]],
        health_results: List[Dict[str, Any]],
    ) -> str:
        filepath = os.path.join(C.REPORT_OUTPUT_DIR, f"app_health_report_{self.timestamp}.html")

        # Summarise
        total = len(coverage_results)
        by_cov = {}
        for r in coverage_results:
            c = r["coverage_classification"]
            by_cov[c] = by_cov.get(c, 0) + 1

        by_health = {}
        for r in health_results:
            h = r["health_status"]
            by_health[h] = by_health.get(h, 0) + 1

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Dynatrace App Health Report — {self.timestamp}</title>
<style>
  body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 2rem; background: #f8f9fa; }}
  h1 {{ color: #1b2a4a; }}
  h2 {{ color: #2c5282; border-bottom: 2px solid #e2e8f0; padding-bottom: .3rem; }}
  .summary {{ display: flex; gap: 1.5rem; flex-wrap: wrap; margin-bottom: 2rem; }}
  .card {{ background: #fff; border-radius: 8px; padding: 1rem 1.5rem; box-shadow: 0 1px 3px rgba(0,0,0,.1); min-width: 160px; }}
  .card .num {{ font-size: 2rem; font-weight: 700; }}
  .card .label {{ color: #718096; font-size: .9rem; }}
  table {{ border-collapse: collapse; width: 100%; background: #fff; margin-bottom: 2rem; }}
  th {{ background: #1b2a4a; color: #fff; padding: .5rem .7rem; text-align: left; font-size: .85rem; }}
  td {{ padding: .45rem .7rem; border-bottom: 1px solid #e2e8f0; font-size: .85rem; }}
  tr:nth-child(even) {{ background: #f7fafc; }}
  .FULL {{ color: #276749; font-weight: 600; }}
  .PARTIAL {{ color: #c05621; font-weight: 600; }}
  .HOSTS_ONLY {{ color: #975a16; font-weight: 600; }}
  .NOT_MONITORED {{ color: #c53030; font-weight: 600; }}
  .HEALTHY {{ color: #276749; font-weight: 600; }}
  .WARNING {{ color: #c05621; font-weight: 600; }}
  .CRITICAL {{ color: #c53030; font-weight: 600; }}
  .UNKNOWN {{ color: #718096; font-weight: 600; }}
</style>
</head>
<body>
<h1>Dynatrace App Health &amp; Coverage Report</h1>
<p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

<div class="summary">
  <div class="card"><div class="num">{total}</div><div class="label">Total Apps</div></div>
  <div class="card"><div class="num">{by_cov.get('FULL', 0)}</div><div class="label">Full Coverage</div></div>
  <div class="card"><div class="num">{by_cov.get('PARTIAL', 0)}</div><div class="label">Partial</div></div>
  <div class="card"><div class="num">{by_cov.get('HOSTS_ONLY', 0)}</div><div class="label">Hosts Only</div></div>
  <div class="card"><div class="num">{by_cov.get('NOT_MONITORED', 0)}</div><div class="label">Not Monitored</div></div>
</div>

<div class="summary">
  <div class="card"><div class="num">{by_health.get('HEALTHY', 0)}</div><div class="label">Healthy</div></div>
  <div class="card"><div class="num">{by_health.get('WARNING', 0)}</div><div class="label">Warning</div></div>
  <div class="card"><div class="num">{by_health.get('CRITICAL', 0)}</div><div class="label">Critical</div></div>
  <div class="card"><div class="num">{by_health.get('UNKNOWN', 0)}</div><div class="label">Unknown</div></div>
</div>

<h2>Coverage Summary</h2>
<table>
<tr><th>App ID</th><th>App Name</th><th>Owner</th><th>Criticality</th>
<th>Tag Matched</th><th>Coverage</th><th>Entities</th></tr>
"""
        for r in sorted(coverage_results, key=lambda x: x.get("coverage_classification", "")):
            cov_cls = r["coverage_classification"]
            tag = f'{r.get("matched_tag_key", "")}={r.get("matched_tag_value", "")}'
            counts = r.get("entity_counts", {})
            entity_str = ", ".join(f"{k}:{v}" for k, v in counts.items() if v) or "—"
            html += (
                f'<tr><td>{_esc(r["app_id"])}</td><td>{_esc(r["app_name"])}</td>'
                f'<td>{_esc(r["owner"])}</td><td>{_esc(r["criticality"])}</td>'
                f'<td>{_esc(tag)}</td>'
                f'<td class="{cov_cls}">{cov_cls}</td>'
                f'<td>{entity_str}</td></tr>\n'
            )

        html += """</table>

<h2>Health Details (Monitored Apps)</h2>
<table>
<tr><th>App ID</th><th>App Name</th><th>Health</th><th>Problems</th>
<th>CPU %</th><th>Mem %</th><th>Resp Time</th><th>Fail Rate %</th>
<th>Hosts</th><th>Services</th></tr>
"""
        for r in sorted(health_results, key=lambda x: _health_sort(x["health_status"])):
            hs = r["health_status"]
            html += (
                f'<tr><td>{_esc(r["app_id"])}</td><td>{_esc(r["app_name"])}</td>'
                f'<td class="{hs}">{hs}</td>'
                f'<td>{r["open_problems_count"]}</td>'
                f'<td>{_fmt(r.get("avg_cpu_usage"))}</td>'
                f'<td>{_fmt(r.get("avg_memory_usage"))}</td>'
                f'<td>{_fmt(r.get("avg_response_time"))}</td>'
                f'<td>{_fmt(r.get("avg_failure_rate"))}</td>'
                f'<td>{r.get("host_count", 0)}</td>'
                f'<td>{r.get("service_count", 0)}</td></tr>\n'
            )

        html += """</table>
</body></html>"""

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html)

        print(f"HTML report: {filepath}")
        return filepath

    # ──────────────────────────────────────────────
    # Unity Catalog Gold Tables
    # ──────────────────────────────────────────────

    def write_coverage_to_delta(self, coverage_results: List[Dict[str, Any]]) -> None:
        if not self.spark:
            print("SKIP: No Spark session — cannot write to Unity Catalog.")
            return

        import pyspark.sql.functions as F

        rows = []
        for r in coverage_results:
            counts = r.get("entity_counts", {})
            rows.append({
                "app_id": r["app_id"],
                "app_service_id": r["app_service_id"],
                "app_service_name": r["app_service_name"],
                "app_name": r["app_name"],
                "owner": r["owner"],
                "criticality": r["criticality"],
                "matched_tag_key": r.get("matched_tag_key"),
                "matched_tag_value": r.get("matched_tag_value"),
                "coverage_classification": r["coverage_classification"],
                "total_entities": r["total_entities"],
                "host_count": counts.get("HOST", 0),
                "service_count": counts.get("SERVICE", 0),
                "application_count": counts.get("APPLICATION", 0),
                "process_group_count": counts.get("PROCESS_GROUP", 0),
            })

        df = self.spark.createDataFrame(rows)
        df = df.withColumn("updated_at", F.current_timestamp())

        target = C.GOLD_APP_COVERAGE
        if self.spark.catalog.tableExists(target):
            df.createOrReplaceTempView("src_coverage")
            self.spark.sql(f"""
                MERGE INTO {target} AS tgt
                USING src_coverage AS src
                ON tgt.app_id = src.app_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        else:
            df.write.format("delta").mode("overwrite").saveAsTable(target)

        print(f"Delta table written: {target}")

    def write_health_to_delta(self, health_results: List[Dict[str, Any]]) -> None:
        if not self.spark:
            print("SKIP: No Spark session — cannot write to Unity Catalog.")
            return

        import pyspark.sql.functions as F

        # Flatten problem_severities dict to JSON string
        rows = []
        for r in health_results:
            row = {k: v for k, v in r.items() if k != "problem_severities"}
            row["problem_severities"] = json.dumps(r.get("problem_severities", {}))
            rows.append(row)

        df = self.spark.createDataFrame(rows)
        df = df.withColumn("updated_at", F.current_timestamp())

        target = C.GOLD_APP_HEALTH
        if self.spark.catalog.tableExists(target):
            df.createOrReplaceTempView("src_health")
            self.spark.sql(f"""
                MERGE INTO {target} AS tgt
                USING src_health AS src
                ON tgt.app_id = src.app_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        else:
            df.write.format("delta").mode("overwrite").saveAsTable(target)

        print(f"Delta table written: {target}")


# ──────────────────────────────────────────────
# Module-level helpers
# ──────────────────────────────────────────────

def _esc(val: Any) -> str:
    """HTML-escape a value."""
    if val is None:
        return ""
    s = str(val)
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _fmt(val: Any) -> str:
    """Format a numeric value for display, or '—' if None."""
    if val is None:
        return "—"
    return f"{val:.2f}" if isinstance(val, float) else str(val)


def _health_sort(status: str) -> int:
    """Sort order: CRITICAL first, then WARNING, UNKNOWN, HEALTHY."""
    return {"CRITICAL": 0, "WARNING": 1, "UNKNOWN": 2, "HEALTHY": 3}.get(status, 9)
