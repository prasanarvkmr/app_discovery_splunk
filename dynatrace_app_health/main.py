# Databricks notebook source
# MAGIC %md
# MAGIC # Dynatrace App Health & Coverage — Orchestrator
# MAGIC
# MAGIC **Pipeline steps:**
# MAGIC 1. Load CMDB application inventory from Databricks table
# MAGIC 2. Scan Dynatrace for entity coverage (cascading tag lookup)
# MAGIC 3. Assess health (problems + metrics) for monitored apps
# MAGIC 4. Generate reports — CSV, HTML, and Unity Catalog gold tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from config import AppHealthConfig as C

# ── Resolve Dynatrace secrets ──
dt_url = dbutils.secrets.get(scope=C.DT_SECRET_SCOPE, key=C.DT_URL_KEY)
dt_token = dbutils.secrets.get(scope=C.DT_SECRET_SCOPE, key=C.DT_TOKEN_KEY)

print(f"Dynatrace URL : {dt_url[:30]}...")
print(f"CMDB table    : {C.CMDB_TABLE}")
print(f"Output catalog: {C.CATALOG}.{C.GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Load CMDB

# COMMAND ----------

from cmdb_loader import CMDBLoader

loader = CMDBLoader(spark)
apps = loader.load_apps()

print(f"Apps loaded: {len(apps)}")
displayHTML(f"<b>{len(apps)}</b> CMDB applications loaded for scanning.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Scan Dynatrace Coverage

# COMMAND ----------

from dt_api_extensions import DynatraceAppClient
from coverage_scanner import CoverageScanner

dt_client = DynatraceAppClient(dt_url, dt_token, C.DT_SSL_CONFIG)
scanner = CoverageScanner(dt_client)

coverage_results = scanner.scan_all(apps)

# Quick summary
cov_counts = {}
for r in coverage_results:
    c = r["coverage_classification"]
    cov_counts[c] = cov_counts.get(c, 0) + 1
print(f"Coverage breakdown: {cov_counts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Assess Health

# COMMAND ----------

from health_assessor import HealthAssessor

assessor = HealthAssessor(dt_client)
health_results = assessor.assess_all(coverage_results)

health_counts = {}
for r in health_results:
    h = r["health_status"]
    health_counts[h] = health_counts.get(h, 0) + 1
print(f"Health breakdown: {health_counts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Generate Reports

# COMMAND ----------

from report_generator import ReportGenerator

reporter = ReportGenerator(spark)

# CSV reports
cov_csv = reporter.generate_coverage_csv(coverage_results)
health_csv = reporter.generate_health_csv(health_results)

# HTML report
html_path = reporter.generate_html(coverage_results, health_results)

# Unity Catalog gold tables
reporter.write_coverage_to_delta(coverage_results)
reporter.write_health_to_delta(health_results)

print("\n=== Pipeline Complete ===")
print(f"  Coverage CSV : {cov_csv}")
print(f"  Health CSV   : {health_csv}")
print(f"  HTML report  : {html_path}")
print(f"  Delta tables : {C.GOLD_APP_COVERAGE}, {C.GOLD_APP_HEALTH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Preview

# COMMAND ----------

# Show coverage table
if spark.catalog.tableExists(C.GOLD_APP_COVERAGE):
    display(spark.table(C.GOLD_APP_COVERAGE).orderBy("coverage_classification"))

# COMMAND ----------

# Show health table
if spark.catalog.tableExists(C.GOLD_APP_HEALTH):
    display(spark.table(C.GOLD_APP_HEALTH).orderBy("health_status"))
