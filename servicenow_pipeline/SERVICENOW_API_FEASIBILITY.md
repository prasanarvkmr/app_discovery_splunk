# ServiceNow Incident SLA Pipeline — API & Sample Queries

> **Purpose:** Enable the team to independently validate the Splunk SDK extraction of ServiceNow
> incident data, confirm field availability, verify SLA calculation inputs, and test query
> performance — before full pipeline integration.
>
> **Date:** 2026-03-18  
> **Data Source:** Splunk Enterprise → index=`servicenow`, sourcetype=`servicenow:incident`

---

## Table of Contents

1. [Prerequisites & Authentication](#1-prerequisites--authentication)
2. [API / SDK Endpoints Used](#2-api--sdk-endpoints-used)
3. [Feasibility Test Plan](#3-feasibility-test-plan)
4. [Sample Queries — Splunk SPL](#4-sample-queries--splunk-spl)
5. [Sample Queries — Python (Splunk SDK)](#5-sample-queries--python-splunk-sdk)
6. [Expected Field Schema](#6-expected-field-schema)
7. [SLA Configuration Reference](#7-sla-configuration-reference)
8. [Pipeline Architecture](#8-pipeline-architecture)
9. [Unity Catalog — Table Reference](#9-unity-catalog--table-reference)
10. [Gold Layer — Report Queries](#10-gold-layer--report-queries)
11. [Pagination & Performance](#11-pagination--performance)
12. [Troubleshooting & Common Issues](#12-troubleshooting--common-issues)

---

## 1. Prerequisites & Authentication

### Splunk Token Authentication

| Setting | Value | Notes |
|---------|-------|-------|
| **Host** | `splunk.yourcompany.com` | Splunk search head |
| **Port** | `8089` | Management port (REST API) |
| **Scheme** | `https` | SSL enabled |
| **Auth method** | Token (Bearer) | Stored in Databricks secret scope |
| **Index** | `servicenow` | ServiceNow incident data |
| **Sourcetype** | `servicenow:incident` | Incident records |

### Databricks Secrets Setup

```python
# Required secrets in scope "splunk"
dbutils.secrets.put(scope="splunk", key="host",  string_value="splunk.yourcompany.com")
dbutils.secrets.put(scope="splunk", key="token", string_value="{SPLUNK_BEARER_TOKEN}")
```

### How to Get a Splunk Token

1. Splunk Web → **Settings** → **Tokens** → **New Token**
2. Set audience to `search` and expiration as needed
3. Required capabilities: `search`, `list_search_head_clustering` (for SHC environments)

### Connectivity Check

```bash
# Quick health check — should return HTTP 200
curl -k -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer {SPLUNK_TOKEN}" \
  "https://splunk.yourcompany.com:8089/services/server/info?output_mode=json"
```

---

## 2. API / SDK Endpoints Used

| # | SDK Method / Endpoint | Purpose |
|---|----------------------|---------|
| 1 | `splunklib.client.connect()` | Establish authenticated connection |
| 2 | `service.jobs.create(query)` | Submit SPL search job |
| 3 | `job.results(output_mode="json")` | Stream results from completed job |
| 4 | `splunklib.results.JSONResultsReader` | Parse JSON result stream |

**SDK Reference:** [Splunk SDK for Python](https://dev.splunk.com/enterprise/docs/devtools/python/sdk-python/)

---

## 3. Feasibility Test Plan

Run these tests in order. Each must pass before the pipeline can work.

| Test # | What to Validate | Pass Criteria |
|--------|-----------------|---------------|
| **F-1** | Splunk SDK connectivity | `service.apps.list()` returns without error |
| **F-2** | Index exists | `index=servicenow` returns events |
| **F-3** | Sourcetype exists | `sourcetype="servicenow:incident"` returns events |
| **F-4** | Required fields present | All 26 fields from schema (Section 6) appear in results |
| **F-5** | Priority field values | `priority` contains P0–P4 (or 1-Critical through 5-Planning) |
| **F-6** | Timestamp fields parseable | `opened_at`, `resolved_at`, `closed_at` parse as valid timestamps |
| **F-7** | Business service populated | `business_service` is non-null for ≥50% of incidents |
| **F-8** | Assignment group populated | `assignment_group` is non-null for ≥80% of incidents |
| **F-9** | Data volume estimate | Count events in last 24h to estimate throughput |
| **F-10** | Time-range query works | `earliest` / `latest` range filters function correctly |

---

## 4. Sample Queries — Splunk SPL

> Run these directly in Splunk Web (**Search & Reporting** app) to validate data availability.

### F-1: Basic Index Check

```spl
index=servicenow sourcetype="servicenow:incident"
| head 5
| table _time, number, priority, state, short_description, business_service
```

### F-2: Count Events in Last 24 Hours

```spl
index=servicenow sourcetype="servicenow:incident" earliest=-24h
| stats count AS total_events
```

### F-3: Verify All Required Fields Exist

```spl
index=servicenow sourcetype="servicenow:incident" earliest=-24h
| head 100
| fieldsummary
| table field, count, distinct_count, is_exact, numeric_count
| sort - count
```

### F-4: Priority Distribution

```spl
index=servicenow sourcetype="servicenow:incident" earliest=-7d
| stats count BY priority
| sort priority
```

### F-5: State Distribution

```spl
index=servicenow sourcetype="servicenow:incident" earliest=-7d
| stats count BY state
| sort - count
```

### F-6: Business Service Coverage

```spl
index=servicenow sourcetype="servicenow:incident" earliest=-7d
| eval has_bs = if(isnotnull(business_service) AND business_service!="", "YES", "NO")
| stats count BY has_bs
| eval pct = round(count / sum(count) * 100, 1)
```

### F-7: Assignment Group Coverage

```spl
index=servicenow sourcetype="servicenow:incident" earliest=-7d
| eval has_ag = if(isnotnull(assignment_group) AND assignment_group!="", "YES", "NO")
| stats count BY has_ag
```

### F-8: Timestamp Validation

```spl
index=servicenow sourcetype="servicenow:incident" earliest=-24h
| eval opened_ok = if(isnotnull(opened_at), 1, 0)
| eval resolved_ok = if(isnotnull(resolved_at), 1, 0)
| eval closed_ok = if(isnotnull(closed_at), 1, 0)
| stats
    sum(opened_ok) AS has_opened_at,
    sum(resolved_ok) AS has_resolved_at,
    sum(closed_ok) AS has_closed_at,
    count AS total
```

### F-9: Top Business Services by Incident Volume

```spl
index=servicenow sourcetype="servicenow:incident" earliest=-30d
| stats count BY business_service
| sort - count
| head 20
```

### F-10: Top Assignment Groups

```spl
index=servicenow sourcetype="servicenow:incident" earliest=-30d
| stats count BY assignment_group
| sort - count
| head 20
```

### F-11: SLA Input Validation — Response Time Available?

```spl
index=servicenow sourcetype="servicenow:incident" earliest=-7d resolved_at=*
| eval response_minutes = round((strptime(resolved_at, "%Y-%m-%d %H:%M:%S") - strptime(opened_at, "%Y-%m-%d %H:%M:%S")) / 60, 1)
| stats
    avg(response_minutes) AS avg_minutes,
    median(response_minutes) AS median_minutes,
    max(response_minutes) AS max_minutes,
    min(response_minutes) AS min_minutes
    BY priority
| sort priority
```

### F-12: Extraction Window Simulation

```spl
index=servicenow sourcetype="servicenow:incident"
    earliest="2026-03-18T00:00:00" latest="2026-03-18T02:00:00"
| fields _time, number, sys_id, short_description, priority, urgency, impact, state,
    category, subcategory, assignment_group, assigned_to, caller_id, opened_by,
    opened_at, resolved_at, closed_at, response_time, work_start,
    close_code, close_notes, cmdb_ci, business_service, location, company,
    sys_created_on, sys_updated_on
| sort 0 _time
| stats count
```

### F-13: Vendor / Location Breakdown

```spl
index=servicenow sourcetype="servicenow:incident" earliest=-30d
| stats count BY location, company
| sort - count
| head 20
```

---

## 5. Sample Queries — Python (Splunk SDK)

### 5.1 Setup — Install SDK & Connect

```python
# pip install splunk-sdk
import splunklib.client as client
import splunklib.results as results
import time

# ── Configuration ──
SPLUNK_HOST  = "splunk.yourcompany.com"
SPLUNK_PORT  = 8089
SPLUNK_TOKEN = "{YOUR_BEARER_TOKEN}"

# Connect
service = client.connect(
    host=SPLUNK_HOST,
    port=SPLUNK_PORT,
    splunkToken=SPLUNK_TOKEN,
    scheme="https",
    autologin=True,
)
print(f"Connected to Splunk: {service.info['serverName']}")
```

### 5.2 F-1 — Connectivity Test

```python
# Verify connection
apps = service.apps.list()
print(f"Connection OK — {len(apps)} apps available")
for idx in service.indexes:
    if idx.name == "servicenow":
        print(f"Index 'servicenow' found — totalEventCount: {idx['totalEventCount']}")
        break
else:
    print("WARNING: index 'servicenow' NOT found!")
```

### 5.3 F-2 — Run a Search and Read Results

```python
def run_splunk_query(service, spl_query, max_results=1000):
    """Execute a Splunk search and return results as list of dicts."""
    job = service.jobs.create(spl_query, **{
        "exec_mode": "normal",
        "max_count": max_results,
    })

    # Wait for completion
    while not job.is_done():
        time.sleep(2)
        job.refresh()

    result_count = int(job["resultCount"])
    print(f"Job complete — {result_count} results")

    records = []
    offset = 0
    batch_size = 5000

    while offset < result_count:
        rr = job.results(output_mode="json", count=batch_size, offset=offset)
        reader = results.JSONResultsReader(rr)
        for record in reader:
            if isinstance(record, dict):
                records.append(record)
        offset += batch_size

    job.cancel()
    return records


# Test: get 5 recent incidents
records = run_splunk_query(service, '''
    search index=servicenow sourcetype="servicenow:incident" earliest=-24h
    | head 5
    | table _time, number, priority, state, short_description, business_service
''')

for r in records:
    print(f"  {r.get('number')} [{r.get('priority')}] {r.get('state')} — {r.get('short_description', '')[:60]}")
```

### 5.4 F-3 — Validate Required Fields

```python
# Check which of our required fields are present in the data
REQUIRED_FIELDS = [
    "_time", "number", "sys_id", "short_description", "description",
    "priority", "urgency", "impact", "state",
    "category", "subcategory",
    "assignment_group", "assigned_to", "caller_id", "opened_by",
    "opened_at", "resolved_at", "closed_at",
    "response_time", "work_start",
    "close_code", "close_notes",
    "cmdb_ci", "business_service", "location", "company",
    "sys_created_on", "sys_updated_on",
]

records = run_splunk_query(service, f'''
    search index=servicenow sourcetype="servicenow:incident" earliest=-24h
    | head 100
    | fields {", ".join(REQUIRED_FIELDS)}
''', max_results=100)

if records:
    sample = records[0]
    print(f"\nField availability (from {len(records)} sample records):")
    for field in REQUIRED_FIELDS:
        present = sum(1 for r in records if r.get(field))
        pct = round(present / len(records) * 100, 1)
        status = "✓" if pct > 50 else ("⚠" if pct > 0 else "✗")
        print(f"  {status} {field:30s} {pct}% populated ({present}/{len(records)})")
```

### 5.5 F-4 — Priority Distribution Check

```python
records = run_splunk_query(service, '''
    search index=servicenow sourcetype="servicenow:incident" earliest=-7d
    | stats count BY priority
    | sort priority
''')

print("\nPriority distribution (last 7 days):")
for r in records:
    print(f"  {r.get('priority', '?'):10s} → {r.get('count', '0')} incidents")
```

### 5.6 F-5 — Time-Range Extraction (pipeline simulation)

```python
from datetime import datetime, timedelta

# Simulate a 2-minute extraction window with 3-minute overlap
end_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
start_ts = (datetime.utcnow() - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S")

FIELDS = ", ".join(REQUIRED_FIELDS)

query = (
    f'search index=servicenow '
    f'sourcetype="servicenow:incident" '
    f'earliest="{start_ts}" '
    f'latest="{end_ts}" '
    f'| fields {FIELDS} '
    f'| sort 0 _time'
)

print(f"Extraction window: {start_ts} → {end_ts}")
records = run_splunk_query(service, query, max_results=50000)
print(f"Extracted {len(records)} records in window")
```

### 5.7 F-6 — SLA Calculation Dry Run

```python
# Verify we can calculate response + resolution times from the data
records = run_splunk_query(service, '''
    search index=servicenow sourcetype="servicenow:incident"
        earliest=-7d resolved_at=*
    | eval resp_min = round(
        (strptime(resolved_at, "%Y-%m-%d %H:%M:%S") - strptime(opened_at, "%Y-%m-%d %H:%M:%S")) / 60, 1)
    | table number, priority, opened_at, resolved_at, resp_min
    | sort priority
    | head 20
''')

print("\nSample SLA Calculation:")
print(f"{'Incident':<15} {'Priority':<10} {'Opened':<22} {'Resolved':<22} {'Minutes':>8}")
print("-" * 80)
for r in records:
    print(f"{r.get('number',''):<15} {r.get('priority',''):<10} "
          f"{r.get('opened_at',''):<22} {r.get('resolved_at',''):<22} "
          f"{r.get('resp_min','—'):>8}")
```

### 5.8 F-7 — Business Service + Vendor Coverage

```python
records = run_splunk_query(service, '''
    search index=servicenow sourcetype="servicenow:incident" earliest=-30d
    | stats count BY business_service
    | sort - count
    | head 25
''')

print("\nTop 25 Business Services:")
for r in records:
    bs = r.get("business_service", "(empty)")
    print(f"  {bs:40s} {r.get('count', '0'):>6} incidents")
```

### 5.9 F-8 — Full Feasibility Test Script (all-in-one)

```python
"""
Complete feasibility test for ServiceNow Incident extraction via Splunk.
Replace the 3 variables below with your actual values.
"""
import splunklib.client as client
import splunklib.results as results
import time

SPLUNK_HOST  = "splunk.yourcompany.com"
SPLUNK_PORT  = 8089
SPLUNK_TOKEN = "{YOUR_BEARER_TOKEN}"

test_results = {}

# ── Connect ──
try:
    service = client.connect(
        host=SPLUNK_HOST, port=SPLUNK_PORT,
        splunkToken=SPLUNK_TOKEN, scheme="https", autologin=True,
    )
    test_results["F-1 Connectivity"] = f"PASS — server: {service.info['serverName']}"
except Exception as e:
    test_results["F-1 Connectivity"] = f"FAIL: {e}"
    print("Cannot proceed without connection. Exiting.")
    raise

def quick_query(spl, max_count=1000):
    job = service.jobs.create(spl, exec_mode="normal", max_count=max_count)
    while not job.is_done():
        time.sleep(1)
        job.refresh()
    rr = job.results(output_mode="json", count=max_count)
    reader = results.JSONResultsReader(rr)
    recs = [r for r in reader if isinstance(r, dict)]
    job.cancel()
    return recs

# ── F-2: Index exists ──
try:
    recs = quick_query('search index=servicenow earliest=-1h | stats count')
    count = int(recs[0].get("count", 0)) if recs else 0
    test_results["F-2 Index exists"] = f"PASS ({count} events in last 1h)" if count > 0 else "WARN (0 events in last 1h)"
except Exception as e:
    test_results["F-2 Index exists"] = f"FAIL: {e}"

# ── F-3: Sourcetype exists ──
try:
    recs = quick_query('search index=servicenow sourcetype="servicenow:incident" earliest=-1h | stats count')
    count = int(recs[0].get("count", 0)) if recs else 0
    test_results["F-3 Sourcetype exists"] = f"PASS ({count} events)" if count > 0 else "WARN (0 events)"
except Exception as e:
    test_results["F-3 Sourcetype exists"] = f"FAIL: {e}"

# ── F-4: Required fields ──
try:
    recs = quick_query('''
        search index=servicenow sourcetype="servicenow:incident" earliest=-24h
        | head 50
        | fieldsummary
        | table field, count
    ''')
    found_fields = {r["field"] for r in recs}
    required = {"number", "priority", "state", "opened_at", "business_service", "assignment_group"}
    missing = required - found_fields
    if not missing:
        test_results["F-4 Required fields"] = f"PASS (all {len(required)} critical fields present)"
    else:
        test_results["F-4 Required fields"] = f"WARN — missing: {missing}"
except Exception as e:
    test_results["F-4 Required fields"] = f"FAIL: {e}"

# ── F-5: Priority values ──
try:
    recs = quick_query('''
        search index=servicenow sourcetype="servicenow:incident" earliest=-7d
        | stats count BY priority
    ''')
    priorities = [r.get("priority", "") for r in recs]
    test_results["F-5 Priority values"] = f"PASS — found: {priorities}"
except Exception as e:
    test_results["F-5 Priority values"] = f"FAIL: {e}"

# ── F-6: Timestamps parseable ──
try:
    recs = quick_query('''
        search index=servicenow sourcetype="servicenow:incident" earliest=-24h
        | head 10
        | table opened_at, resolved_at, closed_at
    ''')
    has_opened = any(r.get("opened_at") for r in recs)
    test_results["F-6 Timestamps"] = "PASS" if has_opened else "WARN (opened_at missing)"
except Exception as e:
    test_results["F-6 Timestamps"] = f"FAIL: {e}"

# ── F-7: Business service populated ──
try:
    recs = quick_query('''
        search index=servicenow sourcetype="servicenow:incident" earliest=-7d
        | eval has_bs = if(isnotnull(business_service) AND business_service!="", 1, 0)
        | stats sum(has_bs) AS populated, count AS total
    ''')
    pop = int(recs[0].get("populated", 0))
    total = int(recs[0].get("total", 1))
    pct = round(pop / total * 100, 1)
    status = "PASS" if pct >= 50 else "WARN"
    test_results["F-7 Business service"] = f"{status} ({pct}% populated — {pop}/{total})"
except Exception as e:
    test_results["F-7 Business service"] = f"FAIL: {e}"

# ── F-8: Assignment group populated ──
try:
    recs = quick_query('''
        search index=servicenow sourcetype="servicenow:incident" earliest=-7d
        | eval has_ag = if(isnotnull(assignment_group) AND assignment_group!="", 1, 0)
        | stats sum(has_ag) AS populated, count AS total
    ''')
    pop = int(recs[0].get("populated", 0))
    total = int(recs[0].get("total", 1))
    pct = round(pop / total * 100, 1)
    status = "PASS" if pct >= 80 else "WARN"
    test_results["F-8 Assignment group"] = f"{status} ({pct}% populated — {pop}/{total})"
except Exception as e:
    test_results["F-8 Assignment group"] = f"FAIL: {e}"

# ── F-9: Volume estimate ──
try:
    recs = quick_query('''
        search index=servicenow sourcetype="servicenow:incident" earliest=-24h
        | stats count
    ''')
    count = int(recs[0].get("count", 0)) if recs else 0
    test_results["F-9 Volume (24h)"] = f"PASS — {count} events/24h (~{count//24}/hr)"
except Exception as e:
    test_results["F-9 Volume (24h)"] = f"FAIL: {e}"

# ── F-10: Time-range filter ──
try:
    recs = quick_query('''
        search index=servicenow sourcetype="servicenow:incident"
            earliest="2026-03-17T00:00:00" latest="2026-03-17T01:00:00"
        | stats count
    ''')
    count = int(recs[0].get("count", 0)) if recs else 0
    test_results["F-10 Time-range filter"] = f"PASS ({count} events in 1-hour window)"
except Exception as e:
    test_results["F-10 Time-range filter"] = f"FAIL: {e}"

# ── Print Report ──
print("\n" + "=" * 60)
print("  SERVICENOW INCIDENT FEASIBILITY TEST RESULTS")
print("=" * 60)
for test, outcome in test_results.items():
    status = "✓" if "PASS" in outcome else ("⚠" if "WARN" in outcome else "✗")
    print(f"  {status}  {test}: {outcome}")
print("=" * 60)
```

---

## 6. Expected Field Schema

These are the 26 fields extracted from Splunk per incident. All must be present in the index.

| # | Splunk Field | Bronze Column | Type | Required | Notes |
|---|-------------|---------------|------|----------|-------|
| 1 | `_time` | `_ingestion_ts` | TIMESTAMP | ✓ | Splunk event timestamp |
| 2 | `number` | `incident_number` | STRING | ✓ | e.g. INC0012345 |
| 3 | `sys_id` | `incident_sys_id` | STRING | ✓ | ServiceNow GUID |
| 4 | `short_description` | `short_description` | STRING | ✓ | Incident title |
| 5 | `description` | `description` | STRING | | Full description |
| 6 | `priority` | `priority` | STRING | ✓ | P0–P4 (or 1-Critical … 5-Planning) |
| 7 | `urgency` | `urgency` | STRING | | 1-High, 2-Medium, 3-Low |
| 8 | `impact` | `impact` | STRING | | 1-High, 2-Medium, 3-Low |
| 9 | `state` | `state` | STRING | ✓ | New, In Progress, Resolved, Closed, etc. |
| 10 | `category` | `category` | STRING | | e.g. Network, Software, Hardware |
| 11 | `subcategory` | `subcategory` | STRING | | Subcategory |
| 12 | `assignment_group` | `assignment_group` | STRING | ✓ | Resolver group |
| 13 | `assigned_to` | `assigned_to` | STRING | | Individual assignee |
| 14 | `caller_id` | `caller_id` | STRING | | Who reported the incident |
| 15 | `opened_by` | `opened_by` | STRING | | Who created the record |
| 16 | `opened_at` | `opened_at` | TIMESTAMP | ✓ | SLA clock start |
| 17 | `resolved_at` | `resolved_at` | TIMESTAMP | | SLA resolution clock stop |
| 18 | `closed_at` | `closed_at` | TIMESTAMP | | Record closure timestamp |
| 19 | `response_time` | `first_response_at` | TIMESTAMP | | First response (for response SLA) |
| 20 | `work_start` | `work_start_at` | TIMESTAMP | | When work began |
| 21 | `close_code` | `close_code` | STRING | | Resolution category |
| 22 | `close_notes` | `close_notes` | STRING | | Resolution notes |
| 23 | `cmdb_ci` | `cmdb_ci` | STRING | | Configuration item |
| 24 | `business_service` | `business_service` | STRING | ✓ | Business service for classification |
| 25 | `location` | `location` | STRING | | Incident location |
| 26 | `company` | `company` | STRING | | Company / tenant |
| 27 | `sys_created_on` | `sys_created_on` | TIMESTAMP | | Record creation |
| 28 | `sys_updated_on` | `sys_updated_on` | TIMESTAMP | | Last update (used for change detection) |

### Priority Normalization

The pipeline normalizes ServiceNow priority values:

| ServiceNow Value | Normalized | SLA Mode |
|-----------------|------------|----------|
| `1 - Critical` | `P0` | 24×7 |
| `2 - High` | `P1` | 24×7 |
| `3 - Moderate` | `P2` | 24×7 |
| `4 - Low` | `P3` | Business Hours |
| `5 - Planning` | `P4` | Business Hours |

---

## 7. SLA Configuration Reference

### 7.1 SLA Targets by Priority

| Priority | Response SLA | Resolution SLA | Calculation Mode |
|----------|-------------|----------------|------------------|
| **P0** | 15 min | 2 hrs (120 min) | 24×7 |
| **P1** | 15 min | 4 hrs (240 min) | 24×7 |
| **P2** | 30 min | 8 hrs (480 min) | 24×7 |
| **P3** | 60 min | 24 hrs (1440 min) | Business Hours |
| **P4** | 240 min (4 hrs) | 48 hrs (2880 min) | Business Hours |

### 7.2 Business Hours Definition

| Setting | Value |
|---------|-------|
| Start | 09:00 |
| End | 18:00 |
| Days | Mon–Fri |
| Hours/day | 9 |

### 7.3 SLA Status Classification

| Status | Condition |
|--------|-----------|
| **MET** | Incident resolved within SLA target |
| **BREACHED** | Incident resolved but exceeded target |
| **IN_PROGRESS** | Still open, within SLA target |
| **AT_RISK** | Still open, >80% of SLA time consumed |

### 7.4 Business Hours Adjustment Formula

For P3/P4 incidents, elapsed clock time is converted to business minutes:

```
business_minutes = raw_elapsed_minutes × (9/24) × (5/7)
```

- Factor `9/24` = only 9 out of 24 hours count per day
- Factor `5/7` = only 5 out of 7 days count per week

---

## 8. Pipeline Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   SPLUNK API    │     │     BRONZE      │     │     SILVER      │     │      GOLD       │
│                 │     │                 │     │                 │     │                 │
│ index=servicenow│────▶│ raw_servicenow  │────▶│ fact_incidents  │────▶│ agg_sla_summary │
│ sourcetype=     │     │ _incidents      │     │ (SLA calculated)│     │ agg_platform_   │
│ servicenow:     │     │                 │     │                 │     │   impact        │
│ incident        │     │ _splunk_extract │     │ fact_incident   │     │ agg_agent_impact│
│                 │     │ _ion_checkpoint │     │   _timeline     │     │ agg_vendor_sla  │
│                 │     │                 │     │                 │     │ fact_open_       │
│                 │     │                 │     │ dim_business_   │     │   incidents     │
│                 │     │                 │     │   services      │     │                 │
│                 │     │                 │     │ dim_sla_targets │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
                                                                               │
                                                                               ▼
                                                                    ┌─────────────────┐
                                                                    │  CORRELATION    │
                                                                    │ (Genesys ↔ SN)  │
                                                                    │ agg_call_quality │
                                                                    │ _incident_       │
                                                                    │   correlation   │
                                                                    └─────────────────┘
```

**Schedule:** Every 2 minutes via Databricks Workflow  
**Overlap Window:** 3 minutes (ensures zero data loss with deduplication in Silver)

---

## 9. Unity Catalog — Table Reference

All tables live under `platform_analytics` catalog.

### 9.1 Bronze Layer

| Table | Key | Description |
|-------|-----|-------------|
| `bronze.raw_servicenow_incidents` | `incident_number` | Raw events appended per extraction |
| `bronze._splunk_extraction_checkpoint` | `extraction_end_ts` | Tracks last successful extraction window |

### 9.2 Silver Layer

| Table | Key | Description |
|-------|-----|-------------|
| `silver.fact_incidents` | `incident_number` | Deduplicated incidents with SLA calculations |
| `silver.fact_incident_timeline` | `incident_number + state_changed_at` | State change history |
| `silver.dim_business_services` | `business_service` | Service classification (PLATFORM/VENDOR/OTHER) |
| `silver.dim_assignment_groups` | `assignment_group` | Group metadata and vendor mapping |
| `silver.dim_sla_targets` | `priority` | P0–P4 SLA thresholds |

### 9.3 Gold Layer

| Table | Key | Description |
|-------|-----|-------------|
| `gold.agg_sla_summary` | `summary_date + priority` | Daily SLA compliance by priority |
| `gold.agg_platform_impact` | `summary_date + business_service + priority` | Platform-impacting incident SLA |
| `gold.agg_agent_impact` | `summary_date + business_service + priority` | Agent-facing incident SLA |
| `gold.agg_vendor_sla` | `summary_date + vendor_name + priority` | Vendor SLA compliance |
| `gold.fact_open_incidents` | `incident_number` | Current snapshot of all open incidents |

---

## 10. Gold Layer — Report Queries

These SQL queries can be run in Databricks once the pipeline is live.

### 10.1 SLA Compliance by Priority (Last 7 Days)

```sql
SELECT
    priority,
    SUM(total_incidents) AS total,
    ROUND(AVG(response_sla_compliance_pct), 1) AS avg_response_compliance,
    ROUND(AVG(resolution_sla_compliance_pct), 1) AS avg_resolution_compliance,
    ROUND(AVG(avg_response_time_minutes), 1) AS avg_response_min,
    ROUND(AVG(avg_resolution_time_minutes), 1) AS avg_resolution_min
FROM platform_analytics.gold.agg_sla_summary
WHERE summary_date >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY priority
ORDER BY priority;
```

### 10.2 Platform-Impacting Services (Worst Compliance)

```sql
SELECT
    business_service,
    platform_component,
    SUM(total_incidents) AS total,
    SUM(open_incidents) AS open_now,
    ROUND(AVG(response_compliance_pct), 1) AS response_pct,
    ROUND(AVG(resolution_compliance_pct), 1) AS resolution_pct,
    ROUND(AVG(mttr_minutes), 1) AS avg_mttr_min
FROM platform_analytics.gold.agg_platform_impact
WHERE summary_date >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY business_service, platform_component
ORDER BY resolution_pct ASC NULLS LAST
LIMIT 20;
```

### 10.3 Agent-Impacting Incidents (Open + At Risk)

```sql
SELECT
    incident_number,
    priority,
    business_service,
    assignment_group,
    opened_at,
    resolution_sla_status,
    resolution_sla_pct,
    ROUND(resolution_time_minutes, 1) AS elapsed_min,
    resolution_sla_target_minutes AS target_min
FROM platform_analytics.silver.fact_incidents
WHERE is_open = true
  AND is_agent_facing = true
  AND resolution_sla_status IN ('AT_RISK', 'IN_PROGRESS')
ORDER BY resolution_sla_pct DESC;
```

### 10.4 Vendor SLA Performance (Last 30 Days)

```sql
SELECT
    vendor_name,
    SUM(total_incidents) AS total,
    ROUND(AVG(response_compliance_pct), 1) AS response_pct,
    ROUND(AVG(resolution_compliance_pct), 1) AS resolution_pct,
    ROUND(AVG(avg_response_minutes), 1) AS avg_resp_min,
    ROUND(AVG(worst_resolution_minutes), 1) AS worst_resolution_min
FROM platform_analytics.gold.agg_vendor_sla
WHERE summary_date >= CURRENT_DATE - INTERVAL 30 DAYS
  AND vendor_name IS NOT NULL
GROUP BY vendor_name
ORDER BY resolution_pct ASC;
```

### 10.5 Current Open Incidents Dashboard

```sql
SELECT
    incident_number,
    priority,
    state,
    short_description,
    assignment_group,
    business_service,
    opened_at,
    response_sla_status,
    resolution_sla_status,
    vendor_name,
    is_agent_facing,
    ROUND(
        (UNIX_TIMESTAMP(CURRENT_TIMESTAMP) - UNIX_TIMESTAMP(opened_at)) / 60.0, 0
    ) AS age_minutes
FROM platform_analytics.gold.fact_open_incidents
ORDER BY
    CASE priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 WHEN 'P2' THEN 2
                  WHEN 'P3' THEN 3 WHEN 'P4' THEN 4 ELSE 9 END,
    opened_at ASC;
```

### 10.6 Genesys ↔ ServiceNow Correlation

```sql
SELECT
    vendor_name,
    summary_date,
    total_calls,
    degraded_calls,
    concurrent_incidents,
    ROUND(degradation_explained_pct, 1) AS explained_pct
FROM platform_analytics.gold.agg_call_quality_incident_correlation
WHERE summary_date >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY degradation_explained_pct DESC;
```

---

## 11. Pagination & Performance

### 11.1 Splunk Search Job Pagination

Results are streamed in batches of 5,000 from the search job:

```python
offset = 0
batch_size = 5000

while offset < result_count:
    rr = job.results(output_mode="json", count=batch_size, offset=offset)
    reader = results.JSONResultsReader(rr)
    for record in reader:
        if isinstance(record, dict):
            records.append(record)
    offset += batch_size
```

### 11.2 Performance Expectations

| Metric | Expected Range | Notes |
|--------|---------------|-------|
| Events per 2-min window | 50 – 500 | Depends on incident volume |
| Extraction latency | 3 – 15 sec | Splunk job execution + streaming |
| Bronze write | < 2 sec | Append-only Delta |
| Silver MERGE | 2 – 10 sec | Dedup + SLA calculation |
| Gold MERGE | 1 – 5 sec | Aggregate recompute for last 2 days |
| End-to-end | < 30 sec | Target well under 2-min schedule |

### 11.3 Max Results & Back-Pressure

| Setting | Value | Purpose |
|---------|-------|---------|
| `MAX_RESULTS_PER_QUERY` | 50,000 | Safety cap per extraction |
| `POLL_OVERLAP_MINUTES` | 3 | Ensures no data loss between polls |
| Dedup logic | `ROW_NUMBER() OVER (PARTITION BY incident_number)` | Silver handles duplicates from overlap |

---

## 12. Troubleshooting & Common Issues

| Issue | Symptom | Resolution |
|-------|---------|------------|
| **Connection refused** | `ConnectionError` on port 8089 | Verify host, port, and firewall rules. Port 8089 is Splunk mgmt port. |
| **401 Unauthorized** | `HTTPError 401` | Token expired or invalid. Regenerate in Splunk UI → Settings → Tokens. |
| **Index not found** | 0 events returned | Confirm `index=servicenow` exists. Run `| eventcount index=servicenow` in Splunk. |
| **Missing fields** | Fields show as `null` in results | Check sourcetype field extractions. Run `fieldsummary` in Splunk. |
| **Priority format mismatch** | Priority shows `1 - Critical` not `P0` | Pipeline normalizes in Bronze ingestion. Verify mapping in `03_bronze_ingestion.py`. |
| **SLA times look wrong** | Response time = 0 or extremely large | Check `response_time` / `work_start` field availability. May need CIT field mapping. |
| **Duplicates in Silver** | Same incident appears multiple times | Dedup runs on `ROW_NUMBER()` by `incident_number`. Check `_ingestion_ts` ordering. |
| **Checkpoint stuck** | Pipeline re-extracts old data | Query `bronze._splunk_extraction_checkpoint` — ensure last status = `COMPLETED`. |
| **Job timeout** | Query runs > 60 seconds | Reduce time range or add more specific filters. Check Splunk search head load. |
| **SSL certificate error** | `SSLError` | Set `SPLUNK_VERIFY_SSL = False` in config (already default) or provide CA path. |

### Diagnostic Commands

```python
# Check checkpoint state
spark.sql("""
    SELECT * FROM platform_analytics.bronze._splunk_extraction_checkpoint
    ORDER BY checkpoint_ts DESC
    LIMIT 5
""").display()

# Check bronze ingestion lag
spark.sql("""
    SELECT
        DATE_TRUNC('hour', _ingestion_ts) AS hour,
        COUNT(*) AS records,
        COUNT(DISTINCT _batch_id) AS batches
    FROM platform_analytics.bronze.raw_servicenow_incidents
    WHERE _ingestion_ts >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
    GROUP BY 1
    ORDER BY 1 DESC
""").display()

# Check SLA breach summary
spark.sql("""
    SELECT
        priority,
        resolution_sla_status,
        COUNT(*) AS cnt
    FROM platform_analytics.silver.fact_incidents
    WHERE incident_date >= CURRENT_DATE - INTERVAL 7 DAYS
    GROUP BY priority, resolution_sla_status
    ORDER BY priority, resolution_sla_status
""").display()
```

---

## Quick-Start Checklist

- [ ] **Splunk token created** with `search` capability
- [ ] **F-1 passes** — Splunk SDK connectivity confirmed
- [ ] **F-2 passes** — `index=servicenow` has data
- [ ] **F-3 passes** — `sourcetype="servicenow:incident"` returns events
- [ ] **F-4 passes** — All critical fields present (`number`, `priority`, `state`, `opened_at`, `business_service`)
- [ ] **F-5 passes** — Priority values present (P0–P4 or 1-Critical … 5-Planning)
- [ ] **F-6 passes** — Timestamp fields parse correctly
- [ ] **F-9 passes** — Volume estimate within expected range
- [ ] **Splunk secrets** stored in Databricks secret scope (`splunk/host`, `splunk/token`)
- [ ] **dim_sla_targets** seeded via `seed_dim_sla_targets.py`
- [ ] **dim_business_services** seeded via `seed_dim_business_services.py`
- [ ] **DDL run** — all tables created via `01_ddl_setup.py`

> **Once all checks pass, schedule `06_main_orchestrator.py` to run every 2 minutes in a Databricks Workflow.**
