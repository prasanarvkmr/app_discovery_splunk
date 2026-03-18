# Dynatrace API — Feasibility & Sample Queries

> **Purpose:** Enable the team to independently validate each Dynatrace API v2 endpoint,
> confirm tag availability, verify metric IDs, and test pagination — before full pipeline integration.
>
> **Date:** 2026-03-18  
> **Base URL pattern:** `https://{your-env-id}.live.dynatrace.com` (SaaS) or `https://{your-activegate}/e/{env-id}` (Managed)

---

## Table of Contents

1. [Prerequisites & Token Scopes](#1-prerequisites--token-scopes)
2. [API Endpoints Used](#2-api-endpoints-used)
3. [Feasibility Test Plan](#3-feasibility-test-plan)
4. [Sample Queries — cURL](#4-sample-queries--curl)
5. [Sample Queries — Python](#5-sample-queries--python)
6. [Expected Response Structures](#6-expected-response-structures)
7. [Tag Validation Queries](#7-tag-validation-queries)
8. [Metrics Reference](#8-metrics-reference)
9. [Pagination & Rate Limits](#9-pagination--rate-limits)
10. [Coverage Classification Logic](#10-coverage-classification-logic)
11. [Health Derivation Logic](#11-health-derivation-logic)
12. [Troubleshooting & Common Issues](#12-troubleshooting--common-issues)

---

## 1. Prerequisites & Token Scopes

### API Token — Required Scopes

| Scope | Purpose |
|-------|---------|
| `entities.read` | Read entities (HOST, SERVICE, APPLICATION, PROCESS_GROUP) |
| `metrics.read` | Query metric data points (CPU, memory, response time, etc.) |
| `problems.read` | Read open and closed problems |
| `DataExport` | (Optional) If using metric export for large datasets |

### How to Create the Token

1. Dynatrace UI → **Settings** → **Integration** → **Dynatrace API** → **Generate token**
2. Select the scopes above
3. Copy and store securely (Databricks Secret Scope: `dynatrace/prod_api_token`)

### Connectivity Check

```bash
# Quick health check — should return 200
curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Api-Token {YOUR_TOKEN}" \
  "https://{ENV_URL}/api/v2/entities?entitySelector=type(\"HOST\")&pageSize=1"
```

---

## 2. API Endpoints Used

| # | Endpoint | Method | Purpose |
|---|----------|--------|---------|
| 1 | `/api/v2/entities` | GET | Find entities by type + tag selector (with pagination) |
| 2 | `/api/v2/entities/{entityId}` | GET | Get single entity with relationships |
| 3 | `/api/v2/metrics/query` | GET | Query time-series metric data |
| 4 | `/api/v2/problems` | GET | Fetch open problems for entity set |

**Base API Docs:** `https://{ENV_URL}/rest-api-doc/index.jsp`

---

## 3. Feasibility Test Plan

Run these tests in order. Each must pass before the pipeline can work.

| Test # | What to Validate | Pass Criteria | Endpoint |
|--------|-----------------|---------------|----------|
| **F-1** | API connectivity & auth | HTTP 200 returned | `/api/v2/entities` |
| **F-2** | Tags exist on entities | At least 1 entity returned for a known AppId tag | `/api/v2/entities` |
| **F-3** | Tag format matches CMDB | Tag key/value matches `AppId:12345` pattern | `/api/v2/entities` |
| **F-4** | Multi-entity-type support | Entities returned for HOST, SERVICE, APPLICATION | `/api/v2/entities` |
| **F-5** | Pagination works | `nextPageKey` returned when > 500 results | `/api/v2/entities` |
| **F-6** | Host metrics available | Non-null data for `builtin:host.cpu.usage` | `/api/v2/metrics/query` |
| **F-7** | Service metrics available | Non-null data for `builtin:service.response.time` | `/api/v2/metrics/query` |
| **F-8** | Application metrics available | Non-null data for `builtin:apps.web.actionCount` | `/api/v2/metrics/query` |
| **F-9** | Problems API works | Response with `problems` array | `/api/v2/problems` |
| **F-10** | Relationships readable | `fromRelationships` / `toRelationships` populated | `/api/v2/entities/{id}` |

---

## 4. Sample Queries — cURL

> Replace `{ENV_URL}` with your Dynatrace base URL and `{TOKEN}` with your API token.

### F-1: Connectivity Test

```bash
curl -X GET \
  "https://{ENV_URL}/api/v2/entities?entitySelector=type(%22HOST%22)&pageSize=1&fields=+properties,+tags" \
  -H "Authorization: Api-Token {TOKEN}" \
  -H "Content-Type: application/json"
```

### F-2: Find HOSTs by AppId Tag

```bash
curl -X GET \
  "https://{ENV_URL}/api/v2/entities?entitySelector=type(%22HOST%22),tag(%22AppId%22,%2212345%22)&fields=+properties,+tags&pageSize=500" \
  -H "Authorization: Api-Token {TOKEN}"
```

### F-3: Find SERVICEs by AppId Tag

```bash
curl -X GET \
  "https://{ENV_URL}/api/v2/entities?entitySelector=type(%22SERVICE%22),tag(%22AppId%22,%2212345%22)&fields=+properties,+tags&pageSize=500" \
  -H "Authorization: Api-Token {TOKEN}"
```

### F-4: Find APPLICATIONs by AppServiceId Tag

```bash
curl -X GET \
  "https://{ENV_URL}/api/v2/entities?entitySelector=type(%22APPLICATION%22),tag(%22AppServiceId%22,%22APPSVC0001%22)&fields=+properties,+tags&pageSize=500" \
  -H "Authorization: Api-Token {TOKEN}"
```

### F-5: Find PROCESS_GROUPs by AppServiceName Tag

```bash
curl -X GET \
  "https://{ENV_URL}/api/v2/entities?entitySelector=type(%22PROCESS_GROUP%22),tag(%22AppServiceName%22,%22Trading%20Platform%22)&fields=+properties,+tags&pageSize=500" \
  -H "Authorization: Api-Token {TOKEN}"
```

### F-6: Host CPU Metrics

```bash
curl -X GET \
  "https://{ENV_URL}/api/v2/metrics/query?metricSelector=builtin:host.cpu.usage:avg&entitySelector=type(%22HOST%22),tag(%22AppId%22,%2212345%22)&from=now-1h&resolution=1h" \
  -H "Authorization: Api-Token {TOKEN}"
```

### F-7: Service Response Time

```bash
curl -X GET \
  "https://{ENV_URL}/api/v2/metrics/query?metricSelector=builtin:service.response.time:avg&entitySelector=type(%22SERVICE%22),tag(%22AppId%22,%2212345%22)&from=now-1h&resolution=1h" \
  -H "Authorization: Api-Token {TOKEN}"
```

### F-8: Application Action Count

```bash
curl -X GET \
  "https://{ENV_URL}/api/v2/metrics/query?metricSelector=builtin:apps.web.actionCount:avg&entitySelector=type(%22APPLICATION%22),tag(%22AppId%22,%2212345%22)&from=now-1h&resolution=1h" \
  -H "Authorization: Api-Token {TOKEN}"
```

### F-9: Open Problems for Specific Entities

```bash
curl -X GET \
  "https://{ENV_URL}/api/v2/problems?from=now-24h&problemSelector=status(%22OPEN%22)&entitySelector=entityId(%22HOST-ABC123%22)" \
  -H "Authorization: Api-Token {TOKEN}"
```

### F-10: Entity Relationships

```bash
curl -X GET \
  "https://{ENV_URL}/api/v2/entities/HOST-ABC123?fields=+properties,+tags,+toRelationships,+fromRelationships" \
  -H "Authorization: Api-Token {TOKEN}"
```

---

## 5. Sample Queries — Python

### 5.1 Setup — Reusable Client

```python
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Configuration (replace with your values) ──
DT_URL   = "https://{your-env}.live.dynatrace.com"
DT_TOKEN = "{your-api-token}"

HEADERS = {
    "Authorization": f"Api-Token {DT_TOKEN}",
    "Content-Type": "application/json",
}

def dt_get(endpoint, params=None):
    """Helper: GET request to Dynatrace API v2."""
    url = f"{DT_URL}{endpoint}"
    resp = requests.get(url, headers=HEADERS, params=params, verify=False, timeout=60)
    resp.raise_for_status()
    return resp.json()
```

### 5.2 F-1 — Auth & Connectivity

```python
# Quick test: fetch 1 HOST entity
result = dt_get("/api/v2/entities", params={
    "entitySelector": 'type("HOST")',
    "pageSize": 1,
    "fields": "+tags",
})
print(f"Success! Total hosts: {result.get('totalCount', '?')}")
print(f"First host: {result['entities'][0]['displayName']}")
```

### 5.3 F-2 — Find Entities by Tag (single type)

```python
def find_entities_by_tag(entity_type, tag_key, tag_value):
    """Find all entities of a type that carry a specific tag."""
    selector = f'type("{entity_type}"),tag("{tag_key}","{tag_value}")'
    all_entities = []
    next_page = None

    while True:
        params = {
            "entitySelector": selector,
            "fields": "+properties,+tags",
            "pageSize": 500,
        }
        if next_page:
            params["nextPageKey"] = next_page

        data = dt_get("/api/v2/entities", params=params)
        all_entities.extend(data.get("entities", []))

        next_page = data.get("nextPageKey")
        if not next_page:
            break

    return all_entities

# Example: Find all HOSTs tagged with AppId=12345
hosts = find_entities_by_tag("HOST", "AppId", "12345")
print(f"Found {len(hosts)} hosts for AppId=12345")
for h in hosts:
    print(f"  {h['entityId']} — {h['displayName']}")
```

### 5.4 F-3 — Scan All Entity Types for One App (cascading tags)

```python
ENTITY_TYPES = ["HOST", "SERVICE", "APPLICATION", "PROCESS_GROUP"]

# Tag precedence: try AppId first, then AppServiceId, then AppServiceName
TAG_LOOKUPS = [
    ("AppId",          "12345"),
    ("AppServiceId",   "APPSVC0001"),
    ("AppServiceName", "Trading Platform"),
]

def scan_app_coverage(tag_lookups, entity_types=ENTITY_TYPES):
    """Cascading tag search across all entity types."""
    for tag_key, tag_value in tag_lookups:
        result = {}
        total = 0
        for etype in entity_types:
            entities = find_entities_by_tag(etype, tag_key, tag_value)
            result[etype] = entities
            total += len(entities)

        if total > 0:
            print(f"✓ Matched on tag {tag_key}={tag_value}")
            for etype, elist in result.items():
                print(f"  {etype}: {len(elist)} entities")
            return tag_key, tag_value, result

    print("✗ No entities found under any tag")
    return None, None, {}

tag_key, tag_value, entity_map = scan_app_coverage(TAG_LOOKUPS)
```

### 5.5 F-4 — Query Metrics for Entities

```python
def get_metrics_for_entities(metric_id, entity_type, entity_ids, time_from="now-1h"):
    """Fetch a single metric for a list of entity IDs."""
    if not entity_ids:
        return {}

    id_clauses = ",".join(f'entityId("{eid}")' for eid in entity_ids)
    entity_selector = f'type("{entity_type}"),({id_clauses})'

    data = dt_get("/api/v2/metrics/query", params={
        "metricSelector": f"{metric_id}:avg",
        "entitySelector": entity_selector,
        "from": time_from,
        "resolution": "1h",
    })

    # Parse: entity_id → latest value
    values = {}
    for res in data.get("result", []):
        for series in res.get("data", []):
            dims = series.get("dimensions", [])
            vals = series.get("values", [])
            if dims:
                eid = dims[0]
                # Take the latest non-null value
                latest = next((v for v in reversed(vals) if v is not None), None)
                values[eid] = round(latest, 2) if isinstance(latest, float) else latest
    return values


# Example: Host CPU for tagged hosts
host_ids = [h["entityId"] for h in hosts]

cpu = get_metrics_for_entities("builtin:host.cpu.usage", "HOST", host_ids)
mem = get_metrics_for_entities("builtin:host.mem.usage", "HOST", host_ids)

print("\nHost Metrics:")
for eid in host_ids:
    print(f"  {eid}: CPU={cpu.get(eid, '—')}%, Mem={mem.get(eid, '—')}%")
```

### 5.6 F-5 — Service Metrics

```python
# Assuming you found services from scan_app_coverage
services = entity_map.get("SERVICE", [])
svc_ids = [s["entityId"] for s in services]

resp_time    = get_metrics_for_entities("builtin:service.response.time",       "SERVICE", svc_ids)
error_count  = get_metrics_for_entities("builtin:service.errors.total.count",  "SERVICE", svc_ids)
request_count = get_metrics_for_entities("builtin:service.requestCount.total", "SERVICE", svc_ids)
failure_rate = get_metrics_for_entities("builtin:service.errors.total.rate",   "SERVICE", svc_ids)

print("\nService Metrics:")
for eid in svc_ids:
    print(f"  {eid}: RespTime={resp_time.get(eid, '—')}µs, "
          f"Errors={error_count.get(eid, '—')}, "
          f"Requests={request_count.get(eid, '—')}, "
          f"FailRate={failure_rate.get(eid, '—')}%")
```

### 5.7 F-6 — Application (RUM) Metrics

```python
apps = entity_map.get("APPLICATION", [])
app_ids = [a["entityId"] for a in apps]

action_count = get_metrics_for_entities("builtin:apps.web.actionCount",            "APPLICATION", app_ids)
app_errors   = get_metrics_for_entities("builtin:apps.web.errors.httpErrorsCount", "APPLICATION", app_ids)
load_dur     = get_metrics_for_entities("builtin:apps.web.action.duration",        "APPLICATION", app_ids)

print("\nApplication Metrics:")
for eid in app_ids:
    print(f"  {eid}: Actions={action_count.get(eid, '—')}, "
          f"HTTPErrors={app_errors.get(eid, '—')}, "
          f"LoadDuration={load_dur.get(eid, '—')}ms")
```

### 5.8 F-7 — Open Problems

```python
def get_open_problems(entity_ids, time_from="now-24h"):
    """Fetch open problems affecting any of the given entities."""
    if not entity_ids:
        return []

    id_clauses = ",".join(f'entityId("{eid}")' for eid in entity_ids)
    data = dt_get("/api/v2/problems", params={
        "from": time_from,
        "problemSelector": 'status("OPEN")',
        "entitySelector": f"({id_clauses})",
    })
    return data.get("problems", [])


# Collect ALL entity IDs across types
all_entity_ids = host_ids + svc_ids + app_ids
problems = get_open_problems(all_entity_ids)

print(f"\nOpen Problems: {len(problems)}")
for p in problems:
    print(f"  [{p.get('severityLevel')}] {p.get('title')} — {p.get('problemId')}")
```

### 5.9 F-8 — Entity Relationships

```python
def get_entity_relationships(entity_id):
    """Fetch entity with full relationship graph."""
    data = dt_get(f"/api/v2/entities/{entity_id}", params={
        "fields": "+properties,+tags,+toRelationships,+fromRelationships",
    })
    return data

# Example: inspect a host's relationships
if host_ids:
    entity = get_entity_relationships(host_ids[0])
    print(f"\nRelationships for {entity.get('displayName')}:")

    for direction in ["fromRelationships", "toRelationships"]:
        rels = entity.get(direction, {})
        for rel_type, entries in rels.items():
            ids = [e["id"] for e in entries if "id" in e]
            print(f"  {direction}.{rel_type}: {len(ids)} → {ids[:3]}")
```

### 5.10 Full Feasibility Test Script (all-in-one)

```python
"""
Complete feasibility test — run this in a Databricks notebook or standalone Python.
Replace the 3 variables below with your actual values.
"""
import requests, urllib3, json
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DT_URL   = "https://{your-env}.live.dynatrace.com"
DT_TOKEN = "{your-api-token}"
TEST_APP_ID = "12345"  # A known AppId tag value in your Dynatrace environment

HEADERS = {"Authorization": f"Api-Token {DT_TOKEN}", "Content-Type": "application/json"}

def dt_get(endpoint, params=None):
    resp = requests.get(f"{DT_URL}{endpoint}", headers=HEADERS, params=params, verify=False, timeout=60)
    resp.raise_for_status()
    return resp.json()

results = {}

# ── F-1: Connectivity ──
try:
    data = dt_get("/api/v2/entities", {"entitySelector": 'type("HOST")', "pageSize": 1})
    results["F-1 Connectivity"] = "PASS" if data.get("entities") else "WARN (0 hosts)"
except Exception as e:
    results["F-1 Connectivity"] = f"FAIL: {e}"

# ── F-2: Tags on HOSTs ──
try:
    data = dt_get("/api/v2/entities", {
        "entitySelector": f'type("HOST"),tag("AppId","{TEST_APP_ID}")',
        "fields": "+tags", "pageSize": 10,
    })
    count = len(data.get("entities", []))
    results["F-2 HOST by AppId tag"] = f"PASS ({count} hosts)" if count > 0 else "FAIL (0 hosts)"
except Exception as e:
    results["F-2 HOST by AppId tag"] = f"FAIL: {e}"

# ── F-3: Tags on SERVICEs ──
try:
    data = dt_get("/api/v2/entities", {
        "entitySelector": f'type("SERVICE"),tag("AppId","{TEST_APP_ID}")',
        "fields": "+tags", "pageSize": 10,
    })
    count = len(data.get("entities", []))
    results["F-3 SERVICE by AppId tag"] = f"PASS ({count} services)" if count > 0 else "WARN (0 services)"
except Exception as e:
    results["F-3 SERVICE by AppId tag"] = f"FAIL: {e}"

# ── F-4: Tags on APPLICATIONs ──
try:
    data = dt_get("/api/v2/entities", {
        "entitySelector": f'type("APPLICATION"),tag("AppId","{TEST_APP_ID}")',
        "fields": "+tags", "pageSize": 10,
    })
    count = len(data.get("entities", []))
    results["F-4 APPLICATION by AppId tag"] = f"PASS ({count})" if count > 0 else "WARN (0 — may not have RUM)"
except Exception as e:
    results["F-4 APPLICATION by AppId tag"] = f"FAIL: {e}"

# ── F-5: Pagination ──
try:
    data = dt_get("/api/v2/entities", {
        "entitySelector": 'type("HOST")', "pageSize": 2,
    })
    has_next = "nextPageKey" in data
    results["F-5 Pagination"] = f"PASS (nextPageKey present={has_next})"
except Exception as e:
    results["F-5 Pagination"] = f"FAIL: {e}"

# ── F-6: Host metrics ──
try:
    data = dt_get("/api/v2/metrics/query", {
        "metricSelector": "builtin:host.cpu.usage:avg",
        "entitySelector": f'type("HOST"),tag("AppId","{TEST_APP_ID}")',
        "from": "now-1h", "resolution": "1h",
    })
    has_data = any(
        series.get("values")
        for res in data.get("result", [])
        for series in res.get("data", [])
    )
    results["F-6 Host CPU metric"] = "PASS" if has_data else "WARN (no data points)"
except Exception as e:
    results["F-6 Host CPU metric"] = f"FAIL: {e}"

# ── F-7: Service metrics ──
try:
    data = dt_get("/api/v2/metrics/query", {
        "metricSelector": "builtin:service.response.time:avg",
        "entitySelector": f'type("SERVICE"),tag("AppId","{TEST_APP_ID}")',
        "from": "now-1h", "resolution": "1h",
    })
    has_data = any(
        series.get("values")
        for res in data.get("result", [])
        for series in res.get("data", [])
    )
    results["F-7 Service response time"] = "PASS" if has_data else "WARN (no data points)"
except Exception as e:
    results["F-7 Service response time"] = f"FAIL: {e}"

# ── F-8: Application metrics ──
try:
    data = dt_get("/api/v2/metrics/query", {
        "metricSelector": "builtin:apps.web.actionCount:avg",
        "entitySelector": f'type("APPLICATION"),tag("AppId","{TEST_APP_ID}")',
        "from": "now-1h", "resolution": "1h",
    })
    has_data = any(
        series.get("values")
        for res in data.get("result", [])
        for series in res.get("data", [])
    )
    results["F-8 Application action count"] = "PASS" if has_data else "WARN (no RUM data)"
except Exception as e:
    results["F-8 Application action count"] = f"FAIL: {e}"

# ── F-9: Problems ──
try:
    data = dt_get("/api/v2/problems", {
        "from": "now-24h",
        "problemSelector": 'status("OPEN")',
    })
    count = len(data.get("problems", []))
    results["F-9 Problems API"] = f"PASS ({count} open problems)"
except Exception as e:
    results["F-9 Problems API"] = f"FAIL: {e}"

# ── F-10: Relationships ──
try:
    # Get one HOST ID first
    data = dt_get("/api/v2/entities", {
        "entitySelector": 'type("HOST")', "pageSize": 1,
    })
    host_id = data["entities"][0]["entityId"]
    entity = dt_get(f"/api/v2/entities/{host_id}", {
        "fields": "+toRelationships,+fromRelationships",
    })
    has_rels = bool(entity.get("fromRelationships") or entity.get("toRelationships"))
    results["F-10 Relationships"] = "PASS" if has_rels else "WARN (no relationships on this host)"
except Exception as e:
    results["F-10 Relationships"] = f"FAIL: {e}"

# ── Print Report ──
print("\n" + "="*60)
print("  DYNATRACE API FEASIBILITY TEST RESULTS")
print("="*60)
for test, outcome in results.items():
    status = "✓" if outcome.startswith("PASS") else ("⚠" if outcome.startswith("WARN") else "✗")
    print(f"  {status}  {test}: {outcome}")
print("="*60)
```

---

## 6. Expected Response Structures

### 6.1 Entities Response (`/api/v2/entities`)

```json
{
  "totalCount": 42,
  "pageSize": 500,
  "nextPageKey": "AQMAAAAxAA...",     // null if last page
  "entities": [
    {
      "entityId": "HOST-1A2B3C4D5E6F",
      "displayName": "prod-web-server-01",
      "type": "HOST",
      "properties": {
        "osType": "LINUX",
        "monitoringMode": "FULL_STACK",
        "osVersion": "Ubuntu 22.04",
        "isMonitoringCandidate": false
      },
      "tags": [
        { "context": "CONTEXTLESS", "key": "AppId",        "value": "12345" },
        { "context": "CONTEXTLESS", "key": "AppServiceId", "value": "APPSVC0001" },
        { "context": "CONTEXTLESS", "key": "Environment",  "value": "PROD" }
      ]
    }
  ]
}
```

### 6.2 Metrics Response (`/api/v2/metrics/query`)

```json
{
  "totalCount": 1,
  "nextPageKey": null,
  "resolution": "1h",
  "result": [
    {
      "metricId": "builtin:host.cpu.usage:avg",
      "dataPointCountRatio": 0.012,
      "dimensionCountRatio": 0.001,
      "data": [
        {
          "dimensions": ["HOST-1A2B3C4D5E6F"],
          "dimensionMap": { "dt.entity.host": "HOST-1A2B3C4D5E6F" },
          "timestamps": [1742234400000],
          "values": [42.87]
        },
        {
          "dimensions": ["HOST-7G8H9I0J1K2L"],
          "dimensionMap": { "dt.entity.host": "HOST-7G8H9I0J1K2L" },
          "timestamps": [1742234400000],
          "values": [67.13]
        }
      ]
    }
  ]
}
```

### 6.3 Problems Response (`/api/v2/problems`)

```json
{
  "totalCount": 2,
  "pageSize": 50,
  "problems": [
    {
      "problemId": "P-2603180001",
      "displayId": "P-2603180001",
      "title": "High CPU detected on host prod-web-server-01",
      "impactLevel": "INFRASTRUCTURE",
      "severityLevel": "RESOURCE_CONTENTION",
      "status": "OPEN",
      "startTime": 1742220000000,
      "endTime": -1,
      "affectedEntities": [
        { "entityId": { "id": "HOST-1A2B3C4D5E6F", "type": "HOST" }, "name": "prod-web-server-01" }
      ],
      "rootCauseEntity": {
        "entityId": { "id": "HOST-1A2B3C4D5E6F", "type": "HOST" },
        "name": "prod-web-server-01"
      }
    }
  ]
}
```

### 6.4 Entity with Relationships (`/api/v2/entities/{id}`)

```json
{
  "entityId": "HOST-1A2B3C4D5E6F",
  "displayName": "prod-web-server-01",
  "type": "HOST",
  "properties": { "osType": "LINUX" },
  "tags": [
    { "context": "CONTEXTLESS", "key": "AppId", "value": "12345" }
  ],
  "fromRelationships": {
    "isNetworkClientOfHost": [
      { "id": "HOST-XXXXXX", "type": "HOST" }
    ]
  },
  "toRelationships": {
    "isProcessOf": [
      { "id": "PROCESS_GROUP_INSTANCE-AAAAAA", "type": "PROCESS_GROUP_INSTANCE" }
    ],
    "runsOn": [
      { "id": "SERVICE-BBBBBB", "type": "SERVICE" }
    ]
  }
}
```

---

## 7. Tag Validation Queries

Our pipeline uses three tag keys in cascading precedence. Validate that your Dynatrace environment has these tags applied.

### 7.1 List All Distinct Tag Keys

```bash
# Get a sample of entities and inspect their tags
curl -X GET \
  "https://{ENV_URL}/api/v2/entities?entitySelector=type(%22HOST%22)&fields=+tags&pageSize=100" \
  -H "Authorization: Api-Token {TOKEN}" | python -c "
import sys, json
data = json.load(sys.stdin)
tags = set()
for e in data.get('entities', []):
    for t in e.get('tags', []):
        tags.add(f\"{t['key']}={t.get('value', '*')}\")
for t in sorted(tags):
    print(t)
"
```

### 7.2 Count Entities per Tag Key

```python
# Run in Databricks notebook or standalone
for tag_key in ["AppId", "AppServiceId", "AppServiceName"]:
    for etype in ["HOST", "SERVICE", "APPLICATION", "PROCESS_GROUP"]:
        try:
            data = dt_get("/api/v2/entities", {
                "entitySelector": f'type("{etype}"),tag("{tag_key}")',
                "pageSize": 1,
            })
            total = data.get("totalCount", 0)
            print(f"  {etype:20s} tag={tag_key:20s} → {total} entities")
        except:
            print(f"  {etype:20s} tag={tag_key:20s} → ERROR")
```

### 7.3 Tag Format Reference

| Tag Key | CMDB Column | Example Value | Selector Syntax |
|---------|-------------|---------------|-----------------|
| `AppId` | `app_id` | `12345` | `tag("AppId","12345")` |
| `AppServiceId` | `app_service_id` | `APPSVC0001` | `tag("AppServiceId","APPSVC0001")` |
| `AppServiceName` | `app_service_name` | `Trading Platform` | `tag("AppServiceName","Trading Platform")` |

**Precedence:** AppId → AppServiceId → AppServiceName (stop at first hit)

---

## 8. Metrics Reference

### 8.1 Host Metrics

| Metric Name | Metric ID | Unit | Aggregation |
|-------------|-----------|------|-------------|
| CPU Usage | `builtin:host.cpu.usage` | % (0–100) | avg |
| Memory Usage | `builtin:host.mem.usage` | % (0–100) | avg |
| Disk Used | `builtin:host.disk.usedPct` | % (0–100) | avg |
| Host Availability | `builtin:host.availability` | % (0–100) | avg |

### 8.2 Service Metrics

| Metric Name | Metric ID | Unit | Aggregation |
|-------------|-----------|------|-------------|
| Response Time | `builtin:service.response.time` | µs | avg |
| Error Count | `builtin:service.errors.total.count` | count | sum |
| Request Count | `builtin:service.requestCount.total` | count | sum |
| Failure Rate | `builtin:service.errors.total.rate` | % (0–100) | avg |

### 8.3 Application (RUM) Metrics

| Metric Name | Metric ID | Unit | Aggregation |
|-------------|-----------|------|-------------|
| Action Count | `builtin:apps.web.actionCount` | count | sum |
| HTTP Error Count | `builtin:apps.web.errors.httpErrorsCount` | count | sum |
| Load Action Duration | `builtin:apps.web.action.duration` | ms | avg |

### 8.4 Metric Query Parameters

| Parameter | Value | Notes |
|-----------|-------|-------|
| `from` | `now-1h` | Relative time range |
| `resolution` | `1h` | Single data point per entity |
| `metricSelector` | `{metric_id}:avg` | Append `:avg`, `:sum`, `:max` etc. |
| `entitySelector` | `type("HOST"),(entityId("X"),entityId("Y"))` | Filter to specific entities |

---

## 9. Pagination & Rate Limits

### 9.1 Pagination

The Entities API returns max **500 entities per page** (configurable via `pageSize`).

```
Response includes:
  "nextPageKey": "AQMAAAAxAA..."    ← pass this in next request
  "totalCount": 1247                ← total matching entities
```

**Implementation pattern:**
```python
all_entities = []
next_page = None

while True:
    params = {"entitySelector": selector, "pageSize": 500, "fields": "+tags"}
    if next_page:
        params["nextPageKey"] = next_page

    data = dt_get("/api/v2/entities", params)
    all_entities.extend(data.get("entities", []))

    next_page = data.get("nextPageKey")
    if not next_page:
        break
    time.sleep(0.5)  # rate-limit courtesy delay
```

### 9.2 Rate Limits

| Limit Type | Threshold | Notes |
|------------|-----------|-------|
| Requests per minute | ~200 (varies by license) | Monitor HTTP 429 responses |
| Entities per page | 500 (max) | Default if not specified |
| Metrics data points | 15,000 per query | Split large entity sets into batches |
| Problems per page | 50 (default) | Supports `nextPageKey` pagination |

**Best practice:** Add a 0.5-second delay between consecutive API calls.

### 9.3 Handling 429 (Too Many Requests)

```python
import time

def dt_get_with_retry(endpoint, params=None, max_retries=3):
    for attempt in range(max_retries):
        resp = requests.get(f"{DT_URL}{endpoint}", headers=HEADERS, params=params, verify=False, timeout=60)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 5))
            print(f"Rate limited — waiting {wait}s (attempt {attempt+1}/{max_retries})")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise Exception("Max retries exceeded due to rate limiting")
```

---

## 10. Coverage Classification Logic

After scanning all entity types for an app, we classify monitoring coverage:

```
┌──────────────────┬────────────────────────────────────────────────────┐
│ Classification   │ Rule                                               │
├──────────────────┼────────────────────────────────────────────────────┤
│ FULL             │ At least HOST + SERVICE entities found             │
│ PARTIAL          │ Some entity types found, but missing HOST or SVC  │
│ HOSTS_ONLY       │ Only HOST entities found                          │
│ NOT_MONITORED    │ Zero entities across all types & all tag keys     │
└──────────────────┴────────────────────────────────────────────────────┘
```

**Cascading tag lookup:**
```
For each app:
  1. Try tag("AppId", "{app_id}")         across HOST, SERVICE, APPLICATION, PROCESS_GROUP
  2. If 0 results → try tag("AppServiceId", "{app_service_id}")
  3. If 0 results → try tag("AppServiceName", "{app_service_name}")
  4. If still 0   → mark NOT_MONITORED
```

---

## 11. Health Derivation Logic

For each monitored app, we derive a health status from problems and metrics:

```
┌───────────┬────────────────────────────────────────────────────────┐
│ Status    │ Condition                                              │
├───────────┼────────────────────────────────────────────────────────┤
│ CRITICAL  │ Any open problem with severity AVAILABILITY or ERROR  │
│ WARNING   │ Any PERFORMANCE / RESOURCE_CONTENTION / CUSTOM_ALERT  │
│           │ OR avg CPU > 90% OR avg Memory > 90%                  │
│           │ OR avg failure_rate > 5%                               │
│ HEALTHY   │ No problems and all metrics within thresholds         │
│ UNKNOWN   │ No problems and no metrics data returned              │
└───────────┴────────────────────────────────────────────────────────┘
```

**Problem severity levels (Dynatrace):**

| Severity | Meaning |
|----------|---------|
| `AVAILABILITY` | Entity or service is down/unreachable |
| `ERROR` | High error rate detected |
| `PERFORMANCE` | Degraded performance (response time, throughput) |
| `RESOURCE_CONTENTION` | CPU, memory, or disk saturation |
| `CUSTOM_ALERT` | User-defined threshold breached |

---

## 12. Troubleshooting & Common Issues

| Issue | Symptom | Resolution |
|-------|---------|------------|
| **401 Unauthorized** | HTTP 401 on any call | Verify API token is valid and has required scopes |
| **No entities for tag** | F-2/F-3 return 0 entities | Check tag key spelling (case-sensitive!), confirm tags exist in DT UI |
| **SSL errors** | `SSLError` / `CERTIFICATE_VERIFY_FAILED` | Set `verify=False` for testing, or point to internal CA bundle |
| **Empty metrics** | Metrics query returns no `data` | Entity may exist but have no OneAgent / no traffic in `from` window |
| **429 rate limit** | HTTP 429 after many calls | Add `time.sleep(0.5)` between calls, implement retry-after |
| **Tag on wrong context** | Tag shows in UI but selector fails | Ensure tag context is `CONTEXTLESS` — auto-tags use different context |
| **PROCESS_GROUP vs PROCESS_GROUP_INSTANCE** | 0 results for PROCESS_GROUP | Try `PROCESS_GROUP_INSTANCE` — some metrics sit at instance level |
| **Metric ID not found** | `metricSelector` error | Run `/api/v2/metrics?text=cpu` to search available metric IDs |

### Useful Diagnostic Queries

```bash
# List all available metric IDs matching "cpu"
curl "https://{ENV_URL}/api/v2/metrics?text=cpu&pageSize=50" \
  -H "Authorization: Api-Token {TOKEN}"

# List all entity types in the environment
curl "https://{ENV_URL}/api/v2/entityTypes?pageSize=100" \
  -H "Authorization: Api-Token {TOKEN}"

# Check token permissions
curl "https://{ENV_URL}/api/v2/apiTokens/lookup" \
  -X POST -H "Authorization: Api-Token {TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"token\": \"{TOKEN}\"}"
```

---

## Quick-Start Checklist

- [ ] **Token created** with scopes: `entities.read`, `metrics.read`, `problems.read`
- [ ] **F-1 passes** — API connectivity confirmed
- [ ] **F-2 passes** — At least one entity found by `AppId` tag
- [ ] **F-6 passes** — Host CPU metric returns data
- [ ] **F-7 passes** — Service response time returns data
- [ ] **F-9 passes** — Problems API accessible
- [ ] **Tag coverage audit** — Counted how many entities per tag key (Section 7.2)
- [ ] **CMDB column mapping** confirmed — `app_id`, `app_service_id`, `app_service_name` exist
- [ ] **Dynatrace secrets** stored in Databricks secret scope (`dynatrace/prod_url`, `dynatrace/prod_api_token`)

> **Once all checks pass, the pipeline is ready to run via `dynatrace_app_health/main.py`.**
