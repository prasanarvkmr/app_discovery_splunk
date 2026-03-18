# Genesys Call Quality Pipeline

Medallion architecture pipeline on Azure Databricks + Unity Catalog.  
Ingests Genesys conversation data from ELK API every 10 minutes and produces call quality dashboards.

## Architecture

```
ELK API (Genesys Conversations)
     │  PIT + search_after pagination
     ▼
 Bronze: raw_genesys_conversations (append-only)
     │
     ▼
 Silver:
     ├── fact_conversation_participants  (grain: conversation + purpose + participant)
     ├── fact_conversations              (grain: conversation — overall quality)
     ├── fact_agent_legs                 (grain: conversation + agent — per-leg quality)
     ├── dim_agents                      (SCD Type 2)
     └── dim_locations                   (reference — continent/country/site)
     │
     ▼
 Gold:
     ├── agg_call_quality_summary        (daily summary: errored/degraded/total)
     ├── agg_vendor_location             (vendor × continent/country/site)
     ├── agg_agent_flagged               (agents > 10% errored or > 20% degraded)
     └── agg_agent_performance_detail    (per-agent with conversation drill-down)
```

## Files

| File | Purpose |
|---|---|
| `config.py` | Central configuration (catalog names, thresholds, ELK settings) |
| `01_ddl_setup.py` | Creates all tables and views — **run once** |
| `02_elk_extraction.py` | ELK API client with PIT + search_after pagination |
| `03_bronze_ingestion.py` | Flattens JSON and appends to bronze |
| `04_silver_transformations.py` | MERGE into silver facts + SCD2 dim_agents |
| `05_gold_aggregations.py` | MERGE into gold aggregates |
| `06_main_orchestrator.py` | Main entry point — runs the full pipeline |
| `seed_dim_locations.py` | One-time seed for location dimension |
| `workflow_config.json` | Databricks Workflow definition (10-min schedule) |

## Setup

1. **Configure secrets**: Add ELK API key to Databricks secrets  
   ```bash
   databricks secrets put --scope elk --key api_key
   ```

2. **Update config**: Edit `config.py` with your:
   - ELK base URL
   - Catalog name
   - MOS thresholds (if different from defaults)

3. **Run DDL**: Execute `01_ddl_setup.py` once to create all tables

4. **Seed locations**: Run `seed_dim_locations.py` with your actual site list

5. **Deploy workflow**: Import `workflow_config.json` or create via Databricks UI  
   Schedule: Every 10 minutes, `max_concurrent_runs: 1`

## Key Design Decisions

- **Call quality classification** happens once in Silver (`call_status`), consumed by all Gold tables
- **Per-leg agent attribution**: each agent's leg is evaluated independently (fair for transfers)
- **`overall_call_status`** = worst of agent + customer (drives summary/vendor reporting)
- **2-minute overlap window** on ELK extraction prevents data loss from indexing delays
- **Gold recomputes last 2 days** to catch late-arriving data
- **Page-level checkpoints** enable crash recovery during ELK pagination
