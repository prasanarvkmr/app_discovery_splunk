# Databricks notebook source
# MAGIC %md
# MAGIC # Seed — dim_business_services
# MAGIC
# MAGIC Populates the business‑service dimension that classifies each ServiceNow
# MAGIC `business_service` value as **PLATFORM**, **VENDOR**, **AGENT**, or **INTERNAL**.
# MAGIC
# MAGIC Update the `SEED_DATA` list below to match your actual business‑service
# MAGIC catalogue, then run this notebook once (or after any changes).

# COMMAND ----------

from config import PipelineConfig as C

# COMMAND ----------

SEED_DATA = [
    # ── Platform Services ──
    ("Trading Platform Core",   "Trading Platform Core",   "PLATFORM", None,                     "Order Engine",         True,   "Platform Engineering", "P0"),
    ("Market Data Feed",        "Market Data Feed",        "PLATFORM", None,                     "Market Data",          True,   "Platform Engineering", "P0"),
    ("FIX Gateway",             "FIX Gateway",             "PLATFORM", None,                     "Connectivity",         True,   "Platform Engineering", "P1"),
    ("Risk Engine",             "Risk Engine",             "PLATFORM", None,                     "Risk & Compliance",    True,   "Platform Engineering", "P1"),
    ("Post-Trade Settlement",   "Post-Trade Settlement",   "PLATFORM", None,                     "Settlement",           False,  "Platform Engineering", "P2"),

    # ── Vendor Services ──
    ("Bloomberg Terminal",      "Bloomberg Terminal",      "VENDOR",   "Bloomberg",              "Market Data",          True,   "Vendor Management",  "P1"),
    ("Refinitiv Eikon",         "Refinitiv Eikon",         "VENDOR",   "Refinitiv",              "Market Data",          True,   "Vendor Management",  "P1"),
    ("CrowdStrike Falcon",      "CrowdStrike Falcon",      "VENDOR",   "CrowdStrike",            "Endpoint Security",    False,  "Security Ops",       "P2"),
    ("ServiceNow ITSM",         "ServiceNow ITSM",         "VENDOR",   "ServiceNow",             "IT Service Management",False,  "IT Operations",      "P2"),
    ("Genesys Cloud CX",        "Genesys Cloud CX",        "VENDOR",   "Genesys",                "Contact Centre",       True,   "Contact Centre Ops", "P1"),
    ("Cisco Webex Contact",     "Cisco Webex Contact",     "VENDOR",   "Cisco",                  "Contact Centre",       True,   "Contact Centre Ops", "P1"),

    # ── Agent‑Facing Services ──
    ("Desktop Environment",     "Agent Desktop Env",       "AGENT",    None,                     "Agent Desktop",        True,   "Desktop Engineering", "P2"),
    ("Softphone Client",        "Softphone Client",        "AGENT",    None,                     "Telephony",            True,   "Unified Comms",      "P2"),
    ("CRM Portal",              "CRM Portal",              "AGENT",    None,                     "CRM",                  True,   "CRM Team",           "P2"),
    ("Knowledge Base",          "Knowledge Base Portal",   "AGENT",    None,                     "Knowledge Mgmt",       True,   "Knowledge Ops",      "P3"),

    # ── Internal / Back‑Office Services ──
    ("Active Directory",        "Active Directory",        "INTERNAL", None,                     "Identity",             False,  "Identity & Access",  "P1"),
    ("Email Exchange",          "Email Exchange",          "INTERNAL", None,                     "Messaging",            False,  "Messaging Team",     "P2"),
    ("JIRA Project Mgmt",      "JIRA Project Mgmt",      "INTERNAL", None,                     "Dev Tooling",          False,  "Engineering Ops",    "P3"),
]

COLUMNS = [
    "business_service", "service_display_name", "service_type",
    "vendor_name", "platform_component", "is_agent_facing",
    "owner_team", "criticality",
]

# COMMAND ----------

df_seed = spark.createDataFrame(SEED_DATA, schema=COLUMNS)

# MERGE so re-running the notebook updates existing rows without duplicating
df_seed.createOrReplaceTempView("v_seed_business_services")

spark.sql(f"""
    MERGE INTO {C.SILVER_DIM_BUSINESS_SERVICES} AS tgt
    USING v_seed_business_services AS src
    ON tgt.business_service = src.business_service
    WHEN MATCHED THEN UPDATE SET
        tgt.service_display_name = src.service_display_name,
        tgt.service_type         = src.service_type,
        tgt.vendor_name          = src.vendor_name,
        tgt.platform_component   = src.platform_component,
        tgt.is_agent_facing      = src.is_agent_facing,
        tgt.owner_team           = src.owner_team,
        tgt.criticality          = src.criticality
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

display(spark.table(C.SILVER_DIM_BUSINESS_SERVICES))
print(f"dim_business_services seeded — {spark.table(C.SILVER_DIM_BUSINESS_SERVICES).count()} rows.")
