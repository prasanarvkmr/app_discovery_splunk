# Databricks notebook source
# MAGIC %md
# MAGIC # CMDB Loader
# MAGIC
# MAGIC Reads the application inventory from a Databricks table (CMDB)
# MAGIC and builds a lookup list for Dynatrace tag scanning.

# COMMAND ----------

from typing import List, Dict, Any
from config import AppHealthConfig as C

# COMMAND ----------

class CMDBLoader:
    """
    Loads the CMDB application inventory from a Databricks table
    and prepares tag lookup tuples for Dynatrace scanning.
    """

    def __init__(self, spark):
        self.spark = spark
        self.cols = C.CMDB_COLUMNS

    def load_apps(self) -> List[Dict[str, Any]]:
        """
        Read CMDB apps from Databricks, filter to PROD, return as list of dicts.

        Each dict contains the CMDB columns plus a `tag_lookups` list:
            [
                {"dt_tag": "AppId",          "value": "12345"},
                {"dt_tag": "AppServiceId",   "value": "APPSVC0001"},
                {"dt_tag": "AppServiceName", "value": "Trading Platform"},
            ]
        Only entries with non-null/non-empty values are included.
        """
        col_map = self.cols

        # Read and filter
        df = self.spark.table(C.CMDB_TABLE)

        # Filter to target environment if the column exists
        env_col = col_map.get("environment")
        if env_col and env_col in df.columns and C.CMDB_ENVIRONMENT_FILTER:
            df = df.filter(f"UPPER({env_col}) = '{C.CMDB_ENVIRONMENT_FILTER}'")

        rows = df.collect()
        print(f"CMDB: loaded {len(rows)} PROD applications from {C.CMDB_TABLE}")

        apps: List[Dict[str, Any]] = []
        for row in rows:
            row_dict = row.asDict()

            # Build tag lookups in precedence order
            tag_lookups = []
            for tag_cfg in C.TAG_KEYS:
                cmdb_col = tag_cfg["cmdb_col"]
                real_col = col_map.get(cmdb_col, cmdb_col)
                value = str(row_dict.get(real_col, "") or "").strip()
                if value:
                    tag_lookups.append({
                        "dt_tag": tag_cfg["dt_tag"],
                        "value": value,
                    })

            app_record = {
                "app_id": str(row_dict.get(col_map["app_id"], "") or "").strip(),
                "app_service_id": str(row_dict.get(col_map["app_service_id"], "") or "").strip(),
                "app_service_name": str(row_dict.get(col_map["app_service_name"], "") or "").strip(),
                "app_name": str(row_dict.get(col_map.get("app_name", "app_name"), "") or "").strip(),
                "owner": str(row_dict.get(col_map.get("owner", "owner"), "") or "").strip(),
                "criticality": str(row_dict.get(col_map.get("criticality", "criticality"), "") or "").strip(),
                "tag_lookups": tag_lookups,
            }
            apps.append(app_record)

        # Warn about apps with no usable tags
        no_tags = [a for a in apps if not a["tag_lookups"]]
        if no_tags:
            print(f"  WARNING: {len(no_tags)} apps have no AppId / AppServiceId / AppServiceName — cannot scan.")

        return apps
