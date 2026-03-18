# Databricks notebook source
# MAGIC %md
# MAGIC # Coverage Scanner
# MAGIC
# MAGIC For each CMDB application, queries Dynatrace using cascading tag lookups
# MAGIC (AppId → AppServiceId → AppServiceName) and classifies monitoring coverage.

# COMMAND ----------

import time
from typing import List, Dict, Any, Optional
from config import AppHealthConfig as C
from dt_api_extensions import DynatraceAppClient

# COMMAND ----------

class CoverageScanner:
    """
    Scans Dynatrace for entities matching each CMDB application's tags.

    For each app:
      1. Try tag keys in precedence order until entities are found.
      2. Record matched entities per entity type.
      3. Classify coverage: FULL / PARTIAL / HOSTS_ONLY / NOT_MONITORED.
    """

    def __init__(self, dt_client: DynatraceAppClient):
        self.dt = dt_client

    def scan_all(self, apps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Scan coverage for every CMDB app.

        Args:
            apps: list from CMDBLoader.load_apps()

        Returns:
            List of coverage result dicts, one per app.
        """
        results: List[Dict[str, Any]] = []
        total = len(apps)

        for idx, app in enumerate(apps, 1):
            app_label = app.get("app_name") or app.get("app_id") or "unknown"
            print(f"[{idx}/{total}] Scanning: {app_label}")

            coverage = self._scan_app(app)
            results.append(coverage)

        # Summary
        by_class = {}
        for r in results:
            c = r["coverage_classification"]
            by_class[c] = by_class.get(c, 0) + 1
        print(f"\nCoverage summary: {by_class}")

        return results

    def _scan_app(self, app: Dict[str, Any]) -> Dict[str, Any]:
        """
        Scan a single app using cascading tag lookups.

        Returns a coverage dict:
            {
                "app_id": ..., "app_service_id": ..., "app_service_name": ...,
                "app_name": ..., "owner": ..., "criticality": ...,
                "matched_tag_key": "AppId",
                "matched_tag_value": "12345",
                "entities": { "HOST": [...], "SERVICE": [...], ... },
                "entity_counts": { "HOST": 3, "SERVICE": 5, ... },
                "total_entities": 8,
                "entity_types_found": {"HOST", "SERVICE"},
                "coverage_classification": "FULL",
            }
        """
        base = {
            "app_id": app.get("app_id", ""),
            "app_service_id": app.get("app_service_id", ""),
            "app_service_name": app.get("app_service_name", ""),
            "app_name": app.get("app_name", ""),
            "owner": app.get("owner", ""),
            "criticality": app.get("criticality", ""),
        }

        tag_lookups = app.get("tag_lookups", [])
        if not tag_lookups:
            return {
                **base,
                "matched_tag_key": None,
                "matched_tag_value": None,
                "entities": {},
                "entity_counts": {},
                "total_entities": 0,
                "entity_types_found": set(),
                "coverage_classification": "NOT_MONITORED",
            }

        # Cascading: try each tag key in order, stop at first hit
        for tag in tag_lookups:
            entities_map = self.dt.find_entities_for_app(
                tag_key=tag["dt_tag"],
                tag_value=tag["value"],
            )

            total = sum(len(v) for v in entities_map.values())
            if total > 0:
                entity_counts = {k: len(v) for k, v in entities_map.items()}
                types_found = {k for k, v in entities_map.items() if v}

                return {
                    **base,
                    "matched_tag_key": tag["dt_tag"],
                    "matched_tag_value": tag["value"],
                    "entities": entities_map,
                    "entity_counts": entity_counts,
                    "total_entities": total,
                    "entity_types_found": types_found,
                    "coverage_classification": self._classify(types_found),
                }

            time.sleep(C.API_DELAY_SECONDS)

        # No entities found under any tag
        return {
            **base,
            "matched_tag_key": tag_lookups[0]["dt_tag"],
            "matched_tag_value": tag_lookups[0]["value"],
            "entities": {},
            "entity_counts": {},
            "total_entities": 0,
            "entity_types_found": set(),
            "coverage_classification": "NOT_MONITORED",
        }

    @staticmethod
    def _classify(types_found: set) -> str:
        """
        Classify monitoring coverage based on which entity types were found.

        FULL          = at least HOST + SERVICE
        PARTIAL       = multiple types but missing one of HOST/SERVICE
        HOSTS_ONLY    = only HOST entities
        NOT_MONITORED = nothing (handled before this is called)
        """
        required = C.FULL_COVERAGE_REQUIRES  # {"HOST", "SERVICE"}

        if required.issubset(types_found):
            return "FULL"
        if types_found == {"HOST"}:
            return "HOSTS_ONLY"
        if types_found:
            return "PARTIAL"
        return "NOT_MONITORED"
