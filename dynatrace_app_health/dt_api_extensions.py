# Databricks notebook source
# MAGIC %md
# MAGIC # Dynatrace API Extensions
# MAGIC
# MAGIC Extends the base `DynatraceAPI` client from `dt_host.py` with:
# MAGIC - **Tag-based entity queries** (`tag("AppId","12345")`)
# MAGIC - **Full pagination** (`nextPageKey` cursor loop)
# MAGIC - **Multi-entity-type scanning** (HOST, SERVICE, APPLICATION, PROCESS_GROUP)
# MAGIC - **Service & Application metrics** (response time, error rate, etc.)

# COMMAND ----------

import sys, os, time
from typing import List, Dict, Any, Optional, Set
from config import AppHealthConfig as C

# Import the base DynatraceAPI from dt_host.py (parent directory)
sys.path.insert(0, os.path.join(os.path.dirname(os.getcwd())))
from dt_host import DynatraceAPI

# COMMAND ----------

class DynatraceAppClient(DynatraceAPI):
    """
    Extended Dynatrace client for tag-based app discovery and health assessment.
    Inherits auth, SSL, and HTTP plumbing from DynatraceAPI.
    """

    def __init__(self, base_url: str, api_token: str, ssl_config: dict = None):
        super().__init__(base_url, api_token, ssl_config)

    # ──────────────────────────────────────────────
    # Paginated Entity Queries
    # ──────────────────────────────────────────────

    def get_all_entities(
        self,
        entity_selector: str,
        fields: str = "+properties,+tags",
        page_size: int = C.PAGE_SIZE,
    ) -> List[Dict[str, Any]]:
        """
        Fetch ALL entities matching a selector, handling nextPageKey pagination.

        Returns:
            List of entity dicts (across all pages).
        """
        all_entities: List[Dict[str, Any]] = []
        next_page_key: Optional[str] = None

        while True:
            params = {
                "entitySelector": entity_selector,
                "fields": fields,
                "pageSize": page_size,
            }
            if next_page_key:
                params["nextPageKey"] = next_page_key

            result = self.get("/api/v2/entities", params=params)

            if not result.get("success"):
                print(f"  Entity query failed: {result.get('error', 'unknown')}")
                break

            data = result.get("data", {})
            entities = data.get("entities", [])
            all_entities.extend(entities)

            next_page_key = data.get("nextPageKey")
            if not next_page_key:
                break

            time.sleep(C.API_DELAY_SECONDS)

        return all_entities

    # ──────────────────────────────────────────────
    # Tag-Based Entity Lookup
    # ──────────────────────────────────────────────

    def get_entities_by_tag(
        self,
        tag_key: str,
        tag_value: str,
        entity_type: str,
    ) -> List[Dict[str, Any]]:
        """
        Find entities of a given type that carry a specific tag.

        Example selector built:
            type("SERVICE"),tag("AppId","12345")

        Args:
            tag_key:     Dynatrace tag key (e.g. "AppId")
            tag_value:   Tag value to match (e.g. "12345")
            entity_type: One of HOST, SERVICE, APPLICATION, PROCESS_GROUP

        Returns:
            List of matching entity dicts.
        """
        selector = f'type("{entity_type}"),tag("{tag_key}","{tag_value}")'
        return self.get_all_entities(selector)

    def find_entities_for_app(
        self,
        tag_key: str,
        tag_value: str,
        entity_types: List[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Scan all configured entity types for a single app tag.

        Returns:
            {
                "HOST": [entity, ...],
                "SERVICE": [entity, ...],
                "APPLICATION": [entity, ...],
                "PROCESS_GROUP": [entity, ...],
            }
        """
        if entity_types is None:
            entity_types = C.ENTITY_TYPES

        result: Dict[str, List[Dict[str, Any]]] = {}
        for etype in entity_types:
            entities = self.get_entities_by_tag(tag_key, tag_value, etype)
            result[etype] = entities
            time.sleep(C.API_DELAY_SECONDS)

        return result

    # ──────────────────────────────────────────────
    # Problems (across any entity type)
    # ──────────────────────────────────────────────

    def get_problems_for_entities(
        self,
        entity_ids: List[str],
        time_from: str = "now-24h",
    ) -> List[Dict[str, Any]]:
        """
        Fetch open problems that affect any of the supplied entity IDs.

        Uses the Problems API v2 `entitySelector` parameter.
        """
        if not entity_ids:
            return []

        # Build OR selector: entityId("X"),entityId("Y") ...
        id_clauses = ",".join(f'entityId("{eid}")' for eid in entity_ids)
        selector = f"({id_clauses})"

        result = self.get_problems(
            entity_selector=selector,
            time_from=time_from,
            problem_selector='status("OPEN")',
        )

        if result.get("success"):
            return result.get("data", {}).get("problems", [])
        return []

    # ──────────────────────────────────────────────
    # Metrics — Host / Service / Application
    # ──────────────────────────────────────────────

    def _fetch_metric_batch(
        self,
        metric_map: Dict[str, str],
        entity_ids: List[str],
        entity_type: str,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch a set of metrics for a list of entities of the same type.

        Returns:
            { entity_id: { metric_name: value, ... }, ... }
        """
        if not entity_ids:
            return {}

        id_clauses = ",".join(f'entityId("{eid}")' for eid in entity_ids)
        entity_selector = f'type("{entity_type}"),({id_clauses})'

        metrics_data: Dict[str, Dict[str, Any]] = {eid: {} for eid in entity_ids}

        for metric_name, metric_id in metric_map.items():
            result = self.get_metrics(
                metric_selector=f"{metric_id}:avg",
                entity_selector=entity_selector,
                time_from=C.METRICS_TIME_RANGE,
                resolution=C.METRICS_RESOLUTION,
            )

            if result.get("success"):
                for res in result.get("data", {}).get("result", []):
                    for series in res.get("data", []):
                        dims = series.get("dimensions", [])
                        if dims:
                            eid = dims[0]
                            values = series.get("values", [])
                            value = None
                            for v in reversed(values):
                                if v is not None:
                                    value = round(v, 2) if isinstance(v, float) else v
                                    break
                            if eid in metrics_data:
                                metrics_data[eid][metric_name] = value

            time.sleep(C.API_DELAY_SECONDS)

        return metrics_data

    def get_host_metrics(self, entity_ids: List[str]) -> Dict[str, Dict]:
        return self._fetch_metric_batch(C.HOST_METRICS, entity_ids, "HOST")

    def get_service_metrics(self, entity_ids: List[str]) -> Dict[str, Dict]:
        return self._fetch_metric_batch(C.SERVICE_METRICS, entity_ids, "SERVICE")

    def get_application_metrics(self, entity_ids: List[str]) -> Dict[str, Dict]:
        return self._fetch_metric_batch(C.APPLICATION_METRICS, entity_ids, "APPLICATION")

    # ──────────────────────────────────────────────
    # Entity Relationships
    # ──────────────────────────────────────────────

    def get_entity_with_relationships(self, entity_id: str) -> Dict[str, Any]:
        """
        Fetch a single entity including its relationship graph.
        """
        result = self.get(
            f"/api/v2/entities/{entity_id}",
            params={"fields": "+properties,+tags,+toRelationships,+fromRelationships"},
        )
        if result.get("success"):
            return result.get("data", {})
        return {}

    @staticmethod
    def extract_relationship_ids(
        entity: Dict[str, Any],
        direction: str = "from",
        rel_type: str = None,
    ) -> List[str]:
        """
        Extract related entity IDs from an entity's relationship map.

        Args:
            entity:    Entity dict (from get_entity_with_relationships)
            direction: "from" (this entity → others) or "to" (others → this entity)
            rel_type:  Optional filter, e.g. "isProcessOf", "runsOn", "calls"

        Returns:
            List of related entity IDs.
        """
        key = "fromRelationships" if direction == "from" else "toRelationships"
        rels = entity.get(key, {})
        ids: List[str] = []
        for rtype, entries in rels.items():
            if rel_type and rtype != rel_type:
                continue
            ids.extend(e.get("id", "") for e in entries if e.get("id"))
        return ids
