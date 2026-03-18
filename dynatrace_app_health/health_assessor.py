# Databricks notebook source
# MAGIC %md
# MAGIC # Health Assessor
# MAGIC
# MAGIC For each monitored CMDB application, collects open problems and
# MAGIC key metrics across all matched entity types, then derives an
# MAGIC aggregate app-level health status.

# COMMAND ----------

from typing import List, Dict, Any
from config import AppHealthConfig as C
from dt_api_extensions import DynatraceAppClient

# COMMAND ----------

class HealthAssessor:
    """
    Assesses the health of each monitored application by fetching
    problems and metrics from Dynatrace for all matched entities.
    """

    def __init__(self, dt_client: DynatraceAppClient):
        self.dt = dt_client

    def assess_all(self, coverage_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Assess health for every app that has at least one entity.

        Args:
            coverage_results: output from CoverageScanner.scan_all()

        Returns:
            List of health result dicts (only for monitored apps).
        """
        monitored = [r for r in coverage_results if r["total_entities"] > 0]
        print(f"Assessing health for {len(monitored)} monitored apps "
              f"(skipping {len(coverage_results) - len(monitored)} NOT_MONITORED)")

        results: List[Dict[str, Any]] = []
        for idx, cov in enumerate(monitored, 1):
            app_label = cov.get("app_name") or cov.get("app_id")
            print(f"  [{idx}/{len(monitored)}] Health check: {app_label}")

            health = self._assess_app(cov)
            results.append(health)

        return results

    def _assess_app(self, cov: Dict[str, Any]) -> Dict[str, Any]:
        """
        Collect problems + metrics for one app and derive health status.
        """
        entities = cov.get("entities", {})

        # Collect all entity IDs across types
        all_ids = []
        ids_by_type: Dict[str, List[str]] = {}
        for etype, elist in entities.items():
            eids = [e["entityId"] for e in elist if "entityId" in e]
            ids_by_type[etype] = eids
            all_ids.extend(eids)

        # ── Problems ──
        problems = self.dt.get_problems_for_entities(all_ids) if all_ids else []

        # ── Metrics per entity type ──
        host_metrics = self.dt.get_host_metrics(ids_by_type.get("HOST", []))
        service_metrics = self.dt.get_service_metrics(ids_by_type.get("SERVICE", []))
        app_metrics = self.dt.get_application_metrics(ids_by_type.get("APPLICATION", []))

        # ── Aggregate to app level ──
        agg_host = self._aggregate_metrics(host_metrics, C.HOST_METRICS)
        agg_service = self._aggregate_metrics(service_metrics, C.SERVICE_METRICS)
        agg_app = self._aggregate_metrics(app_metrics, C.APPLICATION_METRICS)

        # Derive overall health
        health_status = self._derive_health_status(problems, agg_host, agg_service)

        return {
            "app_id": cov["app_id"],
            "app_service_id": cov["app_service_id"],
            "app_service_name": cov["app_service_name"],
            "app_name": cov["app_name"],
            "owner": cov["owner"],
            "criticality": cov["criticality"],
            "coverage_classification": cov["coverage_classification"],
            "matched_tag_key": cov["matched_tag_key"],
            "matched_tag_value": cov["matched_tag_value"],
            # Problems
            "open_problems_count": len(problems),
            "problem_severities": self._summarise_severities(problems),
            # Host metrics (averages across all hosts)
            "avg_cpu_usage": agg_host.get("cpu_usage"),
            "avg_memory_usage": agg_host.get("memory_usage"),
            "avg_disk_usage": agg_host.get("disk_usage"),
            "avg_host_availability": agg_host.get("host_availability"),
            # Service metrics (averages across all services)
            "avg_response_time": agg_service.get("response_time"),
            "total_error_count": self._sum_metric(service_metrics, "error_count"),
            "total_request_count": self._sum_metric(service_metrics, "request_count"),
            "avg_failure_rate": agg_service.get("failure_rate"),
            # Application metrics
            "total_action_count": self._sum_metric(app_metrics, "action_count"),
            "total_app_error_count": self._sum_metric(app_metrics, "error_count"),
            "avg_load_action_duration": agg_app.get("load_action_duration"),
            # Derived
            "health_status": health_status,
            # Entity counts
            "host_count": len(ids_by_type.get("HOST", [])),
            "service_count": len(ids_by_type.get("SERVICE", [])),
            "application_count": len(ids_by_type.get("APPLICATION", [])),
            "process_group_count": len(ids_by_type.get("PROCESS_GROUP", [])),
        }

    # ──────────────────────────────────────────────
    # Metric Aggregation Helpers
    # ──────────────────────────────────────────────

    @staticmethod
    def _aggregate_metrics(
        metrics_by_entity: Dict[str, Dict[str, Any]],
        metric_map: Dict[str, str],
    ) -> Dict[str, Any]:
        """
        Average each metric across all entities.
        """
        agg: Dict[str, Any] = {}
        for metric_name in metric_map:
            values = [
                m.get(metric_name)
                for m in metrics_by_entity.values()
                if m.get(metric_name) is not None
            ]
            if values:
                agg[metric_name] = round(sum(values) / len(values), 2)
            else:
                agg[metric_name] = None
        return agg

    @staticmethod
    def _sum_metric(
        metrics_by_entity: Dict[str, Dict[str, Any]],
        metric_name: str,
    ) -> Any:
        """
        Sum a metric across all entities (for counts / totals).
        """
        values = [
            m.get(metric_name)
            for m in metrics_by_entity.values()
            if m.get(metric_name) is not None
        ]
        return round(sum(values), 2) if values else None

    @staticmethod
    def _summarise_severities(problems: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Count problems by severity level.
        """
        counts: Dict[str, int] = {}
        for p in problems:
            sev = p.get("severityLevel", "UNKNOWN")
            counts[sev] = counts.get(sev, 0) + 1
        return counts

    @staticmethod
    def _derive_health_status(
        problems: List[Dict[str, Any]],
        agg_host: Dict[str, Any],
        agg_service: Dict[str, Any],
    ) -> str:
        """
        Derive a single health status from problems and key metrics.

        Rules:
            CRITICAL  — any AVAILABILITY or ERROR-level open problem
            WARNING   — any PERFORMANCE/RESOURCE/CUSTOM_ALERT problem
                        OR cpu > 90% OR memory > 90% OR failure_rate > 5%
            HEALTHY   — no problems and metrics within thresholds
        """
        if not problems and not agg_host and not agg_service:
            return "UNKNOWN"

        # Check problem severities
        for p in problems:
            sev = p.get("severityLevel", "")
            if sev in ("AVAILABILITY", "ERROR"):
                return "CRITICAL"

        has_perf_problem = any(
            p.get("severityLevel", "") in ("PERFORMANCE", "RESOURCE_CONTENTION", "CUSTOM_ALERT")
            for p in problems
        )

        cpu = agg_host.get("cpu_usage")
        mem = agg_host.get("memory_usage")
        fail = agg_service.get("failure_rate")

        if has_perf_problem:
            return "WARNING"
        if (cpu is not None and cpu > 90) or (mem is not None and mem > 90):
            return "WARNING"
        if fail is not None and fail > 5:
            return "WARNING"

        return "HEALTHY"
