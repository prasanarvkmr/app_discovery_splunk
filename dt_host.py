import requests
from typing import Optional, List, Generator, Dict, Union, Any
from databricks import sql
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import csv
from datetime import datetime, timedelta
import urllib3

# Suppress InsecureRequestWarning if SSL verification is disabled
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# SSL Configuration Options:
# --------------------------
# Option 1: Custom CA certificate (recommended for self-signed certs)
#   ssl_config = {"verify": "/path/to/ca-bundle.crt"}
#
# Option 2: Client certificate authentication
#   ssl_config = {
#       "verify": "/path/to/ca-bundle.crt",  # or True for system CA
#       "cert": ("/path/to/client.crt", "/path/to/client.key")
#   }
#
# Option 3: Client cert with password-protected key
#   ssl_config = {
#       "verify": True,
#       "cert": "/path/to/client.pem"  # PEM file containing cert + key
#   }
#
# Option 4: Disable SSL verification (NOT recommended for production)
#   ssl_config = {"verify": False}


class DynatraceAPI:
    """Generic Dynatrace API client with SSL support."""
    
    def __init__(self, base_url: str, api_token: str, ssl_config: dict = None):
        """
        Initialize Dynatrace API client.
        
        Args:
            base_url: Dynatrace environment URL (e.g., https://your-env.live.dynatrace.com)
            api_token: Dynatrace API token
            ssl_config: SSL configuration dict:
                - verify: True/False or path to CA bundle
                - cert: Path to client cert or tuple (cert, key)
        """
        self.base_url = base_url.rstrip('/')
        self.api_token = api_token
        self.ssl_config = ssl_config or {}
        
        # SSL settings
        self.verify = self.ssl_config.get("verify", True)
        self.cert = self.ssl_config.get("cert")
        
        # Default headers
        self.headers = {
            "Authorization": f"Api-Token {api_token}",
            "Content-Type": "application/json"
        }
    
    def request(
        self, 
        method: str, 
        endpoint: str, 
        params: dict = None, 
        json_data: dict = None,
        timeout: int = 60
    ) -> Dict[str, Any]:
        """
        Make a generic API request to Dynatrace.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint (e.g., /api/v2/entities)
            params: Query parameters
            json_data: JSON body for POST/PUT requests
            timeout: Request timeout in seconds
        
        Returns:
            dict with success status and response data or error
        """
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.request(
                method=method.upper(),
                url=url,
                headers=self.headers,
                params=params,
                json=json_data,
                timeout=timeout,
                verify=self.verify,
                cert=self.cert
            )
            
            if response.status_code in (200, 201, 204):
                return {
                    "success": True,
                    "status_code": response.status_code,
                    "data": response.json() if response.text else {}
                }
            else:
                return {
                    "success": False,
                    "status_code": response.status_code,
                    "error": response.text
                }
                
        except requests.RequestException as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def get(self, endpoint: str, params: dict = None, timeout: int = 60) -> Dict[str, Any]:
        """Make a GET request."""
        return self.request("GET", endpoint, params=params, timeout=timeout)
    
    def post(self, endpoint: str, json_data: dict = None, params: dict = None, timeout: int = 60) -> Dict[str, Any]:
        """Make a POST request."""
        return self.request("POST", endpoint, params=params, json_data=json_data, timeout=timeout)
    
    def put(self, endpoint: str, json_data: dict = None, params: dict = None, timeout: int = 60) -> Dict[str, Any]:
        """Make a PUT request."""
        return self.request("PUT", endpoint, params=params, json_data=json_data, timeout=timeout)
    
    def delete(self, endpoint: str, params: dict = None, timeout: int = 60) -> Dict[str, Any]:
        """Make a DELETE request."""
        return self.request("DELETE", endpoint, params=params, timeout=timeout)
    
    # Convenience methods for common operations
    def get_entities(self, entity_selector: str, fields: str = "+properties", page_size: int = 500) -> Dict[str, Any]:
        """Get entities with selector."""
        return self.get("/api/v2/entities", params={
            "entitySelector": entity_selector,
            "fields": fields,
            "pageSize": page_size
        })
    
    def get_entity(self, entity_id: str, fields: str = "+properties") -> Dict[str, Any]:
        """Get single entity by ID."""
        return self.get(f"/api/v2/entities/{entity_id}", params={"fields": fields})
    
    def get_metrics(self, metric_selector: str, entity_selector: str = None, 
                    time_from: str = "now-1h", resolution: str = "1h") -> Dict[str, Any]:
        """Query metrics."""
        params = {
            "metricSelector": metric_selector,
            "from": time_from,
            "resolution": resolution
        }
        if entity_selector:
            params["entitySelector"] = entity_selector
        return self.get("/api/v2/metrics/query", params=params)
    
    def get_problems(self, entity_selector: str = None, time_from: str = "now-24h", 
                     problem_selector: str = 'status("OPEN")') -> Dict[str, Any]:
        """Get problems."""
        params = {
            "from": time_from,
            "problemSelector": problem_selector
        }
        if entity_selector:
            params["entitySelector"] = entity_selector
        return self.get("/api/v2/problems", params=params)


# Dynatrace metric IDs for host health
HOST_METRICS = {
    "cpu_usage": "builtin:host.cpu.usage",
    "cpu_system": "builtin:host.cpu.system",
    "cpu_user": "builtin:host.cpu.user",
    "cpu_idle": "builtin:host.cpu.idle",
    "memory_usage": "builtin:host.mem.usage",
    "memory_available": "builtin:host.mem.avail.pct",
    "disk_usage": "builtin:host.disk.usedPct",
    "disk_read_throughput": "builtin:host.disk.throughput.read",
    "disk_write_throughput": "builtin:host.disk.throughput.write",
    "network_in": "builtin:host.net.nic.trafficIn",
    "network_out": "builtin:host.net.nic.trafficOut",
    "host_availability": "builtin:host.availability",
}


def get_host_metrics(
    dt_client: DynatraceAPI,
    entity_ids: List[str],
    time_range: str = "now-1h"
) -> Dict[str, dict]:
    """
    Fetch health metrics for multiple hosts from Dynatrace Metrics API v2.
    
    Args:
        dt_client: DynatraceAPI client instance
        entity_ids: List of host entity IDs (HOST-XXXXX)
        time_range: Time range for metrics (default: last 1 hour)
    
    Returns:
        Dict mapping entity_id to metrics
    """
    # Build entity selector for multiple hosts
    entity_selector = ",".join([f'entityId("{eid}")' for eid in entity_ids])
    
    metrics_data = {eid: {} for eid in entity_ids}
    
    # Query each metric
    for metric_name, metric_id in HOST_METRICS.items():
        result = dt_client.get_metrics(
            metric_selector=f"{metric_id}:avg",
            entity_selector=f"type(HOST),({entity_selector})",
            time_from=time_range,
            resolution="1h"
        )
        
        if result.get("success"):
            data = result.get("data", {})
            for res in data.get("result", []):
                for series in res.get("data", []):
                    dimensions = series.get("dimensions", [])
                    if dimensions:
                        host_id = dimensions[0]
                        values = series.get("values", [])
                        # Get the latest non-null value
                        value = None
                        for v in reversed(values):
                            if v is not None:
                                value = round(v, 2) if isinstance(v, float) else v
                                break
                        if host_id in metrics_data:
                            metrics_data[host_id][metric_name] = value
        else:
            print(f"Warning: Failed to fetch {metric_name}: {result.get('error')}")
    
    return metrics_data


def get_host_health_state(
    dt_client: DynatraceAPI,
    entity_ids: List[str]
) -> Dict[str, dict]:
    """
    Get host health state and problems from Dynatrace.
    
    Args:
        dt_client: DynatraceAPI client instance
        entity_ids: List of host entity IDs
    
    Returns:
        Dict mapping entity_id to health state
    """
    health_data = {}
    
    for entity_id in entity_ids:
        result = dt_client.get_problems(
            entity_selector=f'entityId("{entity_id}")',
            time_from="now-24h",
            problem_selector='status("OPEN")'
        )
        
        if result.get("success"):
            problems = result.get("data", {}).get("problems", [])
            
            # Determine health state based on problem severity
            if not problems:
                health_state = "HEALTHY"
                severity = "NONE"
            else:
                severities = [p.get("severityLevel", "").upper() for p in problems]
                if "ERROR" in severities or "AVAILABILITY" in severities:
                    health_state = "CRITICAL"
                    severity = "ERROR"
                elif "PERFORMANCE" in severities or "RESOURCE_CONTENTION" in severities:
                    health_state = "WARNING"
                    severity = "PERFORMANCE"
                else:
                    health_state = "WARNING"
                    severity = severities[0] if severities else "UNKNOWN"
            
            health_data[entity_id] = {
                "health_state": health_state,
                "open_problems": len(problems),
                "highest_severity": severity,
                "problem_titles": "; ".join([p.get("title", "") for p in problems[:3]])
            }
        else:
            health_data[entity_id] = {
                "health_state": "UNKNOWN",
                "open_problems": 0,
                "highest_severity": "UNKNOWN",
                "problem_titles": result.get("error", "")
            }
    
    return health_data


def get_oneagent_info(entity: dict) -> dict:
    """
    Extract OneAgent installation status and version from host entity.
    
    Args:
        entity: Host entity dict from Dynatrace API
    
    Returns:
        dict with OneAgent details
    """
    properties = entity.get("properties", {})
    
    agent_version = properties.get("agentVersion", "")
    monitoring_mode = properties.get("monitoringMode", "")
    state = properties.get("state", "")
    
    # Determine if OneAgent is installed
    has_oneagent = bool(agent_version) or monitoring_mode in ("FULL_STACK", "INFRASTRUCTURE_ONLY")
    
    return {
        "oneagent_installed": has_oneagent,
        "oneagent_version": agent_version,
        "oneagent_monitoring_mode": monitoring_mode,
        "oneagent_state": state,
        "oneagent_running": state == "RUNNING",
        "oneagent_auto_update": properties.get("autoUpdate", ""),
        "oneagent_update_status": properties.get("updateStatus", ""),
    }


def check_oneagent_status(dt_client: 'DynatraceAPI', host_id: str) -> dict:
    """
    Check if OneAgent is installed on a specific host and get version details.
    
    Args:
        dt_client: DynatraceAPI client instance
        host_id: Dynatrace host entity ID (HOST-XXXXX)
    
    Returns:
        dict with OneAgent status and version info
    """
    result = dt_client.get_entity(host_id, fields="+properties")
    
    if result.get("success"):
        entity = result.get("data", {})
        return {
            "success": True,
            "host_id": host_id,
            "display_name": entity.get("displayName", ""),
            **get_oneagent_info(entity)
        }
    else:
        return {
            "success": False,
            "host_id": host_id,
            "error": result.get("error"),
            "status_code": result.get("status_code")
        }


def check_oneagent_status_batch(dt_client: 'DynatraceAPI', hostnames: List[str] = None, entity_ids: List[str] = None) -> List[dict]:
    """
    Check OneAgent status for multiple hosts.
    
    Args:
        dt_client: DynatraceAPI client instance
        hostnames: List of hostnames to check (optional)
        entity_ids: List of entity IDs to check (optional)
    
    Returns:
        List of dicts with OneAgent status for each host
    """
    results = []
    
    if entity_ids:
        # Direct lookup by entity IDs
        for entity_id in entity_ids:
            results.append(check_oneagent_status(dt_client, entity_id))
    elif hostnames:
        # Search by hostname first
        host_conditions = ",".join([f'entityName.equals("{h}")' for h in hostnames])
        entity_selector = f'type("HOST"),({host_conditions})'
        
        search_result = dt_client.get_entities(entity_selector, fields="+properties")
        
        if search_result.get("success"):
            entities = search_result.get("data", {}).get("entities", [])
            found_hosts = {e.get("displayName", "").lower(): e for e in entities}
            
            for hostname in hostnames:
                if hostname.lower() in found_hosts:
                    entity = found_hosts[hostname.lower()]
                    results.append({
                        "success": True,
                        "hostname": hostname,
                        "host_id": entity.get("entityId", ""),
                        "display_name": entity.get("displayName", ""),
                        "found_in_dynatrace": True,
                        **get_oneagent_info(entity)
                    })
                else:
                    results.append({
                        "success": True,
                        "hostname": hostname,
                        "found_in_dynatrace": False,
                        "oneagent_installed": False,
                        "oneagent_version": "",
                        "oneagent_state": ""
                    })
        else:
            for hostname in hostnames:
                results.append({
                    "success": False,
                    "hostname": hostname,
                    "error": search_result.get("error")
                })
    
    return results


def get_host_properties(entity: dict) -> dict:
    """Extract key properties from host entity."""
    properties = entity.get("properties", {})
    
    # Get OneAgent info (includes monitoring_mode, agent_version, state)
    oneagent_info = get_oneagent_info(entity)
    
    return {
        # System info
        "os_type": properties.get("osType", ""),
        "os_version": properties.get("osVersion", ""),
        "os_architecture": properties.get("osArchitecture", ""),
        "hypervisor_type": properties.get("hypervisorType", ""),
        # Cloud info
        "cloud_type": properties.get("cloudType", ""),
        "cloud_provider": properties.get("cloudProvider", ""),
        "aws_instance_type": properties.get("awsInstanceType", ""),
        "azure_vm_size": properties.get("azureVmSize", ""),
        # Hardware
        "cpu_cores": properties.get("cpuCores", ""),
        "physical_memory_gb": round(properties.get("physicalMemory", 0) / (1024**3), 2) if properties.get("physicalMemory") else "",
        "ip_addresses": "; ".join(properties.get("ipAddress", [])) if properties.get("ipAddress") else "",
        # OneAgent details (no duplicates)
        **oneagent_info
    }


def check_hosts_batch_with_metrics(
    dt_client: DynatraceAPI,
    hostnames: List[str]
) -> dict:
    """
    Check multiple hosts and fetch their metrics.
    
    Args:
        dt_client: DynatraceAPI client instance
        hostnames: List of hostnames to check
    
    Returns:
        dict with hosts list (each has found_in_dynatrace flag)
    """
    # Build entity selector for multiple hosts
    host_conditions = ",".join([f'entityName.equals("{h}")' for h in hostnames])
    entity_selector = f'type("HOST"),({host_conditions})'
    
    result = dt_client.get_entities(entity_selector, fields="+properties")
    
    if result.get("success"):
        entities = result.get("data", {}).get("entities", [])
        found_hosts_map = {e.get("displayName", "").lower(): e for e in entities}
        
        hosts = []
        
        # Get entity IDs for metrics query
        entity_ids = [e.get("entityId") for e in entities if e.get("entityId")]
        
        # Fetch metrics for all found hosts
        metrics_data = {}
        health_data = {}
        
        if entity_ids:
            print(f"    Fetching metrics for {len(entity_ids)} hosts...")
            metrics_data = get_host_metrics(dt_client, entity_ids)
            health_data = get_host_health_state(dt_client, entity_ids)
        
        for hostname in hostnames:
            if hostname.lower() in found_hosts_map:
                entity = found_hosts_map[hostname.lower()]
                entity_id = entity.get("entityId", "")
                
                # Combine all data for found host
                hosts.append({
                    "hostname": hostname,
                    "entity_id": entity_id,
                    "display_name": entity.get("displayName", ""),
                    "found_in_dynatrace": True,
                    **get_host_properties(entity),
                    **metrics_data.get(entity_id, {}),
                    **health_data.get(entity_id, {})
                })
            else:
                # Not found host
                hosts.append({
                    "hostname": hostname,
                    "found_in_dynatrace": False
                })
        
        return {"hosts": hosts}
    else:
        return {"error": result.get("error"), "status_code": result.get("status_code")}


def read_hosts_from_databricks(
    server_hostname: str,
    http_path: str,
    access_token: str,
    table_name: str,
    host_column: str = "hostname",
    chunk_size: int = 100
) -> Generator[List[str], None, None]:
    """
    Read host list from Databricks table in chunks.
    
    Args:
        server_hostname: Databricks workspace hostname (e.g., adb-xxx.azuredatabricks.net)
        http_path: SQL warehouse HTTP path
        access_token: Databricks personal access token
        table_name: Full table name (catalog.schema.table)
        host_column: Column name containing hostnames
        chunk_size: Number of hosts per chunk
    
    Yields:
        List of hostnames in chunks
    """
    with sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    ) as connection:
        with connection.cursor() as cursor:
            # Get total count first
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_count = cursor.fetchone()[0]
            print(f"Total hosts in table: {total_count}")
            
            # Fetch in chunks using OFFSET/LIMIT
            offset = 0
            while offset < total_count:
                cursor.execute(
                    f"SELECT {host_column} FROM {table_name} LIMIT {chunk_size} OFFSET {offset}"
                )
                rows = cursor.fetchall()
                hosts = [row[0] for row in rows if row[0]]
                
                # Remove duplicates within chunk (preserve order)
                hosts = list(dict.fromkeys(hosts))
                
                if hosts:
                    yield hosts
                
                offset += chunk_size
                print(f"Processed {min(offset, total_count)}/{total_count} hosts")


def read_hosts_from_databricks_df(
    server_hostname: str,
    http_path: str,
    access_token: str,
    table_name: str,
    host_column: str = "hostname"
) -> pd.DataFrame:
    """
    Read all hosts from Databricks table into a DataFrame.
    
    Returns:
        DataFrame with all hosts
    """
    with sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    ) as connection:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, connection)
        return df


def chunk_list(lst: List, chunk_size: int) -> Generator[List, None, None]:
    """Split a list into chunks of specified size."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def process_hosts_from_databricks(
    databricks_config: dict,
    dynatrace_config: dict,
    chunk_size: int = 50,
    output_csv: str = None,
    ssl_config: dict = None
) -> dict:
    """
    Main function to read hosts from Databricks, check in Dynatrace, and export to CSV.
    
    Args:
        databricks_config: Dict with server_hostname, http_path, access_token, table_name, host_column
        dynatrace_config: Dict with url, api_token
        chunk_size: Number of hosts to process per batch
        output_csv: Path to output CSV file (optional, will also return results)
        ssl_config: SSL configuration dict (verify, cert) for Dynatrace API
    
    Returns:
        dict with hosts list and summary counts
    """
    all_hosts = []
    errors = []
    seen_hostnames = set()  # Track processed hostnames to avoid duplicates
    
    # Create Dynatrace API client with SSL config
    dt_client = DynatraceAPI(
        base_url=dynatrace_config["url"],
        api_token=dynatrace_config["api_token"],
        ssl_config=ssl_config
    )
    
    print("Reading hosts from Databricks...")
    
    # Read hosts in chunks from Databricks
    for host_chunk in read_hosts_from_databricks(
        server_hostname=databricks_config["server_hostname"],
        http_path=databricks_config["http_path"],
        access_token=databricks_config["access_token"],
        table_name=databricks_config["table_name"],
        host_column=databricks_config.get("host_column", "hostname"),
        chunk_size=chunk_size
    ):
        # Dedupe: only process hostnames not seen before
        unique_chunk = [h for h in host_chunk if h.lower() not in seen_hostnames]
        seen_hostnames.update(h.lower() for h in unique_chunk)
        
        if not unique_chunk:
            print(f"  Skipping batch - all {len(host_chunk)} hosts already processed")
            continue
        
        print(f"Checking batch of {len(unique_chunk)} hosts in Dynatrace...")
        
        # Check this chunk in Dynatrace with metrics
        result = check_hosts_batch_with_metrics(
            dt_client=dt_client,
            hostnames=unique_chunk
        )
        
        if "error" in result:
            errors.append(result)
        else:
            all_hosts.extend(result.get("hosts", []))
        
        # Rate limiting - avoid hitting API too hard
        time.sleep(1)
    
    # Calculate counts from single hosts array
    total_found = sum(1 for h in all_hosts if h.get("found_in_dynatrace"))
    total_not_found = len(all_hosts) - total_found
    
    results = {
        "hosts": all_hosts,
        "total_found": total_found,
        "total_not_found": total_not_found,
        "errors": errors
    }
    
    # Export to CSV if path provided
    if output_csv:
        export_results_to_csv(results, output_csv)
    
    return results


def export_results_to_csv(results: dict, output_prefix: str = "dynatrace_hosts"):
    """
    Export results to CSV files.
    
    Args:
        results: Results dict from process_hosts_from_databricks
        output_prefix: Prefix for output files
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Define CSV columns - all hosts in one file (no duplicates)
    columns = [
        # Identity
        "hostname",
        "found_in_dynatrace",
        "entity_id",
        "display_name",
        # Health
        "health_state",
        "open_problems",
        "highest_severity",
        "problem_titles",
        # OneAgent status
        "oneagent_installed",
        "oneagent_running",
        "oneagent_version",
        "oneagent_monitoring_mode",
        "oneagent_state",
        "oneagent_auto_update",
        "oneagent_update_status",
        # Resource metrics
        "cpu_usage",
        "memory_usage",
        "disk_usage",
        "host_availability",
        "cpu_system",
        "cpu_user",
        "cpu_idle",
        "memory_available",
        "disk_read_throughput",
        "disk_write_throughput",
        "network_in",
        "network_out",
        # System info
        "os_type",
        "os_version",
        "os_architecture",
        "cloud_type",
        "cloud_provider",
        "aws_instance_type",
        "azure_vm_size",
        "cpu_cores",
        "physical_memory_gb",
        "ip_addresses",
    ]
    
    # Get hosts and sort by hostname
    hosts = results.get("hosts", [])
    hosts.sort(key=lambda x: x.get("hostname", "").lower())
    
    # Export all hosts to single CSV
    hosts_file = f"{output_prefix}_{timestamp}.csv"
    with open(hosts_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        for host in hosts:
            writer.writerow(host)
    print(f"All hosts exported to: {hosts_file}")
    
    # Export summary
    summary_file = f"{output_prefix}_summary_{timestamp}.csv"
    with open(summary_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Metric", "Value"])
        writer.writerow(["Total Hosts Checked", len(hosts)])
        writer.writerow(["Hosts Found in Dynatrace", results.get("total_found", 0)])
        writer.writerow(["Hosts Not Found", results.get("total_not_found", 0)])
        writer.writerow(["Errors", len(results.get("errors", []))])
        writer.writerow(["Timestamp", timestamp])
        
        # Health summary (only for hosts found in Dynatrace)
        found_hosts = [h for h in hosts if h.get("found_in_dynatrace")]
        if found_hosts:
            healthy = sum(1 for h in found_hosts if h.get("health_state") == "HEALTHY")
            warning = sum(1 for h in found_hosts if h.get("health_state") == "WARNING")
            critical = sum(1 for h in found_hosts if h.get("health_state") == "CRITICAL")
            writer.writerow([""])
            writer.writerow(["Health Summary", ""])
            writer.writerow(["Healthy Hosts", healthy])
            writer.writerow(["Warning Hosts", warning])
            writer.writerow(["Critical Hosts", critical])
            
            # OneAgent summary
            with_agent = sum(1 for h in found_hosts if h.get("oneagent_installed"))
            without_agent = sum(1 for h in found_hosts if not h.get("oneagent_installed"))
            agent_running = sum(1 for h in found_hosts if h.get("oneagent_running"))
            agent_not_running = sum(1 for h in found_hosts if h.get("oneagent_installed") and not h.get("oneagent_running"))
            
            # Get unique versions
            versions = {}
            for h in found_hosts:
                ver = h.get("oneagent_version", "")
                if ver:
                    versions[ver] = versions.get(ver, 0) + 1
            
            writer.writerow([""])
            writer.writerow(["OneAgent Summary", ""])
            writer.writerow(["Hosts with OneAgent", with_agent])
            writer.writerow(["Hosts without OneAgent", without_agent])
            writer.writerow(["OneAgent Running", agent_running])
            writer.writerow(["OneAgent Not Running", agent_not_running])
            writer.writerow([""])
            writer.writerow(["OneAgent Version Distribution", ""])
            for ver, count in sorted(versions.items(), key=lambda x: -x[1]):
                writer.writerow([f"  {ver}", count])
            
            # Average metrics
            cpu_values = [h.get("cpu_usage") for h in found_hosts if h.get("cpu_usage") is not None]
            mem_values = [h.get("memory_usage") for h in found_hosts if h.get("memory_usage") is not None]
            disk_values = [h.get("disk_usage") for h in found_hosts if h.get("disk_usage") is not None]
            
            writer.writerow([""])
            writer.writerow(["Average Metrics", ""])
            if cpu_values:
                writer.writerow(["Avg CPU Usage (%)", round(sum(cpu_values) / len(cpu_values), 2)])
            if mem_values:
                writer.writerow(["Avg Memory Usage (%)", round(sum(mem_values) / len(mem_values), 2)])
            if disk_values:
                writer.writerow(["Avg Disk Usage (%)", round(sum(disk_values) / len(disk_values), 2)])
    
    print(f"Summary exported to: {summary_file}")
    
    return {
        "hosts_file": hosts_file,
        "summary_file": summary_file
    }


def check_host_in_dynatrace(
    dt_client: DynatraceAPI,
    host_name: Optional[str] = None,
    host_id: Optional[str] = None
) -> dict:
    """
    Check if host details are available in Dynatrace API v2.
    
    Args:
        dt_client: DynatraceAPI client instance
        host_name: Host name to search for (optional)
        host_id: Dynatrace entity ID (e.g., HOST-XXXXXXXXXX) (optional)
    
    Returns:
        dict with status and host details
    """
    # If specific host ID is provided, fetch directly
    if host_id:
        result = dt_client.get_entity(host_id)
        if result.get("success"):
            return {"found": True, "host": result.get("data")}
        elif result.get("status_code") == 404:
            return {"found": False, "message": f"Host {host_id} not found"}
        else:
            return {"found": False, "error": result.get("error"), "status_code": result.get("status_code")}
    
    # Search by host name using entity selector
    entity_selector = 'type("HOST")'
    if host_name:
        entity_selector = f'type("HOST"),entityName.contains("{host_name}")'
    
    result = dt_client.get_entities(entity_selector, fields="+properties,+toRelationships,+fromRelationships")
    
    if result.get("success"):
        entities = result.get("data", {}).get("entities", [])
        if entities:
            return {
                "found": True,
                "count": len(entities),
                "hosts": entities
            }
        else:
            return {"found": False, "message": "No hosts found matching criteria"}
    else:
        return {"found": False, "error": result.get("error"), "status_code": result.get("status_code")}


def get_host_details(dt_client: DynatraceAPI, host_id: str) -> dict:
    """
    Get detailed information about a specific host.
    
    Args:
        dt_client: DynatraceAPI client instance
        host_id: Dynatrace host entity ID
    
    Returns:
        dict with host details
    """
    result = dt_client.get(
        f"/api/v2/entities/{host_id}",
        params={"fields": "+properties,+toRelationships,+fromRelationships"}
    )
    
    if result.get("success"):
        return {"success": True, "data": result.get("data")}
    else:
        return {"success": False, "error": result.get("error"), "status_code": result.get("status_code")}


if __name__ == "__main__":
    # ===========================================
    # CONFIGURATION - Replace with your values
    # ===========================================
    
    # Databricks Configuration
    DATABRICKS_CONFIG = {
        "server_hostname": "adb-xxxxxxxxxxxx.azuredatabricks.net",  # Your Databricks workspace hostname
        "http_path": "/sql/1.0/warehouses/xxxxxxxxxx",               # SQL warehouse HTTP path
        "access_token": "dapiXXXXXXXXXXXXXXXXXX",                     # Personal access token
        "table_name": "catalog.schema.hosts_table",                   # Full table name
        "host_column": "hostname"                                     # Column containing hostnames
    }
    
    # Dynatrace Configuration
    DYNATRACE_CONFIG = {
        "url": "https://your-environment.live.dynatrace.com",
        "api_token": "dt0c01.XXXXXXXX.YYYYYYYY"  # Token with entities.read and metrics.read scope
    }
    
    # ===========================================
    # SSL CERTIFICATE CONFIGURATION
    # ===========================================
    # Choose ONE of the following options:
    
    # Option 1: Use system CA certificates (default - no SSL config needed)
    SSL_CONFIG = None
    
    # Option 2: Custom CA certificate (for self-signed or internal CA)
    # SSL_CONFIG = {
    #     "verify": "/path/to/ca-bundle.crt"  # Path to CA certificate bundle
    # }
    
    # Option 3: Client certificate authentication (mTLS)
    # SSL_CONFIG = {
    #     "verify": "/path/to/ca-bundle.crt",  # CA to verify server, or True for system CA
    #     "cert": ("/path/to/client.crt", "/path/to/client.key")  # Client cert and key
    # }
    
    # Option 4: Client certificate with combined PEM file
    # SSL_CONFIG = {
    #     "verify": True,
    #     "cert": "/path/to/client.pem"  # PEM file containing both cert and key
    # }
    
    # Option 5: Disable SSL verification (NOT RECOMMENDED for production!)
    # SSL_CONFIG = {"verify": False}
    
    # Processing Configuration
    CHUNK_SIZE = 50  # Number of hosts per batch
    OUTPUT_PREFIX = "output/dynatrace_hosts"  # CSV output prefix
    
    # ===========================================
    # Process hosts and export to CSV
    # ===========================================
    print("=" * 60)
    print("Processing hosts from Databricks table")
    print("Fetching health metrics from Dynatrace")
    print("=" * 60)
    
    results = process_hosts_from_databricks(
        databricks_config=DATABRICKS_CONFIG,
        dynatrace_config=DYNATRACE_CONFIG,
        chunk_size=CHUNK_SIZE,
        output_csv=OUTPUT_PREFIX,
        ssl_config=SSL_CONFIG
    )
    
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY")
    print("=" * 60)
    print(f"Hosts found in Dynatrace: {results['total_found']}")
    print(f"Hosts NOT found in Dynatrace: {results['total_not_found']}")
    
    if results['errors']:
        print(f"Errors encountered: {len(results['errors'])}")
    
    # Health summary
    if results['found_hosts']:
        healthy = sum(1 for h in results['found_hosts'] if h.get("health_state") == "HEALTHY")
        warning = sum(1 for h in results['found_hosts'] if h.get("health_state") == "WARNING")
        critical = sum(1 for h in results['found_hosts'] if h.get("health_state") == "CRITICAL")
        
        print(f"\nHealth Status:")
        print(f"  Healthy:  {healthy}")
        print(f"  Warning:  {warning}")
        print(f"  Critical: {critical}")
        
        # Show sample of found hosts with metrics
        print("\nSample of found hosts with metrics:")
        for host in results['found_hosts'][:5]:
            print(f"  - {host['hostname']}")
            print(f"      Health: {host.get('health_state', 'N/A')}")
            print(f"      CPU: {host.get('cpu_usage', 'N/A')}%")
            print(f"      Memory: {host.get('memory_usage', 'N/A')}%")
            print(f"      Disk: {host.get('disk_usage', 'N/A')}%")
            print(f"      Problems: {host.get('open_problems', 0)}")
    
    # Show not found hosts
    if results['not_found_hosts']:
        print(f"\nHosts not found in Dynatrace ({len(results['not_found_hosts'])}):")
        for host in results['not_found_hosts'][:10]:
            print(f"  - {host['hostname']}")
        if len(results['not_found_hosts']) > 10:
            print(f"  ... and {len(results['not_found_hosts']) - 10} more")
    
    print("\n" + "=" * 60)
    print("CSV files generated in 'output/' directory")
    print("=" * 60)