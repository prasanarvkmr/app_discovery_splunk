import requests
from typing import Optional, List, Generator, Dict
from databricks import sql
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import csv
from datetime import datetime, timedelta


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
    dynatrace_url: str,
    api_token: str,
    entity_ids: List[str],
    time_range: str = "now-1h"
) -> Dict[str, dict]:
    """
    Fetch health metrics for multiple hosts from Dynatrace Metrics API v2.
    
    Args:
        dynatrace_url: Dynatrace environment URL
        api_token: Dynatrace API token with metrics.read scope
        entity_ids: List of host entity IDs (HOST-XXXXX)
        time_range: Time range for metrics (default: last 1 hour)
    
    Returns:
        Dict mapping entity_id to metrics
    """
    headers = {
        "Authorization": f"Api-Token {api_token}",
        "Content-Type": "application/json"
    }
    
    base_url = dynatrace_url.rstrip('/')
    endpoint = f"{base_url}/api/v2/metrics/query"
    
    # Build entity selector for multiple hosts
    entity_selector = ",".join([f'entityId("{eid}")' for eid in entity_ids])
    
    metrics_data = {eid: {} for eid in entity_ids}
    
    # Query each metric
    for metric_name, metric_id in HOST_METRICS.items():
        params = {
            "metricSelector": f"{metric_id}:avg",
            "entitySelector": f"type(HOST),({entity_selector})",
            "from": time_range,
            "resolution": "1h"
        }
        
        try:
            response = requests.get(endpoint, headers=headers, params=params, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                for result in data.get("result", []):
                    for series in result.get("data", []):
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
        except requests.RequestException as e:
            print(f"Warning: Failed to fetch {metric_name}: {e}")
    
    return metrics_data


def get_host_health_state(
    dynatrace_url: str,
    api_token: str,
    entity_ids: List[str]
) -> Dict[str, dict]:
    """
    Get host health state and problems from Dynatrace.
    
    Args:
        dynatrace_url: Dynatrace environment URL
        api_token: Dynatrace API token
        entity_ids: List of host entity IDs
    
    Returns:
        Dict mapping entity_id to health state
    """
    headers = {
        "Authorization": f"Api-Token {api_token}",
        "Content-Type": "application/json"
    }
    
    base_url = dynatrace_url.rstrip('/')
    health_data = {}
    
    # Get problems for these hosts
    endpoint = f"{base_url}/api/v2/problems"
    
    for entity_id in entity_ids:
        params = {
            "entitySelector": f'entityId("{entity_id}")',
            "from": "now-24h",
            "problemSelector": 'status("OPEN")'
        }
        
        try:
            response = requests.get(endpoint, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                problems = data.get("problems", [])
                
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
                    "problem_titles": ""
                }
        except requests.RequestException:
            health_data[entity_id] = {
                "health_state": "ERROR",
                "open_problems": 0,
                "highest_severity": "ERROR",
                "problem_titles": "Failed to fetch"
            }
    
    return health_data


def get_host_properties(entity: dict) -> dict:
    """Extract key properties from host entity."""
    properties = entity.get("properties", {})
    
    return {
        "os_type": properties.get("osType", ""),
        "os_version": properties.get("osVersion", ""),
        "os_architecture": properties.get("osArchitecture", ""),
        "hypervisor_type": properties.get("hypervisorType", ""),
        "cloud_type": properties.get("cloudType", ""),
        "cloud_provider": properties.get("cloudProvider", ""),
        "aws_instance_type": properties.get("awsInstanceType", ""),
        "azure_vm_size": properties.get("azureVmSize", ""),
        "cpu_cores": properties.get("cpuCores", ""),
        "physical_memory_gb": round(properties.get("physicalMemory", 0) / (1024**3), 2) if properties.get("physicalMemory") else "",
        "ip_addresses": "; ".join(properties.get("ipAddress", [])) if properties.get("ipAddress") else "",
        "monitoring_mode": properties.get("monitoringMode", ""),
        "agent_version": properties.get("agentVersion", ""),
        "is_monitored": properties.get("state", "") == "RUNNING"
    }


def check_hosts_batch_with_metrics(
    dynatrace_url: str,
    api_token: str,
    hostnames: List[str]
) -> dict:
    """
    Check multiple hosts and fetch their metrics.
    
    Args:
        dynatrace_url: Dynatrace environment URL
        api_token: Dynatrace API token
        hostnames: List of hostnames to check
    
    Returns:
        dict with found hosts including metrics, and not_found hosts
    """
    headers = {
        "Authorization": f"Api-Token {api_token}",
        "Content-Type": "application/json"
    }
    
    base_url = dynatrace_url.rstrip('/')
    endpoint = f"{base_url}/api/v2/entities"
    
    # Build entity selector for multiple hosts
    host_conditions = ",".join([f'entityName.equals("{h}")' for h in hostnames])
    entity_selector = f'type("HOST"),({host_conditions})'
    
    params = {
        "entitySelector": entity_selector,
        "fields": "+properties",
        "pageSize": 500
    }
    
    try:
        response = requests.get(endpoint, headers=headers, params=params, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            entities = data.get("entities", [])
            found_hosts_map = {e.get("displayName", "").lower(): e for e in entities}
            
            results = {
                "found": [],
                "not_found": []
            }
            
            # Get entity IDs for metrics query
            entity_ids = [e.get("entityId") for e in entities if e.get("entityId")]
            
            # Fetch metrics for all found hosts
            metrics_data = {}
            health_data = {}
            
            if entity_ids:
                print(f"    Fetching metrics for {len(entity_ids)} hosts...")
                metrics_data = get_host_metrics(dynatrace_url, api_token, entity_ids)
                health_data = get_host_health_state(dynatrace_url, api_token, entity_ids)
            
            for hostname in hostnames:
                if hostname.lower() in found_hosts_map:
                    entity = found_hosts_map[hostname.lower()]
                    entity_id = entity.get("entityId", "")
                    
                    # Combine all data
                    host_result = {
                        "hostname": hostname,
                        "entity_id": entity_id,
                        "display_name": entity.get("displayName", ""),
                        "found_in_dynatrace": True,
                        **get_host_properties(entity),
                        **metrics_data.get(entity_id, {}),
                        **health_data.get(entity_id, {})
                    }
                    results["found"].append(host_result)
                else:
                    results["not_found"].append({
                        "hostname": hostname,
                        "found_in_dynatrace": False
                    })
            
            return results
        else:
            return {"error": response.text, "status_code": response.status_code}
            
    except requests.RequestException as e:
        return {"error": str(e)}


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
    output_csv: str = None
) -> dict:
    """
    Main function to read hosts from Databricks, check in Dynatrace, and export to CSV.
    
    Args:
        databricks_config: Dict with server_hostname, http_path, access_token, table_name, host_column
        dynatrace_config: Dict with url, api_token
        chunk_size: Number of hosts to process per batch
        output_csv: Path to output CSV file (optional, will also return results)
    
    Returns:
        dict with all results
    """
    all_found = []
    all_not_found = []
    errors = []
    
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
        print(f"Checking batch of {len(host_chunk)} hosts in Dynatrace...")
        
        # Check this chunk in Dynatrace with metrics
        result = check_hosts_batch_with_metrics(
            dynatrace_url=dynatrace_config["url"],
            api_token=dynatrace_config["api_token"],
            hostnames=host_chunk
        )
        
        if "error" in result:
            errors.append(result)
        else:
            all_found.extend(result.get("found", []))
            all_not_found.extend(result.get("not_found", []))
        
        # Rate limiting - avoid hitting API too hard
        time.sleep(1)
    
    results = {
        "total_found": len(all_found),
        "total_not_found": len(all_not_found),
        "found_hosts": all_found,
        "not_found_hosts": all_not_found,
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
    
    # Define CSV columns for found hosts
    found_columns = [
        "hostname",
        "found_in_dynatrace",
        "entity_id",
        "display_name",
        "health_state",
        "open_problems",
        "highest_severity",
        "problem_titles",
        "cpu_usage",
        "cpu_system",
        "cpu_user",
        "cpu_idle",
        "memory_usage",
        "memory_available",
        "disk_usage",
        "disk_read_throughput",
        "disk_write_throughput",
        "network_in",
        "network_out",
        "host_availability",
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
        "monitoring_mode",
        "agent_version",
        "is_monitored"
    ]
    
    # Export found hosts with metrics
    found_file = f"{output_prefix}_found_{timestamp}.csv"
    with open(found_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=found_columns, extrasaction='ignore')
        writer.writeheader()
        for host in results.get("found_hosts", []):
            writer.writerow(host)
    print(f"Found hosts exported to: {found_file}")
    
    # Export not found hosts
    not_found_file = f"{output_prefix}_not_found_{timestamp}.csv"
    with open(not_found_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["hostname", "found_in_dynatrace"], extrasaction='ignore')
        writer.writeheader()
        for host in results.get("not_found_hosts", []):
            writer.writerow(host)
    print(f"Not found hosts exported to: {not_found_file}")
    
    # Export summary
    summary_file = f"{output_prefix}_summary_{timestamp}.csv"
    with open(summary_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Metric", "Value"])
        writer.writerow(["Total Hosts Checked", results.get("total_found", 0) + results.get("total_not_found", 0)])
        writer.writerow(["Hosts Found in Dynatrace", results.get("total_found", 0)])
        writer.writerow(["Hosts Not Found", results.get("total_not_found", 0)])
        writer.writerow(["Errors", len(results.get("errors", []))])
        writer.writerow(["Timestamp", timestamp])
        
        # Health summary
        found_hosts = results.get("found_hosts", [])
        if found_hosts:
            healthy = sum(1 for h in found_hosts if h.get("health_state") == "HEALTHY")
            warning = sum(1 for h in found_hosts if h.get("health_state") == "WARNING")
            critical = sum(1 for h in found_hosts if h.get("health_state") == "CRITICAL")
            writer.writerow([""])
            writer.writerow(["Health Summary", ""])
            writer.writerow(["Healthy Hosts", healthy])
            writer.writerow(["Warning Hosts", warning])
            writer.writerow(["Critical Hosts", critical])
            
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
        "found_file": found_file,
        "not_found_file": not_found_file,
        "summary_file": summary_file
    }


def check_host_in_dynatrace(
    dynatrace_url: str,
    api_token: str,
    host_name: Optional[str] = None,
    host_id: Optional[str] = None
) -> dict:
    """
    Check if host details are available in Dynatrace API v2.
    
    Args:
        dynatrace_url: Dynatrace environment URL (e.g., https://your-env.live.dynatrace.com)
        api_token: Dynatrace API token with entities.read scope
        host_name: Host name to search for (optional)
        host_id: Dynatrace entity ID (e.g., HOST-XXXXXXXXXX) (optional)
    
    Returns:
        dict with status and host details
    """
    headers = {
        "Authorization": f"Api-Token {api_token}",
        "Content-Type": "application/json"
    }
    
    base_url = dynatrace_url.rstrip('/')
    
    # If specific host ID is provided, fetch directly
    if host_id:
        endpoint = f"{base_url}/api/v2/entities/{host_id}"
        try:
            response = requests.get(endpoint, headers=headers, timeout=30)
            if response.status_code == 200:
                return {"found": True, "host": response.json()}
            elif response.status_code == 404:
                return {"found": False, "message": f"Host {host_id} not found"}
            else:
                return {"found": False, "error": response.text, "status_code": response.status_code}
        except requests.RequestException as e:
            return {"found": False, "error": str(e)}
    
    # Search by host name using entity selector
    endpoint = f"{base_url}/api/v2/entities"
    params = {
        "entitySelector": 'type("HOST")',
        "fields": "+properties,+toRelationships,+fromRelationships",
        "pageSize": 500
    }
    
    if host_name:
        params["entitySelector"] = f'type("HOST"),entityName.contains("{host_name}")'
    
    try:
        response = requests.get(endpoint, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            entities = data.get("entities", [])
            
            if entities:
                return {
                    "found": True,
                    "count": len(entities),
                    "hosts": entities
                }
            else:
                return {"found": False, "message": "No hosts found matching criteria"}
        else:
            return {"found": False, "error": response.text, "status_code": response.status_code}
            
    except requests.RequestException as e:
        return {"found": False, "error": str(e)}


def get_host_details(dynatrace_url: str, api_token: str, host_id: str) -> dict:
    """
    Get detailed information about a specific host.
    
    Args:
        dynatrace_url: Dynatrace environment URL
        api_token: Dynatrace API token
        host_id: Dynatrace host entity ID
    
    Returns:
        dict with host details
    """
    headers = {
        "Authorization": f"Api-Token {api_token}",
        "Content-Type": "application/json"
    }
    
    base_url = dynatrace_url.rstrip('/')
    endpoint = f"{base_url}/api/v2/entities/{host_id}"
    
    params = {
        "fields": "+properties,+toRelationships,+fromRelationships"
    }
    
    try:
        response = requests.get(endpoint, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            return {"success": False, "error": response.text, "status_code": response.status_code}
            
    except requests.RequestException as e:
        return {"success": False, "error": str(e)}


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
        output_csv=OUTPUT_PREFIX
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