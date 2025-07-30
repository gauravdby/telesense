import pandas as pd
import json
import os
from datetime import datetime, timedelta


DATA_DIR = 'telemetry_json_output' 
COMBINED_JSON_FILENAME = 'ansible_all_telemetry_combined_reliable_data.json' 
COMBINED_JSON_PATH = os.path.join(DATA_DIR, COMBINED_JSON_FILENAME)


df_combined_telemetry = pd.DataFrame() 
if os.path.exists(COMBINED_JSON_PATH):
    try:
        with open(COMBINED_JSON_PATH, 'r') as f:
            combined_data = json.load(f)
        if 'data_records' in combined_data and isinstance(combined_data['data_records'], list):
            df_combined_telemetry = pd.DataFrame(combined_data['data_records'])
            print(f"Successfully loaded {len(df_combined_telemetry)} rows from {COMBINED_JSON_PATH}")
        else:
            print(f"Warning: 'data_records' key not found or not a list in {COMBINED_JSON_PATH}. Check JSON structure.")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {COMBINED_JSON_PATH}. File might be corrupted.")
    except Exception as e:
        print(f"Error loading combined data from {COMBINED_JSON_PATH}: {e}")
else:
    print(f"Error: Combined JSON file '{COMBINED_JSON_PATH}' not found. Please run 'get_telesense_data.py' first.")


# --- Analysis Functions ---

def get_jobs_run_summary(n_days=30):
    """
    Analyzes job execution data from the combined telemetry and returns a summary for the last N days.
    """
    if df_combined_telemetry.empty:
        return "No combined telemetry data available to analyze for jobs."

    df_jobs = df_combined_telemetry.copy()
    df_jobs['job_created_timestamp'] = pd.to_datetime(df_jobs['job_created_timestamp'], errors='coerce')
    df_filtered = df_jobs.dropna(subset=['job_created_timestamp'])
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=n_days)
    
    df_recent_jobs = df_filtered[
        (df_filtered['job_created_timestamp'] >= start_date) & 
        (df_filtered['job_created_timestamp'] <= end_date)
    ]
    
    if df_recent_jobs.empty:
        return f"No job data found in the last {n_days} days."

    total_jobs = len(df_recent_jobs)
    successful_jobs = df_recent_jobs[df_recent_jobs['job_status'] == 'successful'].shape[0]
    failed_jobs = df_recent_jobs[df_recent_jobs['job_status'] == 'failed'].shape[0]
    
    total_hosts_processed_by_jobs = df_recent_jobs['job_host_count'].sum() if 'job_host_count' in df_recent_jobs.columns else 0

    output = f"## Job Run Summary (Last {n_days} Days):\n\n"
    output += f"- Total Jobs Run: {total_jobs}\n"
    output += f"- Successful Jobs: {successful_jobs}\n"
    output += f"- Failed Jobs: {failed_jobs}\n"
    if total_jobs > 0:
        output += f"- Success Rate: {(successful_jobs / total_jobs):.2%}\n"
    output += f"- Total Hosts Touched by These Jobs: {total_hosts_processed_by_jobs:.0f}\n\n"

    output += "### Sample Job Data:\n"
    sample_columns = [col for col in ['job_created_date', 'job_name', 'job_status', 'job_elapsed_seconds', 'job_org_id'] if col in df_recent_jobs.columns]
    output += df_recent_jobs[sample_columns].head(5).to_markdown(index=False)
    
    return output

def get_top_modules_used(top_n=10):
    """
    Analyzes module usage data from the combined telemetry and returns the top N modules.
    """
    if df_combined_telemetry.empty:
        return "No combined telemetry data available to analyze for modules."
    
    df_modules = df_combined_telemetry.copy()

    if 'collection_name' not in df_modules.columns or 'module_name' not in df_modules.columns or 'module_invocation_count' not in df_modules.columns:
        return "Missing 'collection_name', 'module_name', or 'module_invocation_count' in combined data for module analysis. Check the combined query in get_telesense_data.py."

    df_modules['collection_name'] = df_modules['collection_name'].astype(str)
    df_modules['module_name'] = df_modules['module_name'].astype(str)
    df_modules['module_invocation_count'] = pd.to_numeric(df_modules['module_invocation_count'], errors='coerce').fillna(0)

    df_clean = df_modules[df_modules['module_name'].notna() & (df_modules['module_name'] != '') & (df_modules['module_name'] != 'nan')]
    
    if df_clean.empty:
        return "No valid module usage data after cleaning in combined data."

    top_modules = df_clean.groupby(['collection_name', 'module_name'])['module_invocation_count'].sum().reset_index()
    top_modules = top_modules.sort_values(by='module_invocation_count', ascending=False).head(top_n)

    output = f"## Top {top_n} Ansible Modules Used (Last Year):\n\n"
    output += top_modules.to_markdown(index=False)
    return output

def get_cluster_compliance_summary():
    """
    Analyzes cluster compliance data from the combined telemetry and provides a summary.
    """
    if df_combined_telemetry.empty:
        return "No combined telemetry data available to analyze for cluster compliance."
    
    df_clusters = df_combined_telemetry.copy()

    if 'is_compliant' not in df_clusters.columns:
        return "Missing 'is_compliant' column in combined data for compliance analysis. Check query in get_telesense_data.py."
    
    df_clusters['is_compliant'] = df_clusters['is_compliant'].astype(str).str.lower()
    
    
    df_clusters_sorted = df_clusters.sort_values(by='job_created_date', ascending=False)
    df_unique_clusters = df_clusters_sorted.drop_duplicates(subset=['job_org_id', 'job_cluster_id'])
    
    compliant_count = df_unique_clusters[df_unique_clusters['is_compliant'] == 'true'].shape[0]
    non_compliant_count = df_unique_clusters[df_unique_clusters['is_compliant'] == 'false'].shape[0]
    total_clusters = df_unique_clusters.shape[0]

    output = "## Cluster Compliance Summary (Last Year):\n\n"
    output += f"- Total Unique Clusters Monitored: {total_clusters}\n"
    output += f"- Compliant Clusters: {compliant_count}\n"
    output += f"- Non-Compliant Clusters: {non_compliant_count}\n"
    if total_clusters > 0:
        output += f"- Compliance Rate: {(compliant_count / total_clusters):.2%}\n"
    
    if non_compliant_count > 0:
        output += "\n### Sample Non-Compliant Clusters:\n"
        sample_cols = [col for col in ['job_org_id', 'job_cluster_id', 'tower_version', 'cluster_url'] if col in df_unique_clusters.columns]
        output += df_unique_clusters[df_unique_clusters['is_compliant'] == 'false'][sample_cols].head(3).to_markdown(index=False)
    
    return output

# --- Main Execution Block ---

if __name__ == "__main__":
    print("--- Ansible Telemetry Analysis Tool ---")
    
    if df_combined_telemetry.empty:
        print("Please ensure 'get_telesense_data.py' was run successfully to create "
              f"'{COMBINED_JSON_PATH}' before running analysis.")
    else:
        # Example 1: Get job run summary for the last 30 days
        print(get_jobs_run_summary(30))
        
        # Example 2: Get top 5 modules used
        print(get_top_modules_used(5))

        # Example 3: Get cluster compliance summary
        print(get_cluster_compliance_summary())

    print("\n--- Analysis Complete ---")
