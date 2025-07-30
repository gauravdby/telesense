import psycopg2
import pandas as pd
import json
import os

# --- Database Connection Details ---
DB_HOST = 'host_name'
DB_PORT = 5439
DB_NAME = 'db_name'
DB_USER = 'username'
DB_PASSWORD = 'password'

# --- Timeout Configuration ---
CONNECT_TIMEOUT_SECONDS = 60
STATEMENT_TIMEOUT_MS = 120000

# --- Combined Query ---

TABLE_QUERIES = {
    "ansible_all_telemetry_combined_reliable": {
        "query": """
            SELECT
                aji.created AS job_created_timestamp,
                DATE(aji.created) AS job_created_date,
                aji.job_id,
                aji.job_name,
                aji.status AS job_status,
                aji.failed AS job_failed_flag,
                aji.elapsed AS job_elapsed_seconds,
                aji.launch_type,
                aji.org_id AS job_org_id,
                aji.cluster_id AS job_cluster_id,

                ajr.host_count AS job_host_count,
                ajr.ok_host_count AS job_ok_host_count,
                ajr.failed_host_count AS job_failed_host_count,
                ajr.host_task_count AS job_task_count,
                ajr.host_task_ok_count AS job_task_ok_count,
                ajr.host_task_failed_count AS job_task_failed_count,
                ajr.average_elapsed_per_host AS job_avg_elapsed_per_host,

                aci.tower_version,
                aci.license_type,
                aci.total_licensed_instances,
                aci.automated_instances AS cluster_automated_instances,
                aci.current_instances AS cluster_current_inventory_hosts,
                aci.is_compliant AS cluster_is_compliant,
                aci.subscription_name,
                aci.in_trial AS cluster_in_trial_flag,

                acm.url_base_value AS cluster_url,

                adhc.failed_hosts AS daily_host_failed_count,
                adhc.success_hosts AS daily_host_success_count,
                adhc.total_hosts AS daily_host_total_count,

                ahi.host_id AS daily_host_activity_id,
                ahi.created_date AS daily_host_activity_date,

                ahm.hostname AS host_metric_hostname,
                ahm.deleted AS host_metric_deleted_flag,
                ahm.automated_counter AS host_metric_automation_count,
                ahm.last_seen AS host_metric_last_seen,

                amr.collection_name,
                amr.module_name,
                amr.total_count AS module_invocation_count,

                alpf.rating_value AS lightspeed_feedback_rating,
                alpf.model_name AS lightspeed_feedback_model,

                alr.action AS lightspeed_recommendation_action,
                alr.collection_name AS lightspeed_rec_collection_name,
                alr.module_name AS lightspeed_rec_module_name,

                aed.final_global_customer_name AS customer_global_name,
                aed.entl_nodes_type,
                aed.entl_quantity AS customer_entitled_nodes,
                aed.startdate AS entitlement_start_date,
                aed.enddate AS entitlement_end_date,
                aed.status AS entitlement_status,
                aed.has_entl_info,
                aed.cyq

            FROM
                cee_insights.ansible_job_info aji
            LEFT JOIN
                cee_insights.ansible_job_rollup ajr ON aji.cluster_id = ajr.cluster_id AND aji.job_id = ajr.job_id
            LEFT JOIN
                cee_insights.ansible_cluster_info aci ON aji.cluster_id = aci.cluster_id AND DATE(aji.created) = DATE(aci.created_date)
            LEFT JOIN
                cee_insights.ansible_cluster_meta acm ON aji.cluster_id = acm.cluster_id
            LEFT JOIN
                cee_insights.ansible_daily_host_count adhc ON aji.org_id = adhc.org_id AND DATE(aji.created) = DATE(adhc.created_date)
            LEFT JOIN
                cee_insights.ansible_host_info ahi ON aji.org_id = ahi.org_id AND DATE(aji.created) = DATE(ahi.created_date)
            LEFT JOIN
                cee_insights.ansible_host_metrics ahm ON aji.cluster_id = ahm.cluster_id AND aji.org_id = ahm.org_id
            LEFT JOIN
                cee_insights.ansible_modules_rules amr ON aji.cluster_id = amr.cluster_id AND DATE(aji.created) = DATE(amr.created_date)
            LEFT JOIN
                cee_insights.ansible_lightspeed_product_feedback alpf ON aji.org_id = alpf.org_id AND DATE(alpf.created) = DATE(aji.created) -- Assuming date/org link
            LEFT JOIN
                cee_insights.ansible_lightspeed_recommendation alr ON aji.org_id = alr.org_id AND DATE(alr.created) = DATE(aji.created) -- Assuming date/org link
            LEFT JOIN
                cee_insights.ansible_entitlements_data aed ON aji.org_id = aed.ebs_account

            WHERE
                aji.created >= DATEADD(year, -1, CURRENT_DATE)
            LIMIT 50000;
        """,
        "description": "Comprehensive, denormalized Ansible telemetry data: job execution, cluster, host, modules, Lightspeed, and entitlements."
    }
}

# --- Data Fetching Logic ---
def fetch_data_from_redshift(query_sql, table_name, chunk_size=50000):
    conn = None
    all_chunks = []
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=CONNECT_TIMEOUT_SECONDS,
            options=f'-c statement_timeout={STATEMENT_TIMEOUT_MS}'
        )
        print(f"Fetching data for '{table_name}' in chunks of {chunk_size}...")
        for i, chunk in enumerate(pd.read_sql(query_sql, conn, chunksize=chunk_size)):
            all_chunks.append(chunk)
            print(f"  Fetched chunk {i+1}: {len(chunk)} rows.")
        
        if all_chunks:
            return pd.concat(all_chunks, ignore_index=True)
        else:
            print(f"No data fetched for '{table_name}'.")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"Error fetching data for '{table_name}': {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

# --- Main Execution ---
if __name__ == "__main__":
    output_dir = "telemetry_json_output" 
    os.makedirs(output_dir, exist_ok=True)

    print("Starting Redshift data extraction...")
    print(f"Output will be saved to '{output_dir}' directory.")

    for table_key, details in TABLE_QUERIES.items():
        print(f"\n--- Processing query: {table_key} ---")
        df = fetch_data_from_redshift(details["query"], table_key)

        if not df.empty:
            print(f"Finished fetching all {len(df)} rows for '{table_key}'.")
            
            structured_entry = {
                "table_name": table_key,
                "description": details["description"],
                "data_schema": {col: str(df[col].dtype) for col in df.columns},
                "data_records": df.to_dict(orient='records')
            }
            
            output_filename = os.path.join(output_dir, f"{table_key}_data.json") 
            try:
                with open(output_filename, 'w') as f:
                    json.dump(structured_entry, f, indent=2, default=str)
                print(f"Saved data for '{table_key}' to '{output_filename}' successfully.")
            except Exception as e:
                print(f"Error saving data for '{table_key}' to file: {e}")
        else:
            print(f"No data fetched for '{table_key}' or an error occurred. Skipping file creation.")

    print("\nData extraction process complete.")
