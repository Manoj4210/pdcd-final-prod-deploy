import sys
import os
import psycopg2

# Add deploy_scripts directory to path so we can import
sys.path.append("/Users/manoj_anumalla/Desktop/PDCD_PYTHON_DEPLOY/pdcd-final-prod-deploy/deploy_scripts")

from execute_database_changes_v2 import PDCDPartitionRunner

def get_direct_conn(config):
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        database=config["dbname"],
        user=config["user"],
        password=config["password"]
    )

def query_scalar(config, query, params=None):
    with get_direct_conn(config) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {config['schema_name']};")
            cur.execute(query, params)
            return cur.fetchone()[0]

def main():
    config = {
        "host": "localhost",
        "port": "5432",
        "dbname": "blocking_check",
        "user": "manoj_anumalla",
        "password": "",
        "schema_name": "data_monitoring"
    }

    print("--- STEP 1: INITIAL STATE ---")
    latest_snapshot = query_scalar(config, "SELECT COALESCE(max(snapshot_id), 0) FROM metadata_snapshot;")
    print(f"Latest successful snapshot ID before test: {latest_snapshot}")

    print("\n--- STEP 2: RUNNING SUCCESSFUL RUN ---")
    runner = PDCDPartitionRunner("test_database", config)
    runner.run()
    
    post_success_snapshot = query_scalar(config, "SELECT COALESCE(max(snapshot_id), 0) FROM metadata_snapshot;")
    print(f"Latest successful snapshot ID after successful run: {post_success_snapshot}")
    if post_success_snapshot > latest_snapshot:
        print("Success: Snapshot incremented correctly on successful run.")
    else:
        print(f"Error: Expected snapshot to increment, got {post_success_snapshot}")

    print("\n--- STEP 3: RUNNING FAILED RUN WITH LOCK TIMEOUT SIMULATION ---")
    fail_runner = PDCDPartitionRunner("test_database", config)
    
    # We monkey-patch run_step on the instance to fail on compare_load_md5_table_metadata
    original_run_step = fail_runner.run_step
    def failing_run_step(conn, step_name, query, params=None, result="Completed"):
        if step_name == "compare_load_md5_table_metadata":
            print("SIMULATING LOCK TIMEOUT ERROR...")
            raise RuntimeError("canceling statement due to lock timeout")
        return original_run_step(conn, step_name, query, params, result)
        
    fail_runner.run_step = failing_run_step
    
    # We also monkey-patch run_scalar_step to capture the snapshot_id that was generated
    failed_snapshot_id = None
    original_run_scalar_step = fail_runner.run_scalar_step
    def capture_run_scalar_step(conn, step_name, query, params=None, result_prefix="Completed"):
        val = original_run_scalar_step(conn, step_name, query, params, result_prefix)
        if step_name == "load_snapshot_table":
            nonlocal failed_snapshot_id
            failed_snapshot_id = val
        return val
    fail_runner.run_scalar_step = capture_run_scalar_step
    
    try:
        fail_runner.run()
        print("Error: Runner did not raise simulated exception!")
    except Exception as exc:
        print(f"Caught expected exception from runner: {exc}")
        print(f"Failed snapshot ID attempted: {failed_snapshot_id}")
        
        # Check if the failed snapshot ID is in the database or if it was rolled back/deleted
        snapshot_exists = query_scalar(config, 
            "SELECT EXISTS (SELECT 1 FROM metadata_snapshot WHERE snapshot_id = %s);", 
            (failed_snapshot_id,)
        )
        
        part_table_exists = query_scalar(config,
            "SELECT to_regclass(%s) IS NOT NULL;",
            (f"metadata_md5_staging_table_objects_p{failed_snapshot_id}",)
        )
        part_non_table_exists = query_scalar(config,
            "SELECT to_regclass(%s) IS NOT NULL;",
            (f"metadata_md5_staging_non_table_objects_p{failed_snapshot_id}",)
        )
        
        print(f"Does failed snapshot row still exist? {snapshot_exists}")
        print(f"Does table staging partition _p{failed_snapshot_id} still exist? {part_table_exists}")
        print(f"Does non-table staging partition _p{failed_snapshot_id} still exist? {part_non_table_exists}")
        
        if not snapshot_exists and not part_table_exists and not part_non_table_exists:
            print("\nSUCCESS: The failed snapshot ID and its partition tables were completely rolled back and cleaned up!")
        else:
            print("\nFAILURE: Failed snapshot state was left in the database.")

if __name__ == "__main__":
    main()
