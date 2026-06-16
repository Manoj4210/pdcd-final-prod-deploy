# Concurrency and Rollback Optimization Walkthrough

We have successfully designed, created, and verified the production concurrency and rollback optimizations using the new files, leaving the baseline code untouched.

---

## Technical Accomplishments & Fixes

1. **Created Rollback-Safe V2 Files (New Files):**
   * **SQL schema initialization:** [initialize_pdcd_functions_v2.sql](file:///Users/manoj_anumalla/Desktop/PDCD_PYTHON_DEPLOY/pdcd-final-prod-deploy/deploy_scripts/initialize_pdcd_functions_v2.sql)
   * **Python orchestration runner:** [execute_database_changes_v2.py](file:///Users/manoj_anumalla/Desktop/PDCD_PYTHON_DEPLOY/pdcd-final-prod-deploy/deploy_scripts/execute_database_changes_v2.py)
   * **Deployment script:** [deploy_pdcd_v2.sh](file:///Users/manoj_anumalla/Desktop/PDCD_PYTHON_DEPLOY/pdcd-final-prod-deploy/deploy_scripts/deploy_pdcd_v2.sh)

2. **Mitigated Long-Held ETL Table Locks (`autocommit = True`):**
   * Modified the execution runner to run with `conn.autocommit = True`. 
   * This prevents locking all catalog tables under a single transaction for the entire 2-3 minutes run, ensuring that concurrent business ETLs performing `TRUNCATE`, `ALTER TABLE`, or other DDLs are not blocked by PDCD.

3. **Prevented Partial Snapshot Progression (Orphan Snapshots):**
   * Added the `rollback_failed_snapshot(p_snapshot_id INT)` function to [initialize_pdcd_functions_v2.sql](file:///Users/manoj_anumalla/Desktop/PDCD_PYTHON_DEPLOY/pdcd-final-prod-deploy/deploy_scripts/initialize_pdcd_functions_v2.sql). This function detaches/drops the staging partitions for a failed snapshot and deletes its row in `metadata_snapshot`.
   * Migrated the foreign key reference in `metadata_md5_metrics` to use `ON DELETE CASCADE`.
   * Modified `execute_database_changes_v2.py` to capture `self.current_snapshot_id` immediately upon assignment. If a step fails, the python script catches the error and executes `SELECT rollback_failed_snapshot(%s);` to clean up the database state.

---

## Verification & Test Results

A test script was executed against the local `blocking_check` database under the `data_monitoring` schema:
* **Test Scenario 1 (Successful Run):** The runner executed successfully. The snapshot ID was correctly incremented and registered in `metadata_snapshot` (advanced from 5 to 6).
* **Test Scenario 2 (Failed Run / Lock Timeout Simulation):** An exception was raised during the `compare_load_md5_table_metadata` step.
  * **Result:** The runner caught the exception and called `rollback_failed_snapshot(7)`.
  * **Verification Query Outputs:**
    * *Does failed snapshot row still exist?* **False**
    * *Does table staging partition _p7 still exist?* **False**
    * *Does non-table staging partition _p7 still exist?* **False**
  * **Status:** **SUCCESS**. The failed snapshot and its tables were completely purged from the database, preventing progression of corrupted metadata snapshots.

---

## File Overview

* **New SQL initialization script:** [initialize_pdcd_functions_v2.sql](file:///Users/manoj_anumalla/Desktop/PDCD_PYTHON_DEPLOY/pdcd-final-prod-deploy/deploy_scripts/initialize_pdcd_functions_v2.sql)
* **New python orchestration runner:** [execute_database_changes_v2.py](file:///Users/manoj_anumalla/Desktop/PDCD_PYTHON_DEPLOY/pdcd-final-prod-deploy/deploy_scripts/execute_database_changes_v2.py)
* **New deploy shell script:** [deploy_pdcd_v2.sh](file:///Users/manoj_anumalla/Desktop/PDCD_PYTHON_DEPLOY/pdcd-final-prod-deploy/deploy_scripts/deploy_pdcd_v2.sh)
