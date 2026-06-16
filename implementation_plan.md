# Production Concurrency and Transactional Rollback Optimization (V2)

Optimize the connection transactional behavior and retry flow of the PDCD runner to prevent long-held locks on catalog tables and clean up failed snapshot attempts. As requested, all modifications will be implemented in new files to avoid changing the existing ones.

## User Review Required

> [!NOTE]
> All changes will be written to new files (`initialize_pdcd_functions_v2.sql`, `execute_database_changes_v2.py`, and `deploy_pdcd_v2.sh`). No existing files will be touched.

## Open Questions

None. The proposed new files solve both concerns: they prevent locking issues for other ETLs and ensure failed retries do not pollute snapshot IDs or leave behind orphan staging tables.

---

## Proposed Changes

### Database Functions and Schema

#### [NEW] [initialize_pdcd_functions_v2.sql](file:///Users/manoj_anumalla/Desktop/PDCD_PYTHON_DEPLOY/pdcd-final-prod-deploy/deploy_scripts/initialize_pdcd_functions_v2.sql)
This new file will contain the contents of `initialize_pdcd_functions.sql` with the following enhancements:
1. Ensure the foreign key reference in `metadata_md5_metrics` on `snapshot_id` has `ON DELETE CASCADE`.
2. Define a database-side helper function `rollback_failed_snapshot(p_snapshot_id INT)` to:
   - Detach and drop staging partitions for the failed `snapshot_id` (`metadata_md5_staging_table_objects_p<id>` and `metadata_md5_staging_non_table_objects_p<id>`).
   - Delete the snapshot record from `metadata_snapshot` (which cascades to `metadata_md5_changes` and `metadata_md5_metrics`).

---

### Python Runner Script

#### [NEW] [execute_database_changes_v2.py](file:///Users/manoj_anumalla/Desktop/PDCD_PYTHON_DEPLOY/pdcd-final-prod-deploy/deploy_scripts/execute_database_changes_v2.py)
This new file will be a copy of `execute_database_changes.py` modified as follows:
1. Run connection in `autocommit = True` to avoid holding catalog locks across multiple steps.
2. Track the `snapshot_id` returned by `run_initial` and `run_subsequent` calls.
3. Catch any exception in the `run()` method; if a `snapshot_id` has been allocated, call `rollback_failed_snapshot(snapshot_id)` to purge any partial state and delete the failed snapshot row.
4. Ensure `run_initial` and `run_subsequent` return the `snapshot_id` they created.

---

### Shell Deployment Script

#### [NEW] [deploy_pdcd_v2.sh](file:///Users/manoj_anumalla/Desktop/PDCD_PYTHON_DEPLOY/pdcd-final-prod-deploy/deploy_scripts/deploy_pdcd_v2.sh)
This new file will deploy the functions in `initialize_pdcd_functions_v2.sql` to the target database.

---

## Verification Plan

### Automated Tests
- Deploy the updated database SQL scripts using the new `deploy_pdcd_v2.sh`.
- Run `python execute_database_changes_v2.py`.
- Simulate a failure during execution (e.g., by canceling a query or raising an error) and verify that the created snapshot ID is removed and staging partitions are dropped.

### Manual Verification
- Verify that consecutive failed attempts do not advance the `snapshot_id` value stored in the `metadata_snapshot` table (it should only increment when a run succeeds).
