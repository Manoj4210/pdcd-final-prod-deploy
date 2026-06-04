-- ============================================================
-- Production-safe revert script for objects created by
-- initialize_pdcd_functions.sql
-- ============================================================
-- Usage:
--   psql -h <host> -p <port> -U <user> -d <dbname> \
--     -v schema_name=<pdcd_schema> \
--     -f revert_pdcd_fucntions.sql
--
-- Notes:
--   Uses RESTRICT instead of CASCADE. If any external object depends on a PDCD
--   table/function, this script fails and rolls back instead of dropping that
--   dependent object.
--
--   This script also drops PDCD-owned child partitions under:
--     metadata_md5_staging_table_objects
--     metadata_md5_staging_non_table_objects
--
--   It does not drop the schema itself.
--   This is destructive to PDCD snapshot/change/metrics history if it succeeds.
-- ============================================================

\set ON_ERROR_STOP on

BEGIN;

-- SET search_path TO :"schema_name";

SET search_path TO data_monitoring;

-- ------------------------------------------------------------
-- Drop child partitions first so partitioned parents can be
-- dropped with RESTRICT.
-- ------------------------------------------------------------
DO $$
DECLARE
    partition_record record;
BEGIN
    FOR partition_record IN
        SELECT
            child_ns.nspname AS child_schema,
            child.relname AS child_table
        FROM pg_inherits i
        JOIN pg_class parent ON parent.oid = i.inhparent
        JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
        WHERE parent_ns.nspname = current_schema()
          AND parent.relname IN (
              'metadata_md5_staging_table_objects',
              'metadata_md5_staging_non_table_objects'
          )
        ORDER BY child_ns.nspname, child.relname
    LOOP
        EXECUTE format(
            'DROP TABLE IF EXISTS %I.%I RESTRICT',
            partition_record.child_schema,
            partition_record.child_table
        );
    END LOOP;
END $$;

-- ------------------------------------------------------------
-- Drop partition management functions
-- ------------------------------------------------------------
DROP FUNCTION IF EXISTS create_staging_partitions(integer) RESTRICT;
DROP FUNCTION IF EXISTS drop_old_staging_partitions(integer) RESTRICT;

-- ------------------------------------------------------------
-- Drop orchestration/load/compare functions
-- ------------------------------------------------------------
DROP FUNCTION IF EXISTS process_metadata_md5_changes(text[], text) RESTRICT;
DROP FUNCTION IF EXISTS compare_load_md5_table_metadata(text[]) RESTRICT;
DROP FUNCTION IF EXISTS compare_load_md5_non_table_metadata(text[]) RESTRICT;
DROP FUNCTION IF EXISTS load_snapshot_table() RESTRICT;
DROP FUNCTION IF EXISTS load_metadata_md5_metrics() RESTRICT;
DROP FUNCTION IF EXISTS load_md5_metadata_table(text[]) RESTRICT;
DROP FUNCTION IF EXISTS load_md5_metadata_staging_table_objects(text[]) RESTRICT;
DROP FUNCTION IF EXISTS load_md5_metadata_staging_non_table_objects(text[]) RESTRICT;

-- ------------------------------------------------------------
-- Drop compute functions
-- ------------------------------------------------------------
DROP FUNCTION IF EXISTS compute_views_md5(text[]) RESTRICT;
DROP FUNCTION IF EXISTS compute_triggers_md5(text[]) RESTRICT;
DROP FUNCTION IF EXISTS compute_sequences_md5(text[]) RESTRICT;
DROP FUNCTION IF EXISTS compute_references_md5(text[]) RESTRICT;
DROP FUNCTION IF EXISTS compute_materialized_views_md5(text[]) RESTRICT;
DROP FUNCTION IF EXISTS compute_indexes_md5(text[]) RESTRICT;
DROP FUNCTION IF EXISTS compute_functions_md5(text[]) RESTRICT;
DROP FUNCTION IF EXISTS compute_constraints_md5(text[]) RESTRICT;
DROP FUNCTION IF EXISTS compute_columns_md5(text[]) RESTRICT;

-- ------------------------------------------------------------
-- Drop fetch/detail functions
-- ------------------------------------------------------------
DROP FUNCTION IF EXISTS fetch_view_details(text[]) RESTRICT;
DROP FUNCTION IF EXISTS fetch_trigger_details(text[]) RESTRICT;
DROP FUNCTION IF EXISTS fetch_sequence_details(text[]) RESTRICT;
DROP FUNCTION IF EXISTS fetch_reference_details(text[]) RESTRICT;
DROP FUNCTION IF EXISTS fetch_materialized_view_details(text[]) RESTRICT;
DROP FUNCTION IF EXISTS fetch_index_details(text[]) RESTRICT;
DROP FUNCTION IF EXISTS fetch_function_details(text[]) RESTRICT;
DROP FUNCTION IF EXISTS fetch_constraint_details(text[]) RESTRICT;
DROP FUNCTION IF EXISTS fetch_column_details(text[]) RESTRICT;

-- ------------------------------------------------------------
-- Drop PDCD tables. RESTRICT prevents accidental dependent-object drops.
-- ------------------------------------------------------------
DROP TABLE IF EXISTS metadata_md5_metrics RESTRICT;
DROP TABLE IF EXISTS metadata_md5_changes RESTRICT;
DROP TABLE IF EXISTS metadata_md5_staging_non_table_objects RESTRICT;
DROP TABLE IF EXISTS metadata_md5_staging_table_objects RESTRICT;
DROP TABLE IF EXISTS metadata_snapshot RESTRICT;

COMMIT;

