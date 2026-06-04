# PDCD Functions Flow Guide

This guide explains, at a high level, what [initialize_pdcd_functions.sql](/Users/manoj_anumalla/Desktop/PDCD_PYTHON_DEPLOY/pdcd-final-prod-deploy/deploy_scripts/initialize_pdcd_functions.sql) creates and how the end-to-end change detection flow works.

## Purpose

The SQL script sets up a metadata change detection framework for PostgreSQL objects. It does four main things:

1. Creates core tables used to store snapshots, staging metadata, detected changes, and metrics.
2. Creates helper functions that read PostgreSQL catalog metadata for tables, columns, constraints, functions, views, sequences, triggers, and references.
3. Creates hashing and comparison functions that turn catalog metadata into MD5 fingerprints and compare the current state with the previous snapshot.
4. Creates one top-level orchestration function, `process_metadata_md5_changes(...)`, that runs the full workflow for an initial load or a subsequent compare run.

## Core Objects

### Tables

- `metadata_snapshot`
  Stores one row per monitoring run. Each run gets a new `snapshot_id`.

- `metadata_md5_changes`
  Stores detected changes for a snapshot, including object type, object name, details, MD5 hash, and change type such as `ADDED`, `MODIFIED`, `RENAMED`, or `DELETED`.

- `metadata_md5_staging_table_objects`
  Partitioned staging table for table-related metadata such as columns, constraints, indexes, references, triggers, and table sequences.

- `metadata_md5_staging_non_table_objects`
  Partitioned staging table for non-table metadata such as functions, views, materialized views, and schema-level sequences.

- `metadata_md5_metrics`
  Stores per-snapshot summary metrics such as schemas monitored, total change operations, and object-type counts.

### Partition helpers

- `create_staging_partitions(snapshot_id)`
  Creates staging partitions for the current snapshot and adds indexes on those partitions.

- `drop_old_staging_partitions(snapshot_id)`
  Detaches and drops old staging partitions, keeping only the partition for the supplied snapshot.

This is the mechanism that replaces truncate-heavy staging cleanup with partition rotation.

## Function Layers

### 1. Fetch layer

These functions read raw PostgreSQL catalog metadata:

- `fetch_column_details(...)`
- `fetch_constraint_details(...)`
- `fetch_function_details(...)`
- `fetch_index_details(...)`
- `fetch_materialized_view_details(...)`
- `fetch_reference_details(...)`
- `fetch_sequence_details(...)`
- `fetch_trigger_details(...)`
- `fetch_view_details(...)`

Their job is to return normalized metadata rows for each object type.

### 2. MD5 compute layer

These functions convert the fetched metadata into deterministic text and MD5 hashes:

- `compute_columns_md5(...)`
- `compute_constraints_md5(...)`
- `compute_functions_md5(...)`
- `compute_indexes_md5(...)`
- `compute_materialized_views_md5(...)`
- `compute_references_md5(...)`
- `compute_sequences_md5(...)`
- `compute_triggers_md5(...)`
- `compute_views_md5(...)`

Each compute function returns:

- schema
- object type
- object name
- optional subtype and subtype name
- normalized detail text
- `object_md5`

This is the fingerprint used later for change detection.

### 3. Load layer

These functions bulk-load the current state into staging or into the initial changes table:

- `load_md5_metadata_staging_table_objects(...)`
- `load_md5_metadata_staging_non_table_objects(...)`
- `load_md5_metadata_table(...)`

What they do:

- `load_md5_metadata_table(...)`
  Used during the initial run to treat the current state as the baseline and insert those objects into `metadata_md5_changes` as `ADDED`.

- `load_md5_metadata_staging_table_objects(...)`
  Loads current table-related metadata into the table staging partition for the current snapshot.

- `load_md5_metadata_staging_non_table_objects(...)`
  Loads current non-table metadata into the non-table staging partition for the current snapshot.

### 4. Compare layer

These functions compare the current run with the previous snapshot:

- `compare_load_md5_table_metadata(...)`
- `compare_load_md5_non_table_metadata(...)`

Their job is to:

1. Read current metadata.
2. Read the previous snapshot from the staging partition.
3. Detect `ADDED`, `MODIFIED`, `RENAMED`, and `DELETED` objects.
4. Insert the detected differences into `metadata_md5_changes`.

A notable optimization in this version:

- The compare functions scan the current catalog state once into temporary tables.
- They insert the current state into staging for the new snapshot.
- They compare the new state against the previous snapshot partition.

That avoids recomputing some metadata multiple times in the same run.

### 5. Metrics layer

- `load_metadata_md5_metrics()`

This function summarizes the results of the latest snapshot and stores metrics into `metadata_md5_metrics`.

It always inserts base metrics first:

- Schemas Monitored
- Schemas With Changes
- Total Change Operations

If there are no detected changes, it stops there. Otherwise it also inserts object-level metrics such as:

- Tables Added
- Columns Modified
- Functions Dropped
- Views Renamed
- Sequences Modified

## Snapshot Lifecycle

### `load_snapshot_table()`

This creates a new snapshot row and returns:

- `snapshot_id`
- `snapshot_name`
- `processed_time`

Every monitoring run starts by creating a new snapshot.

## Top-Level Orchestration

### `process_metadata_md5_changes(p_schemas text[], p_mode text default 'INCLUDE')`

This is the main entry point that coordinates the whole process.

It supports two modes:

- `INCLUDE`
  Only process schemas listed in `p_schemas`.

- `EXCLUDE`
  Process all non-system schemas except those listed in `p_schemas`.

It returns step-by-step status rows:

- `step_name`
- `result`
- `duration`

## End-to-End Flow

### Initial run

This happens when `metadata_snapshot` is empty.

Flow:

1. Mark run mode as `Initial Load`.
2. Create a new snapshot row.
3. Create staging partitions for that snapshot.
4. Run `load_md5_metadata_table(...)` to capture the current metadata as the baseline in `metadata_md5_changes`.
5. Load current table metadata into staging.
6. Load current non-table metadata into staging.
7. Compute and store metrics.

Result:

- All current objects become the baseline state for future comparisons.
- Staging partitions now contain the current snapshot.

### Subsequent run

This happens when there is already at least one snapshot in `metadata_snapshot`.

Flow:

1. Mark run mode as `Subsequent Compare Run`.
2. Get the previous snapshot ID.
3. Create a new snapshot row.
4. Compare current table metadata against the previous table staging partition.
5. Compare current non-table metadata against the previous non-table staging partition.
6. Create staging partitions for the new snapshot.
7. Load current table metadata into the new table staging partition.
8. Load current non-table metadata into the new non-table staging partition.
9. Drop old staging partitions and keep only the latest one.
10. Compute and store metrics.

Result:

- `metadata_md5_changes` contains only the delta for the new snapshot.
- Staging reflects the latest database state.
- Old staging data is removed through partition rotation.

## What Gets Compared

The script tracks both table-related and non-table-related objects.

Table-related:

- columns
- constraints
- indexes
- foreign-key references
- triggers
- table-owned sequences

Non-table-related:

- functions
- views
- materialized views
- schema-level sequences

## Why This Design Works

The overall design is a layered pipeline:

1. Read catalog metadata.
2. Normalize it.
3. Hash it.
4. Store current state in staging.
5. Compare current state to previous state.
6. Write changes.
7. Summarize results into metrics.

The partitioned staging tables are the key production optimization:

- they avoid repeated large-table cleanup
- they reduce dead tuples
- they make old-snapshot cleanup predictable
- they keep compare logic scoped to previous and current snapshots

## Operational Notes

- The script assumes it is run with `SET search_path TO :"schema_name"`.
- All PDCD tables and functions are created in that schema.
- If the current schema itself is included in monitoring, PDCD may detect changes to its own functions, partitions, and metadata tables.
- `load_metadata_md5_metrics()` treats `UNCHANGED` rows as non-changes and only summarizes actual deltas.

## Short Summary

`initialize_pdcd_functions.sql` builds a full metadata monitoring engine inside PostgreSQL. It captures a snapshot of object definitions, fingerprints them with MD5, compares the latest state against the previous snapshot, stores the detected changes, rotates staging partitions, and writes reporting metrics for each run.
