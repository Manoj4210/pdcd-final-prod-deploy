-- ============================================================================
-- Script: pdcd_functions_init.sql
-- Purpose: Initializes all monitoring tables, partition staging structures,
--          index setups, and optimized catalog change detection procedures.
-- ============================================================================

BEGIN;

SET search_path TO :"schema_name";

-- ==========================================
-- Table: metadata_snapshot
-- ==========================================

CREATE TABLE IF NOT EXISTS metadata_snapshot (
  snapshot_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  snapshot_name TEXT NOT NULL,
  processed_time TIMESTAMP DEFAULT clock_timestamp()
);

--=================================================
-- Table: metadata_md5_changes
--=================================================
CREATE TABLE IF NOT EXISTS metadata_md5_changes (
  metadata_id BIGSERIAL PRIMARY KEY,
  snapshot_id INT NOT NULL REFERENCES metadata_snapshot(snapshot_id) ON DELETE CASCADE,
  schema_name TEXT NOT NULL,
  object_type TEXT NOT NULL,          -- TABLE, VIEW, FUNCTION, ...
  object_type_name TEXT NOT NULL,     -- table/view name
  object_subtype TEXT,                -- Column, Index, Trigger, ...
  object_subtype_name TEXT,           -- column name or index name
  object_subtype_details TEXT,        -- raw detail string
  object_md5 TEXT NOT NULL,           -- md5 fingerprint of the object_subtype_details (or full row)
  processed_time TIMESTAMP DEFAULT clock_timestamp(),
  change_type TEXT DEFAULT 'ADDED'   -- ADDED | MODIFIED | UNCHANGED | DELETED (we'll use ADDED/MODIFIED/DELETED on insert)
);

--=================================================
-- Table: metadata_md5_staging_table_objects (PARTITIONED)
-- Each snapshot gets its own partition for zero-lock rotation.
-- DROP partition = instant cleanup, no dead tuples, no VACUUM.
--=================================================
CREATE TABLE IF NOT EXISTS metadata_md5_staging_table_objects (
  metadata_id BIGSERIAL,
  snapshot_id INT NOT NULL,
  schema_name TEXT NOT NULL,
  object_type TEXT NOT NULL,
  object_type_name TEXT NOT NULL,
  object_subtype TEXT,
  object_subtype_name TEXT,
  object_subtype_details TEXT,
  object_md5 TEXT NOT NULL,
  processed_time TIMESTAMP DEFAULT clock_timestamp()
) PARTITION BY LIST (snapshot_id);


--=================================================
-- Table: metadata_md5_staging_non_table_objects (PARTITIONED)
--=================================================
CREATE TABLE IF NOT EXISTS metadata_md5_staging_non_table_objects (
  metadata_id BIGSERIAL,
  snapshot_id INT NOT NULL,
  schema_name TEXT NOT NULL,
  object_type TEXT NOT NULL,
  object_type_name TEXT NOT NULL,
  object_subtype TEXT,
  object_subtype_name TEXT,
  object_subtype_details TEXT,
  object_md5 TEXT NOT NULL,
  processed_time TIMESTAMP DEFAULT clock_timestamp()
) PARTITION BY LIST (snapshot_id);


--=================================================
-- Partition Management Functions
--=================================================

-- Creates a new partition for a given snapshot_id
CREATE OR REPLACE FUNCTION create_staging_partitions(p_snapshot_id INT)
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS metadata_md5_staging_table_objects_p%s PARTITION OF metadata_md5_staging_table_objects FOR VALUES IN (%s)',
        p_snapshot_id, p_snapshot_id
    );
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS metadata_md5_staging_non_table_objects_p%s PARTITION OF metadata_md5_staging_non_table_objects FOR VALUES IN (%s)',
        p_snapshot_id, p_snapshot_id
    );

    -- Create indexes on the new partitions for fast comparison
    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS idx_stg_tbl_p%s_lookup ON metadata_md5_staging_table_objects_p%s (schema_name, object_type_name, object_subtype, object_subtype_name)',
        p_snapshot_id, p_snapshot_id
    );
    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS idx_stg_tbl_p%s_md5 ON metadata_md5_staging_table_objects_p%s (object_md5)',
        p_snapshot_id, p_snapshot_id
    );
    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS idx_stg_ntbl_p%s_lookup ON metadata_md5_staging_non_table_objects_p%s (schema_name, object_type, object_type_name)',
        p_snapshot_id, p_snapshot_id
    );
    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS idx_stg_ntbl_p%s_md5 ON metadata_md5_staging_non_table_objects_p%s (object_md5)',
        p_snapshot_id, p_snapshot_id
    );

    RAISE NOTICE 'Partition created for snapshot_id: %', p_snapshot_id;
END;
$$;


-- Detaches and drops ALL partitions EXCEPT the one for the given snapshot_id
CREATE OR REPLACE FUNCTION drop_old_staging_partitions(p_keep_snapshot_id INT)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    r RECORD;
BEGIN
    -- Drop old TABLE staging partitions
    FOR r IN
        SELECT inhrelid::regclass::text AS part_name
        FROM pg_inherits
        WHERE inhparent = 'metadata_md5_staging_table_objects'::regclass
    LOOP
        -- Skip the partition we want to keep
        IF r.part_name NOT LIKE '%_p' || p_keep_snapshot_id THEN
            EXECUTE format('ALTER TABLE metadata_md5_staging_table_objects DETACH PARTITION %I', r.part_name);
            EXECUTE format('DROP TABLE IF EXISTS %I', r.part_name);
            RAISE NOTICE 'Dropped old partition: %', r.part_name;
        END IF;
    END LOOP;

    -- Drop old NON-TABLE staging partitions
    FOR r IN
        SELECT inhrelid::regclass::text AS part_name
        FROM pg_inherits
        WHERE inhparent = 'metadata_md5_staging_non_table_objects'::regclass
    LOOP
        IF r.part_name NOT LIKE '%_p' || p_keep_snapshot_id THEN
            EXECUTE format('ALTER TABLE metadata_md5_staging_non_table_objects DETACH PARTITION %I', r.part_name);
            EXECUTE format('DROP TABLE IF EXISTS %I', r.part_name);
            RAISE NOTICE 'Dropped old partition: %', r.part_name;
        END IF;
    END LOOP;
END;
$$;
-- Detaches and drops staging partitions for a failed snapshot_id and deletes the snapshot record
CREATE OR REPLACE FUNCTION rollback_failed_snapshot(p_snapshot_id INT)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    v_part_table TEXT;
    v_part_non_table TEXT;
BEGIN
    v_part_table := format('metadata_md5_staging_table_objects_p%s', p_snapshot_id);
    v_part_non_table := format('metadata_md5_staging_non_table_objects_p%s', p_snapshot_id);
    
    -- Detach and drop table staging partition if it exists
    IF to_regclass(v_part_table) IS NOT NULL THEN
        EXECUTE format('ALTER TABLE metadata_md5_staging_table_objects DETACH PARTITION %I', v_part_table);
        EXECUTE format('DROP TABLE IF EXISTS %I', v_part_table);
    END IF;
    
    -- Detach and drop non-table staging partition if it exists
    IF to_regclass(v_part_non_table) IS NOT NULL THEN
        EXECUTE format('ALTER TABLE metadata_md5_staging_non_table_objects DETACH PARTITION %I', v_part_non_table);
        EXECUTE format('DROP TABLE IF EXISTS %I', v_part_non_table);
    END IF;

    -- Delete the snapshot record (this cascades to changes and metrics tables)
    DELETE FROM metadata_snapshot WHERE snapshot_id = p_snapshot_id;
END;
$$;


--=================================================
-- Table: metadata_md5_metrics
--=================================================

CREATE TABLE IF NOT EXISTS metadata_md5_metrics (
    metrics_id     BIGSERIAL PRIMARY KEY,
    snapshot_id    INT NOT NULL REFERENCES metadata_snapshot(snapshot_id) ON DELETE CASCADE,
    metric_name    TEXT NOT NULL,
    metric_value   INT NOT NULL,
    metrics_time   TIMESTAMP NOT NULL DEFAULT clock_timestamp()
);

-- Ensure metadata_md5_metrics has ON DELETE CASCADE for its foreign key reference to metadata_snapshot
DO $$
DECLARE
    v_constraint_name TEXT;
BEGIN
    SELECT con.conname INTO v_constraint_name
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
    WHERE rel.relname = 'metadata_md5_metrics'
      AND con.contype = 'f'
      AND con.confrelid = 'metadata_snapshot'::regclass;

    IF v_constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE metadata_md5_metrics DROP CONSTRAINT %I', v_constraint_name);
    END IF;
    
    ALTER TABLE metadata_md5_metrics ADD CONSTRAINT metadata_md5_metrics_snapshot_id_fkey 
        FOREIGN KEY (snapshot_id) REFERENCES metadata_snapshot(snapshot_id) ON DELETE CASCADE;
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END $$;


-- Prevent duplicate metrics per snapshot
CREATE UNIQUE INDEX IF NOT EXISTS ux_md5_metrics_snapshot_metric
    ON metadata_md5_metrics (snapshot_id, metric_name);

CREATE INDEX IF NOT EXISTS idx_metadata_changes_snapshot 
    ON metadata_md5_changes(snapshot_id, processed_time);

CREATE INDEX IF NOT EXISTS idx_metadata_changes_lookup 
    ON metadata_md5_changes(schema_name, object_type, object_type_name, object_subtype, object_subtype_name);

CREATE INDEX IF NOT EXISTS idx_metadata_changes_md5 
    ON metadata_md5_changes(object_md5);

CREATE INDEX IF NOT EXISTS idx_metadata_changes_time 
    ON metadata_md5_changes(processed_time);

CREATE INDEX IF NOT EXISTS idx_metadata_changes_type 
    ON metadata_md5_changes(object_type, object_subtype);

-- Staging table indexes
CREATE INDEX IF NOT EXISTS idx_staging_table_lookup 
    ON metadata_md5_staging_table_objects(snapshot_id, schema_name, object_type_name, object_subtype, object_subtype_name);

CREATE INDEX IF NOT EXISTS idx_staging_table_md5 
    ON metadata_md5_staging_table_objects(object_md5);

CREATE INDEX IF NOT EXISTS idx_staging_non_table_lookup 
    ON metadata_md5_staging_non_table_objects(snapshot_id, schema_name, object_type, object_type_name);

CREATE INDEX IF NOT EXISTS idx_staging_non_table_md5 
    ON metadata_md5_staging_non_table_objects(object_md5);

-- Snapshot table index
CREATE INDEX IF NOT EXISTS idx_snapshot_processed_time 
    ON metadata_snapshot(processed_time DESC);

-- Metrics table index
CREATE INDEX IF NOT EXISTS idx_metrics_snapshot 
    ON metadata_md5_metrics(snapshot_id, metric_name);

-- =====================================
-- fetch_column_details.sql
-- =====================================
CREATE OR REPLACE FUNCTION fetch_column_details(p_table_list text[] DEFAULT NULL::text[])
RETURNS TABLE(schema_name text, object_type text, object_type_name text, column_name text, data_type text, character_maximum_length integer, numeric_precision integer, numeric_scale integer, is_nullable text, column_default text, is_identity text, is_generated text, generation_expression text, constraint_name text, ordinal_position integer)
LANGUAGE sql
AS $function$
WITH col_constraints AS (
    -- Pre-aggregate constraint names by table and column attribute number
    SELECT 
        con.conrelid,
        col_num AS attnum,
        string_agg(DISTINCT con.conname, ',' ORDER BY con.conname) AS constraint_name
    FROM pg_constraint con
    CROSS JOIN LATERAL unnest(con.conkey) AS col_num
    GROUP BY con.conrelid, col_num
)
SELECT
    ns.nspname AS schema_name,

    CASE
        WHEN cls.relkind = 'r' THEN 'Table'
        WHEN cls.relkind = 'v' THEN 'View'
        WHEN cls.relkind = 'm' THEN 'Materialized View'
        ELSE 'Other'
    END AS object_type,

    cls.relname AS object_type_name,
    att.attname AS column_name,

    CASE typ.typname
        WHEN 'varchar'   THEN 'character varying'
        WHEN 'bpchar'    THEN 'character'
        WHEN 'int4'      THEN 'integer'
        WHEN 'int8'      THEN 'bigint'
        WHEN 'int2'      THEN 'smallint'
        WHEN 'float4'    THEN 'real'
        WHEN 'float8'    THEN 'double precision'
        WHEN 'bool'      THEN 'boolean'
        WHEN 'timestamptz' THEN 'timestamp with time zone'
        WHEN 'timestamp'   THEN 'timestamp without time zone'
        WHEN 'timetz'      THEN 'time with time zone'
        WHEN 'time'        THEN 'time without time zone'
        ELSE typ.typname
    END AS data_type,

    CASE 
        WHEN typ.typname IN ('varchar','bpchar')
            THEN att.atttypmod - 4
        ELSE NULL
    END AS character_maximum_length,

    CASE
        WHEN typ.typname = 'numeric'
            THEN ((att.atttypmod - 4) >> 16) & 65535
        WHEN typ.typname = 'int2' THEN 16
        WHEN typ.typname = 'int4' THEN 32
        WHEN typ.typname = 'int8' THEN 64
        WHEN typ.typname = 'float4' THEN 24
        WHEN typ.typname = 'float8' THEN 53
        ELSE NULL
    END AS numeric_precision,

    CASE
        WHEN typ.typname = 'numeric'
            THEN (att.atttypmod - 4) & 65535
        WHEN typ.typname IN ('int2','int4','int8','float4','float8')
            THEN 0
        ELSE NULL
    END AS numeric_scale,

    CASE WHEN att.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,

    pg_get_expr(ad.adbin, ad.adrelid) AS column_default,

    'NO' AS is_identity,
    'NEVER' AS is_generated,
    NULL AS generation_expression,

    cc.constraint_name,

    att.attnum AS ordinal_position

FROM pg_class cls
JOIN pg_namespace ns ON ns.oid = cls.relnamespace
JOIN pg_attribute att ON att.attrelid = cls.oid 
   AND att.attnum > 0 
   AND NOT att.attisdropped
JOIN pg_type typ ON typ.oid = att.atttypid
LEFT JOIN pg_attrdef ad ON ad.adrelid = cls.oid AND ad.adnum = att.attnum
LEFT JOIN col_constraints cc ON cc.conrelid = cls.oid AND cc.attnum = att.attnum
WHERE cls.relkind IN ('r','v','m')
AND (
    p_table_list IS NULL
    AND ns.nspname NOT IN ('pg_catalog','information_schema')
    OR
    p_table_list IS NOT NULL
    AND ns.nspname = ANY(p_table_list)
)
ORDER BY
    ns.nspname,
    cls.relname,
    att.attnum;
$function$;


-- =====================================
-- fetch_constraint_details.sql
-- =====================================
CREATE OR REPLACE FUNCTION fetch_constraint_details(p_table_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(schema_name text, object_type text, object_type_name text, constraint_name text, constraint_type text, column_name text, definition text)
 LANGUAGE sql
AS $function$

WITH input_tables AS (

    -- CASE 1: No input → fetch ALL tables
    SELECT n.nspname AS schema_name, c.relname AS table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND p_table_list IS NULL
      AND n.nspname NOT IN ('pg_catalog','information_schema')

    UNION ALL

    -- CASE 2: Schema only input
    SELECT n.nspname, c.relname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND EXISTS (
          SELECT 1 FROM unnest(p_table_list) t
          WHERE position('.' IN t) = 0 AND n.nspname = t
      )

    UNION ALL

    -- CASE 3: schema.table input
    SELECT split_part(t, '.', 1), split_part(t, '.', 2)
    FROM unnest(p_table_list) t
    WHERE position('.' IN t) > 0
),

constraints AS (
    SELECT 
        n.nspname               AS schema_name,
        'Table'                 AS object_type,
        c.relname               AS object_type_name,
        con.conname             AS constraint_name,

        CASE con.contype
            WHEN 'p' THEN 'PRIMARY KEY'
            WHEN 'u' THEN 'UNIQUE'
            WHEN 'c' THEN 'CHECK'
            WHEN 'f' THEN 'FOREIGN KEY'
            ELSE con.contype::TEXT
        END AS constraint_type,

        -- Column names or FK source column
        CASE
            WHEN con.contype IN ('p','u') THEN 
                (SELECT string_agg(att.attname, ',' ORDER BY att.attnum)
                 FROM unnest(con.conkey) AS colnum
                 JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = colnum)

            WHEN con.contype = 'f' THEN 
                (SELECT string_agg(att.attname, ',' ORDER BY att.attnum)
                 FROM unnest(con.conkey) AS colnum
                 JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = colnum)

            WHEN con.contype = 'c' THEN NULL
        END AS column_name,

        -- Canonical constraint definition
        pg_get_constraintdef(con.oid, true)::TEXT AS definition

    FROM pg_constraint con
    JOIN pg_class c       ON c.oid = con.conrelid
    JOIN pg_namespace n   ON n.oid = c.relnamespace

    WHERE (n.nspname, c.relname) IN (
        SELECT schema_name, table_name FROM input_tables
    )
)

SELECT *
FROM constraints
ORDER BY schema_name, object_type_name, constraint_name;

$function$;


-- =====================================
-- fetch_function_details.sql
-- =====================================
CREATE OR REPLACE FUNCTION fetch_function_details(
    p_schema_list text[] DEFAULT NULL::text[]
)
RETURNS TABLE(
    schema_name text,
    function_name text,
    argument_types text,
    argument_modes text,
    return_type text,
    language text,
    volatility text,
    parallel_safe text,
    owner_role text,
    privileges text,
    dependencies text,
    is_security_definer boolean,
    config_settings_text text,
    function_body text
)
LANGUAGE sql
AS $function$
    WITH deps AS (
        SELECT
            d.objid,
            string_agg(
                d.refobjid::regclass::text,
                ','
                ORDER BY d.refclassid, d.refobjid, d.refobjsubid
            ) AS dependencies
        FROM pg_depend d
        WHERE d.classid = 'pg_proc'::regclass
        GROUP BY d.objid
    )
    SELECT
        n.nspname AS schema_name,
        concat(p.proname, '(', pg_get_function_identity_arguments(p.oid), ')') AS function_name,

        pg_get_function_arguments(p.oid) AS argument_types,
        pg_get_function_identity_arguments(p.oid) AS argument_modes,
        format_type(p.prorettype, NULL) AS return_type,

        l.lanname AS language,
        CASE p.provolatile
            WHEN 'i' THEN 'IMMUTABLE'
            WHEN 's' THEN 'STABLE'
            WHEN 'v' THEN 'VOLATILE'
        END AS volatility,

        CASE p.proparallel
            WHEN 's' THEN 'SAFE'
            WHEN 'r' THEN 'RESTRICTED'
            WHEN 'u' THEN 'UNSAFE'
        END AS parallel_safe,

        pg_get_userbyid(p.proowner) AS owner_role,
        p.proacl::text AS privileges,
        deps.dependencies,

        p.prosecdef AS is_security_definer,
        array_to_string(p.proconfig, ', ') AS config_settings_text,

        CASE
            WHEN l.lanname = 'sql' THEN
                regexp_replace(trim(p.prosrc), '\s+', ' ', 'g')
            WHEN l.lanname = 'plpgsql' THEN
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            pg_get_functiondef(p.oid),
                            '.*?(DECLARE|BEGIN)(.*?)END;?\s*\$[^$]*\$.*$',
                            '\1\2',
                            'nsi'
                        ),
                        '\s*RETURN\s+[^;]+;?\s*$',
                        '',
                        'nsi'
                    ),
                    '\s+',
                    ' ',
                    'g'
                )
            ELSE trim(p.prosrc)
        END AS function_body

    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    JOIN pg_language l ON l.oid = p.prolang
    LEFT JOIN deps ON deps.objid = p.oid
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND (p_schema_list IS NULL OR n.nspname = ANY(p_schema_list));
$function$;


-- =====================================
-- fetch_index_details.sql
-- =====================================
CREATE OR REPLACE FUNCTION fetch_index_details(p_table_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(schema_name text, object_type text, object_type_name text, index_name text, tablespace text, indexdef text, is_unique boolean, is_primary boolean, index_columns text, index_predicate text, access_method text)
 LANGUAGE sql
AS $function$
WITH input_objects AS (
    SELECT n.nspname || '.' || c.relname AS full_object_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r','m')
      AND (p_table_list IS NULL OR array_length(p_table_list,1) IS NULL)
      AND n.nspname NOT IN ('pg_catalog','information_schema')

    UNION ALL
    SELECT n.nspname || '.' || c.relname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r','m')
      AND EXISTS (SELECT 1 FROM unnest(p_table_list) t
                  WHERE position('.' IN t) = 0 AND n.nspname = t)

    UNION ALL
    SELECT unnest(p_table_list)
)

SELECT
    n.nspname::TEXT AS schema_name,
    CASE WHEN t.relkind='r' THEN 'Table'
         WHEN t.relkind='m' THEN 'Materialized View' END AS object_type,
    t.relname::TEXT AS object_type_name,
    i.relname::TEXT AS index_name,
    ts.spcname::TEXT AS tablespace,

    /* NORMALIZED INDEXDEF */
    regexp_replace(
        regexp_replace(
            regexp_replace(pg_get_indexdef(i.oid), '\s+', ' ', 'g'),
            '\(\s+', '(', 'g'
        ),
        '\s+\)', ')', 'g'
    ) AS indexdef,

    x.indisunique AS is_unique,
    x.indisprimary AS is_primary,

    /* NORMALIZED index columns */
    (
        SELECT string_agg(a.attname, ',' ORDER BY ord)
        FROM unnest(x.indkey::int[]) WITH ORDINALITY AS u(attnum, ord)
        JOIN pg_attribute a ON a.attnum = u.attnum AND a.attrelid = t.oid
    ) AS index_columns,

    /* NORMALIZED predicate */
    regexp_replace(
        COALESCE(pg_get_expr(x.indpred, x.indrelid)::TEXT,''),
        '\s+',' ','g'
    ) AS index_predicate,

    am.amname::TEXT AS access_method

FROM pg_class t
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_index x ON x.indrelid = t.oid
JOIN pg_class i ON i.oid = x.indexrelid
LEFT JOIN pg_tablespace ts ON ts.oid = i.reltablespace
JOIN pg_am am ON am.oid = i.relam
WHERE (n.nspname || '.' || t.relname) IN (SELECT full_object_name FROM input_objects)
ORDER BY n.nspname, t.relname, i.relname;
$function$;


-- =====================================
-- fetch_materialized_view_details.sql
-- =====================================
CREATE OR REPLACE FUNCTION fetch_materialized_view_details(p_schema_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(schema_name text, view_name text, view_type text, view_definition text, base_tables text, is_populated boolean, view_owner text, dependent_objects text)
 LANGUAGE sql
AS $function$

WITH base_tables_cte AS (
    SELECT
        v_ns.nspname AS view_schema,
        v.relname    AS view_name,
        string_agg(
            DISTINCT bt_ns.nspname || '.' || bt.relname,
            ', ' ORDER BY bt_ns.nspname || '.' || bt.relname
        ) AS base_tables
    FROM pg_class v
    JOIN pg_namespace v_ns ON v.relnamespace = v_ns.oid
    JOIN pg_rewrite r ON r.ev_class = v.oid
    JOIN pg_depend d ON d.objid = r.oid
    JOIN pg_class bt ON bt.oid = d.refobjid
    JOIN pg_namespace bt_ns ON bt_ns.oid = bt.relnamespace
    WHERE v.relkind = 'm'
      AND bt.relkind = 'r'
    GROUP BY v_ns.nspname, v.relname
),

dependent_views AS (
    SELECT
        base_ns.nspname AS base_schema,
        base_v.relname  AS base_view,
        string_agg(
            DISTINCT child_ns.nspname || '.' || child_v.relname,
            ', ' ORDER BY child_ns.nspname || '.' || child_v.relname
        ) AS dependent_views
    FROM pg_depend d
    JOIN pg_rewrite r ON r.oid = d.objid
    JOIN pg_class base_v ON base_v.oid = r.ev_class
    JOIN pg_namespace base_ns ON base_v.relnamespace = base_ns.oid

    JOIN pg_class child_v ON child_v.oid = d.refobjid
    JOIN pg_namespace child_ns ON child_v.relnamespace = child_ns.oid

    WHERE base_v.relkind IN ('v','m')
      AND child_v.relkind IN ('v','m')
      AND base_v.oid <> child_v.oid
    GROUP BY base_ns.nspname, base_v.relname
),

index_deps AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS view_name,
        string_agg(i.relname, ', ' ORDER BY i.relname) AS indexes
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    JOIN pg_index x ON x.indrelid = c.oid
    JOIN pg_class i ON i.oid = x.indexrelid
    WHERE c.relkind = 'm'
    GROUP BY n.nspname, c.relname
)

SELECT
    v_ns.nspname::TEXT AS schema_name,
    v.relname::TEXT    AS view_name,
    'MATERIALIZED VIEW'::TEXT AS view_type,

    /* Normalize whitespace in view definition */
    regexp_replace(pg_get_viewdef(v.oid, true)::TEXT, '\s+', ' ', 'g') AS view_definition,

    COALESCE(bt.base_tables, '') AS base_tables,
    v.relispopulated AS is_populated,
    pg_get_userbyid(v.relowner)::TEXT AS view_owner,

    trim(both ', ' FROM concat_ws(
        ', ',
        CASE WHEN dv.dependent_views IS NOT NULL
             THEN 'Dependent views: ' || dv.dependent_views END,
        CASE WHEN id.indexes IS NOT NULL
             THEN 'Indexes: ' || id.indexes END
    )) AS dependent_objects

FROM pg_class v
JOIN pg_namespace v_ns ON v_ns.oid = v.relnamespace
LEFT JOIN base_tables_cte bt
       ON bt.view_schema = v_ns.nspname AND bt.view_name = v.relname
LEFT JOIN dependent_views dv
       ON dv.base_schema = v_ns.nspname AND dv.base_view = v.relname
LEFT JOIN index_deps id
       ON id.schema_name = v_ns.nspname AND id.view_name = v.relname

WHERE v.relkind = 'm'
  AND v_ns.nspname NOT IN ('pg_catalog','information_schema')
  AND (
        p_schema_list IS NULL
        OR v_ns.nspname = ANY(p_schema_list)
  )
ORDER BY schema_name, view_name;

$function$;


-- =====================================
-- fetch_reference_details.sql
-- =====================================
CREATE OR REPLACE FUNCTION fetch_reference_details(p_table_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(schema_name text, table_name text, source_column text, target_schema text, target_table text, target_column text, constraint_name text)
 LANGUAGE sql
AS $function$

WITH resolved_tables AS (

    /* CASE 1: No input → include all user tables */
    SELECT array_agg(n.nspname || '.' || c.relname) AS tbls
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE (p_table_list IS NULL OR array_length(p_table_list, 1) IS NULL)
      AND c.relkind = 'r'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')

    UNION ALL

    /* CASE 2: Schema-only → all tables in those schemas */
    SELECT array_agg(n.nspname || '.' || c.relname) AS tbls
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE p_table_list IS NOT NULL
      AND NOT EXISTS (
            SELECT 1 FROM unnest(p_table_list) t WHERE position('.' IN t) > 0
      )
      AND n.nspname = ANY(p_table_list)
      AND c.relkind = 'r'

    UNION ALL

    /* CASE 3: Full schema.table list provided */
    SELECT p_table_list AS tbls
    WHERE p_table_list IS NOT NULL
      AND EXISTS (
            SELECT 1 FROM unnest(p_table_list) t WHERE position('.' IN t) > 0
      )
),
final_tables AS (
    SELECT tbls FROM resolved_tables WHERE tbls IS NOT NULL LIMIT 1
)

SELECT
    src_ns.nspname        AS schema_name,
    src_tbl.relname       AS table_name,
    src_col.attname       AS source_column,
    tgt_ns.nspname        AS target_schema,
    tgt_tbl.relname       AS target_table,
    tgt_col.attname       AS target_column,
    con.conname           AS constraint_name
FROM pg_constraint con
JOIN pg_class src_tbl ON src_tbl.oid = con.conrelid
JOIN pg_namespace src_ns ON src_ns.oid = src_tbl.relnamespace
JOIN unnest(con.conkey)     WITH ORDINALITY AS src_cols(attnum, ord) ON TRUE
JOIN pg_attribute src_col
     ON src_col.attrelid = con.conrelid
    AND src_col.attnum = src_cols.attnum
JOIN pg_class tgt_tbl ON tgt_tbl.oid = con.confrelid
JOIN pg_namespace tgt_ns ON tgt_ns.oid = tgt_tbl.relnamespace
JOIN unnest(con.confkey)    WITH ORDINALITY AS tgt_cols(attnum, ord)
     ON tgt_cols.ord = src_cols.ord
JOIN pg_attribute tgt_col
     ON tgt_col.attrelid = con.confrelid
    AND tgt_col.attnum = tgt_cols.attnum
WHERE con.contype = 'f'
  AND (src_ns.nspname || '.' || src_tbl.relname) = ANY(
        ARRAY(SELECT unnest(tbls) FROM final_tables)
      )
ORDER BY src_ns.nspname, src_tbl.relname, con.conname, src_cols.ord;

$function$;


-- =====================================
-- fetch_sequence_details.sql
-- =====================================
CREATE OR REPLACE FUNCTION fetch_sequence_details(p_sequence_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(schema_name text, object_type text, object_type_name text, table_name text, owned_by text, sequence_type text, privileges text, data_type text, start_value bigint, minimum_value bigint, maximum_value bigint, increment_by bigint, cycle_option text, cache_size bigint)
 LANGUAGE sql
AS $function$

WITH seqs AS (
    SELECT 
        n.nspname AS schema_name,
        c.relname AS object_type_name,
        'Sequence'::TEXT AS object_type,
        s.seqstart AS start_value,
        s.seqmin AS minimum_value,
        s.seqmax AS maximum_value,
        s.seqincrement AS increment_by,
        s.seqcycle AS cycle_bool,
        s.seqcache AS cache_size,
        s.seqtypid,
        c.oid AS seq_oid,
        c.relacl AS relacl
    FROM pg_sequence s
    JOIN pg_class c ON c.oid = s.seqrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'S'
),

normalized_deps AS (
    SELECT DISTINCT ON (d.objid)
        d.objid AS seq_oid,
        t.relname AS table_name,
        a.attname AS column_name,
        tn.nspname AS table_schema,
        d.deptype
    FROM pg_depend d
    JOIN pg_class t ON t.oid = d.refobjid
    JOIN pg_namespace tn ON tn.oid = t.relnamespace
    LEFT JOIN pg_attribute a 
        ON a.attrelid = t.oid
       AND a.attnum = d.refobjsubid
    WHERE d.classid = 'pg_class'::regclass
      AND d.refclassid = 'pg_class'::regclass
    ORDER BY d.objid,
             (a.attname IS NULL),  -- prefer sequences owned by a column
             a.attname
)

SELECT
    seq.schema_name,
    seq.object_type,
    seq.object_type_name,
    dep.table_name,

    CASE 
        WHEN dep.table_name IS NOT NULL AND dep.column_name IS NOT NULL THEN
            format('%I.%I.%I', dep.table_schema, dep.table_name, dep.column_name)
        ELSE NULL
    END AS owned_by,

    CASE
        WHEN dep.deptype = 'i' THEN 'IDENTITY'
        WHEN dep.deptype = 'a' THEN 'SERIAL'
        ELSE 'MANUAL'
    END AS sequence_type,

    seq.relacl::TEXT AS privileges,
    pg_catalog.format_type(seq.seqtypid, NULL) AS data_type,
    seq.start_value,
    seq.minimum_value,
    seq.maximum_value,
    seq.increment_by,
    CASE WHEN seq.cycle_bool THEN 'YES' ELSE 'NO' END AS cycle_option,
    seq.cache_size

FROM seqs seq
LEFT JOIN normalized_deps dep
       ON dep.seq_oid = seq.seq_oid

WHERE
    (
        p_sequence_list IS NULL
        AND seq.schema_name NOT IN ('pg_catalog', 'information_schema')
    )
    OR (
        p_sequence_list IS NOT NULL
        AND seq.schema_name = ANY(p_sequence_list)
    )
    OR (
        p_sequence_list IS NOT NULL
        AND (seq.schema_name || '.' || seq.object_type_name) = ANY(p_sequence_list)
    )

ORDER BY seq.schema_name, seq.object_type_name;

$function$;


-- =====================================
-- fetch_trigger_details.sql
-- =====================================
CREATE OR REPLACE FUNCTION fetch_trigger_details(p_table_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(schema_name text, object_type text, object_type_name text, trigger_name text, trigger_definition text, trigger_event text, trigger_timing text, trigger_level text, trigger_enabled boolean, trigger_function_name text, trigger_function_arguments text, trigger_function_definition text)
 LANGUAGE sql
AS $function$

WITH trg AS (
    SELECT
        n.nspname AS schema_name,
        c.relkind,
        c.relname AS object_type_name,
        t.tgname AS trigger_name,

        pg_get_triggerdef(t.oid, true) AS trigger_definition,

        t.tgenabled,
        t.tgtype,

        -- Correct for PG11+
        t.tgfoid::regproc AS trigger_function_name,
        pg_get_function_identity_arguments(t.tgfoid) AS trigger_function_arguments,
        pg_get_functiondef(t.tgfoid) AS trigger_function_definition

    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE NOT t.tgisinternal
      AND (
            p_table_list IS NULL
            OR n.nspname = ANY(p_table_list)
            OR (n.nspname || '.' || c.relname) = ANY(p_table_list)
          )
),

normalized AS (
    SELECT
        schema_name,

        CASE WHEN relkind = 'r' THEN 'Table'
             WHEN relkind = 'v' THEN 'View'
             ELSE 'Table' END AS object_type,

        object_type_name,
        trigger_name,

        regexp_replace(trigger_definition, '\s+', ' ', 'g') AS trigger_definition,

        CASE 
            WHEN (tgtype & 4) = 4 THEN 'BEFORE'
            WHEN (tgtype & 8) = 8 THEN 'AFTER'
            WHEN (tgtype & 16) = 16 THEN 'INSTEAD OF'
        END AS trigger_timing,

        CASE 
            WHEN (tgtype & 1) = 1 THEN 'ROW'
            ELSE 'STATEMENT'
        END AS trigger_level,

        CASE 
            WHEN (tgtype & 2) = 2 THEN 'INSERT'
            WHEN (tgtype & 4) = 4 THEN 'DELETE'
            WHEN (tgtype & 8) = 8 THEN 'UPDATE'
            ELSE 'UNKNOWN'
        END AS trigger_event,

        (tgenabled = 'O') AS trigger_enabled,

        trigger_function_name,
        trigger_function_arguments,

        regexp_replace(trigger_function_definition, '\s+', ' ', 'g')
        AS trigger_function_definition
    FROM trg
)

SELECT
    schema_name,
    object_type,
    object_type_name,
    trigger_name,
    trigger_definition,
    trigger_event,
    trigger_timing,
    trigger_level,
    trigger_enabled,
    trigger_function_name,
    trigger_function_arguments,
    trigger_function_definition
FROM normalized
ORDER BY schema_name, object_type_name, trigger_name;

$function$;


-- =====================================
-- fetch_view_details.sql
-- =====================================
CREATE OR REPLACE FUNCTION fetch_view_details(p_schema_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(schema_name text, view_name text, view_type text, view_definition text, base_tables text, view_owner text, dependent_objects text)
 LANGUAGE sql
AS $function$

WITH base_tables_cte AS (
    SELECT
        v_ns.nspname AS view_schema,
        v.relname    AS view_name,
        string_agg(
            DISTINCT bt_ns.nspname || '.' || bt.relname,
            ', ' ORDER BY bt_ns.nspname || '.' || bt.relname
        ) AS base_tables
    FROM pg_class v
    JOIN pg_namespace v_ns ON v.relnamespace = v_ns.oid
    JOIN pg_rewrite r ON r.ev_class = v.oid
    JOIN pg_depend d ON d.objid = r.oid
    JOIN pg_class bt ON bt.oid = d.refobjid
    JOIN pg_namespace bt_ns ON bt_ns.oid = bt.relnamespace
    WHERE v.relkind = 'v'
      AND bt.relkind = 'r'
    GROUP BY v_ns.nspname, v.relname
),

dependent_views AS (
    SELECT
        base_ns.nspname AS base_schema,
        base_v.relname  AS base_view,
        string_agg(
            DISTINCT child_ns.nspname || '.' || child_v.relname,
            ', '
        ) AS dependent_views
    FROM pg_depend d
    JOIN pg_rewrite r ON r.oid = d.objid
    JOIN pg_class base_v ON base_v.oid = r.ev_class
    JOIN pg_namespace base_ns ON base_v.relnamespace = base_ns.oid

    JOIN pg_class child_v ON child_v.oid = d.refobjid
    JOIN pg_namespace child_ns ON child_v.relnamespace = child_ns.oid

    WHERE base_v.relkind IN ('v','m')
      AND child_v.relkind IN ('v','m')
      AND base_v.oid <> child_v.oid
    GROUP BY base_ns.nspname, base_v.relname
),

instead_of_triggers AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS view_name,
        string_agg(t.tgname, ', ') AS triggers
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'v'
      AND (t.tgtype & 64) <> 0
    GROUP BY n.nspname, c.relname
)

SELECT
    v_ns.nspname::TEXT AS schema_name,
    v.relname::TEXT    AS view_name,
    'VIEW'::TEXT       AS view_type,
    pg_get_viewdef(v.oid, true)::TEXT AS view_definition,
    COALESCE(bt.base_tables, '') AS base_tables,
    pg_get_userbyid(v.relowner)::TEXT AS view_owner,

    trim(both ', ' FROM concat_ws(', ',
        CASE WHEN dv.dependent_views IS NOT NULL
             THEN 'Dependent views: ' || dv.dependent_views END,
        CASE WHEN it.triggers IS NOT NULL
             THEN 'INSTEAD OF triggers: ' || it.triggers END
    )) AS dependent_objects

FROM pg_class v
JOIN pg_namespace v_ns ON v_ns.oid = v.relnamespace
LEFT JOIN base_tables_cte bt
       ON bt.view_schema = v_ns.nspname AND bt.view_name = v.relname
LEFT JOIN dependent_views dv
       ON dv.base_schema = v_ns.nspname AND dv.base_view = v.relname
LEFT JOIN instead_of_triggers it
       ON it.schema_name = v_ns.nspname AND it.view_name = v.relname

WHERE v.relkind = 'v'
  AND v_ns.nspname NOT IN ('pg_catalog','information_schema')
  AND (
        p_schema_list IS NULL
        OR v_ns.nspname = ANY(p_schema_list)
  )
ORDER BY schema_name, view_name;

$function$;


-- =====================================
-- compute_columns_md5.sql
-- =====================================
CREATE OR REPLACE FUNCTION compute_columns_md5(p_table_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(schema_name text, object_type text, object_type_name text, object_subtype text, object_subtype_name text, object_subtype_details text, object_md5 text)
 LANGUAGE sql
AS $function$
SELECT
    gtd.schema_name,

    -- Dynamic object type (Table / View / Materialized View)
    gtd.object_type,

    gtd.object_type_name,
    'Column' AS object_subtype,
    gtd.column_name AS object_subtype_name,

    /* Normalized column definition string */
    CONCAT_WS(
        ',',
        CONCAT('data_type:', COALESCE(gtd.data_type, '')),
        CONCAT('max_length:', COALESCE(gtd.character_maximum_length::TEXT, '')),
        CONCAT('numeric_precision:', COALESCE(gtd.numeric_precision::TEXT, '')),
        CONCAT('numeric_scale:', COALESCE(gtd.numeric_scale::TEXT, '')),
        CONCAT('nullable:', COALESCE(gtd.is_nullable, '')),
        CONCAT('default_value:', COALESCE(gtd.column_default, '')),
        CONCAT('is_identity:', COALESCE(gtd.is_identity, '')),
        CONCAT('is_generated:', COALESCE(gtd.is_generated, '')),
        CONCAT('generation_expression:', COALESCE(gtd.generation_expression, '')),
        CONCAT('constraint_name:', COALESCE(gtd.constraint_name, '')),
        CONCAT('ordinal_position:', gtd.ordinal_position::TEXT)
    ) AS object_subtype_details,

    /* Stable MD5 hash */
    MD5(
        CONCAT_WS(
            ',',
            CONCAT('data_type:', COALESCE(gtd.data_type, '')),
            CONCAT('max_length:', COALESCE(gtd.character_maximum_length::TEXT, '')),
            CONCAT('numeric_precision:', COALESCE(gtd.numeric_precision::TEXT, '')),
            CONCAT('numeric_scale:', COALESCE(gtd.numeric_scale::TEXT, '')),
            CONCAT('nullable:', COALESCE(gtd.is_nullable, '')),
            CONCAT('default_value:', COALESCE(gtd.column_default, '')),
            CONCAT('is_identity:', COALESCE(gtd.is_identity, '')),
            CONCAT('is_generated:', COALESCE(gtd.is_generated, '')),
            CONCAT('generation_expression:', COALESCE(gtd.generation_expression, '')),
            CONCAT('constraint_name:', COALESCE(gtd.constraint_name, '')),
            CONCAT('ordinal_position:', gtd.ordinal_position::TEXT)
        )
    ) AS object_md5
-- Updated source function
FROM fetch_column_details(p_table_list) AS gtd

ORDER BY
    gtd.schema_name,
    gtd.object_type,
    gtd.object_type_name,
    gtd.ordinal_position;
$function$;


-- =====================================
-- compute_constraints_md5.sql
-- =====================================
CREATE OR REPLACE FUNCTION compute_constraints_md5(p_table_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(schema_name text, object_type text, object_type_name text, object_subtype text, object_subtype_name text, object_subtype_details text, object_md5 text)
 LANGUAGE sql
AS $function$

WITH raw AS (
    SELECT
        fd.schema_name,
        fd.object_type,
        fd.object_type_name,

        'Constraint' AS object_subtype,
        fd.constraint_name AS object_subtype_name,

        -- Canonical normalized constraint details
        concat_ws(
            ',',
            'constraint_type:' || COALESCE(fd.constraint_type, ''),
            'column_name:' || COALESCE(fd.column_name, ''),
            'definition:' || COALESCE(regexp_replace(fd.definition, '\s+', ' ', 'g'), '')
        ) AS object_subtype_details
    FROM fetch_constraint_details(p_table_list) fd
),

canon AS (
    SELECT
        schema_name,
        object_type,
        object_type_name,
        object_subtype,
        object_subtype_name,

        -- Normalize details before hashing
        regexp_replace(object_subtype_details, '\s+', ' ', 'g') AS normalized_details
    FROM raw
)

SELECT
    schema_name,
    object_type,
    object_type_name,
    object_subtype,
    object_subtype_name,
    normalized_details AS object_subtype_details,

    md5(
        concat_ws(
            ':',
            'schema:' || schema_name,
            'table:' || object_type_name,
            'constraint:' || object_subtype_name,
            normalized_details
        )
    ) AS object_md5

FROM canon
ORDER BY schema_name, object_type_name, object_subtype_name;
$function$;


-- =====================================
-- compute_functions_md5.sql
-- =====================================
CREATE OR REPLACE FUNCTION compute_functions_md5(p_table_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(schema_name text, object_type text, object_type_name text, object_subtype text, object_subtype_name text, object_subtype_details text, object_md5 text)
 LANGUAGE sql
AS $function$
    SELECT
        gtd.schema_name,
        'Function' AS object_type,
        gtd.function_name AS object_type_name,
        NULL AS object_subtype,
        NULL AS object_subtype_name,

        -- Build column details for tracking changes
        CONCAT_WS(
            ',',
            CONCAT('argument_types:', COALESCE(gtd.argument_types, '')),
            CONCAT('argument_modes:', COALESCE(gtd.argument_modes::TEXT, '')),
            CONCAT('return_type:', COALESCE(gtd.return_type::TEXT, '')),
            CONCAT('language:', COALESCE(gtd.language::TEXT, '')),
            CONCAT('volatility:', COALESCE(gtd.volatility, '')),
            CONCAT('parallel_safe:', COALESCE(gtd.parallel_safe, '')),
            CONCAT('owner_role:', COALESCE(gtd.owner_role, '')),
            CONCAT('privileges:', COALESCE(gtd.privileges, '')),
            CONCAT('dependencies:', COALESCE(gtd.dependencies, '')),
            CONCAT('function_body:', COALESCE(gtd.function_body, ''))
        ) AS object_subtype_details,

        -- Create MD5 hash from normalized concatenated string
        MD5(
              CONCAT_WS(
                ',',
                CONCAT('argument_types:', COALESCE(gtd.argument_types, '')),
                CONCAT('argument_modes:', COALESCE(gtd.argument_modes::TEXT, '')),
                CONCAT('return_type:', COALESCE(gtd.return_type::TEXT, '')),
                CONCAT('language:', COALESCE(gtd.language::TEXT, '')),
                CONCAT('volatility:', COALESCE(gtd.volatility, '')),
                CONCAT('parallel_safe:', COALESCE(gtd.parallel_safe, '')),
                CONCAT('owner_role:', COALESCE(gtd.owner_role, '')),
                CONCAT('privileges:', COALESCE(gtd.privileges, '')),
                CONCAT('dependencies:', COALESCE(gtd.dependencies, '')),
                CONCAT('function_body:', COALESCE(gtd.function_body, ''))
            )
        ) AS object_md5

    FROM fetch_function_details(p_table_list) AS gtd
    ORDER BY gtd.schema_name, gtd.function_name;
$function$;


-- =====================================
-- compute_indexes_md5.sql
-- =====================================
CREATE OR REPLACE FUNCTION compute_indexes_md5(p_table_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(schema_name text, object_type text, object_type_name text, object_subtype text, object_subtype_name text, object_subtype_details text, object_md5 text)
 LANGUAGE sql
AS $function$
WITH norm AS (
    SELECT
        f.schema_name,
        f.object_type,
        f.object_type_name,
        f.index_name,

        /* Normalized fields */
        COALESCE(f.tablespace,'') AS tablespace,
        COALESCE(f.indexdef,'') AS indexdef,
        COALESCE(f.is_unique::TEXT,'') AS is_unique,
        COALESCE(f.is_primary::TEXT,'') AS is_primary,
        COALESCE(f.index_columns,'') AS index_columns,
        COALESCE(f.index_predicate,'') AS index_predicate,
        COALESCE(f.access_method,'') AS access_method
    FROM fetch_index_details(p_table_list) f
)
SELECT
    schema_name,
    object_type,
    object_type_name,
    'Index' AS object_subtype,
    index_name AS object_subtype_name,

    concat_ws(
        ',',
        'tablespace:' || tablespace,
        'indexdef:' || indexdef,
        'is_unique:' || is_unique,
        'is_primary:' || is_primary,
        'index_columns:' || index_columns,
        'index_predicate:' || index_predicate,
        'access_method:' || access_method
    ) AS object_subtype_details,

    md5(
        concat_ws(
            ':',
            'tablespace:' || tablespace,
            'indexdef:' || indexdef,
            'is_unique:' || is_unique,
            'is_primary:' || is_primary,
            'index_columns:' || index_columns,
            'index_predicate:' || index_predicate,
            'access_method:' || access_method
        )
    ) AS object_md5
FROM norm
ORDER BY schema_name, object_type_name, index_name;
$function$;


-- =====================================
-- compute_materialized_views_md5.sql
-- =====================================
CREATE OR REPLACE FUNCTION compute_materialized_views_md5(p_schema_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(schema_name text, object_type text, object_type_name text, object_subtype text, object_subtype_name text, object_subtype_details text, object_md5 text)
 LANGUAGE sql
AS $function$

SELECT
    f.schema_name,
    'Materialized View' AS object_type,
    f.view_name AS object_type_name,
    NULL AS object_subtype,
    NULL AS object_subtype_name,

    concat_ws(
        ',',
        'view_type:' || f.view_type,
        'view_definition:' || f.view_definition,
        'base_tables:' || COALESCE(f.base_tables,''),
        'is_populated:' || f.is_populated::TEXT,
        'view_owner:' || f.view_owner,
        'dependent_objects:' || COALESCE(f.dependent_objects,'')
    ) AS object_subtype_details,

    md5(
        concat_ws(
            ':',
            f.view_type,
            f.view_definition,
            COALESCE(f.base_tables,''),
            f.is_populated::TEXT,
            f.view_owner,
            COALESCE(f.dependent_objects,'')
        )
    ) AS object_md5

FROM fetch_materialized_view_details(p_schema_list) f
ORDER BY f.schema_name, f.view_name;

$function$;


-- =====================================
-- compute_references_md5.sql
-- =====================================
CREATE OR REPLACE FUNCTION compute_references_md5(p_table_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(schema_name text, object_type text, object_type_name text, object_subtype text, object_subtype_name text, object_subtype_details text, object_md5 text)
 LANGUAGE sql
AS $function$
WITH fk AS (
    SELECT
        c.conname AS constraint_name,
        nsp.nspname AS schema_name,
        tbl.relname AS table_name,
        ref_tbl.relname AS target_table,
        ref_nsp.nspname AS target_schema,
        array_agg(src.attname ORDER BY src.attname) AS source_columns,
        array_agg(trg.attname ORDER BY trg.attname) AS target_columns
    FROM pg_constraint c
    JOIN pg_class tbl ON tbl.oid = c.conrelid
    JOIN pg_namespace nsp ON nsp.oid = tbl.relnamespace
    JOIN pg_class ref_tbl ON ref_tbl.oid = c.confrelid
    JOIN pg_namespace ref_nsp ON ref_nsp.oid = ref_tbl.relnamespace
    JOIN unnest(c.conkey) WITH ORDINALITY AS src_key(attnum, pos)
        ON TRUE
    JOIN pg_attribute src ON src.attrelid = tbl.oid AND src.attnum = src_key.attnum
    JOIN unnest(c.confkey) WITH ORDINALITY AS trg_key(attnum, pos)
        ON trg_key.pos = src_key.pos
    JOIN pg_attribute trg ON trg.attrelid = ref_tbl.oid AND trg.attnum = trg_key.attnum
    WHERE c.contype = 'f'
    AND (
           p_table_list IS NULL
        OR nsp.nspname = ANY(p_table_list)
    )
    GROUP BY c.conname, nsp.nspname, tbl.relname, ref_tbl.relname, ref_nsp.nspname
)

SELECT
    schema_name,
    'Table' AS object_type,
    table_name AS object_type_name,
    'Reference' AS object_subtype,
    constraint_name AS object_subtype_name,

    CONCAT(
        'source_columns:{', array_to_string(source_columns, ','), '},',
        'target_schema:', target_schema, ',',
        'target_table:', target_table, ',',
        'target_columns:{', array_to_string(target_columns, ','), '},',
        'constraint_name:', constraint_name
    ) AS object_subtype_details,

    md5(
        CONCAT(
            'src:', array_to_string(source_columns, ','),
            ' tgt:', array_to_string(target_columns, ','),
            ' tbl:', table_name,
            ' ref:', target_table,
            ' cons:', constraint_name
        )
    ) AS object_md5

FROM fk
ORDER BY schema_name, table_name, constraint_name;
$function$;


-- =====================================
-- compute_sequences_md5.sql
-- =====================================
CREATE OR REPLACE FUNCTION compute_sequences_md5(p_sequence_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(schema_name text, object_type text, object_type_name text, object_subtype text, object_subtype_name text, object_subtype_details text, object_md5 text)
 LANGUAGE sql
AS $function$
SELECT
    fs.schema_name,

    CASE 
        WHEN fs.table_name IS NOT NULL THEN 'Table'
        ELSE 'Sequence'
    END AS object_type,

    COALESCE(fs.table_name, fs.object_type_name) AS object_type_name,

    CASE WHEN fs.table_name IS NOT NULL THEN 'Sequence' ELSE NULL END AS object_subtype,
    CASE WHEN fs.table_name IS NOT NULL THEN fs.object_type_name ELSE NULL END AS object_subtype_name,

    CONCAT_WS(
        ',',
        'owned_by:' || COALESCE(fs.owned_by,''),
        'sequence_type:' || COALESCE(fs.sequence_type,''),
        'privileges:' || COALESCE(fs.privileges,''),
        'data_type:' || COALESCE(fs.data_type,''),
        'start_value:' || fs.start_value,
        'minimum_value:' || fs.minimum_value,
        'maximum_value:' || fs.maximum_value,
        'increment_by:' || fs.increment_by,
        'cycle_option:' || fs.cycle_option,
        'cache_size:' || fs.cache_size
    ) AS object_subtype_details,

    MD5(
        CONCAT_WS(
            ':',
            COALESCE(fs.owned_by,''),
            COALESCE(fs.sequence_type,''),
            COALESCE(fs.privileges,''),
            COALESCE(fs.data_type,''),
            fs.start_value,
            fs.minimum_value,
            fs.maximum_value,
            fs.increment_by,
            fs.cycle_option,
            fs.cache_size
        )
    ) AS object_md5

FROM fetch_sequence_details(p_sequence_list) fs
ORDER BY schema_name, object_type_name;
$function$;


-- =====================================
-- compute_triggers_md5.sql
-- =====================================
CREATE OR REPLACE FUNCTION compute_triggers_md5(p_table_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(schema_name text, object_type text, object_type_name text, object_subtype text, object_subtype_name text, object_subtype_details text, object_md5 text)
 LANGUAGE sql
AS $function$

WITH src AS (
    SELECT
        schema_name,
        object_type,
        object_type_name,
        trigger_name AS object_subtype_name,
        
        -- Build normalized detail string
        CONCAT_WS(
            ',',
            'trigger_definition:' || COALESCE(trigger_definition, ''),
            'trigger_event:' || COALESCE(trigger_event, ''),
            'trigger_timing:' || COALESCE(trigger_timing, ''),
            'trigger_level:' || COALESCE(trigger_level, ''),
            'trigger_enabled:' || trigger_enabled::TEXT,
            'trigger_function_name:' || COALESCE(trigger_function_name, ''),
            'trigger_function_arguments:' || COALESCE(trigger_function_arguments, ''),
            'trigger_function_definition:' || COALESCE(trigger_function_definition, '')
        ) AS normalized_details

    FROM fetch_trigger_details(p_table_list)
),

normalized AS (
    SELECT
        schema_name,
        object_type,
        object_type_name,
        'Trigger' AS object_subtype,
        object_subtype_name,

        -- Further normalize by collapsing whitespace
        regexp_replace(normalized_details, '\s+', ' ', 'g') AS object_subtype_details

    FROM src
),

final_md5 AS (
    SELECT
        schema_name,
        object_type,
        object_type_name,
        object_subtype,
        object_subtype_name,
        object_subtype_details,

        -- MD5 of normalized content only
        md5(
            CONCAT_WS(
                '||',
                schema_name,
                object_type,
                object_type_name,
                object_subtype,
                object_subtype_name,
                object_subtype_details
            )
        ) AS object_md5

    FROM normalized
)

SELECT *
FROM final_md5
ORDER BY schema_name, object_type_name, object_subtype_name;

$function$;


-- =====================================
-- compute_views_md5.sql
-- =====================================
CREATE OR REPLACE FUNCTION compute_views_md5(p_table_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(schema_name text, object_type text, object_type_name text, object_subtype text, object_subtype_name text, object_subtype_details text, object_md5 text)
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        gtd.schema_name,
        'View' AS object_type,
        gtd.view_name AS object_type_name,
        NULL::TEXT AS object_subtype,
        NULL::TEXT AS object_subtype_name,

        concat_ws(
            ',',
            'view_type:' || COALESCE(gtd.view_type, ''),
            'view_definition:' || COALESCE(gtd.view_definition, ''),
            'base_tables:' || COALESCE(gtd.base_tables, ''),
           -- 'is_materialized:' || COALESCE(gtd.is_materialized::TEXT, ''),
           -- 'is_ populated:' || COALESCE(gtd.is_populated::TEXT, ''),
            -- 'last_refresh_time:' || COALESCE(gtd.last_refresh_time::TEXT,
            'view_owner:' || COALESCE(gtd.view_owner, ''),
            'dependent_objects:' || COALESCE(gtd.dependent_objects, '')
        ) AS object_subtype_details,

        md5(
            concat_ws(
                ':',
                'view_type:' || COALESCE(gtd.view_type, ''),
                'view_definition:' || COALESCE(gtd.view_definition, ''),
                'base_tables:' || COALESCE(gtd.base_tables, ''),
                -- 'is_materialized:' || COALESCE(gtd.is_materialized::TEXT, ''),
                -- 'is_ populated:' || COALESCE(gtd.is_populated::TEXT, ''),
                -- 'last_refresh_time:' || COALESCE(gtd.last_refresh_time::TEXT,
                'view_owner:' || COALESCE(gtd.view_owner, ''),
                'dependent_objects:' || COALESCE(gtd.dependent_objects, '')
            )
        ) AS object_md5

    FROM fetch_view_details(p_table_list) gtd
    ORDER BY gtd.schema_name, gtd.view_name;
END;
$function$;


-- =====================================
-- load_md5_metadata_staging_non_table_objects.sql
-- =====================================
CREATE OR REPLACE FUNCTION load_md5_metadata_staging_non_table_objects(p_table_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(metadata_id bigint, snapshot_id integer, schema_name text, object_type text, object_type_name text, object_subtype text, object_subtype_name text, object_subtype_details text, object_md5 text, processed_time timestamp without time zone)
 LANGUAGE sql
AS $function$
    -- Extremely fast check: read directly from the already-populated staging partition
    SELECT 
        s.metadata_id, s.snapshot_id, s.schema_name, s.object_type, s.object_type_name,
        s.object_subtype, s.object_subtype_name, s.object_subtype_details, s.object_md5,
        s.processed_time
    FROM metadata_md5_staging_non_table_objects s
    WHERE s.snapshot_id = (SELECT max(ms.snapshot_id) FROM metadata_snapshot ms);
$function$;

-- =====================================
-- load_md5_metadata_staging_table_objects.sql
-- =====================================
CREATE OR REPLACE FUNCTION load_md5_metadata_staging_table_objects(p_table_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(metadata_id bigint, snapshot_id integer, schema_name text, object_type text, object_type_name text, object_subtype text, object_subtype_name text, object_subtype_details text, object_md5 text, processed_time timestamp without time zone)
 LANGUAGE sql
AS $function$
    -- Extremely fast check: read directly from the already-populated staging partition
    SELECT 
        s.metadata_id, s.snapshot_id, s.schema_name, s.object_type, s.object_type_name,
        s.object_subtype, s.object_subtype_name, s.object_subtype_details, s.object_md5,
        s.processed_time
    FROM metadata_md5_staging_table_objects s
    WHERE s.snapshot_id = (SELECT max(ms.snapshot_id) FROM metadata_snapshot ms);
$function$;

-- =====================================
-- load_md5_metadata_table.sql
-- =====================================

CREATE OR REPLACE FUNCTION load_md5_metadata_table(p_table_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(metadata_id bigint, snapshot_id integer, schema_name text, object_type text, object_type_name text, object_subtype text, object_subtype_name text, object_subtype_details text, object_md5 text, processed_time timestamp without time zone, change_type text)
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_snapshot_id integer;
    v_processed_time timestamp;
BEGIN
    -- 1. Get the latest snapshot ID and current time using table alias qualification to avoid PL/pgSQL ambiguity
    SELECT max(ms.snapshot_id) INTO v_snapshot_id FROM metadata_snapshot ms;
    v_processed_time := clock_timestamp();

    -- Ensure the target partitions exist before we perform any inserts
    PERFORM create_staging_partitions(v_snapshot_id);

    -- 2. Scan ALL 9 catalogs EXACTLY ONCE and cache inside session temp table
    CREATE TEMP TABLE temp_current_metadata AS
    SELECT DISTINCT * FROM compute_columns_md5(p_table_list)
    UNION ALL SELECT DISTINCT * FROM compute_constraints_md5(p_table_list)
    UNION ALL SELECT DISTINCT * FROM compute_indexes_md5(p_table_list)
    UNION ALL SELECT DISTINCT * FROM compute_references_md5(p_table_list)
    UNION ALL SELECT DISTINCT * FROM compute_triggers_md5(p_table_list)
    UNION ALL SELECT DISTINCT * FROM compute_sequences_md5(p_table_list)
    UNION ALL SELECT DISTINCT * FROM compute_functions_md5(p_table_list)
    UNION ALL SELECT DISTINCT * FROM compute_views_md5(p_table_list)
    UNION ALL SELECT DISTINCT * FROM compute_materialized_views_md5(p_table_list);

    -- 3. Bulk insert the table-related subset into staging
    INSERT INTO metadata_md5_staging_table_objects (
        snapshot_id, schema_name, object_type, object_type_name,
        object_subtype, object_subtype_name, object_subtype_details, object_md5
    )
    SELECT
        v_snapshot_id, c.schema_name, c.object_type, c.object_type_name,
        c.object_subtype, c.object_subtype_name, c.object_subtype_details, c.object_md5
    FROM temp_current_metadata c
    WHERE c.object_subtype IN ('Column', 'Constraint', 'Index', 'Reference', 'Trigger')
       OR (c.object_type = 'Table' AND c.object_subtype = 'Sequence');

    -- 4. Bulk insert the non-table-related subset into staging
    INSERT INTO metadata_md5_staging_non_table_objects (
        snapshot_id, schema_name, object_type, object_type_name,
        object_subtype, object_subtype_name, object_subtype_details, object_md5
    )
    SELECT
        v_snapshot_id, c.schema_name, c.object_type, COALESCE(c.object_type_name, ''),
        c.object_subtype, c.object_subtype_name, c.object_subtype_details, c.object_md5
    FROM temp_current_metadata c
    WHERE c.object_type IN ('Function', 'View', 'Materialized View')
       OR c.object_type = 'Sequence';

    -- 5. Bulk insert all records into changes table and return them
    RETURN QUERY
    WITH inserted_changes AS (
        INSERT INTO metadata_md5_changes (
            snapshot_id, schema_name, object_type, object_type_name,
            object_subtype, object_subtype_name, object_subtype_details,
            object_md5, change_type
        )
        SELECT
            v_snapshot_id, c.schema_name, c.object_type, c.object_type_name,
            c.object_subtype, c.object_subtype_name, c.object_subtype_details,
            c.object_md5, 'ADDED'
        FROM temp_current_metadata c
        RETURNING metadata_md5_changes.metadata_id, metadata_md5_changes.snapshot_id, metadata_md5_changes.schema_name, metadata_md5_changes.object_type, metadata_md5_changes.object_type_name,
                  metadata_md5_changes.object_subtype, metadata_md5_changes.object_subtype_name, metadata_md5_changes.object_subtype_details,
                  metadata_md5_changes.object_md5, metadata_md5_changes.change_type
    )
    SELECT
        i.metadata_id, i.snapshot_id, i.schema_name, i.object_type, i.object_type_name,
        i.object_subtype, i.object_subtype_name, i.object_subtype_details, i.object_md5,
        v_processed_time, i.change_type
    FROM inserted_changes i;

    -- 6. Clean up temp table
    DROP TABLE temp_current_metadata;
END;
$function$;

-- =====================================
-- load_metadata_md5_metrics.sql
-- =====================================
CREATE OR REPLACE FUNCTION load_metadata_md5_metrics()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_snapshot_id INT;
    v_total_changes INT;
BEGIN
    ----------------------------------------------------------------------
    -- Always get latest snapshot from metadata_snapshot
    ----------------------------------------------------------------------
    SELECT MAX(snapshot_id)
    INTO v_snapshot_id
    FROM metadata_snapshot;

    IF v_snapshot_id IS NULL THEN
        RAISE NOTICE 'No snapshot found.';
        RETURN;
    END IF;


    ----------------------------------------------------------------------
    --  Get TOTAL CHANGES using a local CTE
    ----------------------------------------------------------------------
    WITH changes_all AS (
        SELECT *
        FROM metadata_md5_changes
        WHERE snapshot_id = v_snapshot_id
          AND change_type <> 'UNCHANGED'
    )
    SELECT COUNT(*) INTO v_total_changes FROM changes_all;


    ----------------------------------------------------------------------
    --  Insert BASE METRICS (Schemas Monitored, Schemas With Changes, Total Ops)
    ----------------------------------------------------------------------
    WITH
    schemas_all AS (
        SELECT DISTINCT schema_name
        FROM metadata_md5_staging_table_objects
        WHERE snapshot_id = v_snapshot_id
        UNION
        SELECT DISTINCT schema_name
        FROM metadata_md5_staging_non_table_objects
        WHERE snapshot_id = v_snapshot_id
    ),
    changes_all AS (
        SELECT *
        FROM metadata_md5_changes
        WHERE snapshot_id = v_snapshot_id
          AND change_type <> 'UNCHANGED'
    )
    INSERT INTO metadata_md5_metrics
        (snapshot_id, metric_name, metric_value, metrics_time)
    SELECT
        v_snapshot_id,
        metric_name,
        metric_value,
        clock_timestamp()
    FROM (
        SELECT 'Schemas Monitored' AS metric_name,
               (SELECT COUNT(*) FROM schemas_all)::int AS metric_value
        UNION ALL
        SELECT 'Schemas With Changes',
               (SELECT COUNT(DISTINCT schema_name) FROM changes_all)
        UNION ALL
        SELECT 'Total Change Operations',
               v_total_changes
    ) AS base_metrics
    ON CONFLICT (snapshot_id, metric_name)
    DO UPDATE SET
        metric_value = EXCLUDED.metric_value,
        metrics_time = EXCLUDED.metrics_time;


    ----------------------------------------------------------------------
    -- If NO changes → STOP HERE
    ----------------------------------------------------------------------
    IF v_total_changes = 0 THEN
        RAISE NOTICE 'No changes detected → Only base metrics inserted.';
        RETURN;
    END IF;


    ----------------------------------------------------------------------
    -- Insert ALL REMAINING METRICS (only when changes exist)
    ----------------------------------------------------------------------
    WITH
    changes_all AS (
        SELECT *,
               UPPER(object_type) AS obj_type,
               UPPER(COALESCE(object_subtype,'')) AS obj_subtype
        FROM metadata_md5_changes
        WHERE snapshot_id = v_snapshot_id
          AND change_type <> 'UNCHANGED'
    )
    INSERT INTO metadata_md5_metrics
        (snapshot_id, metric_name, metric_value, metrics_time)

    SELECT
        v_snapshot_id,
        metric_name,
        metric_value,
        clock_timestamp()
    FROM (

        ------------------------------------------------------------------
        -- TABLE-LEVEL METRICS
        ------------------------------------------------------------------
        SELECT 'Tables With Changes',
               COUNT(DISTINCT object_type_name)
        FROM changes_all
        WHERE obj_type = 'TABLE'

        UNION ALL SELECT 'Tables Added',
               COUNT(DISTINCT object_type_name)
        FROM changes_all
        WHERE obj_type = 'TABLE' AND change_type = 'ADDED'

        UNION ALL SELECT 'Tables Modified',
               COUNT(DISTINCT object_type_name)
        FROM changes_all
        WHERE obj_type = 'TABLE' AND change_type = 'MODIFIED'

        UNION ALL SELECT 'Tables Dropped',
               COUNT(DISTINCT object_type_name)
        FROM changes_all
        WHERE obj_type = 'TABLE' AND change_type = 'DELETED'

        UNION ALL SELECT 'Tables Renamed',
               COUNT(DISTINCT object_type_name)
        FROM changes_all
        WHERE obj_type = 'TABLE' AND change_type = 'RENAMED'


        ------------------------------------------------------------------
        -- OBJECT-LEVEL METRICS
        ------------------------------------------------------------------
        UNION ALL
        SELECT cfg.metric_label,
               COUNT(*) AS metric_value
        FROM changes_all src
        JOIN (
            VALUES
                ('Columns Added', NULL, 'COLUMN', 'ADDED'),
                ('Columns Modified', NULL, 'COLUMN', 'MODIFIED'),
                ('Columns Dropped', NULL, 'COLUMN', 'DELETED'),
                ('Columns Renamed', NULL, 'COLUMN', 'RENAMED'),

                ('Constraints Added', NULL, 'CONSTRAINT', 'ADDED'),
                ('Constraints Modified', NULL, 'CONSTRAINT', 'MODIFIED'),
                ('Constraints Dropped', NULL, 'CONSTRAINT', 'DELETED'),
                ('Constraints Renamed', NULL, 'CONSTRAINT', 'RENAMED'),

                ('Indexes Added', NULL, 'INDEX', 'ADDED'),
                ('Indexes Modified', NULL, 'INDEX', 'MODIFIED'),
                ('Indexes Dropped', NULL, 'INDEX', 'DELETED'),
                ('Indexes Renamed', NULL, 'INDEX', 'RENAMED'),

                ('References Added', NULL, 'REFERENCE', 'ADDED'),
                ('References Modified', NULL, 'REFERENCE', 'MODIFIED'),
                ('References Dropped', NULL, 'REFERENCE', 'DELETED'),
                ('References Renamed', NULL, 'REFERENCE', 'RENAMED'),

                ('Triggers Added', NULL, 'TRIGGER', 'ADDED'),
                ('Triggers Modified', NULL, 'TRIGGER', 'MODIFIED'),
                ('Triggers Dropped', NULL, 'TRIGGER', 'DELETED'),
                ('Triggers Renamed', NULL, 'TRIGGER', 'RENAMED'),

                ('Functions Added', 'FUNCTION', NULL, 'ADDED'),
                ('Functions Modified', 'FUNCTION', NULL, 'MODIFIED'),
                ('Functions Dropped', 'FUNCTION', NULL, 'DELETED'),
                ('Functions Renamed', 'FUNCTION', NULL, 'RENAMED'),

                ('Views Added', 'VIEW', NULL, 'ADDED'),
                ('Views Modified', 'VIEW', NULL, 'MODIFIED'),
                ('Views Dropped', 'VIEW', NULL, 'DELETED'),
                ('Views Renamed', 'VIEW', NULL, 'RENAMED'),

                ('Materialized Views Added', 'MATERIALIZED VIEW', NULL, 'ADDED'),
                ('Materialized Views Modified', 'MATERIALIZED VIEW', NULL, 'MODIFIED'),
                ('Materialized Views Dropped', 'MATERIALIZED VIEW', NULL, 'DELETED'),
                ('Materialized Views Renamed', 'MATERIALIZED VIEW', NULL, 'RENAMED'),

                ('Table Sequences Added', 'TABLE', 'SEQUENCE', 'ADDED'),
                ('Table Sequences Modified', 'TABLE', 'SEQUENCE', 'MODIFIED'),
                ('Table Sequences Dropped', 'TABLE', 'SEQUENCE', 'DELETED'),
                ('Table Sequences Renamed', 'TABLE', 'SEQUENCE', 'RENAMED'),

                ('Schema Sequences Added', 'SEQUENCE', NULL, 'ADDED'),
                ('Schema Sequences Modified', 'SEQUENCE', NULL, 'MODIFIED'),
                ('Schema Sequences Dropped', 'SEQUENCE', NULL, 'DELETED'),
                ('Schema Sequences Renamed', 'SEQUENCE', NULL, 'RENAMED')
        ) cfg(metric_label, cfg_type, cfg_subtype, cfg_change)
        ON (
            src.change_type = cfg_change
            AND (
                (cfg_type IS NOT NULL AND src.obj_type = cfg_type AND src.obj_subtype = COALESCE(cfg_subtype,''))
                OR
                (cfg_subtype IS NOT NULL AND src.obj_subtype = cfg_subtype)
            )
        )
        GROUP BY cfg.metric_label

    ) AS detail_metrics(metric_name, metric_value)

    ON CONFLICT (snapshot_id, metric_name)
    DO UPDATE SET
        metric_value = EXCLUDED.metric_value,
        metrics_time = EXCLUDED.metrics_time;

END;
$function$;


-- =====================================
-- load_snapshot_table.sql
-- =====================================
CREATE OR REPLACE FUNCTION load_snapshot_table()
 RETURNS TABLE(snapshot_id integer, snapshot_name text, processed_time timestamp without time zone)
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_next_id integer;
    v_name text;
    v_time timestamp;
BEGIN
    -- Calculate the next sequential snapshot_id based on actual rows in the table
    SELECT COALESCE(MAX(ms.snapshot_id), 0) + 1 INTO v_next_id FROM metadata_snapshot ms;
    
    v_name := CONCAT_WS('_',
        'snapshot',
        v_next_id,
        TO_CHAR(clock_timestamp(), 'YYYY_MM_DD_HH24MISS')
    );
    
    INSERT INTO metadata_snapshot (snapshot_id, snapshot_name)
    OVERRIDING SYSTEM VALUE
    VALUES (v_next_id, v_name)
    RETURNING metadata_snapshot.snapshot_id, metadata_snapshot.snapshot_name, metadata_snapshot.processed_time
    INTO v_next_id, v_name, v_time;

    -- Reset the sequence so it stays in sync with the table's max ID
    PERFORM setval(pg_get_serial_sequence('metadata_snapshot', 'snapshot_id'), v_next_id, true);

    RETURN QUERY SELECT v_next_id, v_name, v_time;
END;
$function$;


-- =====================================
-- compare_load_md5_non_table_metadata.sql
-- =====================================
CREATE OR REPLACE FUNCTION compare_load_md5_non_table_metadata(p_function_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(metadata_id bigint, snapshot_id integer, schema_name text, object_type text, object_type_name text, object_subtype text, object_subtype_name text, object_subtype_details text, object_md5 text, processed_time timestamp without time zone, change_type text)
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_snapshot_id integer;
    v_prev_snapshot_id integer;
    v_processed_time timestamp;
BEGIN
    -- 1. Fetch current snapshot and previous snapshot identifiers using table alias qualification to avoid PL/pgSQL ambiguity
    SELECT max(ms.snapshot_id) INTO v_snapshot_id FROM metadata_snapshot ms;
    SELECT max(ms.snapshot_id) INTO v_prev_snapshot_id FROM metadata_snapshot ms WHERE ms.snapshot_id < v_snapshot_id;
    v_processed_time := clock_timestamp();

    -- Ensure the target partitions exist before we perform any inserts
    PERFORM create_staging_partitions(v_snapshot_id);

    -- 2. Scan current non-table-related catalogs EXACTLY ONCE
    CREATE TEMP TABLE temp_current_non_table_metadata AS
    SELECT DISTINCT * FROM compute_functions_md5(p_function_list)
    UNION ALL SELECT DISTINCT * FROM compute_views_md5(p_function_list)
    UNION ALL SELECT DISTINCT * FROM compute_materialized_views_md5(p_function_list)
    UNION ALL SELECT DISTINCT * FROM compute_sequences_md5(p_function_list) seq WHERE seq.object_type = 'Sequence';

    -- 3. Bulk push the scanned metadata to staging for the current snapshot
    INSERT INTO metadata_md5_staging_non_table_objects (
        snapshot_id, schema_name, object_type, object_type_name,
        object_subtype, object_subtype_name, object_subtype_details, object_md5
    )
    SELECT
        v_snapshot_id, c.schema_name, c.object_type, COALESCE(c.object_type_name, ''),
        c.object_subtype, c.object_subtype_name, c.object_subtype_details, c.object_md5
    FROM temp_current_non_table_metadata c;

    -- 4. Perform the collision-safe comparison against the previous snapshot staging partition
    RETURN QUERY
    WITH current_objects_raw AS (
        SELECT c.schema_name, c.object_type, c.object_type_name,
               c.object_subtype, c.object_subtype_name,
               c.object_subtype_details, c.object_md5
        FROM temp_current_non_table_metadata c
    ),
    staging_objects_raw AS (
        SELECT s.schema_name, s.object_type, s.object_type_name,
               s.object_subtype, s.object_subtype_name,
               s.object_subtype_details, s.object_md5
        FROM metadata_md5_staging_non_table_objects s
        WHERE s.snapshot_id = v_prev_snapshot_id
    ),
    staging_md5_counts AS (
        SELECT s.schema_name, s.object_type, s.object_md5, COUNT(*) AS md5_count
        FROM staging_objects_raw s
        GROUP BY s.schema_name, s.object_type, s.object_md5
    ),
    current_md5_counts AS (
        SELECT c.schema_name, c.object_type, c.object_md5, COUNT(*) AS md5_count
        FROM current_objects_raw c
        GROUP BY c.schema_name, c.object_type, c.object_md5
    ),
    staging_name_counts AS (
        SELECT s.schema_name, s.object_type, s.object_type_name, COUNT(*) AS name_count
        FROM staging_objects_raw s
        GROUP BY s.schema_name, s.object_type, s.object_type_name
    ),
    current_name_counts AS (
        SELECT c.schema_name, c.object_type, c.object_type_name, COUNT(*) AS name_count
        FROM current_objects_raw c
        GROUP BY c.schema_name, c.object_type, c.object_type_name
    ),
    staging_objects AS (
        SELECT s.*, coalesce(sm.md5_count,0) AS staging_md5_count, coalesce(sn.name_count,0) AS staging_name_count
        FROM staging_objects_raw s
        LEFT JOIN staging_md5_counts sm ON sm.schema_name = s.schema_name AND sm.object_type = s.object_type AND sm.object_md5 = s.object_md5
        LEFT JOIN staging_name_counts sn ON sn.schema_name = s.schema_name AND sn.object_type = s.object_type AND sn.object_type_name = s.object_type_name
    ),
    current_objects AS (
        SELECT c.*, coalesce(cm.md5_count,0) AS current_md5_count, coalesce(cn.name_count,0) AS current_name_count
        FROM current_objects_raw c
        LEFT JOIN current_md5_counts cm ON cm.schema_name = c.schema_name AND cm.object_type = c.object_type AND cm.object_md5 = c.object_md5
        LEFT JOIN current_name_counts cn ON cn.schema_name = c.schema_name AND cn.object_type = c.object_type AND cn.object_type_name = c.object_type_name
    ),
    renamed_objects AS (
        SELECT
            c.schema_name, c.object_type, c.object_type_name,
            c.object_subtype, c.object_subtype_name, c.object_subtype_details, c.object_md5,
            'RENAMED'::text AS change_type,
            s.object_type_name AS prev_object_type_name
        FROM current_objects c
        JOIN staging_objects s
          ON s.schema_name = c.schema_name
         AND s.object_type = c.object_type
         AND s.object_md5 = c.object_md5
         AND s.object_type_name <> c.object_type_name
        WHERE c.current_md5_count = 1
          AND s.staging_md5_count = 1
          AND NOT EXISTS (
            SELECT 1 FROM staging_objects sx
            WHERE sx.schema_name = c.schema_name
              AND sx.object_type = c.object_type
              AND sx.object_type_name = c.object_type_name
              AND sx.object_md5 = c.object_md5
          )
    ),
    modified_objects AS (
        SELECT
            c.schema_name, c.object_type, c.object_type_name,
            c.object_subtype, c.object_subtype_name, c.object_subtype_details, c.object_md5,
            'MODIFIED'::text AS change_type,
            c.object_type_name AS prev_object_type_name
        FROM current_objects c
        JOIN staging_objects s
          ON s.schema_name = c.schema_name
         AND s.object_type = c.object_type
         AND s.object_type_name = c.object_type_name
        WHERE s.object_md5 <> c.object_md5
          AND c.current_name_count = 1
          AND s.staging_name_count = 1
    ),
    processed_objects AS (
        SELECT r.schema_name, r.object_type, r.object_type_name FROM renamed_objects r
        UNION
        SELECT r.schema_name, r.object_type, r.prev_object_type_name AS object_type_name FROM renamed_objects r
        UNION
        SELECT m.schema_name, m.object_type, m.object_type_name FROM modified_objects m
    ),
    added_objects AS (
        SELECT
            c.schema_name, c.object_type, c.object_type_name,
            c.object_subtype, c.object_subtype_name, c.object_subtype_details, c.object_md5,
            'ADDED'::text AS change_type,
            NULL::TEXT AS prev_object_type_name
        FROM current_objects c
        WHERE NOT EXISTS (
            SELECT 1 FROM staging_objects s
            WHERE s.schema_name = c.schema_name
              AND s.object_type = c.object_type
              AND s.object_type_name = c.object_type_name
        )
        AND NOT EXISTS (
            SELECT 1 FROM processed_objects p
            WHERE p.schema_name = c.schema_name
              AND p.object_type = c.object_type
              AND p.object_type_name = c.object_type_name
        )
    ),
    deleted_objects AS (
        SELECT
            s.schema_name, s.object_type, s.object_type_name,
            s.object_subtype, s.object_subtype_name, s.object_subtype_details, s.object_md5,
            'DELETED'::text AS change_type,
            s.object_type_name AS prev_object_type_name
        FROM staging_objects s
        WHERE NOT EXISTS (
            SELECT 1 FROM current_objects c
            WHERE c.schema_name = s.schema_name
              AND c.object_type = s.object_type
              AND c.object_type_name = s.object_type_name
        )
        AND NOT EXISTS (
            SELECT 1 FROM processed_objects p
            WHERE p.schema_name = s.schema_name
              AND p.object_type = s.object_type
              AND p.object_type_name = s.object_type_name
        )
    ),
    unified_changes AS (
        SELECT r.schema_name, r.object_type, r.object_type_name, r.object_subtype, r.object_subtype_name, r.object_subtype_details, r.object_md5, r.change_type, r.prev_object_type_name FROM renamed_objects r
        UNION ALL SELECT m.schema_name, m.object_type, m.object_type_name, m.object_subtype, m.object_subtype_name, m.object_subtype_details, m.object_md5, m.change_type, m.prev_object_type_name FROM modified_objects m
        UNION ALL SELECT a.schema_name, a.object_type, a.object_type_name, a.object_subtype, a.object_subtype_name, a.object_subtype_details, a.object_md5, a.change_type, a.prev_object_type_name FROM added_objects a
        UNION ALL SELECT d.schema_name, d.object_type, d.object_type_name, d.object_subtype, d.object_subtype_name, d.object_subtype_details, d.object_md5, d.change_type, d.prev_object_type_name FROM deleted_objects d
    ),
    inserted_changes AS (
        INSERT INTO metadata_md5_changes (
            snapshot_id, schema_name, object_type, object_type_name,
            object_subtype, object_subtype_name, object_subtype_details,
            object_md5, change_type
        )
        SELECT
            v_snapshot_id, u.schema_name, u.object_type, u.object_type_name,
            u.object_subtype, u.object_subtype_name, u.object_subtype_details,
            u.object_md5, u.change_type
        FROM unified_changes u
        RETURNING metadata_md5_changes.metadata_id, metadata_md5_changes.snapshot_id, metadata_md5_changes.schema_name, metadata_md5_changes.object_type, metadata_md5_changes.object_type_name,
                  metadata_md5_changes.object_subtype, metadata_md5_changes.object_subtype_name, metadata_md5_changes.object_subtype_details, metadata_md5_changes.object_md5, metadata_md5_changes.change_type
    )
    SELECT
        i.metadata_id, i.snapshot_id, i.schema_name, i.object_type, i.object_type_name,
        i.object_subtype, i.object_subtype_name, i.object_subtype_details, i.object_md5,
        v_processed_time, i.change_type
    FROM inserted_changes i;

    -- 5. Clean up temporary table
    DROP TABLE temp_current_non_table_metadata;
END;
$function$;

-- =====================================
-- compare_load_md5_table_metadata.sql
-- =====================================
CREATE OR REPLACE FUNCTION compare_load_md5_table_metadata(p_table_list text[] DEFAULT NULL::text[])
 RETURNS TABLE(metadata_id bigint, snapshot_id integer, schema_name text, object_type text, object_type_name text, object_subtype text, object_subtype_name text, object_subtype_details text, object_md5 text, processed_time timestamp without time zone, change_type text)
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_snapshot_id integer;
    v_prev_snapshot_id integer;
    v_processed_time timestamp;
BEGIN
    -- 1. Fetch current snapshot and previous snapshot identifiers using table alias qualification to avoid PL/pgSQL ambiguity
    SELECT max(ms.snapshot_id) INTO v_snapshot_id FROM metadata_snapshot ms;
    SELECT max(ms.snapshot_id) INTO v_prev_snapshot_id FROM metadata_snapshot ms WHERE ms.snapshot_id < v_snapshot_id;
    v_processed_time := clock_timestamp();

    -- Ensure the target partitions exist before we perform any inserts
    PERFORM create_staging_partitions(v_snapshot_id);

    -- 2. Scan current table-related catalogs EXACTLY ONCE
    CREATE TEMP TABLE temp_current_table_metadata AS
    SELECT DISTINCT * FROM compute_columns_md5(p_table_list)
    UNION ALL SELECT DISTINCT * FROM compute_constraints_md5(p_table_list)
    UNION ALL SELECT DISTINCT * FROM compute_indexes_md5(p_table_list)
    UNION ALL SELECT DISTINCT * FROM compute_references_md5(p_table_list)
    UNION ALL SELECT DISTINCT * FROM compute_triggers_md5(p_table_list)
    UNION ALL SELECT DISTINCT * FROM compute_sequences_md5(p_table_list) seq WHERE seq.object_type = 'Table';

    -- 3. Bulk push the scanned metadata to staging for the current snapshot
    INSERT INTO metadata_md5_staging_table_objects (
        snapshot_id, schema_name, object_type, object_type_name,
        object_subtype, object_subtype_name, object_subtype_details, object_md5
    )
    SELECT
        v_snapshot_id, c.schema_name, c.object_type, c.object_type_name,
        c.object_subtype, c.object_subtype_name, c.object_subtype_details, c.object_md5
    FROM temp_current_table_metadata c;

    -- 4. Perform the comparison against the previous snapshot staging partition
    RETURN QUERY
    WITH staging_md5 AS (
        SELECT 
            s.schema_name, s.object_type, s.object_type_name,
            s.object_subtype, s.object_subtype_name, s.object_subtype_details, s.object_md5
        FROM metadata_md5_staging_table_objects s
        WHERE s.snapshot_id = v_prev_snapshot_id
    ),
    renamed_objects AS (
        SELECT
            c.schema_name, c.object_type, c.object_type_name,
            c.object_subtype, c.object_subtype_name, c.object_subtype_details, c.object_md5,
            'RENAMED'::text AS change_type
        FROM temp_current_table_metadata c
        JOIN staging_md5 s
          ON  s.schema_name = c.schema_name
          AND s.object_type = c.object_type
          AND s.object_type_name = c.object_type_name
          AND s.object_subtype = c.object_subtype
          AND s.object_md5 = c.object_md5
          AND s.object_subtype_name <> c.object_subtype_name
    ),
    modified_objects AS (
        SELECT
            c.schema_name, c.object_type, c.object_type_name,
            c.object_subtype, c.object_subtype_name, c.object_subtype_details, c.object_md5,
            'MODIFIED'::text AS change_type
        FROM temp_current_table_metadata c
        JOIN staging_md5 s
          ON  s.schema_name = c.schema_name
          AND s.object_type = c.object_type
          AND s.object_type_name = c.object_type_name
          AND s.object_subtype = c.object_subtype
          AND s.object_subtype_name = c.object_subtype_name
        WHERE s.object_md5 <> c.object_md5
    ),
    processed_objects AS (
        SELECT r.schema_name, r.object_type, r.object_type_name, r.object_subtype, r.object_subtype_name
        FROM renamed_objects r
        UNION
        SELECT m.schema_name, m.object_type, m.object_type_name, m.object_subtype, m.object_subtype_name
        FROM modified_objects m
    ),
    added_objects AS (
        SELECT
            c.schema_name, c.object_type, c.object_type_name,
            c.object_subtype, c.object_subtype_name, c.object_subtype_details, c.object_md5,
            'ADDED'::text AS change_type
        FROM temp_current_table_metadata c
        WHERE NOT EXISTS (
            SELECT 1 FROM staging_md5 s
            WHERE s.schema_name      = c.schema_name
              AND s.object_type      = c.object_type
              AND s.object_type_name = c.object_type_name
              AND s.object_subtype   = c.object_subtype
              AND s.object_subtype_name = c.object_subtype_name
        )
        AND NOT EXISTS (
            SELECT 1 FROM processed_objects p
            WHERE p.schema_name      = c.schema_name
              AND p.object_type      = c.object_type
              AND p.object_type_name = c.object_type_name
              AND p.object_subtype   = c.object_subtype
              AND p.object_subtype_name = c.object_subtype_name
        )
    ),
    deleted_objects AS (
        SELECT
            s.schema_name, s.object_type, s.object_type_name,
            s.object_subtype, s.object_subtype_name, s.object_subtype_details, s.object_md5,
            'DELETED'::text AS change_type
        FROM staging_md5 s
        WHERE NOT EXISTS (
            SELECT 1 FROM temp_current_table_metadata c
            WHERE c.schema_name      = s.schema_name
              AND c.object_type      = s.object_type
              AND c.object_type_name = s.object_type_name
              AND c.object_subtype   = s.object_subtype
              AND c.object_subtype_name = s.object_subtype_name
        )
        AND NOT EXISTS (
            SELECT 1 FROM processed_objects p
            WHERE p.schema_name      = s.schema_name
              AND p.object_type      = s.object_type
              AND p.object_type_name = s.object_type_name
              AND p.object_subtype   = s.object_subtype
              AND p.object_subtype_name = s.object_subtype_name
        )
    ),
    unified_changes AS (
        SELECT r.schema_name, r.object_type, r.object_type_name, r.object_subtype, r.object_subtype_name, r.object_subtype_details, r.object_md5, r.change_type FROM renamed_objects r
        UNION ALL SELECT m.schema_name, m.object_type, m.object_type_name, m.object_subtype, m.object_subtype_name, m.object_subtype_details, m.object_md5, m.change_type FROM modified_objects m
        UNION ALL SELECT a.schema_name, a.object_type, a.object_type_name, a.object_subtype, a.object_subtype_name, a.object_subtype_details, a.object_md5, a.change_type FROM added_objects a
        UNION ALL SELECT d.schema_name, d.object_type, d.object_type_name, d.object_subtype, d.object_subtype_name, d.object_subtype_details, d.object_md5, d.change_type FROM deleted_objects d
    ),
    inserted_changes AS (
        INSERT INTO metadata_md5_changes (
            snapshot_id, schema_name, object_type, object_type_name,
            object_subtype, object_subtype_name, object_subtype_details,
            object_md5, change_type
        )
        SELECT
            v_snapshot_id, u.schema_name, u.object_type, u.object_type_name,
            u.object_subtype, u.object_subtype_name, u.object_subtype_details,
            u.object_md5, u.change_type
        FROM unified_changes u
        RETURNING metadata_md5_changes.metadata_id, metadata_md5_changes.snapshot_id, metadata_md5_changes.schema_name, metadata_md5_changes.object_type, metadata_md5_changes.object_type_name,
                  metadata_md5_changes.object_subtype, metadata_md5_changes.object_subtype_name, metadata_md5_changes.object_subtype_details,
                  metadata_md5_changes.object_md5, metadata_md5_changes.change_type
    )
    SELECT
        i.metadata_id, i.snapshot_id, i.schema_name, i.object_type, i.object_type_name,
        i.object_subtype, i.object_subtype_name, i.object_subtype_details, i.object_md5,
        v_processed_time, i.change_type
    FROM inserted_changes i;

    -- 5. Clean up temporary table
    DROP TABLE temp_current_table_metadata;
END;
$function$;

-- =====================================
-- process_metadata_md5_changes.sql
-- =====================================
CREATE OR REPLACE FUNCTION process_metadata_md5_changes(p_schemas text[], p_mode text DEFAULT 'INCLUDE'::text)
 RETURNS TABLE(step_name text, result text, duration interval)
 LANGUAGE plpgsql
AS $function$
DECLARE
    is_initial_run BOOLEAN;
    effective_schemas TEXT[];
    step_start TIMESTAMP;
    step_duration INTERVAL;
    v_snapshot_id INT;
    v_prev_snapshot_id INT;
BEGIN
    -- Schema resolution
    IF upper(coalesce(p_mode,'INCLUDE')) = 'EXCLUDE' THEN
        SELECT array_agg(schema_name) INTO effective_schemas
        FROM information_schema.schemata
        WHERE schema_name NOT LIKE 'pg_%'
          AND schema_name <> 'information_schema'
          AND schema_name <> current_schema()
          AND schema_name <> ALL(p_schemas);
        effective_schemas := COALESCE(effective_schemas, ARRAY[]::text[]);
    ELSE
        effective_schemas := COALESCE(p_schemas, ARRAY[]::text[]);
    END IF;

    SELECT NOT EXISTS (SELECT 1 FROM metadata_snapshot) INTO is_initial_run;

    IF is_initial_run THEN
        step_name := 'mode'; result := 'Initial Load'; duration := '0'::interval;
        RETURN NEXT;

        -- Clean any orphan data
        step_start := clock_timestamp();
        step_name := 'clean_initial_tables';
        DELETE FROM metadata_md5_changes;
        DELETE FROM metadata_snapshot;
        DELETE FROM metadata_md5_metrics;
        result := 'Completed';
        duration := clock_timestamp() - step_start;
        RETURN NEXT;

        -- Load snapshot and get new ID
        step_start := clock_timestamp();
        step_name := 'load_snapshot_table';
        PERFORM load_snapshot_table();
        SELECT MAX(snapshot_id) INTO v_snapshot_id FROM metadata_snapshot;
        result := 'Completed (snapshot_id: ' || v_snapshot_id || ')';
        duration := clock_timestamp() - step_start;
        RETURN NEXT;

        -- Create partition for this snapshot
        step_start := clock_timestamp();
        step_name := 'create_staging_partition';
        PERFORM create_staging_partitions(v_snapshot_id);
        result := 'Created partition _p' || v_snapshot_id;
        duration := clock_timestamp() - step_start;
        RETURN NEXT;

        -- Load metadata
        step_start := clock_timestamp();
        step_name := 'load_md5_metadata_table';
        PERFORM load_md5_metadata_table(effective_schemas);
        result := 'Completed';
        duration := clock_timestamp() - step_start;
        RETURN NEXT;

        -- Load staging (table) - inserts go into new partition automatically
        step_start := clock_timestamp();
        step_name := 'load_md5_metadata_staging_table_objects';
        PERFORM load_md5_metadata_staging_table_objects(effective_schemas);
        result := 'Completed';
        duration := clock_timestamp() - step_start;
        RETURN NEXT;

        -- Load staging (non-table) - inserts go into new partition automatically
        step_start := clock_timestamp();
        step_name := 'load_md5_metadata_staging_non_table_objects';
        PERFORM load_md5_metadata_staging_non_table_objects(effective_schemas);
        result := 'Completed';
        duration := clock_timestamp() - step_start;
        RETURN NEXT;

        -- Load metrics
        step_start := clock_timestamp();
        step_name := 'load_metadata_md5_metrics';
        PERFORM load_metadata_md5_metrics();
        result := 'Completed';
        duration := clock_timestamp() - step_start;
        RETURN NEXT;

        RETURN;
    END IF;

    -- ==========================================
    -- SUBSEQUENT RUNS (Partition Rotation)
    -- ==========================================
    step_name := 'mode'; result := 'Subsequent Compare Run'; duration := '0'::interval;
    RETURN NEXT;

    -- Get current (previous) snapshot for reference
    SELECT MAX(snapshot_id) INTO v_prev_snapshot_id FROM metadata_snapshot;

    -- Load new snapshot
    step_start := clock_timestamp();
    step_name := 'load_snapshot_table';
    PERFORM load_snapshot_table();
    SELECT MAX(snapshot_id) INTO v_snapshot_id FROM metadata_snapshot;
    result := 'Completed (snapshot_id: ' || v_snapshot_id || ')';
    duration := clock_timestamp() - step_start;
    RETURN NEXT;

    -- Compare against PREVIOUS partition (still attached, readable)
    step_start := clock_timestamp();
    step_name := 'compare_load_md5_table_metadata';
    PERFORM compare_load_md5_table_metadata(effective_schemas);
    result := 'Completed';
    duration := clock_timestamp() - step_start;
    RETURN NEXT;

    step_start := clock_timestamp();
    step_name := 'compare_load_md5_non_table_metadata';
    PERFORM compare_load_md5_non_table_metadata(effective_schemas);
    result := 'Completed';
    duration := clock_timestamp() - step_start;
    RETURN NEXT;

    -- Create NEW partition for current snapshot
    step_start := clock_timestamp();
    step_name := 'create_staging_partition';
    PERFORM create_staging_partitions(v_snapshot_id);
    result := 'Created partition _p' || v_snapshot_id;
    duration := clock_timestamp() - step_start;
    RETURN NEXT;

    -- Load staging into NEW partition
    step_start := clock_timestamp();
    step_name := 'load_md5_metadata_staging_table_objects';
    PERFORM load_md5_metadata_staging_table_objects(effective_schemas);
    result := 'Completed';
    duration := clock_timestamp() - step_start;
    RETURN NEXT;

    step_start := clock_timestamp();
    step_name := 'load_md5_metadata_staging_non_table_objects';
    PERFORM load_md5_metadata_staging_non_table_objects(effective_schemas);
    result := 'Completed';
    duration := clock_timestamp() - step_start;
    RETURN NEXT;

    -- DETACH + DROP old partitions (instant, zero-lock cleanup)
    step_start := clock_timestamp();
    step_name := 'drop_old_partitions';
    PERFORM drop_old_staging_partitions(v_snapshot_id);
    result := 'Dropped partitions except _p' || v_snapshot_id;
    duration := clock_timestamp() - step_start;
    RETURN NEXT;

    -- Load metrics
    step_start := clock_timestamp();
    step_name := 'load_metadata_md5_metrics';
    PERFORM load_metadata_md5_metrics();
    result := 'Completed';
    duration := clock_timestamp() - step_start;
    RETURN NEXT;

END;
$function$;

-- ==========================================
-- Auto-vacuum settings for heavy-write tables
-- ==========================================

-- ALTER TABLE metadata_md5_changes SET (
--     autovacuum_vacuum_scale_factor = 0.05,
--     autovacuum_analyze_scale_factor = 0.02
-- );

-- ALTER TABLE metadata_md5_staging_table_objects SET (
--     autovacuum_vacuum_scale_factor = 0.1,
--     autovacuum_analyze_scale_factor = 0.05
-- );

-- ALTER TABLE metadata_md5_staging_non_table_objects SET (
--     autovacuum_vacuum_scale_factor = 0.1,
--     autovacuum_analyze_scale_factor = 0.05
-- );

-- ==========================================
-- END
-- ==========================================

COMMIT;