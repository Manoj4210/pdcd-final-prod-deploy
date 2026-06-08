​rpx=> select pid,query,state from rpx_dba.get_pg_stat_activity() where usename = 'tds_etl_app';
-[ RECORD 1 ]--------------------------------------------------------------------------------------------------------------------------------
pid   | 959003
query |                                                                                                                                      +
      |                                                                                                                                      +
      |             WITH effective_schemas AS (                                                                                              +
      |               SELECT COALESCE(array_agg(schema_name), ARRAY[]::text[]) AS schemas                                                    +
      |               FROM information_schema.schemata                                                                                       +
      |               WHERE schema_name NOT LIKE 'pg_%'                                                                                      +
      |                 AND schema_name <> 'information_schema'                                                                              +
      |                 AND schema_name <> current_schema()                                                                                  +
      |             )                                                                                                                        +
      |                                                                                                                                      +
      |             SELECT count(*)                                                                                                          +
      |             FROM effective_schemas, load_md5_metadata_table(effective_schemas.schemas);                                              +
      |
state | active
-[ RECORD 2 ]--------------------------------------------------------------------------------------------------------------------------------
pid   | 957773
query | select data_quality.us_pat_assignee_assignor_normalization();
state | active
-[ RECORD 3 ]--------------------------------------------------------------------------------------------------------------------------------
pid   | 961205
query | SELECT core.update_alias_ent_details_alias_role(p_alias_id:='16935592',p_alias_roles:='{59}');
state | active
-[ RECORD 4 ]--------------------------------------------------------------------------------------------------------------------------------
pid   | 959317
query |                                                                                                                                      +
      | WITH col_constraints AS (                                                                                                            +
      |     -- Pre-aggregate constraint names by table and column attribute number                                                           +
      |     SELECT                                                                                                                           +
      |         con.conrelid,                                                                                                                +
      |         col_num AS attnum,                                                                                                           +
      |         string_agg(DISTINCT con.conname, ',' ORDER BY con.conname) AS constraint_name                                                +
      |     FROM pg_constraint con                                                                                                           +
      |     CROSS JOIN LATERAL unnest(con.conkey) AS col_num                                                                                 +
      |     GROUP BY con.conrelid, col_num                                                                                                   +
      | )                                                                                                                                    +
      | SELECT                                                                                                                               +
      |     ns.nspname AS schema_name,                                                                                                       +
      |                                                                                                                                      +
      |     CASE                                                                                                                             +
      |         WHEN cls.relkind = 'r' THEN 'Table'                                                                                          +
      |         WHEN cls.relkind = 'v' THEN 'View'                                                                                           +
      |         WHEN cls.relkind = 'm' THEN 'Materialized View'                                                                              +
      |         ELSE 'Other'                                                                                                                 +
      |     END AS object_type,                                                                                                              +
      |                                                                                                                                      +
      |     cls.relname AS object_type_name,                                                                                                 +
      |     att.attname AS column_name,                                                                                                      +
      |                                                                                                                                      +
      |     CASE typ.typname                                                                                                                 +
      |         WHEN 'varchar'   THEN 'character varying'                                                                                    +
      |         WHEN 'bpchar'    THEN 'character'                                                                                            +
      |         WHEN 'int4'      THEN 'integer'                                                                                              +
      |         WHEN 'int8'      THEN 'bigint'                                                                                               +
      |         WHEN 'int2'      THEN 'smallint'                                                                                             +
      |         WHEN 'float4'    THEN 'real'                                                                                                 +
      |         WHEN 'float8'    THEN 'double precision'                                                                                     +
      |         WHEN 'bool'      THEN 'boolean'                                                                                              +
      |         WHEN 'timestamptz' THEN 'timestamp with time zone'                                                                           +
      |         WHEN 'timestamp'   THEN 'timestamp without time zone'                                                                        +
      |         WHEN 'timetz'      THEN 'time with time zone'                                                                                +
      |         WHEN 'time'        THEN 'time without time zone'                                                                             +
      |         ELSE typ.typname                                                                                                             +
      |     END AS data_type,                                                                                                                +
      |                                                                                                                                      +
      |     CASE                                                                                                                             +
      |         WHEN typ.typname IN ('varchar','bpchar')                                                                                     +
      |             THEN att.atttypmod - 4                                                                                                   +
      |         ELSE NULL                                                                                                                    +
      |     END AS character_maximum_length,                                                                                                 +
      |                                                                                                                                      +
      |     CASE                                                                                                                             +
      |         WHEN typ.typname = 'numeric'                                                                                                 +
      |             THEN ((att.atttypmod - 4) >> 16) & 65535                                                                                 +
      |         WHEN typ.typname = 'int2' THEN 16                                                                                            +
      |         WHEN typ.typname = 'int4' THEN 32                                                                                            +
      |         WHEN typ.typname = 'int8' THEN 64                                                                                            +
      |         WHEN typ.typname = 'float4' THEN 24                                                                                          +
      |         WHEN typ.typname = 'float8' THEN 53                                                                                          +
      |         ELSE NULL                                                                                                                    +
      |     END AS numeric_precision,                                                                                                        +
      |                                                                                                                                      +
      |     CASE                                                                                                                             +
      |         WHEN typ.typname = 'numeric'                                                                                                 +
      |             THEN (att.atttypmod - 4) & 65535                                                                                         +
      |         WHEN typ.typname IN ('int2','int4','int8','float4','float8')                                                                 +
      |             THEN 0                                                                                                                   +
      |         ELSE NULL                                                                                                                    +
      |     END AS numeric_scale,                                                                                                            +
      |
state | active
-[ RECORD 5 ]--------------------------------------------------------------------------------------------------------------------------------
pid   | 959316
query |                                                                                                                                      +
      | WITH col_constraints AS (                                                                                                            +
      |     -- Pre-aggregate constraint names by table and column attribute number                                                           +
      |     SELECT                                                                                                                           +
      |         con.conrelid,                                                                                                                +
      |         col_num AS attnum,                                                                                                           +
      |         string_agg(DISTINCT con.conname, ',' ORDER BY con.conname) AS constraint_name                                                +
      |     FROM pg_constraint con                                                                                                           +
      |     CROSS JOIN LATERAL unnest(con.conkey) AS col_num                                                                                 +
      |     GROUP BY con.conrelid, col_num                                                                                                   +
      | )                                                                                                                                    +
      | SELECT                                                                                                                               +
      |     ns.nspname AS schema_name,                                                                                                       +
      |                                                                                                                                      +
      |     CASE                                                                                                                             +
      |         WHEN cls.relkind = 'r' THEN 'Table'                                                                                          +
      |         WHEN cls.relkind = 'v' THEN 'View'                                                                                           +
      |         WHEN cls.relkind = 'm' THEN 'Materialized View'                                                                              +
      |         ELSE 'Other'                                                                                                                 +
      |     END AS object_type,                                                                                                              +
      |                                                                                                                                      +
      |     cls.relname AS object_type_name,                                                                                                 +
      |     att.attname AS column_name,                                                                                                      +
      |                                                                                                                                      +
      |     CASE typ.typname                                                                                                                 +
      |         WHEN 'varchar'   THEN 'character varying'                                                                                    +
      |         WHEN 'bpchar'    THEN 'character'                                                                                            +
      |         WHEN 'int4'      THEN 'integer'                                                                                              +
      |         WHEN 'int8'      THEN 'bigint'                                                                                               +
      |         WHEN 'int2'      THEN 'smallint'                                                                                             +
      |         WHEN 'float4'    THEN 'real'                                                                                                 +
      |         WHEN 'float8'    THEN 'double precision'                                                                                     +
      |         WHEN 'bool'      THEN 'boolean'                                                                                              +
      |         WHEN 'timestamptz' THEN 'timestamp with time zone'                                                                           +
      |         WHEN 'timestamp'   THEN 'timestamp without time zone'                                                                        +
      |         WHEN 'timetz'      THEN 'time with time zone'                                                                                +
      |         WHEN 'time'        THEN 'time without time zone'                                                                             +
      |         ELSE typ.typname                                                                                                             +
      |     END AS data_type,                                                                                                                +
      |                                                                                                                                      +
      |     CASE                                                                                                                             +
      |         WHEN typ.typname IN ('varchar','bpchar')                                                                                     +
      |             THEN att.atttypmod - 4                                                                                                   +
      |         ELSE NULL                                                                                                                    +
      |     END AS character_maximum_length,                                                                                                 +
      |                                                                                                                                      +
      |     CASE                                                                                                                             +
      |         WHEN typ.typname = 'numeric'                                                                                                 +
      |             THEN ((att.atttypmod - 4) >> 16) & 65535                                                                                 +
      |         WHEN typ.typname = 'int2' THEN 16                                                                                            +
      |         WHEN typ.typname = 'int4' THEN 32                                                                                            +
      |         WHEN typ.typname = 'int8' THEN 64                                                                                            +
      |         WHEN typ.typname = 'float4' THEN 24                                                                                          +
      |         WHEN typ.typname = 'float8' THEN 53                                                                                          +
      |         ELSE NULL                                                                                                                    +
      |     END AS numeric_precision,                                                                                                        +
      |                                                                                                                                      +
      |     CASE                                                                                                                             +
      |         WHEN typ.typname = 'numeric'                                                                                                 +
      |             THEN (att.atttypmod - 4) & 65535                                                                                         +
      |         WHEN typ.typname IN ('int2','int4','int8','float4','float8')                                                                 +
      |             THEN 0                                                                                                                   +
      |         ELSE NULL                                                                                                                    +
      |     END AS numeric_scale,                                                                                                            +
      |
state | active
-[ RECORD 6 ]--------------------------------------------------------------------------------------------------------------------------------
pid   | 959318
query |                                                                                                                                      +
      | WITH col_constraints AS (                                                                                                            +
      |     -- Pre-aggregate constraint names by table and column attribute number                                                           +
      |     SELECT                                                                                                                           +
      |         con.conrelid,                                                                                                                +
      |         col_num AS attnum,                                                                                                           +
      |         string_agg(DISTINCT con.conname, ',' ORDER BY con.conname) AS constraint_name                                                +
      |     FROM pg_constraint con                                                                                                           +
      |     CROSS JOIN LATERAL unnest(con.conkey) AS col_num                                                                                 +
      |     GROUP BY con.conrelid, col_num                                                                                                   +
      | )                                                                                                                                    +
      | SELECT                                                                                                                               +
      |     ns.nspname AS schema_name,                                                                                                       +
      |                                                                                                                                      +
      |     CASE                                                                                                                             +
      |         WHEN cls.relkind = 'r' THEN 'Table'                                                                                          +
      |         WHEN cls.relkind = 'v' THEN 'View'                                                                                           +
      |         WHEN cls.relkind = 'm' THEN 'Materialized View'                                                                              +
      |         ELSE 'Other'                                                                                                                 +
      |     END AS object_type,                                                                                                              +
      |                                                                                                                                      +
      |     cls.relname AS object_type_name,                                                                                                 +
      |     att.attname AS column_name,                                                                                                      +
      |                                                                                                                                      +
      |     CASE typ.typname                                                                                                                 +
      |         WHEN 'varchar'   THEN 'character varying'                                                                                    +
      |         WHEN 'bpchar'    THEN 'character'                                                                                            +
      |         WHEN 'int4'      THEN 'integer'                                                                                              +
      |         WHEN 'int8'      THEN 'bigint'                                                                                               +
      |         WHEN 'int2'      THEN 'smallint'                                                                                             +
      |         WHEN 'float4'    THEN 'real'                                                                                                 +
      |         WHEN 'float8'    THEN 'double precision'                                                                                     +
      |         WHEN 'bool'      THEN 'boolean'                                                                                              +
      |         WHEN 'timestamptz' THEN 'timestamp with time zone'                                                                           +
      |         WHEN 'timestamp'   THEN 'timestamp without time zone'                                                                        +
      |         WHEN 'timetz'      THEN 'time with time zone'                                                                                +
      |         WHEN 'time'        THEN 'time without time zone'                                                                             +
      |         ELSE typ.typname                                                                                                             +
      |     END AS data_type,                                                                                                                +
      |                                                                                                                                      +
      |     CASE                                                                                                                             +
      |         WHEN typ.typname IN ('varchar','bpchar')                                                                                     +
      |             THEN att.atttypmod - 4                                                                                                   +
      |         ELSE NULL                                                                                                                    +
      |     END AS character_maximum_length,                                                                                                 +
      |                                                                                                                                      +
      |     CASE                                                                                                                             +
      |         WHEN typ.typname = 'numeric'                                                                                                 +
      |             THEN ((att.atttypmod - 4) >> 16) & 65535                                                                                 +
      |         WHEN typ.typname = 'int2' THEN 16                                                                                            +
      |         WHEN typ.typname = 'int4' THEN 32                                                                                            +
      |         WHEN typ.typname = 'int8' THEN 64                                                                                            +
      |         WHEN typ.typname = 'float4' THEN 24                                                                                          +
      |         WHEN typ.typname = 'float8' THEN 53                                                                                          +
      |         ELSE NULL                                                                                                                    +
      |     END AS numeric_precision,                                                                                                        +
      |                                                                                                                                      +
      |     CASE                                                                                                                             +
      |         WHEN typ.typname = 'numeric'                                                                                                 +
      |             THEN (att.atttypmod - 4) & 65535                                                                                         +
      |         WHEN typ.typname IN ('int2','int4','int8','float4','float8')                                                                 +
      |             THEN 0                                                                                                                   +
      |         ELSE NULL                                                                                                                    +
      |     END AS numeric_scale,                                                                                                            +
      |
state | active
-[ RECORD 7 ]--------------------------------------------------------------------------------------------------------------------------------
pid   | 935425
query | update core.unpublished_app_related_documents d                                                                                      +
      | set patnum_for_app_num = t.patnum                                                                                                    +
      | from (                                                                                                                               +
      |  select rd.id, p.patnum                                                                                                              +
      |  from core.unpublished_app_related_documents rd, pair.app_data a, core.pats p                                                        +
      |  where rd.created_at > now() - interval '14 days' and patnum_for_app_num || '' is null and a.app_num = rd.app_num and p.id = a.pat_id+
      |        and rd.relation_type in ('child', 'parent')                                                                                   +
      | ) t                                                                                                                                  +
      | where t.id = d.id                                                                                                                    +
      | ;
state | active
-[ RECORD 8 ]--------------------------------------------------------------------------------------------------------------------------------
pid   | 667357
query | select ent_id,ultimate_parent_id,alias_id from core.alias_ent_details where alias_id = 28139548;
state | idle
-[ RECORD 9 ]--------------------------------------------------------------------------------------------------------------------------------
pid   | 959319
query |                                                                                                                                      +
      | WITH col_constraints AS (                                                                                                            +
      |     -- Pre-aggregate constraint names by table and column attribute number                                                           +
      |     SELECT                                                                                                                           +
      |         con.conrelid,                                                                                                                +
      |         col_num AS attnum,                                                                                                           +
      |         string_agg(DISTINCT con.conname, ',' ORDER BY con.conname) AS constraint_name                                                +
      |     FROM pg_constraint con                                                                                                           +
      |     CROSS JOIN LATERAL unnest(con.conkey) AS col_num                                                                                 +
      |     GROUP BY con.conrelid, col_num                                                                                                   +
      | )                                                                                                                                    +
      | SELECT                                                                                                                               +
      |     ns.nspname AS schema_name,                                                                                                       +
      |                                                                                                                                      +
      |     CASE                                                                                                                             +
      |         WHEN cls.relkind = 'r' THEN 'Table'                                                                                          +
      |         WHEN cls.relkind = 'v' THEN 'View'                                                                                           +
      |         WHEN cls.relkind = 'm' THEN 'Materialized View'                                                                              +
      |         ELSE 'Other'                                                                                                                 +
      |     END AS object_type,                                                                                                              +
      |                                                                                                                                      +
      |     cls.relname AS object_type_name,                                                                                                 +
      |     att.attname AS column_name,                                                                                                      +
      |                                                                                                                                      +
      |     CASE typ.typname                                                                                                                 +
      |         WHEN 'varchar'   THEN 'character varying'                                                                                    +
      |         WHEN 'bpchar'    THEN 'character'                                                                                            +
      |         WHEN 'int4'      THEN 'integer'                                                                                              +
      |         WHEN 'int8'      THEN 'bigint'                                                                                               +
      |         WHEN 'int2'      THEN 'smallint'                                                                                             +
      |         WHEN 'float4'    THEN 'real'                                                                                                 +
      |         WHEN 'float8'    THEN 'double precision'                                                                                     +
      |         WHEN 'bool'      THEN 'boolean'                                                                                              +
      |         WHEN 'timestamptz' THEN 'timestamp with time zone'                                                                           +
      |         WHEN 'timestamp'   THEN 'timestamp without time zone'                                                                        +
      |         WHEN 'timetz'      THEN 'time with time zone'                                                                                +
      |         WHEN 'time'        THEN 'time without time zone'                                                                             +
      |         ELSE typ.typname                                                                                                             +
      |     END AS data_type,                                                                                                                +
      |                                                                                                                                      +
      |     CASE                                                                                                                             +
      |         WHEN typ.typname IN ('varchar','bpchar')                                                                                     +
      |             THEN att.atttypmod - 4                                                                                                   +
      |         ELSE NULL                                                                                                                    +
      |     END AS character_maximum_length,                                                                                                 +
      |                                                                                                                                      +
      |     CASE                                                                                                                             +
      |         WHEN typ.typname = 'numeric'                                                                                                 +
      |             THEN ((att.atttypmod - 4) >> 16) & 65535                                                                                 +
      |         WHEN typ.typname = 'int2' THEN 16                                                                                            +
      |         WHEN typ.typname = 'int4' THEN 32                                                                                            +
      |         WHEN typ.typname = 'int8' THEN 64                                                                                            +
      |         WHEN typ.typname = 'float4' THEN 24                                                                                          +
      |         WHEN typ.typname = 'float8' THEN 53                                                                                          +
      |         ELSE NULL                                                                                                                    +
      |     END AS numeric_precision,                                                                                                        +
      |                                                                                                                                      +
      |     CASE                                                                                                                             +
      |         WHEN typ.typname = 'numeric'                                                                                                 +
      |             THEN (att.atttypmod - 4) & 65535                                                                                         +
      |         WHEN typ.typname IN ('int2','int4','int8','float4','float8')                                                                 +
      |             THEN 0                                                                                                                   +
      |         ELSE NULL                                                                                                                    +
      |     END AS numeric_scale,                                                                                                            +
      |
state | active