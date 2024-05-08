-- This function must be the first call right after the CREATE EXTENSION.

CREATE OR REPLACE
FUNCTION @extschema@.set_tier_config(bucket_name TEXT, access_key TEXT, secret_key TEXT, region TEXT)
  RETURNS boolean
  LANGUAGE plpgsql
AS $function$
DECLARE
  current_user text;
  server_name text := 'pg_tier_s3_srv';
  server_user text := 'mapping';
  ret boolean;
BEGIN
-- Fetch current user
  SELECT
    CURRENT_USER INTO current_user;

-- Create s3_fdw_server
  EXECUTE 'CREATE SERVER ' || server_name || ' FOREIGN DATA WRAPPER parquet_s3_fdw';

--Create mapping user
  EXECUTE 'CREATE USER MAPPING FOR public SERVER ' || server_name ||
    ' OPTIONS (user '|| chr(39) || access_key || chr(39) ||
    ', password ' || chr(39) || secret_key || chr(39) ||
    ', region ' || chr(39) || region || chr(39) || ')';

--Populate server_credential tier catalog table
  INSERT INTO
    @extschema@.server_credential(
        created_on,
        user_name,
        bucket,
        access_key,
        secret_key,
        region,
        fdw_server_name,
        fdw_server_user_created
    )
  VALUES
    (
        now(),
        current_user,
        bucket_name,
        access_key,
        secret_key,
        region,
        server_name,
        TRUE
    );

--Check if bucket exists in S3 otherwise create the top level bucket
  SELECT
    @extschema@.create_top_level_bucket(bucket_name, access_key, secret_key, region) INTO ret;
  RETURN ret;
END;
$function$;

-- User interface function to convert a regular or partition table
-- into foreign table
CREATE OR REPLACE
FUNCTION @extschema@.enable(relation regclass)
  RETURNS boolean
  LANGUAGE plpgsql
AS $function$
DECLARE
  qualified_tab_name text;
  tab_name text;
  tab_spacename text;
  tab_spacename_oid oid;
  tab_relkind char;
  tab_relispartition boolean;
  tab_inhparent_oid oid := NULL;
  tab_new_name text;
  tier_tab_ddl text;
  tier_tab_oid oid;
  tier_tab_spacename text;
  tier_tab_spacename_oid oid;
  tier_tab_relkind char;
  tier_tab_relispartition boolean;
  tier_tab_partitionbound text;
  aws_access_key text;
  aws_secret_key text;
  aws_region text;
  aws_bucket_name text;
  server_name text;
BEGIN

-- Check if source is a parent
  PERFORM * FROM pg_inherits WHERE inhparent = relation LIMIT 1;

  IF FOUND THEN
    RAISE object_not_in_prerequisite_state USING MESSAGE = 'Parent of an Inheritence are not qualified for tiering';
    RETURN FALSE;
  END IF;

-- Fetch source table catalog info.
-- What are we trying to figure out?
-- > qualified name
-- > table kind regular or partition
  SELECT
    format('%s.%s', n.nspname, c.relname),
    n.nspname,
    c.relname,
    c.relnamespace,
    c.relkind,
    c.relispartition,
    pg_get_expr(c.relpartbound, c.oid)
    INTO qualified_tab_name,
    tab_spacename,
    tab_name,
    tab_spacename_oid,
    tab_relkind,
    tab_relispartition,
    tier_tab_partitionbound
    FROM pg_class c
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = relation
    AND c.relkind IN ('r', 'p');

-- RAISE NOTICE 'tab_name: %, tab_spacename = %', tab_name, tab_spacename;
  IF NOT FOUND THEN
    RAISE no_data_found USING MESSAGE = 'Table doesnot exists or not a regular or partition table';
    RETURN FALSE;
  END IF;

-- Check if tiering is not enabled
  PERFORM * FROM @extschema@.source
    WHERE src_oid = relation;

  IF FOUND THEN
    RAISE object_not_in_prerequisite_state USING MESSAGE = 'Tiering is already enabled';
    RETURN FALSE;
  END IF;

  IF tab_relispartition THEN
    SELECT inhparent INTO tab_inhparent_oid FROM pg_inherits WHERE inhrelid = relation;
  END IF;

-- Fetch cloud credentials
  SELECT
    bucket,
    access_key,
    secret_key,
    region,
    fdw_server_name INTO
    aws_bucket_name,
    aws_access_key,
    aws_secret_key,
    aws_region,
    server_name
  FROM @extschema@.server_credential
  WHERE cred_id = 1;

  IF NOT FOUND THEN
    RAISE object_not_in_prerequisite_state
     USING MESSAGE = 'No Object Store Credentials Found',
     HINT = 'To set credentials call set_tier_config() function';
    RETURN FALSE;
  END IF;

-- Rename current table to _old
  SELECT
    CONCAT(tab_name, '_old') INTO tab_new_name;

  SELECT
    @extschema@.gen_foreign_table_ddl(tab_spacename, tab_name, tab_name, aws_bucket_name, server_name)
    INTO tier_tab_ddl;

-- Debug notice
 --RAISE NOTICE 'NEW FOREIGN TABLE DDL = %', tier_tab_ddl;

  EXECUTE 'ALTER TABLE ' || qualified_tab_name || ' RENAME TO ' || tab_new_name;

  EXECUTE tier_tab_ddl;

-- Insert tier source table details into book keeping.
  INSERT INTO
    @extschema@.source
    (
      src_oid,
      src_relnamespace,
      src_inhparent,
      src_orig_relname,
      src_new_relname,
      src_state,
      src_enabled,
      src_dropped,
      src_created_on
    )
  VALUES
    (
      relation,
      tab_spacename_oid,
      tab_inhparent_oid,
      tab_name,
      tab_new_name,
      'SRC_TABLE_RENAMED',
      TRUE,
      FALSE,
      now()
    );

-- Fetch target table catalog data
  SELECT
    c.oid,
    c.relnamespace
    INTO
    tier_tab_oid,
    tier_tab_spacename_oid
    FROM
    pg_class c
    WHERE
    c.relname = tab_name AND
    c.relnamespace = tab_spacename_oid;

-- Insert tier into table details in target book keeping
  INSERT INTO
    @extschema@.target
    (
      tgt_oid,
      tgt_relname,
      tgt_relnamespace,
      tgt_src_oid,
      tgt_tier_state,
      tgt_ddl,
      tgt_tier_dir,
      tgt_src_partition_bound,
      tgt_created_on
    )
    VALUES
    (
      tier_tab_oid,
      tab_name,
      tier_tab_spacename_oid,
      relation,
      'FDW_TABLE_CREATED',
      tier_tab_ddl,
      aws_bucket_name || '/' || tab_spacename || '_' || tab_name,
      tier_tab_partitionbound,
      now()
    );

-- Debug notice
  RAISE NOTICE 'Original Table = % Renamed to = %', qualified_tab_name, tab_new_name;

  RETURN TRUE;
END;
$function$;

-- Internal Function to generate foreign table ddl from regular table
CREATE
OR REPLACE FUNCTION @extschema@.gen_foreign_table_ddl (
  p_schema_name CHARACTER VARYING,
  p_table_name CHARACTER VARYING,
  p_foreign_table_name CHARACTER VARYING,
  p_bucket_name CHARACTER VARYING,
  p_fdw_server_name CHARACTER VARYING
) RETURNS SETOF TEXT AS
$function$
DECLARE
v_table_ddl text;
server_options text := 'server ' || p_fdw_server_name || ' options' || '(dirname ' || chr(39) || 's3://' || p_bucket_name || '/' || p_schema_name || '_' || p_foreign_table_name || '/' || chr(39) || ');';
column_record record;
table_rec record;
constraint_rec record;
firstrec boolean;
BEGIN

FOR table_rec IN
SELECT
    c.relname,
    c.oid
FROM
    pg_catalog.pg_class c
    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE
    c.relkind IN ('r', 'p')
    AND n.nspname = p_schema_name
    AND c.relname = p_table_name
ORDER BY
    c.relname LOOP FOR column_record IN
SELECT
    b.nspname AS schema_name,
    b.relname AS table_name,
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_type,
    CASE
        WHEN (
            SELECT
                substring(
                    pg_catalog.pg_get_expr(d.adbin, d.adrelid) FOR 128
                )
            FROM
                pg_catalog.pg_attrdef d
            WHERE
                d.adrelid = a.attrelid
                AND d.adnum = a.attnum
                AND a.atthasdef
        ) IS NOT NULL THEN 'DEFAULT ' || (
            SELECT
                substring(
                    pg_catalog.pg_get_expr(d.adbin, d.adrelid) FOR 128
                )
            FROM
                pg_catalog.pg_attrdef d
            WHERE
                d.adrelid = a.attrelid
                AND d.adnum = a.attnum
                AND a.atthasdef
        )
        ELSE ''
    END AS column_default_value,
    CASE
        WHEN a.attnotnull = TRUE THEN 'NOT NULL'
        ELSE 'NULL'
    END AS column_not_null,
    a.attnum AS attnum,
    e.max_attnum AS max_attnum
FROM
    pg_catalog.pg_attribute a
    INNER JOIN (
        SELECT
            c.oid,
            n.nspname,
            c.relname
        FROM
            pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE
            c.oid = table_rec.oid
        ORDER BY
            2,
            3
    ) b ON a.attrelid = b.oid
    INNER JOIN (
        SELECT
            a.attrelid,
            max(a.attnum) AS max_attnum
        FROM
            pg_catalog.pg_attribute a
        WHERE
            a.attnum > 0
            AND NOT a.attisdropped
        GROUP BY
            a.attrelid
    ) e ON a.attrelid = e.attrelid
WHERE
    a.attnum > 0
    AND NOT a.attisdropped
ORDER BY
    a.attnum LOOP IF column_record.attnum = 1 THEN v_table_ddl := 'CREATE FOREIGN TABLE ' || column_record.schema_name || '.' || p_foreign_table_name || ' (';
ELSE v_table_ddl := v_table_ddl || ',';
END IF;
IF column_record.attnum <= column_record.max_attnum THEN v_table_ddl := v_table_ddl || chr(10) || '    ' || column_record.column_name || ' ' || column_record.column_type || ' ' || column_record.column_default_value || ' ' || 'OPTIONS(key ' || chr(39) || TRUE || chr(39) || ') ' || column_record.column_not_null;
END IF;
END LOOP;
--      firstrec := TRUE;
--      FOR constraint_rec IN
--          SELECT conname, pg_get_constraintdef(c.oid) as constrainddef
--              FROM pg_constraint c
--                  WHERE conrelid=(
--                      SELECT attrelid FROM pg_attribute
--                      WHERE attrelid = (
--                          SELECT oid FROM pg_class WHERE relname = table_rec.relname
--                              AND relnamespace = (SELECT ns.oid FROM pg_namespace ns WHERE ns.nspname = p_schema_name)
--                      ) AND attname='tableoid'
--                  )
--      LOOP
--          v_table_ddl:=v_table_ddl||','||chr(10);
--          v_table_ddl:=v_table_ddl||'CONSTRAINT '||constraint_rec.conname;
--         v_table_ddl:=v_table_ddl||chr(10)||'    '||constraint_rec.constrainddef;
--          firstrec := FALSE;
--      END LOOP;
v_table_ddl := v_table_ddl || ')' || server_options;
RETURN NEXT v_table_ddl;
END LOOP;
END;
$function$ LANGUAGE plpgsql VOLATILE COST 100;

-- This function will copy data from old regular/partition table to
-- new foreign table. Truncate old regular/partition table.
-- Detach and Attach will be automatic if master
-- doesn't have any pk, fk, uniq constraints defined.
-- Alternative case, it will only detach the existing partition.
CREATE OR REPLACE
  FUNCTION @extschema@.execute(ptgt_table_oid regclass, trunc_old_tbl boolean DEFAULT TRUE, with_force boolean DEFAULT FALSE)
  RETURNS boolean
  LANGUAGE plpgsql
AS $BODY$
DECLARE
  qualified_src_tab_name text;
  qualified_tgt_tab_name text;
  qualified_inhparent_tab_name text;
  source_table_oid oid;
  inhparent oid;
  tab_relispartition boolean;
  tab_partitionbound text;
  tab_pfu_count int := 0;
BEGIN

-- Check target Exists
  PERFORM * FROM @extschema@.target where tgt_oid = ptgt_table_oid AND tgt_tier_state = 'FDW_TABLE_CREATED';

  IF NOT FOUND THEN
    RAISE object_not_in_prerequisite_state USING MESSAGE = 'Tiering not Enabled for Source Table';
  END IF;

  SELECT tgt_src_oid, tgt_src_partition_bound INTO source_table_oid, tab_partitionbound FROM @extschema@.target WHERE tgt_oid = ptgt_table_oid;

  SELECT src_inhparent INTO inhparent FROM @extschema@.source WHERE src_oid = source_table_oid;

  SELECT COUNT(conname) INTO tab_pfu_count
    FROM pg_constraint
    WHERE conrelid = inhparent
    AND contype IN ('p', 'f', 'u');

-- Prepare Src for data movement
  SELECT format('%s.%s', n.nspname, c.relname), relispartition
    INTO qualified_src_tab_name, tab_relispartition
    FROM pg_class c
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = source_table_oid;

-- Prepare Tgt for data movement
  SELECT format('%s.%s', n.nspname, c.relname)
    INTO qualified_tgt_tab_name
    FROM pg_class c
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = ptgt_table_oid;

-- Update State in the target catalog
  UPDATE  @extschema@.target
    SET tgt_tier_state = 'FDW_COPY_BEGIN'
    WHERE tgt_oid = ptgt_table_oid;

-- Initiate Insert Select
  EXECUTE 'INSERT INTO ' || qualified_tgt_tab_name || ' (SELECT * FROM ' || qualified_src_tab_name  || ' )';

-- Update State in the target catalog
  UPDATE  @extschema@.target
    SET tgt_tier_state = 'FDW_COPY_DONE'
    WHERE tgt_oid = ptgt_table_oid;


  IF tab_relispartition IS TRUE AND
    tab_partitionbound IS NOT NULL
    THEN
-- Prepare Parent qualified name
    SELECT format('%s.%s', n.nspname, c.relname)
      INTO qualified_inhparent_tab_name
      FROM pg_class c
      LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.oid = inhparent;

-- Detach Old Partion table
    EXECUTE 'ALTER TABLE ' || qualified_inhparent_tab_name || ' DETACH PARTITION ' || qualified_src_tab_name ;

    IF tab_pfu_count = 0 THEN
      EXECUTE 'ALTER TABLE ' || qualified_inhparent_tab_name || ' ATTACH PARTITION ' || qualified_tgt_tab_name || ' ' || tab_partitionbound ;
    END IF;
  END IF;

  IF trunc_old_tbl IS TRUE THEN
    EXECUTE 'TRUNCATE TABLE ' || qualified_src_tab_name ;
  END IF;

-- Update State in the target catalog
  UPDATE  @extschema@.target
    SET tgt_tier_state = 'FDW_TABLE_COPIED'
    WHERE tgt_oid = ptgt_table_oid;

RETURN TRUE;
END;
$BODY$;

-- This is a SingleShot function.
-- It enables tiering on the given table,
-- and also execute the data movement to object store.
CREATE OR REPLACE
FUNCTION @extschema@.table(relation regclass)
  RETURNS boolean
  LANGUAGE plpgsql
AS $BODY$
DECLARE
  tgt_tab_oid oid;
  ret boolean;
BEGIN
  SELECT @extschema@.enable(relation) INTO ret;
  IF ret = TRUE THEN
    SELECT tgt_oid INTO tgt_tab_oid from @extschema@.target WHERE tgt_src_oid = relation;
    SELECT @extschema@.execute(tgt_tab_oid) INTO ret;
  ELSE
    ROLLBACK;
    RETURN FALSE;
  END IF;
RETURN ret;
END;
$BODY$;
