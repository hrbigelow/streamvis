\set ON_ERROR_STOP on

BEGIN;

	\ir ../teardown/procs.sql
	\ir ../teardown/query.sql
	\ir ../teardown/base.sql
	\ir ../teardown/triggers.sql
	\ir ../teardown/types.sql

  \set QUIET 1

  \echo 'create pg_temp.decode_int_enc_v1'
  CREATE OR REPLACE FUNCTION pg_temp.decode_int_enc_v1(
    vals enc_typ_old
  )
  RETURNS INT[]
  AS '$libdir/patch_helpers', 'decode_int_enc_v1'
  LANGUAGE C IMMUTABLE
  PARALLEL SAFE;

  \echo 'create pg_temp.decode_float_enc_v1'
  CREATE OR REPLACE FUNCTION pg_temp.decode_float_enc_v1(
    vals enc_typ_old
  )
  RETURNS REAL[]
  AS '$libdir/patch_helpers', 'decode_float_enc_v1'
  LANGUAGE C IMMUTABLE
  PARALLEL SAFE;

  \echo 'create pg_temp.decode_text_enc_v1'
  CREATE OR REPLACE FUNCTION pg_temp.decode_text_enc_v1(
    vals enc_typ_old
  )
  RETURNS TEXT[]
  AS '$libdir/patch_helpers', 'decode_text_enc_v1'
  LANGUAGE C IMMUTABLE
  PARALLEL SAFE;

  \echo 'create pg_temp.migrate_enc_val'
  CREATE OR REPLACE FUNCTION pg_temp.migrate_enc_val(e enc_typ_old)
  RETURNS enc_typ
  LANGUAGE sql IMMUTABLE
  PARALLEL SAFE
  AS $$
    SELECT CASE
    WHEN e.int_base IS NOT NULL THEN encode_int_enc(pg_temp.decode_int_enc_v1(e))
    WHEN e.float_base IS NOT NULL THEN encode_float_enc(pg_temp.decode_float_enc_v1(e))
    WHEN e.text_base IS NOT NULL THEN encode_text_enc(pg_temp.decode_text_enc_v1(e))
    END;
  $$;

  \echo 'create pg_temp.check_equal'
  CREATE OR REPLACE FUNCTION pg_temp.check_equal(e_new enc_typ, e_old enc_typ_old)
  RETURNS BOOLEAN
  LANGUAGE sql IMMUTABLE
  PARALLEL SAFE
  AS $$
    SELECT CASE e_new.data_type
    WHEN 'int' THEN decode_int_enc(e_new) = pg_temp.decode_int_enc_v1(e_old)
    WHEN 'float' THEN decode_float_enc(e_new) = pg_temp.decode_float_enc_v1(e_old)
    WHEN 'text' THEN decode_text_enc(e_new) = pg_temp.decode_text_enc_v1(e_old)
    END;
  $$;

  \echo 'alter coord_data'
  ALTER TABLE coord_data
    ALTER COLUMN enc_vals TYPE enc_typ
    USING pg_temp.migrate_enc_val(enc_vals);

  /*
  \echo 'run check_equal on all rows'
  DO $$
    DECLARE
      n_equal BIGINT;
      n_total BIGINT;
    BEGIN
      CREATE TABLE tmp_compare AS
      WITH base AS (
        SELECT 
        coord_id, chunk_id,
        enc_vals e_old, pg_temp.migrate_enc_val(enc_vals) e_new
        FROM coord_data
      )
      SELECT
      coord_id,
      chunk_id,
      (e_new).data_type,
      CASE WHEN (e_new).data_type = 'int' THEN pg_temp.decode_int_enc_v1(e_old) END old_ints,
      CASE WHEN (e_new).data_type = 'int' THEN decode_int_enc(e_new) END new_ints,
      CASE WHEN (e_new).data_type = 'float' THEN pg_temp.decode_float_enc_v1(e_old) END old_floats,
      CASE WHEN (e_new).data_type = 'float' THEN decode_float_enc(e_new) END new_floats,
      CASE WHEN (e_new).data_type = 'text' THEN pg_temp.decode_text_enc_v1(e_old) END old_texts,
      CASE WHEN (e_new).data_type = 'text' THEN decode_text_enc(e_new) END new_texts
      FROM base;
      WITH q AS (
        SELECT enc_vals e_old, migrate_enc_val(enc_vals) e_new
        FROM coord_data
      )
      SELECT count(*) INTO n_equal
      FROM q
      WHERE check_equal(e_new, e_old);

      SELECT count(*) INTO n_total
      FROM coord_data limit 1000;

      IF n_equal != n_total THEN
        RAISE EXCEPTION '% total rows, only % passed', n_total, n_equal;
      ELSE
        RAISE NOTICE 'Found % passing rows', n_equal;
      END IF;
    END;
  $$;
  */

  \echo 'drop enc_typ'
  DROP TYPE enc_typ;

  \echo 'rename enc_typ_old -> enc_typ'
  ALTER TYPE enc_typ_old RENAME TO enc_typ;

  -- needed to release postgres' dependency on patch_helpers.so
  DROP FUNCTION pg_temp.decode_int_enc_v1;
  DROP FUNCTION pg_temp.decode_float_enc_v1;
  DROP FUNCTION pg_temp.decode_text_enc_v1;
  DROP FUNCTION pg_temp.migrate_enc_val;

  \ir ../schema/types.sql
  \ir ../schema/triggers.sql
  \ir ../schema/base.sql
  \ir ../schema/query.sql
  \ir ../schema/procs.sql

COMMIT;


