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
    WHEN 'int' THEN public.decode_int_enc(e_new) = pg_temp.decode_int_enc_v1(e_old)
    WHEN 'float' THEN public.decode_float_enc(e_new) = pg_temp.decode_float_enc_v1(e_old)
    WHEN 'text' THEN public.decode_text_enc(e_new) = pg_temp.decode_text_enc_v1(e_old)
    END;
  $$;

  \echo 'run check_equal on all rows'
  DO $$
    DECLARE
      n_equal BIGINT;
      n_total BIGINT;
    BEGIN
      CREATE TABLE tmp_compare AS
      WITH q AS (
        SELECT enc_vals e_old, pg_temp.migrate_enc_val(enc_vals) e_new
        FROM coord_data
      )
      SELECT e_old, e_new, pg_temp.check_equal(e_new, e_old) is_equal
      FROM q;

      /*
      WITH q AS (
        SELECT enc_vals e_old, migrate_enc_val(enc_vals) e_new
        FROM coord_data
        LIMIT 1000
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
      */
    END;
  $$;

  /*
  \echo 'drop enc_typ'
  DROP TYPE enc_typ;

  \echo 'rename enc_typ_old -> enc_typ'
  ALTER TYPE enc_typ_old RENAME TO enc_typ;
  */

  -- needed to release postgres' dependency on patch_helpers.so
  DROP FUNCTION pg_temp.decode_int_enc_v1;
  DROP FUNCTION pg_temp.decode_float_enc_v1;
  DROP FUNCTION pg_temp.decode_text_enc_v1;

  /*
  ALTER TABLE coord_data
    ALTER COLUMN enc_vals TYPE enc_typ
    USING migrate_enc_val(enc_vals);
  */

  \ir ../schema/types.sql
  \ir ../schema/triggers.sql
  \ir ../schema/base.sql
  \ir ../schema/query.sql
  \ir ../schema/procs.sql

COMMIT;


