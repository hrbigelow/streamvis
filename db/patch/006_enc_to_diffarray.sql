\set ON_ERROR_STOP on

BEGIN;

	\ir ../teardown/procs.sql
	\ir ../teardown/query.sql
	\ir ../teardown/base.sql
	\ir ../teardown/triggers.sql
	\ir ../teardown/types.sql

  \set QUIET 1

  DROP EXTENSION IF EXISTS streamvis_udfs;
  DROP TABLE IF EXISTS tmp_compare;

  \echo 'create streamvis_udfs'
  CREATE EXTENSION IF NOT EXISTS streamvis_udfs;

  \echo 'create migrate_enc_val'
  CREATE OR REPLACE FUNCTION pg_temp.migrate_enc_val(e enc_typ_old)
  RETURNS enc_typ
  LANGUAGE sql IMMUTABLE
  PARALLEL SAFE
  AS $$
    SELECT CASE
    WHEN e.int_base IS NOT NULL THEN encode_int_enc(decode_int_enc_v1(e))
    WHEN e.float_base IS NOT NULL THEN encode_float_enc(decode_float_enc_v1(e))
    WHEN e.text_base IS NOT NULL THEN encode_text_enc(decode_text_enc_v1(e))
    END CASE;
  $$;

  \echo 'create check_equal'
  CREATE OR REPLACE FUNCTION pg_temp.check_equal(e_new enc_typ, e_old enc_typ_old)
  RETURNS BOOLEAN
  LANGUAGE sql IMMUTABLE
  PARALLEL SAFE
  AS $$
    SELECT CASE e_new.data_type
    WHEN 'int' THEN decode_int_enc(e_new) = decode_int_enc_v1(e_old)
    WHEN 'float' THEN decode_float_enc(e_new) = decode_float_enc_v1(e_old)
    WHEN 'text' THEN decode_text_enc(e_new) = decode_text_enc_v1(e_old)
    END CASE;
  $$;

  \echo 'run check_equal on all rows'
  DO $$
    DECLARE
      n_equal BIGINT;
      n_total BIGINT;
    BEGIN
      CREATE TABLE tmp_compare AS
      WITH q AS (
        SELECT enc_vals e_old, migrate_enc_val(enc_vals) e_new
        FROM coord_data limit 10000
      )
      SELECT e_old, e_new, check_equal(e_new, e_old) is_equal
      FROM q;

      WITH q AS (
        SELECT enc_vals e_old, migrate_enc_val(enc_vals) e_new
        FROM coord_data
        LIMIT 1000
      )
      SELECT count(*) INTO n_equal
      FROM q
      WHERE check_equal(e_new, e_old);

      /*
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

  \echo 'drop streamvis_udfs'
  DROP EXTENSION IF EXISTS streamvis_udfs;

  \echo 'drop enc_typ'
  DROP TYPE enc_typ;

  \echo 'rename enc_typ_old -> enc_typ'
  ALTER TYPE enc_typ_old RENAME TO enc_typ;

  /*
  ALTER TABLE coord_data
    ALTER COLUMN enc_vals TYPE enc_typ
    USING migrate_enc_val(enc_vals);
  */

END;

BEGIN;
  -- DROP TYPE enc_typ_old;

  \ir ../schema/types.sql
  \ir ../schema/triggers.sql
  \ir ../schema/base.sql
  \ir ../schema/query.sql
  \ir ../schema/procs.sql


END;


