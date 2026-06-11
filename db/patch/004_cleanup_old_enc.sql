\set ON_ERROR_STOP on
\set QUIET 1

BEGIN;

  \echo 'drop query_run_data_windowed'
  DROP FUNCTION IF EXISTS query_run_data_windowed;

  \echo 'drop get_coord_data'
  DROP FUNCTION IF EXISTS get_coord_data;

  \echo 'drop get_coord_data_windowed'
  DROP FUNCTION IF EXISTS get_coord_data_windowed;

  \echo 'drop get_coord_data_windowed_impl'
  DROP FUNCTION IF EXISTS get_coord_data_windowed_impl;

  \echo 'drop pack_int_enc'
  DROP FUNCTION IF EXISTS pack_int_enc;

  \echo 'drop pack_float_enc'
  DROP FUNCTION IF EXISTS pack_float_enc;

  \echo 'drop pack_bool_enc'
  DROP FUNCTION IF EXISTS pack_bool_enc;

  \echo 'drop pack_text_enc'
  DROP FUNCTION IF EXISTS pack_text_enc;

  \echo 'drop unpack_enc_int'
  DROP FUNCTION IF EXISTS unpack_enc_int;

  \echo 'drop unpack_enc_float'
  DROP FUNCTION IF EXISTS unpack_enc_float;

  \echo 'drop unpack_enc_bool'
  DROP FUNCTION IF EXISTS unpack_enc_bool;

  \echo 'drop unpack_enc_text'
  DROP FUNCTION IF EXISTS unpack_enc_text;

  \echo 'drop valid_enc_typ'
  DROP FUNCTION IF EXISTS valid_enc_typ(enc_typ, field_data_typ); 

	\ir ../teardown/procs.sql
	\ir ../teardown/query.sql
	\ir ../teardown/base.sql
	\ir ../teardown/triggers.sql
	\ir ../teardown/types.sql

  \echo 'rename enc_typ -> enc_typ_old' 
  ALTER TYPE enc_typ RENAME to enc_typ_old;

  \echo 'create enc_typ'
  CREATE TYPE enc_typ AS (
    data_type field_data_typ,
    floats REAL[],
    bools BOOLEAN[],
    texts TEXT[],
    base INT,
    diff INT[],
    size INT
  );

  \ir ../schema/types.sql
  \ir ../schema/triggers.sql
  \ir ../schema/base.sql
  \ir ../schema/query.sql
  \ir ../schema/procs.sql

END;

