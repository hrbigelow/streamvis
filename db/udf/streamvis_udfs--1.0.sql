\echo Use 'CREATE EXTENSION streamvis_udfs' to load this file. \quit

\echo 'create encode_int_enc'
CREATE OR REPLACE FUNCTION encode_int_enc(
  p_vals INT[]
)
RETURNS enc_typ_new
AS 'MODULE_PATHNAME', 'encode_int_enc'
LANGUAGE C IMMUTABLE;

\echo 'create window_avg_sfunc'
CREATE OR REPLACE FUNCTION window_avg_sfunc(
  internal, 
  int, -- window_size
  int, -- stride
  enc_typ,  -- vals
  enc_typ[]  -- group_vals
)
RETURNS internal
AS 'MODULE_PATHNAME', 'window_avg_sfunc'
LANGUAGE C IMMUTABLE;

/*
\echo 'create window_avg_finalfunc'
CREATE OR REPLACE FUNCTION window_avg_finalfunc(
  internal
)
RETURNS enc_typ
AS 'MODULE_PATHNAME', 'window_avg_finalfunc'
LANGUAGE C IMMUTABLE;

\echo 'create window_avg'
CREATE AGGREGATE window_avg(
  int,
  int,
  enc_typ,
  enc_typ[]
) (
  sfunc = window_avg_sfunc,
  stype = internal,
  finalfunc = window_avg_finalfunc
);

*/
