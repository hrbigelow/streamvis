\echo Use 'CREATE EXTENSION streamvis_udfs' to load this file. \quit

\echo 'create encode_int_enc'
CREATE OR REPLACE FUNCTION encode_int_enc(
  p_vals INT[]
)
RETURNS enc_typ_new
AS 'MODULE_PATHNAME', 'encode_int_enc'
LANGUAGE C IMMUTABLE;

\echo 'create encode_float_enc'
CREATE OR REPLACE FUNCTION encode_float_enc(
  p_vals FLOAT[]
)
RETURNS enc_typ_new
LANGUAGE sql
PARALLEL SAFE
STABLE
AS $$
SELECT ROW('float'::field_data_typ, p_vals, NULL, NULL, NULL, NULL, NULL)::enc_typ_new;
$$;

\echo 'create encode_text_enc'
CREATE OR REPLACE FUNCTION encode_text_enc(
  p_vals TEXT[]
)
RETURNS enc_typ_new
AS 'MODULE_PATHNAME', 'encode_text_enc'
LANGUAGE C IMMUTABLE
PARALLEL SAFE;

\echo 'create encode_bool_enc'
CREATE OR REPLACE FUNCTION encode_bool_enc(
  p_vals BOOLEAN[]
)
RETURNS enc_typ_new
AS 'MODULE_PATHNAME', 'encode_bool_enc'
LANGUAGE C IMMUTABLE
PARALLEL SAFE;

\echo 'create decode_int_enc_v1'
CREATE OR REPLACE FUNCTION decode_int_enc_v1(
  vals enc_typ
)
RETURNS INT[]
AS 'MODULE_PATHNAME', 'decode_int_enc_v1'
LANGUAGE C IMMUTABLE
PARALLEL SAFE;

\echo 'create decode_float_enc_v1'
CREATE OR REPLACE FUNCTION decode_float_enc_v1(
  vals enc_typ
)
RETURNS REAL[]
AS 'MODULE_PATHNAME', 'decode_float_enc_v1'
LANGUAGE C IMMUTABLE
PARALLEL SAFE;

\echo 'create decode_text_enc_v1'
CREATE OR REPLACE FUNCTION decode_text_enc_v1(
  vals enc_typ
)
RETURNS TEXT[]
AS 'MODULE_PATHNAME', 'decode_text_enc_v1'
LANGUAGE C IMMUTABLE
PARALLEL SAFE;


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
