\set QUIET 1

\echo 'create field_data_typ'
CREATE TYPE field_data_typ AS ENUM ('int', 'float', 'string', 'bool');


\echo 'create field_typ'
CREATE TYPE field_typ AS (
  handle UUID,
  name TEXT,
  data_type field_data_typ,
  description TEXT
);

\echo 'create coord_typ'
CREATE TYPE coord_typ AS (
  coord_handle UUID,
  field_handle UUID,
  name TEXT,
  data_type field_data_typ,
  description TEXT
);

\echo 'create series_typ'
CREATE TYPE series_typ AS (
	handle UUID,
	name TEXT,
	coords coord_typ[]
);


/*
enc_typ represents an ordered 1D sequence of values, viewed
as a flattened tensor of shape `shape`, and the following logic:

Exactly one of int_spans, float_spans, bool_bcast, string_bcast will be non-null.

spans[dim] == null:  dim has no broadcasting or regular-increment (range) pattern.
spans[dim] != null (>= 0):  the values along dim are evenly spaced from orig[dim]
  to orig[dim] + spans[dim].  A span value of zero represents broadcasting.

bcast[dim]
base:  the flattened values of orig such that if orig repeats along dimension dim, base is
the zero-th slice of this dimension, otherwise, it is the full set of values.

Here, orig means the original tensor which is encoded by this scheme.

For detail, see client/streamvis/dbutil.py: encode_array, decode_array
*/
\echo 'create enc_typ'
CREATE TYPE enc_typ AS (
  base BYTEA,
  shape INT[],
  int_spans INT[],
  float_spans REAL[],
  bool_bcast BOOLEAN[],
  string_bcast BOOLEAN[]
);



-- type used to store an attribute in the run_attr table
\echo 'create field_value_typ'
CREATE TYPE field_value_typ AS (
  field_handle UUID,
  int_val INT,
  float_val FLOAT,
  bool_val BOOLEAN,
  string_val TEXT
);


\echo 'create full_field_value_typ'
CREATE TYPE full_field_value_typ AS (
  handle UUID,
	name TEXT,
  int_val INT,
  float_val FLOAT,
  bool_val BOOLEAN,
  string_val TEXT
);


\echo 'create attribute_filter_typ'
CREATE TYPE attribute_filter_typ AS (
  field_handle UUID,
  include_missing BOOLEAN,
  int_min INT,
  int_max INT,
  int_vals INT[],
  float_min REAL,
  float_max REAL,
  bool_vals BOOLEAN[],
  string_vals TEXT[]
);

\echo 'create tag_filter_typ'
CREATE TYPE tag_filter_typ AS (
	-- determines match as pos_match and not meg_match
  pos_tags TEXT[],
	pos_match_all BOOLEAN, -- if true, pos_match iff pos_tags <@ run tags
	neg_tags TEXT[],
	neg_match_all BOOLEAN -- if true, neg_match iff neg_tags <@ run tags
);


\echo 'create valid_attr_value'
CREATE OR REPLACE FUNCTION valid_attr_value(
  val field_value_typ
)
RETURNS BOOLEAN
IMMUTABLE
LANGUAGE plpgsql
AS $$
DECLARE
  v_data_type field_data_typ;
BEGIN
  SELECT f.data_type INTO v_data_type
  FROM field f
  WHERE f.handle = (val).field_handle;

  IF NOT FOUND THEN
    RETURN FALSE;
  END IF;

  RETURN 
  CASE v_data_type 
    WHEN 'int' THEN
      ((val).int_val IS NOT NULL AND
        (val).float_val IS NULL AND
        (val).bool_val IS NULL AND
        (val).string_val IS NULL
      )
    WHEN 'float' THEN
      ((val).int_val IS NULL AND
        (val).float_val IS NOT NULL AND
        (val).bool_val IS NULL AND
        (val).string_val IS NULL
      )
    WHEN 'bool' THEN
      ((val).int_val IS NULL AND
        (val).float_val IS NULL AND
        (val).bool_val IS NOT NULL AND
        (val).string_val IS NULL
      )
    WHEN 'string' THEN
      ((val).int_val IS NULL AND
        (val).float_val IS NULL AND
        (val).bool_val IS NULL AND
        (val).string_val IS NOT NULL
      )
    ELSE
      FALSE
  END CASE;
END;
$$;



/* Convert a scalar field_value_typ into a broadcasted enc_typ.
   Assume the field_val is valid.
*/
\echo 'create project_field_value'
CREATE OR REPLACE FUNCTION project_field_value(
  field_val field_value_typ,
  num_points INT
)
RETURNS enc_typ 
IMMUTABLE
LANGUAGE plpgsql
AS $$
DECLARE v_enc_val enc_typ := ROW(
  field_val.field_handle, 
	NULL,
  ARRAY[num_points],
  NULL,
  NULL,
  NULL,
  NULL)::enc_typ;
BEGIN
  CASE 
    WHEN field_val.int_val IS NOT NULL THEN
			v_enc_val.base := reverse(int4send(field_val.int_val));
      v_enc_val.int_spans := ARRAY[0];
    WHEN field_val.float_val IS NOT NULL THEN
			v_enc_val.base := reverse(float4send(field_val.float_val::float4));
      v_enc_val.float_spans := ARRAY[0.0];
    WHEN field_val.bool_val IS NOT NULL THEN
			v_enc_val.base := CASE WHEN field_val.bool_val THEN '\x01'::BYTEA ELSE '\x00'::BYTEA END;
      v_enc_val.bool_bcast := ARRAY[TRUE];
    WHEN field_val.string_val IS NOT NULL THEN
			v_enc_val.base := 
				reverse(int4send(length(field_val.string_val)))
				|| convert_to(field_val.string_val, 'UTF8');
      v_enc_val.string_bcast := ARRAY[TRUE];
    ELSE
      RAISE EXCEPTION 'field_val has no non-null fields';
  END CASE;

  RETURN v_enc_val;
END;
$$;


\set QUIET 0

