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
\set QUIET 1

\echo 'field_data_typ'
CREATE TYPE field_data_typ AS ENUM ('int', 'float', 'string', 'bool');

\echo 'field_typ'
CREATE TYPE field_typ AS (
  handle UUID,
  name TEXT,
  data_type field_data_typ,
  description TEXT
);

\echo 'enc_typ'
CREATE TYPE enc_typ AS (
  field_handle UUID,
  base BYTEA,
  shape INT[],
  int_spans INT[],
  float_spans REAL[],
  bool_bcast BOOLEAN[],
  string_bcast BOOLEAN[]
);

-- type used to store an attribute in the run_attr table
\echo 'field_value_typ'
CREATE TYPE field_value_typ AS (
  field_handle UUID,
  int_val INT,
  float_val FLOAT,
  bool_val BOOLEAN,
  string_val TEXT
);

\echo 'attribute_filter_typ'
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

CREATE TYPE tag_filter_typ AS (
  has_any_tag TEXT[],
  has_all_tags TEXT[]
);


\echo 'valid_enc_typ'
CREATE FUNCTION valid_enc_typ(val enc_typ) 
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
        (
          (val).int_spans IS NOT NULL AND
          (val).float_spans IS NULL AND
          (val).bool_bcast IS NULL AND
          (val).string_bcast IS NULL AND
          array_length((val).shape, 1) = array_length((val).int_spans, 1)
        )
      WHEN 'float' THEN
        (
          (val).int_spans IS NULL AND
          (val).float_spans IS NOT NULL AND
          (val).bool_bcast IS NULL AND
          (val).string_bcast IS NULL AND
          array_length((val).shape, 1) = array_length((val).float_spans, 1)
        )
      WHEN 'bool' THEN
        (
          (val).int_spans IS NULL AND
          (val).float_spans IS NULL AND
          (val).bool_bcast IS NOT NULL AND
          (val).string_bcast IS NULL AND
          array_length((val).shape, 1) = array_length((val).bool_bcast, 1)
        )
      WHEN 'string' THEN
        (
          (val).int_spans IS NULL AND
          (val).float_spans IS NULL AND
          (val).bool_bcast IS NULL AND
          (val).string_bcast IS NOT NULL AND
          array_length((val).shape, 1) = array_length((val).string_bcast, 1)
        )
      ELSE
        FALSE
    END CASE;
END;
$$;


\echo 'valid_attr_value'
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

\set QUIET 0

