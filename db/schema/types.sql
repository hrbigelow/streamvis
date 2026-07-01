\echo 'create field_typ'
CREATE TYPE field_typ AS (
  handle UUID,
  name TEXT,
  data_type field_data_typ,
  description TEXT
);

\echo 'create full_field_value_typ'
CREATE TYPE full_field_value_typ AS (
  handle UUID,
	name TEXT,
  int_val INT,
  float_val FLOAT,
  bool_val BOOLEAN,
  text_val TEXT
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
  text_vals TEXT[]
);

\echo 'create tag_filter_typ'
CREATE TYPE tag_filter_typ AS (
	-- determines match as pos_match and not meg_match
  pos_tags TEXT[],
	pos_match_all BOOLEAN, -- if true, pos_match iff pos_tags <@ run tags
	neg_tags TEXT[],
	neg_match_all BOOLEAN -- if true, neg_match iff neg_tags <@ run tags
);

\echo 'create valid_enc_typ'
CREATE FUNCTION valid_enc_typ(e enc_typ) 
RETURNS BOOLEAN
IMMUTABLE
LANGUAGE sql
AS $$
  SELECT 
    CASE e.data_type
    WHEN 'int' THEN
      (
        e.floats IS NULL
        AND e.bools IS NULL
        AND e.texts IS NULL
        AND e.base IS NOT NULL 
        AND e.diff IS NOT NULL 
        AND e.size IS NOT NULL
      )
    WHEN 'float' THEN
      (
        e.floats IS NOT NULL
        AND e.bools IS NULL
        AND e.texts IS NULL
        AND e.base IS NULL
        AND e.diff IS NULL
        AND e.size IS NULL
      )
    WHEN 'bool' THEN
      (
        e.floats IS NULL
        AND e.bools IS NOT NULL
        AND e.texts IS NULL
      )
    WHEN 'text' THEN
      (
        e.floats IS NULL
        AND e.bools IS NULL
        AND e.texts IS NOT NULL
        AND e.base IS NOT NULL
        AND e.diff IS NOT NULL
        AND e.size IS NOT NULL
      )
    END CASE;
$$;


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
        (val).text_val IS NULL
      )
    WHEN 'float' THEN
      ((val).int_val IS NULL AND
        (val).float_val IS NOT NULL AND
        (val).bool_val IS NULL AND
        (val).text_val IS NULL
      )
    WHEN 'bool' THEN
      ((val).int_val IS NULL AND
        (val).float_val IS NULL AND
        (val).bool_val IS NOT NULL AND
        (val).text_val IS NULL
      )
    WHEN 'text' THEN
      ((val).int_val IS NULL AND
        (val).float_val IS NULL AND
        (val).bool_val IS NULL AND
        (val).text_val IS NOT NULL
      )
    ELSE
      FALSE
  END CASE;
END;
$$;

