\set QUIET 1


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

\echo 'create valid_enc_typ'
CREATE FUNCTION valid_enc_typ(e enc_typ, d field_data_typ) 
RETURNS BOOLEAN
IMMUTABLE
LANGUAGE sql
AS $$
  SELECT
		((e.int_base IS NOT NULL)::int
			+ (e.float_base IS NOT NULL)::int
			+ (e.bool_base IS NOT NULL)::int
			+ (e.text_base IS NOT NULL)::int) = 1
		AND
		((e.int_spans IS NOT NULL)::int
			+ (e.float_spans IS NOT NULL)::int
			+ (e.bcast IS NOT NULL)::int) = 1
		AND CASE d
		WHEN 'int' THEN
			e.int_spans IS NOT NULL 
			AND e.int_base IS NOT NULL 
			AND cardinality(e.shape) = cardinality(e.int_spans)
		WHEN 'float' THEN
			e.float_spans IS NOT NULL
			AND e.float_base IS NOT NULL
			AND cardinality(e.shape) = cardinality(e.float_spans)
		WHEN 'bool' THEN
			e.bcast IS NOT NULL
			AND e.bool_base IS NOT NULL
			AND cardinality(e.shape) = cardinality(e.bcast)
		WHEN 'string' THEN
			e.bcast IS NOT NULL
			AND e.text_base IS NOT NULL
			AND cardinality(e.shape) = cardinality(e.bcast)
		END
$$;

\echo 'create pack_float_enc'
CREATE FUNCTION pack_float_enc(
	vals NUMERIC[]
) RETURNS enc_typ
IMMUTABLE PARALLEL SAFE
LANGUAGE sql
AS $$
  SELECT ROW(
		ARRAY[cardinality(vals)]::INT[],
		NULL,
		vals::REAL[],
		NULL,
		NULL,
		NULL,
		ARRAY[NULL]::REAL[],
		NULL
	)::enc_typ;
$$;

\echo 'create unpack_enc_int'
CREATE FUNCTION unpack_enc_int(e enc_typ)
RETURNS INT[] 
IMMUTABLE PARALLEL SAFE
LANGUAGE sql 
AS $$
  WITH RECURSIVE unravel AS (
		SELECT 
			0 AS current_dim,
			0 AS flat_idx,
			0 As offset
		UNION ALL
		SELECT 
			prev.current_dim + 1,
			prev.flat_idx * e.shape[prev.current_dim + 1] + (s.idx - 1),
			prev.offset + CASE
			  WHEN e.int_spans[prev.current_dim + 1] IS NOT NULL
					THEN (s.idx - 1) * e.int_spans[prev.current_dim + 1]
					ELSE 0
			  END
		FROM unravel prev
		JOIN LATERAL generate_series(1, 
			CASE 
				WHEN e.int_spans[prev.current_dim + 1] IS NULL THEN 1
				ELSE e.shape[prev.current_dim + 1]
			END
		) AS s(idx) ON true
		WHERE prev.current_dim < array_length(e.shape, 1)
	)
	SELECT ARRAY(
		SELECT b.elem + u.offset
		FROM unravel u
		CROSS JOIN LATERAL unnest(e.int_base) WITH ORDINALITY AS b(elem, base_idx)
		WHERE u.current_dim = array_length(e.shape, 1)
		ORDER BY u.flat_idx, b.base_idx
	);
$$;


\echo 'create unpack_enc_float'
CREATE FUNCTION unpack_enc_float(e enc_typ)
RETURNS FLOAT[] 
IMMUTABLE PARALLEL SAFE
LANGUAGE sql 
AS $$
  WITH RECURSIVE unravel AS (
		SELECT 
			0 AS current_dim,
			0 AS flat_idx,
			0.0::FLOAT AS offset
		UNION ALL
		SELECT 
			prev.current_dim + 1,
			prev.flat_idx * e.shape[prev.current_dim + 1] + (s.idx - 1),
			prev.offset + CASE
			  WHEN e.float_spans[prev.current_dim + 1] IS NOT NULL
					THEN (s.idx - 1) * e.float_spans[prev.current_dim + 1]
					ELSE 0.0
			  END
		FROM unravel prev
		JOIN LATERAL generate_series(1, 
			CASE 
				WHEN e.float_spans[prev.current_dim + 1] IS NULL THEN 1
				ELSE e.shape[prev.current_dim + 1]
			END
		) AS s(idx) ON true
		WHERE prev.current_dim < array_length(e.shape, 1)
	)
	SELECT ARRAY(
		SELECT b.elem + u.offset
		FROM unravel u
		CROSS JOIN LATERAL unnest(e.float_base) WITH ORDINALITY AS b(elem, base_idx)
		WHERE u.current_dim = array_length(e.shape, 1)
		ORDER BY u.flat_idx, b.base_idx
	);
$$;


\echo 'create unpack_enc_bool'
CREATE FUNCTION unpack_enc_bool(e enc_typ)
RETURNS BOOLEAN[] 
IMMUTABLE PARALLEL SAFE
LANGUAGE sql 
AS $$
  WITH RECURSIVE unravel AS (
		SELECT 
			0 AS current_dim,
			0 AS flat_idx
		UNION ALL
		SELECT 
			prev.current_dim + 1,
			prev.flat_idx * e.shape[prev.current_dim + 1] + (s.idx - 1)
		FROM unravel prev
		JOIN LATERAL generate_series(1, 
			CASE 
				WHEN e.bcast[prev.current_dim + 1] THEN e.shape[prev.current_dim + 1]
				ELSE 1
			END
		) AS s(idx) ON true
		WHERE prev.current_dim < array_length(e.shape, 1)
	)
	SELECT ARRAY(
		SELECT b.elem
		FROM unravel u
		CROSS JOIN LATERAL unnest(e.bool_base) WITH ORDINALITY AS b(elem, base_idx)
		WHERE u.current_dim = array_length(e.shape, 1)
		ORDER BY u.flat_idx, b.base_idx
	);
$$;


\echo 'create unpack_enc_text'
CREATE FUNCTION unpack_enc_text(e enc_typ)
RETURNS TEXT[] 
IMMUTABLE PARALLEL SAFE
LANGUAGE sql 
AS $$
  WITH RECURSIVE unravel AS (
		SELECT 
			0 AS current_dim,
			0 AS flat_idx
		UNION ALL
		SELECT 
			prev.current_dim + 1,
			prev.flat_idx * e.shape[prev.current_dim + 1] + (s.idx - 1)
		FROM unravel prev
		JOIN LATERAL generate_series(1, 
			CASE 
				WHEN e.bcast[prev.current_dim + 1] THEN e.shape[prev.current_dim + 1]
				ELSE 1
			END
		) AS s(idx) ON true
		WHERE prev.current_dim < array_length(e.shape, 1)
	)
	SELECT ARRAY(
		SELECT b.elem
		FROM unravel u
		CROSS JOIN LATERAL unnest(e.text_base) WITH ORDINALITY AS b(elem, base_idx)
		WHERE u.current_dim = array_length(e.shape, 1)
		ORDER BY u.flat_idx, b.base_idx
	);
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

