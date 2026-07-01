\echo 'create field_vw'
CREATE VIEW field_vw AS
SELECT handle, name, data_type, description
FROM field;

\echo 'create run_vw'
CREATE VIEW run_vw AS
SELECT
  r.handle,
  r.tags,
  r.started_at,
  array_agg(ra.attr_value) FILTER (WHERE ra.attr_value IS DISTINCT FROM NULL) attrs
FROM run r
LEFT JOIN run_attr ra ON ra.run_id = r.id
WHERE EXISTS (SELECT 1 FROM chunk c WHERE c.run_id = r.id)
GROUP BY r.handle, r.tags, r.started_at
ORDER BY r.started_at;

\echo 'create tag_vw'
CREATE VIEW tag_vw AS
SELECT DISTINCT unnest(tags) tag
FROM run;

\echo 'create started_at_vw'
CREATE OR REPLACE VIEW started_at_vw AS
SELECT DISTINCT started_at
FROM run
ORDER BY started_at;


\echo 'create attribute_values_vw'
CREATE VIEW attribute_values_vw AS
WITH src AS (
  SELECT DISTINCT
  field_id,
  (attr_value).int_val i,
  (attr_value).float_val f,
  (attr_value).bool_val b,
  (attr_value).text_val s
  FROM run_attr
),
agg AS (
  SELECT
  field_id,
  array_agg(i ORDER BY i) FILTER (WHERE i IS NOT NULL) ints,
  array_agg(f ORDER BY f) FILTER (WHERE f IS NOT NULL) floats,
  array_agg(b ORDER BY b) FILTER (WHERE b IS NOT NULL) bools,
  array_agg(s ORDER BY s) FILTER (WHERE s IS NOT NULL) strings
  FROM src 
  GROUP BY field_id
)
SELECT
ROW(f.handle, f.name, f.data_type, f.description)::field_typ field,
a.ints,
a.floats,
a.bools,
a.strings
FROM agg a 
JOIN field f ON f.id = a.field_id;



\echo 'create list_runs'
CREATE OR REPLACE FUNCTION list_runs(
  IN p_attribute_filters attribute_filter_typ[],
  IN p_tag_filter tag_filter_typ,
  IN p_min_started_at TIMESTAMPTZ,
  IN p_max_started_at TIMESTAMPTZ
) RETURNS TABLE (
  handle UUID,
  tags TEXT[],
  started_at TIMESTAMPTZ,
  attrs full_field_value_typ[]
) 
LANGUAGE plpgsql
AS $$
BEGIN
	IF p_tag_filter.pos_tags IS NULL THEN
		RAISE EXCEPTION 'p_tag_filter.pos_tags cannot be NULL';
	END IF;
	IF p_tag_filter.pos_match_all IS NULL THEN
		RAISE EXCEPTION 'p_tag_filter.pos_match_all cannot be NULL';
	END IF;
	IF p_tag_filter.neg_tags IS NULL THEN
		RAISE EXCEPTION 'p_tag_filter.neg_tags cannot be NULL';
	END IF;
	IF p_tag_filter.neg_match_all IS NULL THEN
		RAISE EXCEPTION 'p_tag_filter.neg_match_all cannot be NULL';
	END IF;
	IF p_attribute_filters IS NULL THEN
		RAISE EXCEPTION 'p_attribute_filters cannot be NULL';
	END IF;

	RETURN QUERY
	SELECT 
		r.handle,
		r.tags,
		r.started_at,
		attr_agg.attrs
	FROM run r
	JOIN list_runs_internal(
		p_attribute_filters,
		p_tag_filter,
		p_min_started_at,
		p_max_started_at
	) ri ON ri.run_id = r.id
	LEFT JOIN LATERAL (
    SELECT array_agg(ROW(
        f.handle,
        f.name, 
        (ra.attr_value).int_val,
        (ra.attr_value).float_val,
        (ra.attr_value).bool_val,
        (ra.attr_value).text_val)::full_field_value_typ)
    FILTER (WHERE ra.attr_value IS DISTINCT FROM NULL) attrs
		FROM run_attr ra 
    JOIN field f ON ra.field_id = f.id
    WHERE ra.run_id = r.id
	) attr_agg ON true
	ORDER BY r.started_at;
END;
$$;


/* Get data from runs identified by p_run_handles with chunk_id 
 * in [p_begin_chunk_id, p_end_chunk_id)
 * and fields specified by p_field_handles
 */
\echo 'create get_chunk_data'
CREATE FUNCTION get_chunk_data(
  p_run_ids INT[],
  p_field_handles UUID[],
  p_begin_chunk_id BIGINT,
  p_end_chunk_id BIGINT
) RETURNS TABLE (
  run_handle UUID,
  enc_vals enc_typ[]
) 
LANGUAGE plpgsql 
AS $$
DECLARE
  v_count INT;
  v_series_ids INT[];
  v_field_ids INT[];
BEGIN

  RETURN QUERY
  -- series that contain all of the requested handles
  WITH full_series AS (
    SELECT fs.series_id
    FROM field_series fs
    JOIN field f ON f.id = fs.field_id
    WHERE f.handle = ANY(p_field_handles)
    GROUP BY fs.series_id
    HAVING COUNT(*) = cardinality(p_field_handles)
  ),
  encs AS (
    SELECT 
      c.run_id, 
      c.id chunk_id,
      fh.ord field_order,
      cd.enc_vals val
    FROM chunk_data cd
    JOIN chunk c ON c.id = cd.chunk_id
    JOIN full_series fs ON fs.series_id = c.series_id
    JOIN unnest(p_run_ids) AS rh(run_id) ON rh.run_id = c.run_id
    JOIN field f ON f.id = cd.field_id
    JOIN unnest(p_field_handles) WITH ORDINALITY AS fh(handle, ord) ON fh.handle = f.handle
    WHERE (p_begin_chunk_id IS NULL OR c.id >= p_begin_chunk_id)
    AND (p_end_chunk_id IS NULL OR c.id < p_end_chunk_id)
  )
  SELECT r.handle, array_agg(val ORDER BY field_order)
  FROM encs e
  JOIN run r ON r.id = e.run_id
  GROUP BY r.handle, e.run_id, e.chunk_id
  ORDER BY e.chunk_id;
END;
$$;


CREATE OR REPLACE FUNCTION query_run_data(
  IN p_field_handles UUID[],
  IN p_begin_chunk_id BIGINT,
  IN p_end_chunk_id BIGINT,
  IN p_attribute_filters attribute_filter_typ[],
  IN p_tag_filter tag_filter_typ,
  IN p_min_started_at TIMESTAMPTZ,
  IN p_max_started_at TIMESTAMPTZ
) RETURNS TABLE (
  run_handle UUID,
  enc_vals enc_typ[]
) 
LANGUAGE sql
STABLE
AS $$
  SELECT gd.*
  FROM get_chunk_data(
    ARRAY(
      SELECT run_id 
      FROM list_runs_internal(
        p_attribute_filters,
        p_tag_filter,
        p_min_started_at,
        p_max_started_at
      )
    ),
    p_field_handles,
    p_begin_chunk_id,
    p_end_chunk_id
  ) AS gd;
$$;


\echo 'create function list_common_attributes'
CREATE OR REPLACE FUNCTION list_common_attributes(
  p_attribute_filters attribute_filter_typ[],
  p_tag_filter tag_filter_typ,
  p_min_started_at TIMESTAMPTZ,
  p_max_started_at TIMESTAMPTZ
) RETURNS TABLE (
  handle UUID,
  name TEXT,
  data_type field_data_typ,
  description TEXT
) 
LANGUAGE sql
STABLE
AS $$
  WITH selected_runs AS (
    SELECT run_id
    FROM list_runs_internal(
      p_attribute_filters, 
      p_tag_filter, 
      p_min_started_at,
      p_max_started_at
    )
  ),
  run_count AS (
    SELECT COUNT(*) total
    FROM selected_runs
  )
  SELECT
    handle, name, data_type, description
  FROM field
  WHERE id IN (
    SELECT ra.field_id
    FROM run_attr ra
    JOIN selected_runs sr ON sr.run_id = ra.run_id
    GROUP BY field_id
    HAVING COUNT(*) = (SELECT total FROM run_count) 
  )
$$;


\echo 'create function get_end_chunk_id'
CREATE OR REPLACE FUNCTION get_end_chunk_id()
RETURNS BIGINT
LANGUAGE sql
AS $$
  SELECT last_value + 1
  FROM pg_sequences
  WHERE sequencename = 'chunk_id_seq';
$$;

