\set QUIET 1

\echo 'create series_vw'
CREATE VIEW series_vw AS
SELECT
  s.name,
  s.handle,
  array_agg(ROW(c.handle, f.handle, f.name, f.data_type, f.description)::coord_typ) coords 
FROM series s
INNER JOIN coord c ON c.series_id = s.id
INNER JOIN field f ON f.id = c.field_id
GROUP BY s.name, s.handle;

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
  (attr_value).string_val s
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
  attrs full_field_value_typ[],
  series series_typ[]
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
		attr_agg.attrs,
		series_agg.series
	FROM run r
	JOIN list_runs_internal(
		p_attribute_filters,
		p_tag_filter,
		p_min_started_at,
		p_max_started_at
	) ri ON ri.run_id = r.id
	LEFT JOIN LATERAL (
    SELECT array_agg(ROW(s.handle, s.name, iss.coords)::series_typ) series
		FROM (
      SELECT 
        s.id,
        array_agg(ROW(co.handle, f.handle, f.name, f.data_type, f.description)::coord_typ) coords
      FROM
      (SELECT DISTINCT series_id FROM chunk WHERE run_id = r.id) c
      JOIN series s ON s.id = c.series_id
      JOIN coord co ON co.series_id = s.id
      JOIN field f ON f.id = co.field_id
      GROUP BY s.id
    ) iss 
    LEFT JOIN series s ON s.id = iss.id
	) series_agg ON true
	LEFT JOIN LATERAL (
    SELECT array_agg(ROW(
        f.handle,
        f.name, 
        (ra.attr_value).int_val,
        (ra.attr_value).float_val,
        (ra.attr_value).bool_val,
        (ra.attr_value).string_val)::full_field_value_typ)
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
 * and coords specified by p_coord_handles
 */
\echo 'create get_coord_data'
CREATE FUNCTION get_coord_data(
  p_run_ids INT[],
  p_coord_handles UUID[],
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
  v_series_id INT;
BEGIN
  
  SELECT COUNT(DISTINCT series_id) INTO v_count
  FROM coord
  WHERE handle = ANY(p_coord_handles);

  IF v_count = 0 THEN
    RAISE EXCEPTION 'No series found for the provided p_coord_handles';
  END IF;

  IF v_count > 1 THEN
    RAISE EXCEPTION 'p_coord_handles come from % different series', v_count;
  END IF;

  SELECT DISTINCT series_id INTO v_series_id
  FROM coord
  WHERE handle = ANY(p_coord_handles);

  RETURN QUERY
  WITH coords AS (
    SELECT 
      c.run_id, 
      c.id chunk_id,
      ch.ord field_order,
      cd.enc_vals val
    FROM coord_data cd
    JOIN coord co ON co.id = cd.coord_id
    JOIN chunk c ON c.id = cd.chunk_id
    JOIN unnest(p_run_ids) AS rh(run_id) ON rh.run_id = c.run_id
    JOIN unnest(p_coord_handles) WITH ORDINALITY AS ch(handle, ord) ON ch.handle = co.handle
    WHERE c.series_id = v_series_id
    AND (p_begin_chunk_id IS NULL OR c.id >= p_begin_chunk_id)
    AND (p_end_chunk_id IS NULL OR c.id < p_end_chunk_id)
  )
  SELECT r.handle, array_agg(val ORDER BY field_order)
  FROM coords c
  JOIN run r ON r.id = c.run_id
  GROUP BY r.handle, c.run_id, c.chunk_id
  ORDER BY c.chunk_id;
END;
$$;

/* Get data from runs identified by p_run_handles with chunk_id
in [p_begin_chunk_id, p_end_chunk_id)
and coords specified by p_coord_handles.
Apply a windowing function of size window_size over the data,
partitioned by p_group_coord_ids ordered by p_order_coord_ids
and outputting p_stride value.
*/
\echo 'create get_window_coord_data'
CREATE FUNCTION get_window_coord_data(
  p_run_ids INT[],
  p_coord_handles UUID[],       -- only int and float coords
  p_group_coord_handles UUID[],
  p_order_coord_handles UUID[], -- only int and float coords
  p_begin_chunk_id BIGINT,
  p_end_chunk_id BIGINT,
  p_window_size INT,
  p_stride INT
) RETURNS TABLE (
  run_handle UUID,
  enc_vals enc_typ[]
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_count INT;
  v_series_id INT;
BEGIN
  
  SELECT COUNT(DISTINCT series_id) INTO v_count
  FROM coord
  WHERE handle = ANY(p_coord_handles);

  IF v_count = 0 THEN
    RAISE EXCEPTION 'No series found for the provided p_coord_handles';
  END IF;

  IF v_count > 1 THEN
    RAISE EXCEPTION 'p_coord_handles come from % different series', v_count;
  END IF;

  SELECT DISTINCT series_id INTO v_series_id
  FROM coord
  WHERE handle = ANY(p_coord_handles);

  RETURN QUERY
  WITH
  AS decoded (
    SELECT 
    cd.coord_id,
    cd.chunk_id,
    v.pos,
    v.val
    FROM coord_data cd
    JOIN coord co ON co.id = cd.coord_id
    JOIN chunk ch ON ch.id = cd.chunk_id
    JOIN field f ON f.id = co.field_id
    WHERE c.run_id = ANY(p_run_ids)
    AND c.handle = ANY(p_coord_handles)
    AND c.series_id = v_series_id
    AND (p_begin_chunk_id IS NULL OR c.id >= p_begin_chunk_id)
    AND (p_end_chunk_id IS NULL OR c.id < p_end_chunk_id)
    CROSS JOIN LATERAL unnest(
      CASE s.data_type
        WHEN 'int' THEN 
          unpack_enc_int(s.enc_vals)::NUMERIC[]
        WHEN 'float' THEN 
          unpack_enc_float(s.enc_vals)::NUMERIC[]
      END 
    ) WITH ORDINALITY AS v(val, pos)
  ),
  --  
  keys AS (
    SELECT
    coord_id,
    chunk_id,
    v.pos,
    jsonb_agg(val ORDER BY coord_id) FILTER (WHERE c.handle = ANY(p_group_coord_handles)) AS group_key,
    jsonb_agg(val ORDER BY coord_id) FILTER (WHERE c.handle = ANY(p_order_coord_handles)) AS order_key,
    FROM coord_data cd
    JOIN coord co ON co.id = cd.coord_id
    JOIN chunk ch ON ch.id = cd.chunk_id
    JOIN field f ON f.id = co.field_id
    WHERE c.run_id = ANY(p_run_ids)
    AND c.handle = ANY(p_group_coord_handles || p_order_coord_handles)
    AND c.series_id = v_series_id
    AND (p_begin_chunk_id IS NULL OR c.id >= p_begin_chunk_id)
    AND (p_end_chunk_id IS NULL OR c.id < p_end_chunk_id)
    CROSS JOIN LATERAL jsonb_array_elements(
      CASE s.data_type
        WHEN 'int' THEN
          to_jsonb(unpack_enc_int(s.enc_vals))
        WHEN 'float' THEN
          to_jsonb(unpack_enc_float(s.enc_vals))
        WHEN 'bool' THEN
          to_jsonb(unpack_enc_bool(s.enc_vals))
        WHEN 'string' THEN
          to_jsonb(unpack_enc_text(s.enc_vals))
      END
    ) WITH ORDINALITY AS v(val, pos)
  ),
  windowed AS (
    SELECT coord_id, group_key
    AVG(val) OVER w AS val_avg 



$$;





CREATE OR REPLACE FUNCTION query_run_data(
  IN p_coord_handles UUID[], -- handles from coord table
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
  FROM get_coord_data(
    ARRAY(
      SELECT run_id 
      FROM list_runs_internal(
        p_attribute_filters,
        p_tag_filter,
        p_min_started_at,
        p_max_started_at
      )
    ),
    p_coord_handles,
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

\echo 'create function list_common_series'
CREATE OR REPLACE FUNCTION list_common_series(
  p_attribute_filters attribute_filter_typ[],
  p_tag_filter tag_filter_typ,
  p_min_started_at TIMESTAMPTZ,
  p_max_started_at TIMESTAMPTZ
) RETURNS TABLE (
  name TEXT,
  handle UUID,
  fields field_typ[]
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
  ),
  complete_series AS (
    SELECT series_id
    FROM (
      SELECT DISTINCT c.series_id, c.run_id
      FROM chunk c
      JOIN selected_runs sr ON sr.run_id = c.run_id
    ) 
    GROUP BY series_id
    HAVING COUNT(*) = (SELECT total FROM run_count) 
  )
  SELECT
    s.name,
    s.handle,
    array_agg(ROW(f.handle, f.name, f.data_type, f.description)::field_typ) fields
    from series s
    JOIN complete_series cs ON cs.series_id = s.id
    JOIN coord co ON co.series_id = s.id
    JOIN field f ON f.id = co.field_id
    GROUP BY s.name, s.handle
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

\set QUIET 0

