\set QUIET 1

\echo 'create series_vw'
CREATE VIEW series_vw AS
SELECT
  s.name,
  s.handle,
  array_agg(ROW(f.handle, f.name, f.data_type, f.description)::field_typ) fields
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
GROUP BY r.handle, r.tags, r.started_at
ORDER BY r.started_at;


\echo 'create filtered_by_attribute'
CREATE OR REPLACE FUNCTION filtered_by_attribute(
  p_data_type field_data_typ,
  p_value field_value_typ,
  p_filter attribute_filter_typ
)
RETURNS BOOLEAN
IMMUTABLE
LANGUAGE plpgsql
AS $$
BEGIN
  IF p_value.field_handle != p_filter.field_handle THEN
    RAISE EXCEPTION 'Field handles of p_value and p_filter unequal';
  END IF;

  IF p_data_type IS NULL AND p_filter.include_missing THEN
    RETURN TRUE;
  END IF;

  -- we just trust that p_data_type matches that of the field handle
  CASE p_data_type
    WHEN 'int' THEN
      IF p_filter.int_vals IS NOT NULL THEN
        RETURN v_int <> ALL(p_filter.int_vals);
      ELSE
        RETURN (
          (p_filter.int_min IS NOT NULL AND p_value.int_val < p_filter.int_min) OR
          (p_filter.int_max IS NOT NULL AND p_value.int_val > p_filter.int_max)
        );
      END IF;
    WHEN 'float' THEN
      RETURN (
        (p_filter.float_min IS NOT NULL AND p_value.float_val < p_filter.float_min) OR
        (p_filter.float_max IS NOT NULL AND p_value.float_val > p_filter.float_max)
      );
    WHEN 'bool' THEN
      RETURN p_value.bool_val = ANY(p_filter.bool_vals);
    WHEN 'string' THEN
      RETURN p_value.string_val = ANY(p_filter.string_vals);
  END CASE;
END;
$$;


\echo 'create filtered_by_tags'
/* Returns true if the run having the set of run tags should be excluded.
*/
CREATE OR REPLACE FUNCTION filtered_by_tags(
  p_run_tags TEXT[],
  p_tag_filter tag_filter_typ
) RETURNS BOOLEAN
IMMUTABLE
LANGUAGE plpgsql
AS $$
BEGIN
  CASE 
    WHEN p_tag_filter.has_any_tag IS NULL AND p_tag_filter.has_all_tags IS NOT NULL THEN
      RETURN NOT p_run_tags <@ p_tag_filter.has_all_tags;
    WHEN p_tag_filter.has_any_tag IS NULL AND p_tag_filter.has_all_tags IS NULL THEN
      RETURN FALSE;
    WHEN p_tag_filter.has_any_tag IS NOT NULL AND p_tag_filter.has_all_tags IS NULL THEN
      RETURN p_run_tags && p_tag_filter.has_any_tag;
    WHEN p_tag_filter.has_any_tag IS NOT NULL AND p_tag_filter.has_all_tags IS NOT NULL THEN
      RAISE EXCEPTION 'Invalid p_tag_filter.  At most one of has_any_tag and has_all_tags can be non-null';
  END CASE;
END;
$$;


\echo 'create list_runs'
/* 
*/
CREATE OR REPLACE FUNCTION list_runs(
  IN p_attribute_filters attribute_filter_typ[],
  IN p_tag_filter tag_filter_typ,
  IN p_min_started_at TIMESTAMPTZ,
  IN p_max_started_at TIMESTAMPTZ
) RETURNS TABLE (
  handle UUID
) 
LANGUAGE sql
AS $$
  WITH excluded_runs AS (
    SELECT
      r.id run_id
    FROM unnest(p_attribute_filters) AS filter_obj
    JOIN field f ON f.handle = filter_obj.field_handle
    CROSS JOIN run r
    LEFT JOIN run_attr ra ON ra.field_id = f.id AND ra.run_id = r.id
    WHERE filtered_by_attribute(f.data_type, ra.attr_value, filter_obj)
    GROUP BY r.id
  )
    SELECT r.handle
    FROM run r
    LEFT JOIN excluded_runs er ON er.run_id = r.id
    WHERE er.run_id IS NULL -- anti-join
    AND NOT filtered_by_tags(r.tags, p_tag_filter)
    AND (p_min_started_at IS NULL OR p_min_started_at <= r.started_at)
    AND (p_max_started_at IS NULL OR r.started_at <= p_max_started_at);
$$;



/* Get all data from runs identified by p_run_handles with chunk_id > p_last_chunk_id
Gets just the coordinates from p_coord_handles, and projects the attribute values in
p_attr_handles into enc_typ.  The returned table packs the enc_vals into the order
[...p_attr_handles, ...p_coord_handles]
*/
\echo 'create get_data'
CREATE FUNCTION get_data(
  p_run_handles UUID[],
  p_attr_handles UUID[],   -- field handles of attributes to return
  p_coord_handles UUID[],
  p_last_chunk_id BIGINT 
) RETURNS TABLE (
  chunk_id BIGINT,
  enc_vals enc_typ[]
) 
LANGUAGE plpgsql 
AS $$
DECLARE
  v_count INT;
  v_series_id INT;
  v_attr_count INT := array_length(p_attr_handles, 1);
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

  WITH attrs AS (
    SELECT 
      ra.run_id, 
      ch.id chunk_id,
      a.ord field_order, 
      project_field_value(ra.attr_value, c.num_points) val
    FROM run_attr ra
    JOIN run r ON r.id = ra.run_id
    JOIN field f ON f.id = ra.field_id
    JOIN chunk ch ON ch.run_id = r.id
    JOIN unnest(p_run_handles) AS rh(handle) ON rh.handle = r.handle
    JOIN unnest(p_attr_handles) WITH ORDINALITY AS a(handle, ord) ON a.handle = f.handle
    WHERE ch.series_id = v_series_id
    AND (p_last_chunk_id IS NULL OR ch.id > p_last_chunk_id)
  ),
  coords AS (
    SELECT 
      c.run_id, 
      c.id chunk_id,
      ch.ord + v_attr_count field_order,
      cd.enc_vals val
    FROM coord_data cd
    JOIN coord co ON co.id = cd.coord_id
    JOIN chunk c ON c.id = cd.chunk_id
    JOIN unnest(p_coord_handles) WITH ORDINALITY AS ch(handle, ord) ON ch.handle = co.handle
    WHERE (p_last_chunk_id IS NULL OR ch.id > p_last_chunk_id)
  ),
  combined AS (
    SELECT * FROM attrs
    UNION ALL
    SELECT * FROM coords
  )
  SELECT chunk_id, array_agg(val ORDER BY field_order)
  FROM combined
  GROUP BY run_id;
END;
$$;


CREATE OR REPLACE FUNCTION query_run_data(
  IN p_attr_handles UUID[], -- field handles of attributes to be returned
  IN p_coord_handles UUID[], 
  IN p_last_chunk_id BIGINT,
  IN p_attribute_filters attribute_filter_typ[],
  IN p_tag_filter tag_filter_typ,
  IN p_min_started_at TIMESTAMPTZ,
  IN p_max_started_at TIMESTAMPTZ
) RETURNS TABLE (
  chunk_id BIGINT,
  enc_vals enc_typ[]
) 
LANGUAGE sql
STABLE
AS $$
  SELECT gd.*
  FROM get_data(
    ARRAY(
      SELECT handle
      FROM list_runs(
        p_attribute_filters,
        p_tag_filter,
        p_min_started_at,
        p_max_started_at
      )
    ),
    p_attr_handles,
    p_coord_handles,
    p_last_chunk_id
  ) AS gd;
$$;


\echo 'create function list_common_attributes'
CREATE OR REPLACE FUNCTION list_common_attributes(
  p_run_handles UUID[]
) RETURNS TABLE (
  handle UUID,
  name TEXT,
  data_type field_data_typ,
  description TEXT
) 
LANGUAGE sql
STABLE
AS $$
  SELECT
    handle, name, data_type, description
  FROM field
  WHERE id IN (
    SELECT ra.field_id
    FROM run_attr ra
    JOIN run r ON r.id = ra.run_id
    WHERE r.handle = ANY(p_run_handles)
    GROUP BY field_id
    HAVING COUNT(*) = (
      SELECT COUNT(*)
      FROM run
      WHERE handle = ANY(p_run_handles)
    )
  )
$$;

\echo 'create function list_common_series'
CREATE OR REPLACE FUNCTION list_common_series(
  p_run_handles UUID[]
) RETURNS TABLE (
  name TEXT,
  handle UUID,
  fields field_typ[]
)
LANGUAGE sql
STABLE
AS $$
  WITH 
  selected_runs AS (
    SELECT id AS run_id
    FROM run
    WHERE handle = ANY(p_run_handles)
  ),
  run_count AS (
    SELECT COUNT(*) total
    FROM selected_runs
  ),
  complete_series AS (
    SELECT series_id
    FROM (
      SELECT DISTINCT series_id, run_id
      FROM chunk
      WHERE run_id IN (SELECT run_id from selected_runs)
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


\set QUIET 0



