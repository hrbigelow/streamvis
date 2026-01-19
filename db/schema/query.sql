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
CREATE OR REPLACE FUNCTION filtered_by_tags(
  p_run_tags TEXT[],
  p_tag_filter tag_filter_typ
) RETURNS BOOLEAN
IMMUTABLE
LANGUAGE plpgsql
AS $$
BEGIN
  IF (p_tag_filter.has_any_tag IS NULL) = (p_tag_filter.has_all_tags IS NULL) THEN
    RAISE EXCEPTION 'Invalid p_tag_filter.  Exactly one of has_any_tag and has_all_tags must be non-NULL';
  END IF;

  IF p_tag_filter.has_any_tag IS NOT NULL THEN
    RETURN NOT p_run_tags && p_tag_filter.has_any_tag;
  ELSE
    RETURN NOT p_run_tags <@ p_tag_filter.has_all_tags;
  END IF;
END;
$$;


\echo 'create list_runs'
CREATE OR REPLACE FUNCTION list_runs(
  IN p_attribute_filters attribute_filter_typ[],
  IN p_tag_filter tag_filter_typ,
  IN p_min_started_at TIMESTAMPTZ,
  IN p_max_started_at TIMESTAMPTZ
) RETURNS TABLE (
  run_handle UUID
) 
LANGUAGE plpgsql
AS $$
DECLARE
  filter_count INT := jsonb_array_length(p_attribute_filters);
BEGIN
  IF filter_count = 0 THEN
    RETURN;
  END IF;

  RETURN QUERY
  WITH matched_filters AS (
    SELECT ra.run_id
      FROM unnest(p_attribute_filters) AS f(filter_obj)
      JOIN field f ON f.handle = f.filter_obj.field_handle
      LEFT JOIN run_attr ra ON ra.field_id = f.id
      WHERE NOT filtered_by_attribute(ra.attr_type, ra.attr_value, f.filter_obj)
  ),
  attr_filtered AS (
    SELECT mf.run_id
    FROM matched_filters mf
    GROUP BY mf.run_id
    HAVING COUNT(*) = filter_count
  )
    SELECT r.run_id
    FROM run r
    JOIN attr_filtered af ON af.run_id = r.id
    WHERE NOT filtered_by_tags(r.run_tags, p_tag_filter)
    AND (p_min_started_at IS NULL OR p_min_started_at <= r.started_at)
    AND (p_max_started_at IS NULL OR r.started_at <= p_max_started_at);

END;
$$;

-- definitions of views and table functions
\echo 'create get_data'
CREATE FUNCTION get_data(
  p_run_handles UUID[],
  p_field_handles UUID[],
  p_last_chunk_id BIGINT 
) RETURNS TABLE (
  chunk_id BIGINT,
  series_name TEXT,
  field_name TEXT,
  run_handle UUID,
  enc_vals enc_typ
) 
LANGUAGE sql
STABLE
AS $$
  SELECT ch.id, s.name, f.name, r.handle, cd.enc_vals
  FROM coord_data cd
  JOIN coord co ON co.id = cd.coord_id
  JOIN chunk ch ON ch.id = cd.chunk_id
  JOIN series s ON s.id = co.series_id
  JOIN field f ON f.id = co.field_id
  JOIN run r ON r.id = ch.run_id
  WHERE r.handle = ANY(p_run_handles)
  AND f.handle = ANY(p_field_handles)
  AND (p_last_chunk_id IS NULL OR ch.id > p_last_chunk_id);
$$;


CREATE OR REPLACE FUNCTION query_run_data(
  IN p_field_handles UUID[],
  IN p_last_chunk_id BIGINT,
  IN p_attribute_filters attribute_filter_typ[],
  IN p_tag_filter tag_filter_typ,
  IN p_min_started_at TIMESTAMPTZ,
  IN p_max_started_at TIMESTAMPTZ
) RETURNS TABLE (
  chunk_id BIGINT,
  series_name TEXT,
  field_name TEXT,
  run_handle UUID,
  enc_vals enc_typ
) 
LANGUAGE sql
STABLE
AS $$
  SELECT gd.*
  FROM get_data(
    ARRAY(
      SELECT run_handle
      FROM list_runs(
        p_attribute_filters,
        p_tag_filter,
        p_min_started_at,
        p_max_started_at
      )
    ),
    p_field_handles,
    p_last_chunk_id
  ) AS gd;
$$;


/*
CREATE OR REPLACE FUNCTION get_common_attributes(
  p_run_handles UUID[]
) RETURNS TABLE (
  attr_handle UUID,
  attr_name TEXT,
  attr_type TEXT,
  attr_desc TEXT
) AS $$
BEGIN
  RETURN QUERY
  WITH runs AS (
    SELECT r.run_id, a.attr_
    FROM run r
    INNER JOIN run_attr ra
    INNER JOIN attr a
      ON r.run_id = ra.run_id
    WHERE run_handle = ANY(p_run_handles)
  ),

  a AS (
    SELECT run_handle, jsonb_object_keys(run_attrs) attr_handle 
    FROM h
  ),
  t AS (
    SELECT count(DISTINCT run_handle) AS total
    FROM h
  )
  SELECT a.attr_handle
  FROM a, t 
  GROUP BY a.attr_handle
  HAVING count(DISTINCT a.run_handle) = t.total; 
END;
$$ LANGUAGE plpgsql STABLE;
*/

\set QUIET 0
