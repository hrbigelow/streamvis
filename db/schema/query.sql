/*
-- definitions of views and table functions
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
) AS $$
BEGIN
  RETURN QUERY
  SELECT c.chunk_id, s.series_name, f.field_name, r.run_handle, d.enc_vals
  FROM series s, field f, run r, chunk c, field_data d
  WHERE r.run_handle = ANY(p_run_handles)
  AND f.field_handle = ANY(p_field_handles)
  AND f.series_id = s.series_id
  AND c.series_id = s.series_id
  AND c.run_id = r.run_id
  AND (p_last_chunk_id IS NULL OR c.chunk_id > p_last_chunk_id)
  AND d.chunk_id = c.chunk_id
  AND d.field_id = f.field_id;
END;
$$ LANGUAGE plpgsql STABLE;
*/

CREATE VIEW series_vw AS
SELECT
  s.name,
  s.handle,
  array_agg(ROW(f.handle, f.name, f.data_type, f.description)::field_typ) fields
FROM series s
INNER JOIN coord c ON c.series_id = s.id
INNER JOIN field f ON f.id = c.field_id
GROUP BY s.name, s.handle;

CREATE VIEW field_vw AS
SELECT handle, name, data_type, description
FROM field;

CREATE VIEW run_vw AS
SELECT
  r.handle,
  r.tags,
  r.started_at,
  array_agg(ra.attr_value) FILTER (WHERE ra.attr_value IS NOT NULL) attrs
FROM run r
LEFT JOIN run_attr ra ON ra.run_id = r.id
GROUP BY r.handle, r.tags, r.started_at;


CREATE OR REPLACE FUNCTION filtered_by_attribute(
  p_attr_type TEXT,
  p_attr_value JSONB,
  p_filter attribute_filter_typ
)
RETURNS BOOLEAN
IMMUTABLE
LANGUAGE plpgsql
AS $$
DECLARE
  v_int INT;
  v_float REAL;
  v_bool BOOLEAN;
  v_string TEXT;
  v_type TEXT := jsonb_typeof(p_attr_value);
BEGIN
  IF p_attr_type IS NULL AND p_filter.include_missing THEN
    RETURN TRUE;
  END IF;

  CASE p_attr_type
    WHEN 'int' THEN
      IF v_type != 'number' THEN
        RAISE EXCEPTION 'p_attr_value must be a number type but got %', v_type;
      END IF;
      v_int := p_attr_value::INT;

      IF p_filter.int_vals IS NOT NULL THEN
        RETURN v_int <> ALL(p_filter.int_vals);
      ELSE
        RETURN (
          (p_filter.int_min IS NOT NULL AND v_int < p_filter.int_min) OR
          (p_filter.int_max IS NOT NULL AND v_int > p_filter.int_max)
        );
      END IF;
    WHEN 'float' THEN
      IF v_type != 'number' THEN
        RAISE EXCEPTION 'p_attr_value must be a number type but got %', v_type;
      END IF;
      v_float := p_attr_value::REAL;
      RETURN (
        (p_filter.float_min IS NOT NULL AND v_float < p_filter.float_min) OR
        (p_filter.float_max IS NOT NULL AND v_float > p_filter.float_max)
      );
    WHEN 'string' THEN
      IF v_type != 'string' THEN
        RAISE EXCEPTION 'p_attr_value must be a string type but got %', v_type;
      END IF;
      v_string := (p_attr_value #>> '{}');
      RETURN v_string = ANY(p_filter.string_vals);
    WHEN 'bool' THEN
      IF v_type != 'boolean' THEN
        RAISE EXCEPTION 'p_attr_value must be boolean type but got %', v_type;
      END IF;
      v_bool := p_attr_value::BOOLEAN;
      RETURN v_bool = ANY(p_filter.bool_vals);
  END CASE;
END;
$$;


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


CREATE OR REPLACE FUNCTION list_runs(
  IN p_attribute_filters JSONB,
  IN p_tag_filter JSONB,
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
    SELECT
      r.run_id,
      ra.attr_type,
      ra.attr_value,
      f.filter_obj
    FROM 
      jsonb_array_elements(p_attribute_filters) AS f(filter_obj)
      INNER JOIN attr a
        ON a.attr_handle = f.filter_obj.attr_handle
      CROSS JOIN run r
      LEFT JOIN run_attr ra
        ON ra.run_id = r.run_id
        AND ra.attr_id = a.attr_id
      WHERE NOT filtered_by_attribute(ra.attr_type, ra.attr_value, f.filter_obj)
  ),
  attr_filtered AS (
    SELECT mf.run_id
    FROM matched_filters mf
    GROUP BY mf.run_id
    HAVING COUNT(*) = filter_count
  )
    SELECT r.run_id
    FROM run r, attr_filtered af
    WHERE r.run_id = af.run_id
    AND NOT filtered_by_tags(r.run_tags, p_tag_filter);

END;
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

