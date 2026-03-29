\set QUIET 1

\echo 'create filter_by_tags'
/* Returns true if the run having the set of run tags should be included 
*/
CREATE OR REPLACE FUNCTION filter_by_tags(
  p_run_tags TEXT[], -- not NULL by constraint on the run table
  p_tag_filter tag_filter_typ
) RETURNS BOOLEAN
IMMUTABLE
LANGUAGE plpgsql
AS $$
BEGIN
  IF cardinality(p_tag_filter.tags) = 0 THEN
    RETURN TRUE; 
  ELSIF p_tag_filter.match_all THEN
    RETURN (p_run_tags @> p_tag_filter.tags);
  ELSE
    RETURN (p_run_tags && p_tag_filter.tags);
  END IF;
END;
$$;


\echo 'create filter_by_attribute'
/* Return TRUE if the value should be included 
*/
CREATE OR REPLACE FUNCTION filter_by_attribute(
  p_data_type field_data_typ,
  p_value field_value_typ,
  p_attr_filter attribute_filter_typ
)
RETURNS BOOLEAN
IMMUTABLE
LANGUAGE plpgsql
AS $$
BEGIN
  IF p_value.field_handle != p_attr_filter.field_handle THEN
    RAISE EXCEPTION 'Field handles of p_value and p_attr_filter unequal';
  END IF;

  IF p_data_type IS NULL AND p_attr_filter.include_missing THEN
    RETURN TRUE;
  END IF;

  -- we just trust that p_data_type matches that of the field handle
  CASE p_data_type
    WHEN 'int' THEN
      IF p_attr_filter.int_vals IS NOT NULL THEN
        RETURN p_value.int_val = ANY(p_attr_filter.int_vals);
      ELSE
        RETURN (
          (p_attr_filter.int_min IS NULL OR p_value.int_val >= p_attr_filter.int_min) AND
          (p_attr_filter.int_max IS NULL OR p_value.int_val <= p_attr_filter.int_max)
        );
      END IF;
    WHEN 'float' THEN
      RETURN (
        (p_attr_filter.float_min IS NULL OR p_value.float_val >= p_attr_filter.float_min) AND 
        (p_attr_filter.float_max IS NULL OR p_value.float_val <= p_attr_filter.float_max)
      );
    WHEN 'bool' THEN
      RETURN p_value.bool_val = ANY(p_attr_filter.bool_vals);
    WHEN 'string' THEN
      RETURN p_value.string_val = ANY(p_attr_filter.string_vals);
  END CASE;
END;
$$;

\echo 'create list_runs_internal'
/* 
*/
CREATE OR REPLACE FUNCTION list_runs_internal(
  IN p_attribute_filters attribute_filter_typ[],
  IN p_tag_filter tag_filter_typ,
  IN p_min_started_at TIMESTAMPTZ,
  IN p_max_started_at TIMESTAMPTZ
) RETURNS TABLE (
  run_id INT
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
    WHERE NOT filter_by_attribute(f.data_type, ra.attr_value, filter_obj)
    GROUP BY r.id
  )
    SELECT r.id
    FROM run r
    LEFT JOIN excluded_runs er ON er.run_id = r.id
    WHERE er.run_id IS NULL -- anti-join
    AND filter_by_tags(r.tags, p_tag_filter)
    AND (p_min_started_at IS NULL OR p_min_started_at <= r.started_at)
    AND (p_max_started_at IS NULL OR r.started_at <= p_max_started_at)
    AND EXISTS (SELECT 1 FROM chunk c WHERE c.run_id = r.id);
$$;
