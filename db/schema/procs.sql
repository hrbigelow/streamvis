CREATE OR REPLACE PROCEDURE add_data_lock(
  IN p_handle UUID,
  IN p_lock_type TEXT,
  IN p_ttl_seconds INTEGER 
)
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO data_lock (handle, lock_type, expires_at)
  VALUES (p_handle, p_lock_type, NOW() + make_interval(secs => p_ttl_seconds))
  ON CONFLICT (handle)
  DO UPDATE SET
    expires_at = NOW() + make_interval(secs => p_ttl_seconds);
END;
$$;

/*
Make an attribute descriptor.  Defines a named type which can be used to describe
a run attribute stored in run_attrs.
*/
CREATE OR REPLACE PROCEDURE create_attribute(
  IN p_attr_name TEXT,
  IN p_attr_type TEXT,
  IN p_attr_desc TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
  IF p_attr_type NOT IN ('int', 'float', 'string') THEN
    RAISE EXCEPTION 'p_attr_type must be one of "int", "float" or "string".  Got %', p_attr_type;
  END IF;

  INSERT INTO attr (attr_name, attr_type, attr_desc)
  VALUES (p_attr_name, p_attr_type, p_attr_desc);
END;
$$;


/*
Create a new series.
*/
CREATE OR REPLACE PROCEDURE create_series(
  IN p_series_name TEXT,
  IN p_series_structure JSONB
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_series_id INT;
BEGIN
  IF p_series_name IS NULL OR p_series_name = '' THEN
    RAISE EXCEPTION 'p_series_name must be a non-empty string';
  END IF;

  IF jsonb_typeof(p_series_structure) != 'object' OR p_series_structure = '{}'::jsonb THEN
    RAISE EXCEPTION 'p_series_structure must be a non-empty object';
  END IF;

  INSERT INTO series (series_name, structure)
  VALUES (p_series_name, p_series_structure)
  RETURNING series_id INTO v_series_id;

  INSERT INTO field (series_id, field_name, field_type)
  SELECT v_series_id, t.field_name, t.field_type
  FROM jsonb_each_text(p_series_structure) as t(field_name, field_type);
END;
$$;

CREATE OR REPLACE PROCEDURE delete_empty_series(
  IN p_series_name TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_handle UUID;
BEGIN
  SELECT series_handle into v_handle
  FROM series
  WHERE series_name = p_series_name;

  IF v_handle IS NULL THEN
    RAISE EXCEPTION 'delete_empty_series: a series named `%` does not exist', p_series_name;
  END IF;

  DELETE FROM series s
  WHERE s.series_handle = v_handle
  AND NOT EXISTS (
    SELECT 1
    FROM chunk c
    WHERE s.series_id = c.series_id
  );

  IF NOT FOUND THEN
    RAISE EXCEPTION 'delete_empty_series: series `%` is not empty; cannot delete', p_series_name;
  END IF;
END;
$$;


CREATE FUNCTION array_product(vals INT[])
RETURNS INT
IMMUTABLE
LANGUAGE plpgsql
AS $$
DECLARE
  prod INT = 1;
  size INT;
BEGIN
  FOREACH size IN ARRAY vals LOOP
    prod := prod * size;
  END LOOP;

  RETURN prod;
END;
$$;


/*
Append data to an existing series
*/
CREATE OR REPLACE PROCEDURE append_to_series(
  IN p_series_handle UUID,
  IN p_run_handle UUID,
  IN field_name TEXT[],
  IN field_vals enc_typ[],
  OUT success BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_series_id INT;
  v_run_id INT;
  v_chunk_id INT;
  v_structure JSONB;
  v_structure_error BOOLEAN;
  v_size INT;
  v_size_error BOOLEAN;
BEGIN
  -- RAISE NOTICE 'field_vals type: %', pg_typeof(field_vals);

  SELECT series_id, structure INTO v_series_id, v_structure
  FROM series
  WHERE series_handle = p_series_handle;

  IF NOT FOUND THEN
    success := FALSE;
    RETURN;
  END IF;

  SELECT run_id INTO v_run_id
  FROM run
  WHERE run_handle = p_run_handle;

  IF NOT FOUND THEN
    success := FALSE;
    RETURN;
  END IF;

  SELECT jsonb_object_agg(sub.fn, get_enc_signature(sub.fd)) != v_structure INTO v_structure_error
  FROM (
    SELECT unnest(field_name) AS fn, unnest(field_vals) AS fd
  ) AS sub;

  SELECT MIN(val), MAX(val) != MIN(val) INTO v_size, v_size_error
  FROM (
    SELECT array_product(fv.shape) AS val
    FROM unnest(field_vals) as fv
  ) AS sub;

  IF (v_structure_error OR v_size_error) THEN
    success := FALSE;
    RETURN;
  END IF;

  INSERT INTO chunk (series_id, run_id, num_points)
  VALUES (v_series_id, v_run_id, v_size)
  RETURNING chunk_id INTO v_chunk_id;

  INSERT INTO field_data (field_id, chunk_id, enc_vals)
  SELECT f.field_id, v_chunk_id, sub.fv
  FROM (
    SELECT unnest(field_name) AS fn, unnest(field_vals) as fv
  ) AS sub,
  field f
  WHERE sub.fn = f.field_name
  AND f.series_id = v_series_id;

  success := TRUE;

END;
$$;


CREATE OR REPLACE PROCEDURE create_run(
  OUT p_run_handle UUID
)
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO run
  DEFAULT VALUES
  RETURNING run_handle into p_run_handle;
END;
$$;


CREATE OR REPLACE PROCEDURE delete_run(
  IN p_run_handle UUID,
  OUT success BOOLEAN
) 
LANGUAGE plpgsql
AS $$
DECLARE
  v_run_id INT;
BEGIN
  SELECT run_id INTO v_run_id
  FROM run
  WHERE run_handle = p_run_handle;

  IF NOT FOUND THEN
    success := FALSE;
    RETURN;
  END IF;

  DELETE FROM run
  WHERE run_id = v_run_id;

  success := TRUE;

END;
$$;

CREATE OR REPLACE FUNCTION valid_attr_value(
  type_name TEXT,
  val JSONB
)
RETURNS BOOLEAN
IMMUTABLE
LANGUAGE plpgsql
AS $$
BEGIN
  CASE type_name
    WHEN 'int' THEN
      RETURN jsonb_typeof(val) = 'number' AND (val::numeric % 1 = 0);
    WHEN 'float' THEN
      RETURN jsonb_typeof(val) = 'number';
    WHEN 'text' THEN
      RETURN jsonb_typeof(val) = 'string';
    WHEN 'bool' THEN
      RETURN jsonb_typeof(val) = 'boolean';
    ELSE
      RAISE EXCEPTION 'Unknown type name: %. Valid types are int, float, text, bool.', type_name; 
  END CASE;
END;
$$;


CREATE OR REPLACE PROCEDURE set_run_attributes(
  IN p_run_handle UUID,
  IN p_attributes JSONB
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_attrs JSONB;
  v_num_attrs INT;
  v_num_valid_attrs INT;
BEGIN
  SELECT COALESCE(jsonb_object_agg(a.attr_handle, v.attr_value), '{}'::jsonb), count(*)
  INTO v_attrs, v_num_attrs
  FROM attr a, jsonb_each(p_attributes) AS v(attr_name, attr_value)
  WHERE a.attr_name = v.attr_name
  AND valid_attr_value(a.attr_type, v.attr_value);

  SELECT count(*)
  INTO v_num_valid_attrs 
  FROM jsonb_each(p_attributes);

  IF v_num_attrs != v_num_valid_attrs THEN
    RAISE EXCEPTION 'One or more run attributes are invalid';
  END IF;

  UPDATE run
  SET run_attrs = run_attrs || v_attrs
  WHERE run_handle = p_run_handle;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Run with handle %s not found', p_run_handle;
  END IF;

END;
$$;


