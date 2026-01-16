\set QUIET 1
\echo 'add_data_lock'
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
\echo 'create_field'
CREATE OR REPLACE PROCEDURE create_field(
  IN p_name TEXT,
  IN p_data_type field_data_typ,
  IN p_description TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO field (name, data_type, description)
  VALUES (p_name, p_data_type, p_description);
END;
$$;


/* Create a new series.
*/
\echo 'create_series'
CREATE OR REPLACE PROCEDURE create_series(
  IN p_series_name TEXT,
  IN p_field_names TEXT[]
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_series_id INT;
BEGIN
  IF p_series_name IS NULL OR p_series_name = '' THEN
    RAISE EXCEPTION 'p_series_name must be a non-empty string';
  END IF;

  INSERT INTO series (name)
  VALUES (p_series_name)
  RETURNING id INTO v_series_id;

  INSERT INTO coord (series_id, field_id)
  SELECT v_series_id, f.id 
  FROM field f 
  INNER JOIN unnest(p_field_names) AS n(field_name)
  ON f.name = n.field_name;

  -- TODO: check number inserted
END;
$$;

\echo 'delete_empty_series'
CREATE OR REPLACE PROCEDURE delete_empty_series(
  IN p_series_name TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_handle UUID;
BEGIN
  SELECT handle into v_handle
  FROM series
  WHERE name = p_series_name;

  IF v_handle IS NULL THEN
    RAISE EXCEPTION 'delete_empty_series: a series named `%` does not exist', p_series_name;
  END IF;

  DELETE FROM series s
  WHERE s.handle = v_handle
  AND NOT EXISTS (
    SELECT 1
    FROM chunk c
    WHERE s.id = c.series_id
  );

  IF NOT FOUND THEN
    RAISE EXCEPTION 'delete_empty_series: series `%` is not empty; cannot delete', p_series_name;
  END IF;
END;
$$;

\echo 'array_product'
CREATE OR REPLACE FUNCTION array_product(vals INT[])
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
\echo 'append_to_series'
CREATE OR REPLACE PROCEDURE append_to_series(
  IN p_series_handle UUID,
  IN p_run_handle UUID,
  IN p_field_vals enc_typ[]
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_field_val enc_typ;
  v_series_id INT;
  v_run_id INT;
  v_chunk_id INT;
  v_series_field_handles UUID[];
BEGIN

  SELECT id INTO v_series_id
  FROM series
  WHERE handle = p_series_handle;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Series with handle %s not found', p_series_handle;
  END IF;

  SELECT id INTO v_run_id
  FROM runp
  WHERE handle = p_run_handle;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Run with handle %s not found', p_run_handle;
  END IF;

  SELECT array_agg(f.handle) INTO v_series_field_handles
  FROM field f
  JOIN coord c ON c.field_id = f.id
  WHERE c.series_id = v_series_id; 

  FOREACH v_field_val IN ARRAY p_field_vals LOOP
    IF NOT (valid_enc_typ(v_field_val) 
      AND (v_field_val).field_handle = ANY(v_series_field_handles)
    ) THEN
      RAISE EXCEPTION 'Field %s invalid', (v_field_val).field_handle;
    END IF;
  END LOOP;

  IF array_length(p_field_vals, 1) != array_length(v_series_field_handles) THEN
    RAISE EXCEPTION 'Received incorrect number of field_vals';
  END IF;

  INSERT INTO chunk (series_id, run_id, num_points)
  VALUES (v_series_id, v_run_id, array_product(p_field_vals[0].shape))
  RETURNING id INTO v_chunk_id;

  INSERT INTO coord_data (coord_id, chunk_id, enc_vals)
  SELECT f.field_id, v_chunk_id, sub.fv
  FROM (
    SELECT unnest(field_name) AS fn, unnest(field_vals) as fv
  ) AS sub,
  field f
  WHERE sub.fn = f.name
  AND f.series_id = v_series_id;

END;
$$;


\echo 'create_run'
CREATE OR REPLACE PROCEDURE create_run(
  OUT p_run_handle UUID
)
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO run DEFAULT VALUES
  RETURNING handle INTO p_run_handle;
END;
$$;

CREATE OR REPLACE PROCEDURE replace_run(
  IN p_run_handle UUID
)
LANGUAGE plpgsql
AS $$
BEGIN
  DELETE FROM run
  WHERE handle = p_run_handle;

  INSERT INTO run (handle)
  VALUES (p_run_handle);
END;
$$;


\echo 'delete_run'
CREATE OR REPLACE PROCEDURE delete_run(
  IN p_run_handle UUID
) 
LANGUAGE plpgsql
AS $$
DECLARE
  v_run_id INT;
BEGIN
  SELECT id INTO v_run_id
  FROM run
  WHERE handle = p_run_handle;

  DELETE FROM run
  WHERE id = v_run_id;

END;
$$;


\echo 'set_run_attributes'
CREATE OR REPLACE PROCEDURE set_run_attributes(
  IN p_run_handle UUID,
  IN p_attributes field_value_typ[]
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_all_valid BOOLEAN;
  v_run_id INT;
  v_attribute field_value_typ;
  errors TEXT[] := ARRAY[]::TEXT[];
BEGIN
  SELECT id INTO v_run_id
  FROM run
  WHERE handle = p_run_handle;

  IF v_run_id IS NULL THEN
    RAISE EXCEPTION 'p_run_handle % did not identify an existing run', p_run_handle;
  END IF;

  FOREACH v_attribute IN ARRAY p_attributes
  LOOP
    IF NOT valid_attr_value(v_attribute) THEN
      errors := array_append(errors, 
        'Invalid attribute:\n' ||
        format('field_handle: %s\n', (v_attribute).field_handle) ||
        format('int_val: %L\n', (v_attribute).int_val) ||
        format('float_val: %L\n', (v_attribute).float_val) ||
        format('bool_val: %L\n', (v_attribute).bool_val) ||
        format('string_val: %L\n', (v_attribute).string_val)
      );
    END IF;
    IF NOT EXISTS (
      SELECT 1 FROM field f WHERE (v_attribute).field_handle = f.handle
    ) THEN
      errors := array_append(errors, format('attribute handle invalid: %s', (v_attribute).field_handle));
    END IF;
  END LOOP;

  IF array_length(errors, 1) > 0 THEN
    RAISE EXCEPTION 'One or more attributes are invalid: %', array_to_string(errors, '\n');
  END IF;

  INSERT INTO run_attr (run_id, field_id, attr_value)
  SELECT v_run_id, f.id, val 
  FROM unnest(p_attributes) AS val
  INNER JOIN field f ON (val).field_handle = f.handle
  ON CONFLICT (run_id, field_id)
  DO UPDATE SET
    attr_value = EXCLUDED.attr_value;

END;
$$;

\set QUIET 0
