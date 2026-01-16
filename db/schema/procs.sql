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


/*
Create a new series.
*/
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
  IN p_field_names TEXT[],
  IN p_field_vals enc_typ[]
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
  IF array_length(p_field_names, 1) != array_length(p_field_vals) THEN
    RAISE EXCEPTION 'p_field_names and p_field_vals have unequal length';
  END IF;




  v_size := 

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
  WHERE handle = p_run_handle;

  IF NOT FOUND THEN
    success := FALSE;
    RETURN;
  END IF;

  DELETE FROM run
  WHERE id = v_run_id;

  success := TRUE;

END;
$$;



CREATE OR REPLACE PROCEDURE set_run_attributes(
  IN p_run_handle UUID,
  IN p_attributes field_value_typ[]
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_all_valid BOOLEAN;
  v_run_id INT;
BEGIN
  SELECT run_id INTO v_run_id
  FROM run
  WHERE run_handle = p_run_handle;

  IF v_run_id IS NULL THEN
    RAISE EXCEPTION 'p_run_handle % did not identify an existing run', p_run_handle;
  END IF;

  WITH valid_attrs AS (
    SELECT valid_attr_value(val) AND
    EXISTS (
      SELECT 1 FROM field f WHERE (val).field_handle = f.field_handle
    ) is_valid
    FROM unnest(p_attributes) AS val
  )
  SELECT bool_and(is_valid) INTO v_all_valid
  FROM valid_attrs;

  IF NOT v_all_valid THEN
    RAISE EXCEPTION 'Not all attributes were valid';
  END IF;

  INSERT INTO run_attr (run_id, field_id, attr_value)
  SELECT v_run_id, f.field_id, val 
  FROM unnest(p_attributes) AS val
  INNER JOIN field f ON (val).field_handle = f.field_handle
  ON CONFLICT (run_id, field_id)
  DO UPDATE SET
    attr_value = EXCLUDED.attr_value;

END;
$$;


