DROP PROCEDURE IF EXISTS add_data_lock;
DROP PROCEDURE IF EXISTS make_or_get_series;
DROP PROCEDURE IF EXISTS append_to_series;
DROP PROCEDURE IF EXISTS create_run;
DROP PROCEDURE IF EXISTS delete_run;
DROP FUNCTION IF EXISTS array_product;


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
Create a new series.  

If delete_existing = True:
  delete any existing series, create a new series, return its handle

If delete_existing = False:
  - If no existing series, create the new series and return its handle
  - If a series of same structure exists, return the existing series_handle
  - If a series of different structure exists, return NULL for p_series_handle (an error)

p_series_structure is a flat dictionary of string => option['f32', i32']

*/
CREATE OR REPLACE PROCEDURE make_or_get_series(
  IN p_series_name TEXT,
  IN p_series_structure JSONB,
  OUT p_series_handle UUID
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_series_id INT;
  v_existing_structure JSONB;
BEGIN

  SELECT series_id, series_handle INTO v_series_id, p_series_handle
  FROM series
  WHERE series_name = p_series_name
  LIMIT 1;

  IF v_series_id IS NULL THEN
    -- create a new series and return its handle
    INSERT INTO series (series_name, structure)
    VALUES (p_series_name, p_series_structure)
    ON CONFLICT DO NOTHING;

    IF NOT FOUND THEN
      -- should never happen because v_series_id was found via (p_series_name)
      p_series_handle := NULL;
      RETURN;
    END IF;

    SELECT series_id, series_handle INTO v_series_id, p_series_handle
    FROM series
    WHERE series_name = p_series_name;

    INSERT INTO field (series_id, field_name, field_type)
    SELECT v_series_id, t.field_name, t.field_type
    FROM jsonb_each_text(p_series_structure) as t(field_name, field_type);
    RETURN;

  ELSE
    -- if existing series is congruent, return its handle
    SELECT jsonb_object_agg(field_name, field_type) INTO v_existing_structure
    FROM field
    WHERE series_id = v_series_id;
    IF v_existing_structure = p_series_structure THEN
      RETURN;
    ELSE
      -- inconsistent structure, but can't delete existing 
      p_series_handle := NULL;
      RETURN;
    END IF;
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



