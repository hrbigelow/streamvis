-- definitions of views and table functions
DROP FUNCTION IF EXISTS get_data;
DROP VIEW IF EXISTS series_vw;
DROP VIEW IF EXISTS field_vw;
DROP VIEW IF EXISTS attribute_vw;

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

CREATE VIEW field_vw AS
SELECT
    s.series_name, f.field_name, f.field_handle
FROM series s, field f
WHERE f.series_id = s.series_id;


CREATE VIEW series_vw AS
SELECT
    series_name, series_handle, structure
FROM series;

CREATE VIEW attribute_vw AS
SELECT
    attr_handle, attr_name, attr_type, attr_desc
FROM attr;


