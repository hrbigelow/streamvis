DROP TABLE IF EXISTS field_data;
DROP TABLE IF EXISTS field;
DROP TABLE IF EXISTS chunk;
DROP TABLE IF EXISTS series;
DROP TABLE IF EXISTS scope;
DROP TABLE IF EXISTS data_lock;
DROP FUNCTION IF EXISTS valid_enc_typ;
DROP FUNCTION IF EXISTS get_enc_signature;

/*
A scope is the top-level grouping of all data
*/
CREATE TABLE scope (
  scope_id SERIAL PRIMARY KEY,
  scope_handle UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  scope_name TEXT NOT NULL UNIQUE
);

/*
A series is conceptually an unordered set of points, each point having the same
structure consisting of an unordered collection of field names and respective types.
The data associated with a given series_id will be append-only.  But, an application
may delete the entire series and re-create a new one with the same series_name and
structure.
*/
CREATE TABLE series (
  series_id SERIAL PRIMARY KEY,
  series_handle UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  scope_id INTEGER NOT NULL REFERENCES scope(scope_id) ON DELETE CASCADE,
  series_name TEXT NOT NULL,
  structure JSONB, -- a convenience field for inspecting the structure
  UNIQUE (scope_id, series_name)
);

/*
An entry in the field table describes a field of a given series.
*/
CREATE TABLE field (
  field_id SERIAL PRIMARY KEY,
  field_handle UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  series_id INT NOT NULL REFERENCES series(series_id) ON DELETE CASCADE,
  field_name TEXT NOT NULL,
  field_type TEXT NOT NULL,
  UNIQUE (series_id, field_name)
);

/*
A chunk is the unit of incrementally logging data to a series.  In the logging
application, it represents the data that has accumulated since the last buffer flush. 
I use BIGSERIAL for chunk_id here since 
*/
CREATE TABLE chunk (
  chunk_id BIGSERIAL PRIMARY KEY,
  series_id INT NOT NULL REFERENCES series(series_id) ON DELETE CASCADE,
  num_points INT NOT NULL
);


CREATE FUNCTION valid_enc_typ(item enc_typ) 
RETURNS BOOLEAN
IMMUTABLE
LANGUAGE sql 
AS $$
SELECT 
  ((item).i32_spans IS NULL) != ((item).f32_spans IS NULL)
$$;

CREATE FUNCTION get_enc_signature(item enc_typ)
RETURNS TEXT
IMMUTABLE
LANGUAGE sql
AS $$
SELECT
  CASE 
    WHEN item.i32_spans IS NOT NULL THEN 'i32'
    WHEN item.f32_spans IS NOT NULL THEN 'f32'
  END
$$;


CREATE TABLE field_data (
  field_id INT NOT NULL REFERENCES field(field_id) ON DELETE CASCADE,
  chunk_id BIGINT NOT NULL REFERENCES chunk(chunk_id) ON DELETE CASCADE,
  enc_vals enc_typ NOT NULL,
  PRIMARY KEY (field_id, chunk_id),
  CONSTRAINT valid_data CHECK (valid_enc_typ(enc_vals)) 
);


/*
Holds locks to prevent resource contention
*/
CREATE TABLE data_lock (
  handle UUID NOT NULL UNIQUE,
  lock_type TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);


