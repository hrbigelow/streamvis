/*
A series is conceptually an unordered set of points, each point having the same
structure consisting of an unordered collection of field names and respective types.
*/
CREATE TABLE series (
  series_id SERIAL PRIMARY KEY,
  series_handle UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  series_name TEXT NOT NULL UNIQUE,
  structure JSONB -- a convenience field for inspecting the structure
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
  UNIQUE (series_id, field_name),
  CONSTRAINT valid_field_type CHECK (field_type IN ('i32', 'f32'))
);

/*
An entry in the run table describes the notion of a "run" in the sense of
running a program or script to generate data.  It is useful to be able to delete
all data associated with a given run, for example if the run parameters or code
are deemed faulty.
*/
CREATE TABLE run (
  run_id SERIAL PRIMARY KEY,
  run_handle UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  run_tags TEXT[] NOT NULL DEFAULT '{}'::TEXT[], 
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


/*
Holds the notion of an "attribute" which will provide basic type enforcement 
(int, float, text) for the associated value.  run.run_attrs holds attr_id => value.
These are run-level attributes.
*/
CREATE TABLE attr (
  attr_id SERIAL PRIMARY KEY,
  attr_handle UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  attr_name TEXT NOT NULL UNIQUE,
  attr_type TEXT NOT NULL, -- int, float, string, bool
  attr_desc TEXT 
);


/*
Holds attribute values associated with runs
*/
CREATE TABLE run_attr (
  run_id INT NOT NULL REFERENCES run(run_id) ON DELETE CASCADE,
  attr_id INT NOT NULL REFERENCES attr(attr_id) ON DELETE CASCADE,
  attr_value JSONB NOT NULL, -- one of 'string', 'number', or 'bool'
  UNIQUE (run_id, attr_id)
);


/*
A chunk is the unit of incrementally logging data to a series.  In the logging
application, it represents the data that has accumulated since the last buffer flush. 
I use BIGSERIAL for chunk_id here since 
*/
CREATE TABLE chunk (
  chunk_id BIGSERIAL PRIMARY KEY,
  series_id INT NOT NULL REFERENCES series(series_id) ON DELETE CASCADE,
  run_id INT NOT NULL REFERENCES run(run_id) ON DELETE CASCADE,
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


