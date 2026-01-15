/* A series is conceptually an unordered set of points, each point having the same
 * set of coordinates.
 */
CREATE TABLE series (
  series_id SERIAL PRIMARY KEY,
  series_handle UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  series_name TEXT NOT NULL UNIQUE
);

/*
Holds the notion of a "field" which will provide basic type enforcement 
(int, float, string, bool) for the associated value.
*/
CREATE TABLE field (
  field_id SERIAL PRIMARY KEY,
  field_handle UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  field_name TEXT NOT NULL UNIQUE,
  field_type field_typ NOT NULL,
  field_desc TEXT 
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

/* Represents a member of a conceptual 'Point', which is the data type of a given
 * series
*/
CREATE TABLE coord (
  coord_id SERIAL PRIMARY KEY,
  coord_handle UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  series_id INT NOT NULL REFERENCES series(series_id) ON DELETE CASCADE,
  field_id INT NOT NULL REFERENCES field(field_id) ON DELETE CASCADE,
  UNIQUE (series_id, field_id)
);

/*
Holds attribute values associated with runs
*/
CREATE TABLE run_attr (
  run_id INT NOT NULL REFERENCES run(run_id) ON DELETE CASCADE,
  field_id INT NOT NULL REFERENCES field(field_id) ON DELETE CASCADE,
  attr_value attr_typ NOT NULL, 
  UNIQUE (run_id, field_id)
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

/* Holds one chunk of data for a given coordinate
 */
CREATE TABLE coord_data (
  coord_id INT NOT NULL REFERENCES coord(coord_id) ON DELETE CASCADE,
  chunk_id BIGINT NOT NULL REFERENCES chunk(chunk_id) ON DELETE CASCADE,
  enc_vals enc_typ NOT NULL,
  PRIMARY KEY (coord_id, chunk_id),
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


