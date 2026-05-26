\set QUIET 1
/* A series is conceptually an unordered set of points, each point having the same
 * set of coordinates.
 */
\echo 'create table series'
CREATE TABLE series (
  id SERIAL PRIMARY KEY,
  handle UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  name TEXT NOT NULL UNIQUE
);

\echo 'create field_data_typ'
CREATE TYPE field_data_typ AS ENUM ('int', 'float', 'string', 'bool');

/*
Holds the notion of a "field" which will provide basic type enforcement 
(int, float, string, bool) for the associated value.
*/
\echo 'create table field'
CREATE TABLE field (
  id SERIAL PRIMARY KEY,
  handle UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  name TEXT NOT NULL UNIQUE,
  data_type field_data_typ NOT NULL,
  description TEXT 
);

/* Represents a member of a conceptual 'Point', which is the data type of a given
 * series
*/
\echo 'create table coord'
CREATE TABLE coord (
  id SERIAL PRIMARY KEY,
  handle UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  series_id INT NOT NULL REFERENCES series(id) ON DELETE CASCADE,
  field_id INT NOT NULL REFERENCES field(id) ON DELETE CASCADE,
  UNIQUE (series_id, field_id)
);

/*
An entry in the run table describes the notion of a "run" in the sense of
running a program or script to generate data.  It is useful to be able to delete
all data associated with a given run, for example if the run parameters or code
are deemed faulty.
*/
\echo 'create table run'
CREATE TABLE run (
  id SERIAL PRIMARY KEY,
  handle UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tags TEXT[] NOT NULL DEFAULT '{}'::TEXT[], 
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- type used to store an attribute in the run_attr table
\echo 'create field_value_typ'
CREATE TYPE field_value_typ AS (
  field_handle UUID,
  int_val INT,
  float_val FLOAT,
  bool_val BOOLEAN,
  string_val TEXT
);

/* Holds attribute values associated with runs
*/
\echo 'create table run_attr'
CREATE TABLE run_attr (
  run_id INT NOT NULL REFERENCES run(id) ON DELETE CASCADE,
  field_id INT NOT NULL REFERENCES field(id) ON DELETE CASCADE,
  attr_value field_value_typ NOT NULL, 
  UNIQUE (run_id, field_id)
);

/*
A chunk is the unit of incrementally logging data to a series.  In the logging
application, it represents the data that has accumulated since the last buffer flush. 
I use BIGSERIAL for chunk_id here since 
*/
\echo 'create table chunk'
CREATE TABLE chunk (
  id BIGSERIAL PRIMARY KEY,
  series_id INT NOT NULL REFERENCES series(id) ON DELETE CASCADE,
  run_id INT NOT NULL REFERENCES run(id) ON DELETE CASCADE,
  num_points INT NOT NULL
);

CREATE INDEX idx_chunk_series_run ON chunk(series_id, run_id);

/*
enc_typ represents an ordered 1D sequence of values, viewed
as a flattened tensor of shape `shape`, and the following logic:

Exactly one of int_spans, float_spans, bool_bcast, string_bcast will be non-null.

spans[dim] == null:  dim has no broadcasting or regular-increment (range) pattern.
spans[dim] != null (>= 0):  the values along dim are evenly spaced from orig[dim]
  to orig[dim] + spans[dim].  A span value of zero represents broadcasting.

bcast[dim]
base:  the flattened values of orig such that if orig repeats along dimension dim, base is
the zero-th slice of this dimension, otherwise, it is the full set of values.

Here, orig means the original tensor which is encoded by this scheme.

For detail, see client/streamvis/dbutil.py: encode_array, decode_array
*/
\echo 'create enc_typ'
CREATE TYPE enc_typ AS (
  base BYTEA,
  shape INT[],
  int_spans INT[],
  float_spans REAL[],
  bool_bcast BOOLEAN[],
  string_bcast BOOLEAN[]
);

/* Holds one chunk of data for a given coordinate
 */
\echo 'create table coord_data'
CREATE TABLE coord_data (
  coord_id INT NOT NULL REFERENCES coord(id) ON DELETE CASCADE,
  chunk_id BIGINT NOT NULL REFERENCES chunk(id) ON DELETE CASCADE,
  enc_vals enc_typ NOT NULL,
  PRIMARY KEY (coord_id, chunk_id)
);

/*
Holds locks to prevent resource contention
*/
\echo 'create table data_lock'
CREATE TABLE data_lock (
  handle UUID NOT NULL UNIQUE,
  lock_type TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);

\set QUIET 0

