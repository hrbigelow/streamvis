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
CREATE TYPE field_data_typ AS ENUM ('int', 'float', 'text', 'bool');

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
  text_val TEXT
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

/* enc_typ encodes an array of values of one type - either FLOAT, INT, BOOLEAN, or
 * TEXT, with the following index decoding:

def decode(base, diff, size):
    ary = np.empty(self.size, dtype=np.int32)
    ary[0] = base
    for i in range(1, size):
        ary[i] = ary[i-1] + diff[(i-1) % len(diff)]
    return ary

 FLOAT array:   floats
 INT array:     decode(base, diff, size)
 BOOLEAN array:
    if bools is NULL:  decode(base, diff, size) == 1
    else: bools
 TEXT array:    texts[decode(base, diff, size)]
*/

\echo 'create enc_typ'
CREATE TYPE enc_typ AS (
  data_type field_data_typ,
  floats REAL[],
  bools BOOLEAN[],
  texts TEXT[],
  base INT,
  diff INT[],
  size INT
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

CREATE INDEX idx_coord_data__chunk ON coord_data(chunk_id); 

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

