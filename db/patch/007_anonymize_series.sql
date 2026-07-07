\set ON_ERROR_STOP on

BEGIN;

  \set QUIET 1

  \echo 'drop create_series'
  DROP PROCEDURE IF EXISTS create_series;

  \echo 'drop delete_empty_series'
  DROP PROCEDURE IF EXISTS delete_empty_series;

  \echo 'drop append_to_series'
  DROP PROCEDURE IF EXISTS append_to_series;

	\ir ../teardown/procs.sql

  \echo 'drop function list_common_series'
  DROP FUNCTION IF EXISTS list_common_series;

  \echo 'drop series_vw'
  DROP VIEW IF EXISTS series_vw; 

  \echo 'drop validate_coord_data_args'
  DROP FUNCTION IF EXISTS validate_coord_data_args;

  \echo 'drop get_coord_data'
  DROP FUNCTION IF EXISTS get_coord_data;

	\ir ../teardown/query.sql
	\ir ../teardown/base.sql

  \echo 'drop coord_data_validate_trg'
  DROP TRIGGER IF EXISTS coord_data_validate_trg ON coord_data;

  \echo 'drop coord_data_validate'
  DROP FUNCTION IF EXISTS coord_data_validate;

  \echo 'drop valid_coord_data'
  DROP FUNCTION IF EXISTS valid_coord_data;

	\ir ../teardown/triggers.sql

  \echo 'drop series_typ'
  DROP TYPE IF EXISTS series_typ;

  \echo 'drop coord_typ'
  DROP TYPE IF EXISTS coord_typ;

	\ir ../teardown/types.sql

  -- These run before `DROP TABLE coord` below, so `coord` is still available
  -- to seed field_series. The rebuilt stateless layer (append_to_run,
  -- get_chunk_data, list_common_attributes) queries field_series, and
  -- append_to_run does `INSERT INTO series DEFAULT VALUES`, which requires
  -- series to have no NOT-NULL columns without defaults -- hence dropping
  -- handle/name here. Leaving this block commented out lets the patch COMMIT
  -- (PL/pgSQL defers name resolution) but breaks pier at runtime.
  \echo 'dropping columns from series'
  ALTER TABLE series
  DROP COLUMN handle,
  DROP COLUMN name;

  \echo 'create field_series'
  CREATE TABLE field_series (
    field_id INT NOT NULL REFERENCES field(id) ON DELETE CASCADE,
    series_id INT NOT NULL REFERENCES series(id) ON DELETE CASCADE,
    PRIMARY KEY (field_id, series_id)
  );

  \echo 'insert into field_series'
  INSERT INTO field_series (field_id, series_id)
  SELECT field_id, series_id
  FROM coord;

  \echo 'create chunk_data'
  CREATE TABLE chunk_data (
    chunk_id BIGINT NOT NULL,
    field_id INT NOT NULL,
    enc_vals enc_typ NOT NULL
  );

  \echo 'insert into chunk_data'
  INSERT INTO chunk_data (chunk_id, field_id, enc_vals)
  SELECT cd.chunk_id, f.id, cd.enc_vals
  FROM coord_data cd
  JOIN coord co ON co.id = cd.coord_id
  JOIN field f ON f.id = co.field_id; 

  ALTER TABLE chunk_data
    ADD PRIMARY KEY (chunk_id, field_id);

  ALTER TABLE chunk_data
    ADD FOREIGN KEY (chunk_id) REFERENCES chunk(id) ON DELETE CASCADE,
    ADD FOREIGN KEY (field_id) REFERENCES field(id) ON DELETE CASCADE;

  DROP INDEX IF EXISTS idx_chunk_series_run;
  CREATE INDEX IF NOT EXISTS idx_chunk_run_series ON chunk(run_id, series_id);

  \echo 'drop coord_data'
  DROP TABLE coord_data;

  \echo 'drop coord'
  DROP TABLE coord;

  \ir ../schema/types.sql
  \ir ../schema/triggers.sql
  \ir ../schema/base.sql
  \ir ../schema/query.sql
  \ir ../schema/procs.sql

  \set QUIET 0

COMMIT;

