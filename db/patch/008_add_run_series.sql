\set ON_ERROR_STOP on

BEGIN;

  \set QUIET 1

  \ir ../teardown/procs.sql
  \ir ../teardown/query.sql
  \ir ../teardown/base.sql
  \ir ../teardown/types.sql

  \echo 'create table run_series'
  CREATE TABLE run_series (
    run_id INT NOT NULL REFERENCES run(id) ON DELETE CASCADE,
    series_id INT NOT NULL REFERENCES series(id) ON DELETE CASCADE,
    PRIMARY KEY (run_id, series_id)
  );

  \echo 'insert into run_series'
  INSERT INTO run_series (run_id, series_id)
  SELECT DISTINCT run_id, series_id
  FROM chunk;

  \ir ../schema/types.sql
  \ir ../schema/base.sql
  \ir ../schema/query.sql
  \ir ../schema/procs.sql

END;
