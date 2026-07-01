\set QUIET 1

\echo 'drop table data_lock'
DROP TABLE IF EXISTS data_lock;

\echo 'drop enc_typ'
DROP TYPE IF EXISTS enc_typ;

\echo 'drop table chunk_data'
DROP TABLE IF EXISTS chunk_data;

\echo 'drop table chunk'
DROP TABLE IF EXISTS chunk;

\echo 'drop table run_attr'
DROP TABLE IF EXISTS run_attr;

\echo 'drop field_value_typ'
DROP TYPE IF EXISTS field_value_typ;

\echo 'drop table run'
DROP TABLE IF EXISTS run;

\echo 'drop table field'
DROP TABLE IF EXISTS field;

\echo 'drop field_data_typ'
DROP TYPE IF EXISTS field_data_typ;

\echo 'drop table field_series'
DROP TABLE IF EXISTS field_series;

\echo 'drop table series'
DROP TABLE IF EXISTS series;

\set QUIET 0

