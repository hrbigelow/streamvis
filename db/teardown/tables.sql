\set QUIET 1

\echo 'drop data_lock'
DROP TABLE IF EXISTS data_lock;

\echo 'drop coord_data'
DROP TABLE IF EXISTS coord_data;

\echo 'drop chunk'
DROP TABLE IF EXISTS chunk;

\echo 'drop run_attr'
DROP TABLE IF EXISTS run_attr;

\echo 'drop coord'
DROP TABLE IF EXISTS coord;

\echo 'drop run'
DROP TABLE IF EXISTS run;

\echo 'drop field'
DROP TABLE IF EXISTS field;

\echo 'drop series'
DROP TABLE IF EXISTS series;

\set QUIET 0

