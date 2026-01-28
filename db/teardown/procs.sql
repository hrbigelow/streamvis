\set QUIET 1

\echo 'drop add_data_lock'
DROP PROCEDURE IF EXISTS add_data_lock;

\echo 'drop create_field'
DROP PROCEDURE IF EXISTS create_field;

\echo 'drop create_series'
DROP PROCEDURE IF EXISTS create_series;

\echo 'drop delete_empty_series'
DROP PROCEDURE IF EXISTS delete_empty_series;

\echo 'drop append_to_series'
DROP PROCEDURE IF EXISTS append_to_series;

\echo 'drop create_run'
DROP PROCEDURE IF EXISTS create_run;

\echo 'drop delete_run'
DROP PROCEDURE IF EXISTS delete_run;

\echo 'drop set_run_attributes'
DROP PROCEDURE IF EXISTS set_run_attributes;

\echo 'drop array_product'
DROP FUNCTION IF EXISTS array_product;

\echo 'drop add_run_tag'
DROP PROCEDURE IF EXISTS add_run_tag;

\echo 'drop delete_run_tag'
DROP PROCEDURE IF EXISTS delete_run_tag;

\set QUIET 0


