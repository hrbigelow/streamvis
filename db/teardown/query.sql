\set QUIET 1
\echo 'drop query_run_data'
DROP FUNCTION IF EXISTS query_run_data;

\echo 'drop get_data'
DROP FUNCTION IF EXISTS get_data;

\echo 'drop field_vw'
DROP VIEW IF EXISTS field_vw;

\echo 'drop series_vw'
DROP VIEW IF EXISTS series_vw;

\echo 'drop attribute_values_vw'
DROP VIEW IF EXISTS attribute_values_vw;

\echo 'drop started_at_vw'
DROP VIEW IF EXISTS started_at_vw;

\echo 'drop tag_vw'
DROP VIEW IF EXISTS tag_vw;

\echo 'drop run_vw'
DROP VIEW IF EXISTS run_vw;

\echo 'drop filtered_by_attribute'
DROP FUNCTION IF EXISTS filtered_by_attribute;

\echo 'drop filtered_by_tags'
DROP FUNCTION IF EXISTS filtered_by_tags;

\echo 'drop list_runs'
DROP FUNCTION IF EXISTS list_runs;

\echo 'drop function list_common_attributes'
DROP FUNCTION IF EXISTS list_common_attributes;

\echo 'drop function list_common_series'
DROP FUNCTION IF EXISTS list_common_series;

\set QUIET 0

