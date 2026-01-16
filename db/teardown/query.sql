\set QUIET 1

\echo 'drop get_data'
DROP FUNCTION IF EXISTS get_data;

\echo 'drop field_vw'
DROP VIEW IF EXISTS field_vw;

\echo 'drop series_vw'
DROP VIEW IF EXISTS series_vw;

\echo 'drop run_vw'
DROP VIEW IF EXISTS run_vw;

\echo 'drop filtered_by_attribute'
DROP FUNCTION IF EXISTS filtered_by_attribute;

\echo 'drop filtered_by_tags'
DROP FUNCTION IF EXISTS filtered_by_tags;

\echo 'drop list_runs'
DROP FUNCTION IF EXISTS list_runs;

\echo 'drop get_common_attributes'
DROP FUNCTION IF EXISTS get_common_attributes;

\set QUIET 0

