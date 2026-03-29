\set QUIET 1

\echo 'drop filter_by_tags'
DROP FUNCTION IF EXISTS filter_by_tags;

\echo 'drop filter_by_attribute'
DROP FUNCTION IF EXISTS filter_by_attribute;

\echo 'drop list_runs_internal'
DROP FUNCTION IF EXISTS list_runs_internal;

\set QUIET 0

