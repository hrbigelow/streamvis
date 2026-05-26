\set QUIET 1

\echo 'drop coord_data_validate_trg'
DROP TRIGGER IF EXISTS coord_data_validate_trg ON coord_data;

\echo 'drop coord_data_validate'
DROP FUNCTION IF EXISTS coord_data_validate;

\echo 'drop valid_coord_data'
DROP FUNCTION IF EXISTS valid_coord_data;


\set QUIET 0

