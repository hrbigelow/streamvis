\set QUIET 1

\echo 'drop project_field_value'
DROP FUNCTION IF EXISTS project_field_value;

\echo 'drop valid_attr_value'
DROP FUNCTION IF EXISTS valid_attr_value;

\echo 'drop valid_enc_typ'
DROP FUNCTION IF EXISTS valid_enc_typ;

\echo 'drop tag_filter_typ'
DROP TYPE IF EXISTS tag_filter_typ;

\echo 'drop attribute_filter_typ'
DROP TYPE IF EXISTS attribute_filter_typ;

\echo 'drop field_value_typ'
DROP TYPE IF EXISTS field_value_typ;

\echo 'drop enc_typ'
DROP TYPE IF EXISTS enc_typ;

\echo 'drop field_typ'
DROP TYPE IF EXISTS field_typ;

\echo 'drop field_data_typ'
DROP TYPE IF EXISTS field_data_typ;

\set QUIET 0
