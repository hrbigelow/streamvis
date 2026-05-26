\set QUIET 1

\echo 'drop project_field_value'
DROP FUNCTION IF EXISTS project_field_value;

\echo 'drop valid_attr_value'
DROP FUNCTION IF EXISTS valid_attr_value;

\echo 'drop tag_filter_typ'
DROP TYPE IF EXISTS tag_filter_typ;

\echo 'drop attribute_filter_typ'
DROP TYPE IF EXISTS attribute_filter_typ;

\echo 'drop field_value_typ'
DROP TYPE IF EXISTS field_value_typ;

\echo 'drop full_field_value_typ'
DROP TYPE IF EXISTS full_field_value_typ;

\echo 'drop enc_typ'
DROP TYPE IF EXISTS enc_typ;

\echo 'drop field_typ'
DROP TYPE IF EXISTS field_typ;

\echo 'drop series_typ'
DROP TYPE IF EXISTS series_typ;

\echo 'drop coord_typ'
DROP TYPE IF EXISTS coord_typ;

\echo 'drop field_data_typ'
DROP TYPE IF EXISTS field_data_typ;

\set QUIET 0
