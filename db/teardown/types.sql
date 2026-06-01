\set QUIET 1

\echo 'drop valid_attr_value'
DROP FUNCTION IF EXISTS valid_attr_value;

\echo 'drop unpack_enc_int'
DROP FUNCTION IF EXISTS unpack_enc_int;

\echo 'drop unpack_enc_float'
DROP FUNCTION IF EXISTS unpack_enc_float;

\echo 'drop unpack_enc_bool'
DROP FUNCTION IF EXISTS unpack_enc_bool;

\echo 'drop unpack_enc_text'
DROP FUNCTION IF EXISTS unpack_enc_text;

\echo 'drop pack_float_enc'
DROP FUNCTION IF EXISTS pack_float_enc;

\echo 'drop valid_enc_typ'
DROP FUNCTION IF EXISTS valid_enc_typ;

\echo 'drop tag_filter_typ'
DROP TYPE IF EXISTS tag_filter_typ;

\echo 'drop attribute_filter_typ'
DROP TYPE IF EXISTS attribute_filter_typ;

\echo 'drop full_field_value_typ'
DROP TYPE IF EXISTS full_field_value_typ;

\echo 'drop field_typ'
DROP TYPE IF EXISTS field_typ;

\echo 'drop series_typ'
DROP TYPE IF EXISTS series_typ;

\echo 'drop coord_typ'
DROP TYPE IF EXISTS coord_typ;

\set QUIET 0
