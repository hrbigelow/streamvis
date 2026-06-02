\set ON_ERROR_STOP on

BEGIN;

  -- These two types are stateful (not in teardown/types.sql), so they survive
  -- the teardown that runs at the top of 001_update_enc_typ.sql and need an
  -- in-place rename via ALTER TYPE.
  ALTER TYPE field_data_typ RENAME VALUE 'string' TO 'text';
  ALTER TYPE field_value_typ RENAME ATTRIBUTE string_val TO text_val CASCADE;

  -- attribute_filter_typ and full_field_value_typ are stateless (dropped by
  -- teardown/types.sql in 001 and recreated by refresh-nodata.sql with the
  -- new attribute names baked in), so no ALTER TYPE is needed for them.

END;
