\set ON_ERROR_STOP on

BEGIN;

  ALTER TYPE field_data_typ RENAME VALUE 'string' TO 'text';
  ALTER TYPE field_value_typ RENAME ATTRIBUTE string_val TO text_val CASCADE;
  ALTER TYPE attribute_filter_typ RENAME ATTRIBUTE string_vals TO text_vals CASCADE;
  ALTER TYPE full_field_value_typ RENAME ATTRIBUTE string_val TO text_val CASCADE;

END;


