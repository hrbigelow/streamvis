\set ON_ERROR_STOP on

BEGIN;

	-- Functions that existed before this migration but are renamed or removed
	-- in the new schema. They are not listed in the teardown files (which only
	-- know about the *current* schema), so they linger and would block the
	-- DROP TYPE enc_typ at the end of this patch via pg_depend. Drop them
	-- explicitly here.
	DROP FUNCTION IF EXISTS get_data(integer[], uuid[], uuid[], bigint, bigint);
	DROP FUNCTION IF EXISTS project_field_value(field_value_typ, integer);

	\ir ../teardown/procs.sql
	\ir ../teardown/query.sql
	\ir ../teardown/base.sql
	\ir ../teardown/triggers.sql
	\ir ../teardown/types.sql

	CREATE EXTENSION IF NOT EXISTS plpython3u;

  CREATE OR REPLACE FUNCTION bytea_to_float4_array_le(b BYTEA)
	RETURNS REAL[]
	LANGUAGE plpython3u
	IMMUTABLE PARALLEL SAFE
	AS $$
		import struct
		n = len(b) // 4
		return list(struct.unpack(f"<{n}f", b))
	$$;

	CREATE OR REPLACE FUNCTION bytea_to_int_array_le(b BYTEA)
	RETURNS INT[]
	LANGUAGE sql
	IMMUTABLE PARALLEL SAFE
	AS $$
		SELECT
		array_agg(
			('x' || encode(reverse(substring(b FROM i for 4)), 'hex'))::bit(32)::INT
			ORDER BY i
		)
		FROM generate_series(1, length(b), 4) AS i;
	$$;

  CREATE OR REPLACE FUNCTION bytea_to_bool_array(b BYTEA)
  RETURNS BOOLEAN[]
  LANGUAGE sql
  IMMUTABLE PARALLEL SAFE
  AS $$
    SELECT
    array_agg(get_byte(b, i) <> 0 ORDER BY i)
    FROM generate_series(0, length(b) - 1) AS i;
  $$;

  CREATE OR REPLACE FUNCTION bytea_to_text_array_le(b BYTEA)
  RETURNS TEXT[]
  LANGUAGE plpython3u 
  IMMUTABLE PARALLEL SAFE
  AS $$
    import struct
    itemsize = struct.unpack("<i", b[0:4])[0]
    payload = b[4:]
    n = len(payload)
    return [
      payload[i:i+itemsize].rstrip(b'\x00').decode('utf-8')
      for i in range(0, n, itemsize)
    ]
  $$;

	CREATE TYPE enc_typ_new AS (
		shape INT[],
		int_base INT[],
		float_base REAL[],
		bool_base BOOLEAN[],
		text_base TEXT[],
		int_spans INT[],
		float_spans REAL[],
		bcast BOOLEAN[]
	);

  CREATE OR REPLACE FUNCTION migrate_enc_val(e enc_typ)
  RETURNS enc_typ_new
  LANGUAGE plpgsql IMMUTABLE
  AS $$
    BEGIN
      IF e.int_spans IS NOT NULL THEN
        RETURN ROW(
          e.shape, bytea_to_int_array_le(e.base), NULL, NULL, NULL, e.int_spans, NULL, NULL
        )::enc_typ_new;
      ELSIF e.float_spans IS NOT NULL THEN
        RETURN ROW(
          e.shape, NULL, bytea_to_float4_array_le(e.base), NULL, NULL, NULL, e.float_spans, NULL
        )::enc_typ_new;
      ELSIF e.bool_bcast IS NOT NULL THEN
        RETURN ROW(
          e.shape, NULL, NULL, bytea_to_bool_array(e.base), NULL, NULL, NULL, e.bool_bcast
        )::enc_typ_new;
      ELSIF e.string_bcast IS NOT NULL THEN
        RETURN ROW(
          e.shape, NULL, NULL, NULL, bytea_to_text_array_le(e.base), NULL, NULL, e.string_bcast
        )::enc_typ_new;
      END IF;
    END;
  $$;

  ALTER TABLE coord_data
    ALTER COLUMN enc_vals TYPE enc_typ_new
    USING migrate_enc_val(enc_vals);

  DROP FUNCTION migrate_enc_val;
  DROP FUNCTION bytea_to_float4_array_le;
  DROP FUNCTION bytea_to_int_array_le;
  DROP FUNCTION bytea_to_bool_array;
  DROP FUNCTION bytea_to_text_array_le; 
  DROP TYPE enc_typ;
  ALTER TYPE enc_typ_new RENAME TO enc_typ;

END;

