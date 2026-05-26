\set QUIET 1

\echo 'create valid_coord_data'
CREATE FUNCTION valid_coord_data(
	p_coord_id INT,
	val enc_typ
)
RETURNS BOOLEAN
STABLE
LANGUAGE sql
AS $$
  SELECT 
    CASE f.data_type
      WHEN 'int' THEN
        (
          (val).int_spans IS NOT NULL AND
          (val).float_spans IS NULL AND
          (val).bool_bcast IS NULL AND
          (val).string_bcast IS NULL AND
          array_length((val).shape, 1) = array_length((val).int_spans, 1)
        )
      WHEN 'float' THEN
        (
          (val).int_spans IS NULL AND
          (val).float_spans IS NOT NULL AND
          (val).bool_bcast IS NULL AND
          (val).string_bcast IS NULL AND
          array_length((val).shape, 1) = array_length((val).float_spans, 1)
        )
      WHEN 'bool' THEN
        (
          (val).int_spans IS NULL AND
          (val).float_spans IS NULL AND
          (val).bool_bcast IS NOT NULL AND
          (val).string_bcast IS NULL AND
          array_length((val).shape, 1) = array_length((val).bool_bcast, 1)
        )
      WHEN 'string' THEN
        (
          (val).int_spans IS NULL AND
          (val).float_spans IS NULL AND
          (val).bool_bcast IS NULL AND
          (val).string_bcast IS NOT NULL AND
          array_length((val).shape, 1) = array_length((val).string_bcast, 1)
        )
      ELSE
        FALSE
    END CASE
		FROM coord c
		JOIN field f ON (f.id = c.field_id)
		WHERE c.id = p_coord_id;
$$;

\echo 'create coord_data_validate'
CREATE FUNCTION coord_data_validate() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
	IF NOT valid_coord_data(NEW.coord_id, NEW.enc_vals) THEN
		RAISE EXCEPTION 'coord_data: enc_vals does not match data_type for coord_id %',
		NEW.coord_id
		USING ERRCODE = 'check_violation';
	END IF;
	RETURN NEW;
END;
$$;

\echo 'create coord_data_validate_trg'
CREATE TRIGGER coord_data_validate_trg
	BEFORE INSERT ON coord_data
	FOR EACH ROW
	EXECUTE FUNCTION coord_data_validate();


\set QUIET 0

