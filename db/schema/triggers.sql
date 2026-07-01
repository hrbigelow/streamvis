\echo 'create valid_chunk_data'
CREATE FUNCTION valid_chunk_data(
	p_field_id INT,
	e enc_typ
)
RETURNS BOOLEAN
STABLE
LANGUAGE sql
AS $$
  SELECT e.data_type = f.data_type AND valid_enc_typ(e)
  FROM field f
  WHERE f.id = p_field_id;
$$;

\echo 'create chunk_data_validate'
CREATE FUNCTION chunk_data_validate() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
	IF NOT valid_chunk_data(NEW.field_id, NEW.enc_vals) THEN
		RAISE EXCEPTION 'chunk_data: enc_vals does not match data_type for chunk_id %',
		NEW.chunk_id
		USING ERRCODE = 'check_violation';
	END IF;
	RETURN NEW;
END;
$$;

\echo 'create chunk_data_validate_trg'
CREATE TRIGGER chunk_data_validate_trg
	BEFORE INSERT ON chunk_data
	FOR EACH ROW
	EXECUTE FUNCTION chunk_data_validate();

