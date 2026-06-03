\set ON_ERROR_STOP on

BEGIN;

	CREATE INDEX idx_coord_data__chunk ON coord_data(chunk_id); 

END;


