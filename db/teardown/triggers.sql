\echo 'drop chunk_data_validate_trg'
DROP TRIGGER IF EXISTS chunk_data_validate_trg ON chunk_data;

\echo 'drop chunk_data_validate'
DROP FUNCTION IF EXISTS chunk_data_validate;

\echo 'drop valid_chunk_data'
DROP FUNCTION IF EXISTS valid_chunk_data;

