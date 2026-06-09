
BEGIN;

	\ir ../teardown/procs.sql
	\ir ../teardown/query.sql
	\ir ../teardown/base.sql
	\ir ../teardown/triggers.sql
	\ir ../teardown/types.sql


  CREATE TYPE enc_typ_new AS (
    data_type field_data_typ,
    floats REAL[],
    bools BOOLEAN[],
    texts TEXT[],
    base INT,
    diff INT[],
    size INT
  );

  \ir ../schema/types.sql
  \ir ../schema/triggers.sql
  \ir ../schema/base.sql
  \ir ../schema/query.sql
  \ir ../schema/procs.sql


END;


