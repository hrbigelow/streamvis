\set ON_ERROR_STOP 1

\ir teardown/extension.sql
\ir teardown/procs.sql
\ir teardown/query.sql
\ir teardown/base.sql
\ir teardown/triggers.sql
\ir teardown/types.sql

\ir schema/types.sql
\ir schema/triggers.sql
\ir schema/base.sql
\ir schema/query.sql
\ir schema/procs.sql
\ir schema/extension.sql

\echo '\n\n'
\echo 'NOTE: ============================='
\echo 'run systemctl restart streamvis-rpc'
\echo '\n'

