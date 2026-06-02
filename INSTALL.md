# gRPC Service refresh commands

```bash
# when pier code changes
cd pier
go build ./cmd/pier
sudo ./install.sh

# when non-persistent part of database changes
cd db
psql -U streamvis -d streamvis -f refresh-nodata.sql
sudo systemctl restart streamvis-rpc
```

## Schema migrations (`db/patch/`)

Some schema changes are *stateful* — they touch persistent tables, types,
or enums that `refresh-nodata.sql` will not rebuild. Each such change lands
as a numbered script in `db/patch/`. Apply them in order, **once**, against
an existing database, before rebuilding the stateless layer:

```bash
cd db/patch
psql -U streamvis -d streamvis -f 001_update_enc_typ.sql
psql -U streamvis -d streamvis -f 002_string_to_text.sql
# ...any newer patches in order...

cd ..
psql -U streamvis -d streamvis -f refresh-nodata.sql   # rebuild stateless layer
sudo systemctl restart streamvis-rpc                    # pick up new pier
```

Stop pier (or any other client) before running patches: a patch will
typically be incompatible with the running binary on either side of the
migration. Take a `pg_dump` first — patches are not rollback-safe.

## `plpython3u` requirement

`001_update_enc_typ.sql` decodes the legacy `BYTEA`-packed `enc_typ` into
typed arrays via `plpython3u`. The extension is `CREATE EXTENSION IF NOT
EXISTS plpython3u;`d inside the patch, but Postgres must have been built
with `--with-python` *and* the matching `libpythonX.Y.so` must be available
on the host. Container users: the upstream `postgres:18-alpine` ships
`plpython3.so` but not the Python runtime; add `apk add python3` (or use
the Debian variant with `postgresql-plpython3-18`) before running the
patches.
