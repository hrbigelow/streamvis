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

