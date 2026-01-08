SVC=streamvis.v1.Service
METHOD=AppendToSeries
HOST=localhost:8001

SERIES_HANDLE=$1

REQ=$(cat <<EOF
{ 
  "series_handle": "$SERIES_HANDLE", 
  "field_names": ["x", "y"], 
  "field_vals": [
    {"base": "XDAwMFwwMDBcMDAwXDAwMAo=", "shape": [1,1,1], "ival": {"values": [{}, {}, {}]}},
    {"base": "XDAwMFwwMDBcMDAwXDAwMAo=", "shape": [1,1,1], "fval": {"values": [{}, {}, {}]}}
  ]
}
EOF
)

grpcurl -plaintext -d "$REQ" $HOST ${SVC}/${METHOD}

