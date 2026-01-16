SVC=streamvis.v1.Service
OPTS=-plaintext
HOST=localhost:8001


D1=$(cat <<EOF
{
  "name": "start-time", 
  "data_type": "float", 
  "description": "Start time"
}
EOF
)

D2=$(cat <<EOF
{
  "name": "experiment-name",
  "data_type": "string",
  "description": "Name of the experiment"
}
EOF
)

D3=$(cat <<EOF
{
  "name": "noisy-channel-epsilon",
  "data_type": "float",
  "description": "Probability of mutating an emitted symbol"
}
EOF
)

D4=$(cat <<EOF
{
  "name": "with-BOS-token",
  "data_type": "bool",
  "description": "Whether the generating process uses a BOS token"
}
EOF
)

# grpcurl -d "$D1" $OPTS $HOST $SVC/CreateField 
# grpcurl -d "$D2" $OPTS $HOST $SVC/CreateField 
# grpcurl -d "$D3" $OPTS $HOST $SVC/CreateField 
# grpcurl -d "$D4" $OPTS $HOST $SVC/CreateField 

D5=$(cat <<EOF
{
  "series_name": "series3",
  "field_names": ["experiment-name", "noisy-channel-epsilon", "start-time"]
}
EOF
)

grpcurl -d "$D5" $OPTS $HOST $SVC/CreateSeries

