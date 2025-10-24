#!/bin/bash

if [ -z "$REPO_DIR" ]; then
  echo "Usage:"
  echo "REPO_DIR=/path/to/streamvis $0"
  exit
fi

protoc \
  --proto_path=$REPO_DIR/proto \
  --go_out=$REPO_DIR/data-server/pb \
  --go-grpc_out=$REPO_DIR/data-server/pb \
  $REPO_DIR/proto/streamvis/v1/data.proto


python -m grpc_tools.protoc \
  --proto_path=$REPO_DIR/proto \
  --python_out=$REPO_DIR/client \
  --grpc_python_out=$REPO_DIR/client \
  $REPO_DIR/proto/streamvis/v1/data.proto

