#!/bin/bash

if [ -z "$REPO_DIR" ]; then
  echo "Usage:"
  echo "REPO_DIR=/path/to/streamvis $0"
  exit
fi

# required for running buf generate
export PATH=${PATH}:$REPO_DIR/web-client/node_modules/.bin

# This generates both go and protobuf-es
cd $REPO_DIR/proto && buf generate

# this is the old way to generate the go-based proto files
# protoc \
  # --proto_path=$REPO_DIR/proto \
  # --go_out=$REPO_DIR \
  # --go-grpc_out=$REPO_DIR \
  # $REPO_DIR/proto/streamvis/v1/data.proto


python -m grpc_tools.protoc \
  --proto_path=$REPO_DIR/proto \
  --python_out=$REPO_DIR/client \
  --grpc_python_out=$REPO_DIR/client \
  $REPO_DIR/proto/streamvis/v1/data.proto

