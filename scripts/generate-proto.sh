#!/bin/bash

python -m grpc_tools.protoc \
  --proto_path=proto \
  --python_out=client/streamvis \
  --grpc_python_out=client/streamvis \
  proto/data.proto

protoc \
  --proto_path=proto \
  --go_out=data-server/pb \
  --go-grpc_out=data-server/pb \
  proto/data.proto

