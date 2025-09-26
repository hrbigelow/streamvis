#!/bin/bash

protoc \
  --proto_path=proto \
  --go_out=data-server/pb \
  --go-grpc_out=data-server/pb \
  proto/streamvis/data.proto


python -m grpc_tools.protoc \
  --proto_path=proto \
  --python_out=client \
  --grpc_python_out=client \
  proto/streamvis/data.proto

