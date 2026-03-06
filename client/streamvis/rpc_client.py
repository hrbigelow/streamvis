from collections.abc import Iterable
import grpc
import os
from .v1 import data_pb2 as pb
from .v1 import data_pb2_grpc as pb_grpc
from .v1.data_pb2_grpc import ServiceStub

def get_service_stub() -> ServiceStub:
    uri = os.getenv('STREAMVIS_GRPC_URI')
    if uri is None:
        raise RuntimeError("streamvis requires STREAMVIS_GRPC_URI variable set")
    chan = grpc.insecure_channel(uri)
    return pb_grpc.ServiceStub(chan)

def list_series(stub: ServiceStub) -> Iterable[pb.Series]:
    req = pb.ListSeriesRequest()
    yield from stub.ListSeries(req)

def list_fields(stub: ServiceStub) -> Iterable[pb.Field]:
    req = pb.ListFieldsRequest()
    yield from stub.ListFields(req)

def get_data(stub: ServiceStub, req: pb.QueryRunDataRequest) -> Iterable[pb.ChunkData]:
    yield from stub.QueryRunData(req)



