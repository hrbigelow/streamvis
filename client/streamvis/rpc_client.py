from collections.abc import Iterable
from dataclasses import dataclass
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
    return stub.ListFields(req)

def get_end_chunk_id(stub: ServiceStub) -> int:
	req = pb.GetEndChunkIdRequest()
	resp = stub.GetEndChunkId(req)
	return resp.id

def get_data(
    stub: ServiceStub, 
    req: pb.QueryRunDataRequest,
) -> Iterable[pb.ChunkData]:
    return stub.QueryRunData(req)

@dataclass
class QueryRunInfo:
    attrs: list[pb.Field]
    coords: list[pb.Coord]

    @property
    def field_names(self):
        return [a.name for a in self.attrs] + [c.name for c in self.coords]

    @property
    def field_name_map(self):
        return { f.name: f.description for f in (*self.attrs, *self.coords) }

def get_data_columns(
    stub: ServiceStub,
    series_name: str,
    field_names: list[str],
) -> QueryRunInfo:
    """
    Find 
    """
    attrs = []
    coords = []

    series = None
    for ser in list_series(stub):
        if ser.name == series_name:
            series = ser
            break
    if series is None:
        raise RuntimeError(f"Series {series_name} not found")

    field_map = { f.name: f for f in list_fields(stub) }
    field_map.update({ c.name: c for c in series.coords })

    for fname in field_names:
        msg = field_map.get(fname)
        if msg is None:
            raise RuntimeError(f"Could not find field name {fname} as coord or field")
        match type(msg):
            case pb.Field:
                attrs.append(msg)
            case pb.Coord:
                coords.append(msg)
    
    return QueryRunInfo(attrs, coords)




