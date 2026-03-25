from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
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

def list_runs(stub: ServiceStub, run_filter: pb.RunFilter) -> Iterable[pb.Run]:
    return stub.ListRuns(pb.ListRunsRequest(run_filter=run_filter))

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

def get_run_filter(
    tags: list[str],
    match_all_tags: bool,
    min_started_at: datetime|None,
    max_started_at: datetime|None,
) -> pb.RunFilter:
    msg = pb.RunFilter(
        min_started_at=min_started_at,
        max_started_at=max_started_at,
    )
    msg.tag_filter.tags.extend(tags)
    msg.tag_filter.match_all = match_all_tags
    return msg

def get_query_run_data_request(
        stub: ServiceStub,
        series: str,
        fields: list[str],
        tags: list[str],
        match_all_tags: bool,
        min_started_at: datetime|None, 
        max_started_at: datetime|None,

        ) -> tuple[pb.QueryRunDataRequest, QueryRunInfo]:

    info = get_data_columns(stub, series, fields)
    req = pb.QueryRunDataRequest()
    req.coord_handles.extend((c.coord_handle for c in info.coords))
    req.attr_handles.extend((a.handle for a in info.attrs))

    req.run_filter.tag_filter.tags.extend(tags)
    req.run_filter.tag_filter.match_all = match_all_tags

    if min_started_at is not None:
        req.run_filter.min_started_at = min_started_at
    if max_started_at is not None:
        req.run_filter.max_started_at = max_started_at

    return req, info

def get_attribute_filter(
        stub: ServiceStub,
        field_name: str,
        lo: int|float|None,
        hi: int|float|None,
        vals: list[int|float|str|bool],
        include_missing: bool
        ) -> pb.AttributeFilter:

    field = None
    for f in list_fields(stub):
        if f.name == field_name:
            field = f
            break
    if field is None:
        raise RuntimeError(f"Couldn't find field named `{field_name}`")

    af = pb.AttributeFilter(field_handle=field.handle, include_missing=include_missing)

    match field.data_type:
        case pb.FieldDataType.FIELD_DATA_TYPE_INT:
            if isinstance(lo, int) and isinstance(hi, int):
                af.int_range.imin = lo
                af.int_range.imax = hi
            elif all(isinstance(v, int) for v in vals):
                af.int_list.vals.extend(vals)
            else:
                raise RuntimeError(
                        f"field {field.name} is an integer field. "
                        f"You must provide either an integer [lo, hi] or "
                        f"integer-containing vals.  Got {lo=}, {hi=}, {vals=}")

        case pb.FieldDataType.FIELD_DATA_TYPE_FLOAT:
            if not (isinstance(lo, float) and isinstance(hi, float)):
                raise RuntimeError(
                        f"filter {filt.name} is a float field. "
                        f"You must provide float-valued [lo, hi]. "
                        f"Got {lo=}, {hi=}")
            af.float_range.fmin = lo
            af.float_range.fmax = hi

        case pb.FieldDataType.FIELD_DATA_TYPE_BOOL:
            if not isinstance(vals, list) or not all(isinstance(v, bool) for v in vals):  
                raise RuntimeError(
                        f"field {field.name} is a boolean field. "
                        f"You must provide a bool-valued vals argument. "
                        f"Got {vals=}")
            af.bool_list.vals.extend(vals)

        case pb.FieldDataType.FIELD_DATA_TYPE_STRING:
            if not isinstance(vals, list) or not all(isinstance(v, str) for v in vals):
                raise RuntimeError(
                        f"field {field.name} is a string field. "
                        f"You must provide a string-valued vals argument. "
                        f"Got {vals=}")
            af.string_list.vals.extend(vals)

    return af


