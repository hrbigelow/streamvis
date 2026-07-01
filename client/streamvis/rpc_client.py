from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any
import grpc
import os
from .v1 import data_pb2 as pb
from .v1 import data_pb2_grpc as pb_grpc
from .v1.data_pb2_grpc import ServiceStub
from urllib.parse import urlparse

"""
"""

def get_channel() -> grpc.Channel:
    uri = os.getenv('STREAMVIS_GRPC_URI')
    if uri is None:
        raise RuntimeError("streamvis requires STREAMVIS_GRPC_URI variable set")
    if not "://" in uri:
        raise RuntimeError(f"Read STREAMVIS_GRPC_URI={uri}.  Should contain '://'")
    parsed = urlparse(uri)
    target = parsed.netloc
    match parsed.scheme:
        case 'https':
            if not parsed.port:
                target = f"{target}:443" 
            chan = grpc.secure_channel(target, grpc.ssl_channel_credentials())
        case 'http':
            if not parsed.port:
                target = f"{target}:80"
            chan = grpc.insecure_channel(target)
        case _:
            raise RuntimeError(f"Got URI scheme '{parsed.scheme}'.  Must be 'https' or 'http'")
    return chan

def get_service_stub() -> ServiceStub:
    chan = get_channel()
    return pb_grpc.ServiceStub(chan)

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

def get_oneof(pbmsg) -> Any:
    field_name = pbmsg.WhichOneof('value')
    if field_name is None:
        return None
    return getattr(pbmsg, field_name)


@dataclass
class QueryRunInfo:
    attrs: list[pb.Field]
    fields: list[pb.Field]

    @property
    def all_field_names(self):
        return tuple(a.name for a in self.attrs) + tuple(f.name for f in self.fields)

    @property
    def attr_names(self):
        return tuple(a.name for a in self.attrs)

    @property
    def field_names(self):
        return tuple(f.name for f in self.fields)

    @property
    def field_name_map(self):
        return { f.name: f for f in (*self.attrs, *self.fields) }

def get_data_columns(
        stub: ServiceStub,
        field_names: list[str],
        ) -> QueryRunInfo:
    """
    Resolve `field_names` into attrs or coords belonging to `series_name` 
    """
    attrs = []
    coords = []
    field_map = { f.name: f for f in list_fields(stub) }

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
    pos_tags: list[str],
    pos_match_all_tags: bool,
    neg_tags: list[str],
    neg_match_all_tags: bool,
    min_started_at: datetime|None,
    max_started_at: datetime|None,
) -> pb.RunFilter:
    msg = pb.RunFilter(
        min_started_at=min_started_at,
        max_started_at=max_started_at,
    )
    msg.tag_filter.pos_tags.extend(pos_tags)
    msg.tag_filter.pos_match_all = pos_match_all_tags
    msg.tag_filter.neg_tags.extend(neg_tags)
    msg.tag_filter.neg_match_all = neg_match_all_tags
    return msg


"""
def get_window_spec(
    info: QueryRunInfo,
    group_fields: list[str],
    order_field: str,
    window_size: int,
    stride: int,
) -> pb.WindowSpec:
    for gc in group_fields:
        if gc not in info.field_names:
            raise RuntimeError(
                f"`{gc}` given in group_fields is not a field name. "
                f"Valid field names are {', '.join(info.field_names)}")
    if order_field not in info.field_names:
        raise RuntimeError(
            f"order_field `{order_field}` not a valid field name. "
            f"Valid names are {', '.join(info.field_names)}")
    if window_size == 0 or stride == 0:
        raise RuntimeError(
            f"window_size and stride must both be > 0. "
            f"Got {window_size=}, {stride=}")
    group_handles = tuple(info.field_name_map[gc].field_handle for gc in group_fields)
    order_handle = info.field_name_map[order_field].field_handle
    return pb.WindowSpec(
        group_field_handles=group_handles,
        order_field_handle=order_handle,
        size=window_size,
        stride=stride
    )
"""


def get_query_run_data_request(
    stub: ServiceStub,
    field_names: list[str],
    pos_tags: list[str],
    pos_match_all: bool,
    neg_tags: list[str],
    neg_match_all: bool,
    min_started_at: datetime|None, 
    max_started_at: datetime|None,
) -> pb.QueryRunDataRequest:
    """
    Resolves `fields` into handles for attrs and pb.fields, then
    constructs the QueryRunDataRequest from that.
    """
    req = pb.QueryRunDataRequest()
    field_map = { f.name: f for f in list_fields(stub) }
    for fname in field_names:
        msg = field_map.get(fname)
        if msg is None:
            raise RuntimeError(f"Could not find field name `{fname}`")
        req.field_handles.append(msg.handle)

    req.run_filter.CopyFrom(get_run_filter(
        pos_tags, pos_match_all, neg_tags, neg_match_all, min_started_at, max_started_at)
    )
    return req

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
                        f"filter {field.name} is a float field. "
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

        case pb.FieldDataType.FIELD_DATA_TYPE_TEXT:
            if not isinstance(vals, list) or not all(isinstance(v, str) for v in vals):
                raise RuntimeError(
                        f"field {field.name} is a text field. "
                        f"You must provide a text-valued vals argument. "
                        f"Got {vals=}")
            af.string_list.vals.extend(vals)

    return af


