from typing import Any, Iterable, Union
import itertools
import fcntl
import copy
import re
import datetime
from dataclasses import dataclass
import os
import struct
import numpy as np
from streamvis.v1 import data_pb2 as pb
from streamvis.v1 import data_pb2_grpc as pb_grpc
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.struct_pb2 import Struct

def get_log_handle(path, mode):
    """
    Provide an ordinary filehandle or GFile, whichever is needed, to avoid
    unnecessary tensorflow dependency
    """
    if path.startswith('gs://'):
        try:
            from tensorflow.io.gfile import GFile
        except ModuleNotFoundError:
            raise RuntimeError(f'{path=} is a GFile path, install tensorflow first')
        fh = GFile(path, mode)
    else:
        fh = open(path, mode)
    return fh

def index_file(path: str) -> str:
    return f"{path}.idx"

def data_file(path: str) -> str:
    return f"{path}.log"


DTYPE_TO_PROTO = { 
    np.dtype('<i4'): pb.DType.D_TYPE_I32, 
    np.dtype('<f4'): pb.DType.D_TYPE_F32 
}

PROTO_TO_DTYPE = {
    pb.DType.D_TYPE_I32: np.dtype('<i4'),
    pb.DType.D_TYPE_F32: np.dtype('<f4'),
}


def pack_message(message):
    """Create a delimited protobuf message as bytes."""
    content = message.SerializeToString()
    length_code = struct.pack('>I', len(content))
    return length_code + content 


def pack_scope(scope_id, scope: str) -> bytes:
    timestamp = Timestamp()
    timestamp.FromDatetime(datetime.datetime.now(datetime.UTC))
    scope = pb.Scope(scope_id=scope_id, scope=scope, time=timestamp)
    return pack_message(scope)


# This is Python-only
def make_data_messages(
    name_id: int,
    content: dict[str, np.ndarray],
    start_index: int,
    field_sig: list[tuple[str, pb.DType]]
) -> list[pb.Data]:
    """pack the data into a protobuf message.

    content contains only 2-D data
    """
    content_types = { k: v.dtype for k, v in content.items() }
    if (len(field_sig) != len(content_types)
        or any(content_types.get(field.name) != PROTO_TO_DTYPE[field.dtype] 
            for field in field_sig)):
        raise RuntimeError(
            f"content has signature {content_types.items()} which mismatches "
            f"expected signature {field_sig}")

    shapes = set(a.shape for a in content.values())
    assert len(shapes) == 1, f"got mixed shape contents: {shapes}"
    shape = shapes.pop()
    num_slices = shape[0]

    datas = []
    for index in range(num_slices):
        data = pb.Data(name_id=name_id, index=index+start_index)
        for field in field_sig:
            field_data = content.get(field.name)[index]
            vals = data.axes.add()
            vals.length = field_data.size
            if np.issubdtype(field_data.dtype, np.floating):
                vals.dtype = pb.D_TYPE_F32
            elif np.issubdtype(field_data.dtype, np.integer):
                vals.dtype = pb.D_TYPE_I32
            else:
                raise RuntimeError(f"field data is an unsupported dtype: {dty}")
            vals.data = field_data.astype(PROTO_TO_DTYPE[vals.dtype]).tobytes()
        datas.append(data)
    return datas


# Python-only
def make_name_message(
    scope_id: int, name: str, field_sig: list[tuple[str, np.dtype]]
) -> pb.Name:
    name = pb.Name(scope_id=scope_id, name=name)
    for field_name, dtype in field_sig:
        field = name.fields.add()
        field.name = field_name
        field.dtype = DTYPE_TO_PROTO[dtype] 
    return name 


def pack_delete_scope(scope: str) -> bytes:
    control = pb.Control(scope=scope, name="", action=pb.DELETE_SCOPE)
    return pack_message(control)

def pack_delete_name(scope: str, name: str) -> bytes:
    control = pb.Control(scope=scope, name=name, action=pb.DELETE_NAME)
    return pack_message(control)


def pack_config_entry(entry_id: int, scope_id: int, beg_offset: int, end_offset: int) -> bytes:
    config_entry = pb.ConfigEntry(
        entry_id=entry_id, 
        scope_id=scope_id, 
        beg_offset=beg_offset,
        end_offset=end_offset
    )
    return pack_message(config_entry)


def unpack(packed: bytes):
    """Unpack bytes representing zero or more packed messages.
    
    yields all completely parsed items from the `packed` protobuf bytes.
    returns the number of unprocessed bytes.

    Use this as:

    gen = unpack(bytes)
    while True:
        try:
            item = next(gen)
        except StopIteration as exc
            remain = exc.value
            break
        # do something with item
    # do something with remain
    """
    off = 0
    end = len(packed)
    view = memoryview(packed)

    while off != end:
        if off + 5 > end:
            break

        length = struct.unpack(">I", view[off:off+4])[0]

        if off + 4 + length > end:
            break

        item = pb.Stored()
        item.ParseFromString(bytes(view[off+5:off+5+length]))
        off += 4 + length
        yield item
    return len(packed) - off


@dataclass(frozen=True, order=True)
class DataKey:
    scope_id: int
    scope: str
    name_id: int
    name: str
    index: int


def load_data(
    fh, 
    entries: list[pb.DataEntry | pb.ConfigEntry],
) -> dict[int, list[pb.Data | pb.Config]]:   # entry_id => list[pb.Data]
    """Load pb.Data from data file corresponding to entries."""
    entries = sorted(entries, key=lambda ent: ent.beg_offset)
    content_packs = []
    content_map = {}

    for ent in entries:
        fh.seek(ent.beg_offset, os.SEEK_SET)
        pack = fh.read(ent.end_offset - ent.beg_offset)
        content_packs.append(pack)
    content_pack = b''.join(content_packs)
    contents = list(unpack(content_pack))

    for content in contents:
        dl = content_map.setdefault(content.entry_id, [])
        dl.append(content)

    return content_map 

# used only in client
async def get_new_data(
    request: pb.DataRequest,
    stub: pb_grpc.ServiceStub,
) -> tuple[pb.RecordResult, dict[DataKey, 'cds_data']]:
    """Given current state of index, get new data and return updated index."""
    datas = []
    record = None
    async for msg in stub.QueryData(request):
        match msg.WhichOneof("value"):
            case "record":
                record = msg.record
            case "data":
                datas.append(msg.data)
            case other:
                raise ValueError(
                    "QueryData should only return index or data.  Returned {other}")
    cds_map = data_to_cds(record.scopes, record.names, datas)
    return record, cds_map

# client only
def _concatenate(nums_list: list[Iterable], dtype: np.dtype) -> np.ndarray:
    total_length = sum(len(nums) for nums in nums_list)
    out = np.empty(total_length, dtype=dtype)
    offset = 0
    for nums in nums_list:
        n = len(nums)
        out[offset:offset+n] = nums
        offset += n
    return out

def _get_key(
    scopes: dict[int, pb.Scope],
    names: dict[int, pb.Name],
    data: pb.Data
) -> DataKey:
    name = names[data.name_id]
    scope = scopes[name.scope_id]
    return DataKey(scope.scope_id, scope.scope, name.name_id, name.name, data.index)


# client only
def _data_to_cds(
    scopes: dict[int, pb.Scope],
    names: dict[int, pb.Name],
    datas: list[pb.Data], 
    flatten: bool
) -> dict[Union[DataKey, tuple], 'cds_data']:
    collate = {} # DataKey => cds_data
    tmpdata = {} # DataKey => (str => ndarray)
    for data in datas:
        key = _get_key(scopes, names, data)
        name = names[data.name_id] 
        if flatten:
            key = key.scope, key.name, key.index
        if key not in collate:
            cds = {f.name: PROTO_TO_DTYPE[f.dtype] for f in name.fields}
            tmp = {f.name: [] for f in name.fields}
            collate[key] = cds
            tmpdata[key] = tmp
        cds = collate[key]
        tmp = tmpdata[key]
        for axis, field in zip(data.axes, name.fields):
            if axis.dtype != field.dtype:
                raise RuntimeError(
                        f"Mismatched field dtypes between data and name object"
                        f"data axis dtype = {axis.dtype}, name field dtype = {field.dtype}"
                )
            if axis.dtype == pb.DType.D_TYPE_F32:
                ary = np.frombuffer(axis.data, dtype="<f4")
            elif axis.dtype == pb.DType.D_TYPE_I32:
                ary = np.frombuffer(axis.data, dtype="<i4")
            tmp[field.name].append(ary)

    for key, tmp in tmpdata.items():
        cds = collate[key]
        for field, nums_list in tmp.items():
            cds[field] = _concatenate(nums_list, cds[field])

    return collate

# client only
def data_to_cds(
    scopes: dict[int, pb.Scope],
    names: dict[int, pb.Name],
    datas: list[pb.Data]
) -> dict[DataKey, 'cds_data']:
    return _data_to_cds(scopes, names, datas, flatten=False)

# client only
def data_to_cds_flat(
    scopes: dict[int, pb.Scope],
    names: dict[int, pb.Name], 
    datas: list[pb.Data]
) -> dict[tuple, 'cds_data']:
    return _data_to_cds(scopes, names, datas, flatten=True)


# client only
def _struct_to_dict(struct: Struct) -> dict:
    def convert_value(value):
        kind = value.WhichOneof("kind")
        match kind:
            case "struct_value":
                return _struct_to_dict(value.struct_value)
            case "list_value":
                return [convert_value(v) for v in value.list_value.values]
            case _:
                return getattr(value, kind)
    return {k: convert_value(v) for k, v in struct.fields.items()}

# client only
def export_configs(
    scopes: dict[int, pb.Scope], 
    configs: list[pb.Config]
) -> dict[str, Any]:
    """Convert the pb.Config object to a python dictionary."""
    res = {}
    for cfg in configs:
        d = _struct_to_dict(cfg.attributes)
        scope = scopes[cfg.scope_id]
        l = res.setdefault(scope.scope, [])
        l.append(d)
    return res


def safe_write(fh, content: bytes) -> int:
    """Write to fh in concurrency-safe way.  Return current offset after write."""
    if not isinstance(content, bytes):
        raise RuntimeError(f"content must be bytes")

    try:
        # Only lock during the actual write operation
        fcntl.flock(fh, fcntl.LOCK_EX)
        _ = fh.write(content)
        fh.flush()
        os.fsync(fh.fileno())
        current_offset = fh.tell()
        return current_offset
    finally:
        fcntl.flock(fh, fcntl.LOCK_UN)

# client only
def fill_defaults(defaults: dict, settings: dict):
    """Update the possibly nested `settings` dict with any defaults.

    Creates any key path in settings that is not previously in settings but present
    in defaults.
    """
    for k, v in defaults.items():
        if k not in settings:
            settings[k] = copy.deepcopy(v)
        if isinstance(v, dict):
            fill_defaults(v, settings[k])
    
