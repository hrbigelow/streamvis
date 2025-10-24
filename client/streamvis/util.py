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
from .v1 import data_pb2 as pb
from .v1 import data_pb2_grpc as pb_grpc
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
    np.dtype('int32'): pb.FieldType.FIELD_TYPE_INT, 
    np.dtype('float32'): pb.FieldType.FIELD_TYPE_FLOAT 
}

PROTO_TO_DTYPE = {
    pb.FieldType.FIELD_TYPE_INT: np.int32,
    pb.FieldType.FIELD_TYPE_FLOAT: np.float32,
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
    field_sig: list[tuple[str, pb.FieldType]]
) -> list[pb.Data]:
    """pack the data into a protobuf message.

    content contains only 2-D data
    """
    content_types = { k: v.dtype for k, v in content.items() }
    if (len(field_sig) != len(content_types)
        or any(content_types.get(field.name) != PROTO_TO_DTYPE[field.type] 
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
            if np.issubdtype(field_data.dtype, np.floating):
                vals.floats.value.extend(field_data)
            elif np.issubdtype(field_data.dtype, np.integer):
                vals.ints.value.extend(field_data)
            else:
                raise RuntimeError(f"field data is an unsupported dtype: {dty}")
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
        field.type = DTYPE_TO_PROTO[dtype] 
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


class Index:
    scopes:          dict[int, pb.Scope]    # scope_id => Scope
    names:           dict[int, pb.Name]      # name_id => Name
    entries:         dict[int, pb.DataEntry]  # entry_id => DataEntry
    config_entries:  dict[int, pb.ConfigEntry]  # entry_id => ConfigEntry
    _tag_to_names:    dict[tuple[str, str], set[int]] # (scope, name) => set[name_id]
    _name_to_entries: dict[int, set[int]]  # name_id => set[entry_id]
    _scope_to_configs:dict[str, list[pb.ConfigEntry]] # scope => list[pb.ConfigEntry]
    file_offset: int

    def __init__(self, scopes, names, file_offset):
        self.scopes = scopes
        self.names = names
        self.entries = {} 
        self.config_entries = {}
        self._tag_to_names = {}
        self._name_to_entries = {}
        self._scope_to_configs = {}
        self.file_offset = file_offset

    @classmethod
    def from_record_result(cls, result: pb.RecordResult):
        return cls(
            scopes=dict(result.scopes),
            names=dict(result.names),
            file_offset=result.file_offset
        )

    def __repr__(self):
        return (f"scopes: {len(self.scopes)}, "
                f"names: {len(self.names)}, "
                f"entries: {len(self.entries)}, "
                f"config_entries: {len(self.config_entries)}, "
                f"file_offset: {self.file_offset})")
    
    @property
    def entry_list(self):
        return tuple(self.entries.values())

    @property
    def config_entry_list(self):
        return tuple(self.config_entries.values())

    @property
    def scope_list(self) -> tuple[str]:
        """Return a list of scope names that have content"""
        scopes = set() 
        for scope_id, scope in self.scopes.items():
            gen = (pb for pb in self.names.values() if pb.scope_id == scope_id)
            if next(gen, None) != None:
                scopes.add(scope.scope)
        return tuple(scopes)

    @property
    def name_list(self) -> tuple[str]:
        return tuple(set(pb.name for pb in self.names.values()))

    def get_key(self, data: pb.Data) -> DataKey:
        name = self.names[data.name_id]
        scope = self.scopes[name.scope_id]
        return DataKey(scope.scope_id, scope.scope, name.name_id, name.name, data.index)

    def get_name(self, data: pb.Data) -> pb.Name:
        return self.names[data.name_id]

    def _filter(self, /, scope: str=None, name: str=None):
        if scope is not None and self.scope_filter.match(scope) is None:
            return False
        if name is not None:
            return any(nf.match(name) for nf in self.name_filters) 
        return True

    def _update_with_item(self, item):
        """Updates the index with the item."""
        match item:
            case pb.Scope(scope_id=scope_id, scope=scope):
                assert scope_id not in self.scopes, "Duplicate scope_id in index"
                self.scopes[scope_id] = item 

            case pb.Name(name_id=name_id, scope_id=scope_id, name=name):
                if self._filter(name=name) and scope_id in self.scopes:
                    assert name_id not in self.names, "Duplicate name_id in index"
                    self.names[name_id] = item
                    scope = self.scopes[scope_id].scope
                    _scope_to_names = self._tag_to_names.setdefault(scope, dict())
                    _scope_to_names.setdefault(name, list()).append(name_id)

            case pb.Control(scope=scope, name=name, action=pb.Action.DELETE_NAME):
                if not self._filter(scope=scope, name=name):
                    return
                _scope_to_names = self._tag_to_names.get(scope, {})
                for name_id in _scope_to_names.pop(name, list()):
                    del self.names[name_id]
                    for entry_id in self._name_to_entries.pop(name_id, tuple()):
                        del self.entries[entry_id]
                """
                # this doesn't work since scope is logged, then DELETE_NAMEs are
                # logged before any data.
                if len(_scope_to_names) == 0:
                    # scope is now empty
                    self._tag_to_names.pop(scope, None)
                    scopes_to_del = set()
                    for entry_id in self._scope_to_configs.pop(scope, tuple()):
                        scope_id = self.config_entries.pop(entry_id).scope_id
                        scopes_to_del.add(scope_id)
                    for scope_id in scopes_to_del:
                        del self.scopes[scope_id]
                """

            case pb.DataEntry(entry_id=entry_id, name_id=name_id):
                if name_id in self.names:
                    self.entries[entry_id] = item
                    self._name_to_entries.setdefault(name_id, list()).append(entry_id)

            case pb.ConfigEntry(entry_id=entry_id, scope_id=scope_id):
                if scope_id in self.scopes:
                    scope = self.scopes[scope_id].scope
                    self.config_entries[entry_id] = item
                    self._scope_to_configs.setdefault(scope, list()).append(entry_id)

    def update(self, fh):
        """Updates using any new data that may have been written to fh."""
        fh.seek(self.file_offset)
        pack = fh.read()
        gen = unpack(pack)
        while True:
            try:
                item = next(gen)
            except StopIteration as exc:
                self.file_offset += len(pack) - exc.value
                break
            self._update_with_item(item)

    def to_bytes(self) -> bytes:
        """Serialize the index to protobuf message bytes."""
        packs = []
        messages = itertools.chain(
            self.scopes.values(), 
            self.names.values(),
            self.entries.values(),
            self.config_entries.values()
        )
        for msg in messages:
            pack = pack_message(msg)
            packs.append(pack)
        return b''.join(packs)

    @property
    def max_id(self):
        return max(
            itertools.chain(
                self.scopes.keys(), 
                self.names.keys(), 
                self.entries.keys(), 
                self.config_entries.keys()),
            default=0)


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
def get_new_data(
    scope_pattern: str,
    name_pattern: str,
    file_offset: int,
    stub: pb_grpc.ServiceStub,
) -> tuple[Index, dict[DataKey, 'cds_data']]:
    """Given current state of index, get new data and return updated index."""
    req = pb.RecordRequest(
        scope_pattern=scope_pattern,
        name_pattern=name_pattern,
        file_offset=file_offset
    )
    datas = []
    for msg in stub.QueryRecords(req):
        match msg.WhichOneOf("record"):
            case "index":
                index = Index.from_record_result(msg.index)
            case "data":
                datas.append(msg.data)
            case other:
                raise ValueError(
                    "QueryRecords should only return index or data.  Returned {other}")
    cds_map = data_to_cds(index, datas)
    return index, cds_map

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


# client only
def _data_to_cds(
    index: Index, datas: list[pb.Data], flatten: bool
) -> dict[Union[DataKey, tuple], 'cds_data']:
    collate = {} # DataKey => cds_data
    tmpdata = {} # DataKey => (str => list[num])
    for data in datas:
        key = index.get_key(data)
        name = index.get_name(data)
        if flatten:
            key = key.scope, key.name, key.index
        if key not in collate:
            cds = {f.name: PROTO_TO_DTYPE[f.type] for f in name.fields}
            tmp = {f.name: [] for f in name.fields}
            collate[key] = cds
            tmpdata[key] = tmp
        cds = collate[key]
        tmp = tmpdata[key]
        for axis, field in zip(data.axes, name.fields):
            if field.type == pb.FieldType.FIELD_TYPE_FLOAT:
                nums = axis.floats.value
            elif field.type == pb.FieldType.FIELD_TYPE_INT:
                nums = axis.ints.value
            tmp[field.name].append(nums)

    for key, tmp in tmpdata.items():
        cds = collate[key]
        for field, nums_list in tmp.items():
            cds[field] = _concatenate(nums_list, cds[field])

    return collate

# client only
def data_to_cds(index: Index, datas: list[pb.Data]) -> dict[DataKey, 'cds_data']:
    return _data_to_cds(index, datas, flatten=False)

# client only
def data_to_cds_flat(index: Index, datas: list[pb.Data]) -> dict[tuple, 'cds_data']:
    return _data_to_cds(index, datas, flatten=True)


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
def export_configs(index: Index, configs: list[pb.Config]) -> dict[str, Any]:
    """Convert the pb.Config object to a python dictionary."""
    res = {}
    for cfg in configs:
        d = _struct_to_dict(cfg.attributes)
        scope = index.scopes[cfg.scope_id]
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
    
