from typing import Any, Iterable
import itertools
import fcntl
import copy
import re
import datetime
from dataclasses import dataclass
import os
import struct
import numpy as np
from . import data_pb2 as pb
from . import data_pb2_grpc as pb_grpc
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


KIND_CODES = { 
    pb.Scope: pb.StoredType.SCOPE,
    pb.Name: pb.StoredType.NAME,
    pb.DataEntry: pb.StoredType.DATA_ENTRY,
    pb.ConfigEntry: pb.StoredType.CONFIG_ENTRY,
    pb.Data: pb.StoredType.DATA,
    pb.Config: pb.StoredType.CONFIG,
    pb.Control: pb.StoredType.CONTROL,
}

MESSAGE_TYPES = { v: k for k, v in KIND_CODES.items() } 


DTYPE_TO_PROTO = { 
    np.dtype('int32'): pb.FieldType.INT, 
    np.dtype('float32'): pb.FieldType.FLOAT 
}

PROTO_TO_DTYPE = {
    pb.FieldType.INT: np.int32,
    pb.FieldType.FLOAT: np.float32,
}

def get_name_hash(scope_id: int, name: str) -> int:
    return hash((scope_id, name)) % ((1 << 32) - 1)


def pack_message(message):
    """Create a delimited protobuf message as bytes."""
    kind_code = KIND_CODES.get(type(message)).to_bytes(1, "big")
    content = message.SerializeToString()
    length_code = struct.pack('>I', len(content))
    return kind_code + length_code + content 


def pack_scope(scope_id, scope: str) -> bytes:
    timestamp = Timestamp()
    timestamp.FromDatetime(datetime.datetime.now(datetime.UTC))
    scope = pb.Scope(scope_id=scope_id, scope=scope, time=timestamp)
    return pack_message(scope)


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

        kind = view[off]
        length = struct.unpack(">I", view[off+1:off+5])[0]

        if off + 5 + length > end:
            break

        if kind not in MESSAGE_TYPES:
            raise RuntimeError(f'Unknown kind {kind}, length {length}')
        item = MESSAGE_TYPES[kind]()
        item.ParseFromString(bytes(view[off+5:off+5+length]))
        off += 5 + length
        yield item
    return len(packed) - off


@dataclass(frozen=True)
class DataKey:
    scope_id: int
    scope: str
    name_id: int
    name: str
    index: int

class Index:
    scope_filter: re.Pattern 
    name_filters: tuple[re.Pattern] 
    scopes: dict[int, pb.Scope]    # scope_id => Scope
    names: dict[int, pb.Name]      # name_id => Name
    entries: dict[int, pb.DataEntry]  # entry_id => DataEntry
    config_entries: dict[int, pb.ConfigEntry]  # entry_id => ConfigEntry
    file_offset: int

    def __init__(self, scope_filter, name_filters, scopes, names, file_offset):
        self.scope_filter = scope_filter
        self.name_filters = name_filters
        self.scopes = scopes
        self.names = names
        self.entries = {} 
        self.config_entries = {}
        self.file_offset = file_offset

    @classmethod
    def from_message(cls, request: pb.Index):
        return cls(
            scope_filter=re.compile(request.scope_filter),
            name_filters=tuple(re.compile(nf) for nf in request.name_filters),
            scopes=dict(request.scopes),
            names=dict(request.names),
            file_offset=request.file_offset
        )

    @classmethod
    def from_filters(cls, scope_filter: str=None, name_filters: tuple[str]=None):
        if scope_filter is None:
            scope_filter = ".*"
        if name_filters is None:
            name_filters = (".*",)
        assert isinstance(scope_filter, str), "scope_filter must be a string"
        assert isinstance(name_filters, Iterable), "name_filters must be an iterable"
        scope_filter = re.compile(scope_filter)
        name_filters = tuple(re.compile(n) for n in name_filters)
        return cls(
            scope_filter=scope_filter, 
            name_filters=name_filters, 
            scopes={}, 
            names={}, 
            file_offset=0
        )

    @classmethod
    def from_scope_name(cls, scope: str=None, name: str=None):
        scope_filter = ".*" if scope is None else f"^{scope}$"
        name_filters = ".*" if name is None else (f"^{name}$",)
        return cls.from_filters(scope_filter, name_filters) 


    def __repr__(self):
        return (f"Index(scope_filter={self.scope_filter!r}, "
                f"name_filters={self.name_filters!r}, "
                f"scopes: {len(self.scopes)}, "
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
    def scope_list(self):
        return tuple(self.scopes.values())

    @property
    def name_list(self):
        return tuple(self.names.values())

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
                if self._filter(scope=scope):
                    self.scopes[scope_id] = item 

            case pb.Name(name_id=name_id, scope_id=scope_id, name=name):
                if self._filter(name=name) and scope_id in self.scopes:
                    assert name_id not in self.names, "Duplicate name_id in index"
                    self.names[name_id] = item

            case pb.Control(scope=scope, action=pb.Action.DELETE_SCOPE):
                if not self._filter(scope=scope):
                    return
                scopes_to_del = {}
                names_to_del = {} 
                entries_to_del = set() 
                config_entries_to_del = set()
                for scope_id, pb_scope in self.scopes.items():
                    if pb_scope.scope == scope:
                        scopes_to_del[scope_id] = pb_scope
                for name_id, pb_name in self.names.items():
                    if pb_name.scope_id in scopes_to_del:
                        names_to_del[name_id] = pb_name
                for entry_id, pb_entry in self.entries.items():
                    if pb_entry.name_id in names_to_del:
                        entries_to_del.add(entry_id)
                for centry_id, pb_centry in self.config_entries.items():
                    if pb_centry.scope_id in scopes_to_del:
                        config_entries_to_del.add(centry_id)
                for name_id in names_to_del:
                    del self.names[name_id]
                for scope_id in scopes_to_del:
                    del self.scopes[scope_id]
                for entry_id in entries_to_del:
                    del self.entries[entry_id]
                for centry_id in config_entries_to_del:
                    del self.config_entries[centry_id]

            case pb.DataEntry(entry_id=entry_id, name_id=name_id):
                if name_id in self.names:
                    self.entries[entry_id] = item

            case pb.ConfigEntry(entry_id=entry_id, scope_id=scope_id):
                if scope_id in self.scopes:
                    self.config_entries[entry_id] = item

    def update(self, fh):
        """Updates using any new data that may have been written to fh."""
        fh.seek(self.file_offset)
        pack = fh.read()
        gen = unpack(pack)
        while True:
            try:
                item = next(gen)
                self._update_with_item(item)
            except StopIteration as exc:
                self.file_offset += len(pack) - exc.value
                break

    def to_message(self) -> pb.Index:
        """Converts index into protobuf Message object.
        
        Content of entries and config_entries are discarded, but file_offset is retained
        """
        msg = pb.Index(
            scope_filter=self.scope_filter.pattern,
            name_filters=tuple(n.pattern for n in self.name_filters),
            scopes=self.scopes, 
            names=self.names,
            file_offset=self.file_offset)
        return msg

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


def get_new_data(
    index: Index, 
    stub: pb_grpc.RecordServiceStub,
) -> tuple[Index, dict[DataKey, 'cds_data']]:
    """Given current state of index, get new data and return updated index."""
    pb_index = index.to_message()
    datas = []
    for record in stub.QueryRecords(pb_index):
        match record.type:
            case pb.INDEX:
                index = Index.from_message(record.index)
            case pb.DATA:
                datas.append(record.data)
    cds_map = data_to_cds(index, datas)
    return index, cds_map




def data_to_cds(index: Index, datas: list[pb.Data]) -> dict[DataKey, 'cds_data']:
    collate = {} # DataKey => cds_data
    for data in datas:
        key = index.get_key(data)
        name = index.get_name(data)
        if key not in collate:
            cds = {f.name: np.array((), dtype=PROTO_TO_DTYPE[f.type]) for f in name.fields}
            collate[key] = cds
        cds = collate[key]
        for axis, field in zip(data.axes, name.fields):
            if field.type == pb.FieldType.FLOAT:
                nums = axis.floats.value
            elif field.type == pb.FieldType.INT:
                nums = axis.ints.value
            cds[field.name] = np.append(cds[field.name], nums)
    return collate


def fetch_cds_data(data_fh, index: Index) -> dict[DataKey, 'cds_data']:
    datas_map = load_data(data_fh, index.entries_list)
    datas = []
    for name_id, entry_map in index.entries.items():
        for entry_id, entry in entry_map.items():
            datas_list = datas_map[entry_id]
            for data in datas_list:
                data.name_id = entry.name_id
                datas.append(data)
    return data_to_cds(index, datas)


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


def export_configs(index: Index, configs: list[pb.Config]) -> dict[str, Any]:
    """Convert the pb.Config object to a python dictionary."""
    res = {}
    for cfg in configs:
        d = _struct_to_dict(cfg.attributes)
        scope = index.scopes[cfg.scope_id]
        l = res.setdefault(scope.scope, [])
        l.append(d)
    return res


def flatten_keys(cds_map: dict[DataKey, 'cds_data']) -> dict[tuple, 'cds_data']:
    out = {}
    for key, cds in cds_map.items():
        ekey = key.scope, key.name, key.index
        out[ekey] = cds
    return out


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
    
