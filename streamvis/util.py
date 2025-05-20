from typing import List, Dict, Iterable, Tuple, Union, Optional, Any
from dataclasses import dataclass
import os
import struct
import numpy as np
import random
from . import data_pb2 as pb
import pdb

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

def index_file(log_file: str) -> str:
    return f"{log_file}.idx"


KIND_CODES = { 
    pb.Metadata: b'\x00', 
    pb.Control: b'\x01',
    pb.Entry: b'\x02', 
    pb.Data: b'\x03', 
}

MESSAGE_TYPES = {
    0: pb.Metadata,
    1: pb.Control,
    2: pb.Entry,
    3: pb.Data,
}

DTYPE_TO_PROTO = { 
    np.dtype('int32'): pb.FieldType.INT, 
    np.dtype('float32'): pb.FieldType.FLOAT 
}

PROTO_TO_DTYPE = {
    pb.FieldType.INT: np.int32,
    pb.FieldType.FLOAT: np.float32,
}

def metadata_id(scope: str, name: str) -> int:
    return hash((scope, name)) % ((1 << 32) - 1)

def pack_message(message):
    """Create a delimited protobuf message as bytes."""
    kind_code = KIND_CODES.get(type(message))
    content = message.SerializeToString()
    length_code = struct.pack('>I', len(content))
    return kind_code + length_code + content 

def pack_data(
    entry_id: int, 
    content: dict[str, np.ndarray],
    start_index: int,
    field_sig: list[tuple[str, np.dtype]]
) -> bytes:
    """pack the data into a protobuf message.

    content contains only 2-D data
    """
    content_types = { k: v.dtype for k, v in content.items() }
    if (len(field_sig) != len(content_types)
        or any(content_types.get(name) != ty for name, ty in field_sig)):
        raise RuntimeError(
            f"content has signature {content_types.items()} which mismatches "
            f"expected signature {field_sig}")

    shapes = set(a.shape for a in content.values())
    assert len(shapes) == 1
    shape = shapes.pop()
    num_slices = shape[0]

    packed = []
    for index in range(num_slices):
        data = pb.Data(entry_id=entry_id, index=index+start_index)
        for name, ty in field_sig:
            field_data = content.get(name)[index]
            vals = data.axes.add()
            if np.issubdtype(field_data.dtype, np.floating):
                vals.floats.value.extend(field_data)
            elif np.issubdtype(field_data.dtype, np.integer):
                vals.ints.value.extend(field_data)
            else:
                raise RuntimeError(f"field data is an unsupported dtype: {dty}")
        packed.append(pack_message(data))
    return b''.join(packed)

def pack_entry(entry_id: int, meta_id: int, beg_offset: int, end_offset: int) -> bytes:
    entry = pb.Entry(
        entry_id=entry_id, meta_id=meta_id, beg_offset=beg_offset, end_offset=end_offset)
    return pack_message(entry)


def pack_metadata(meta_id: int, scope: str, name: str,
                  field_sig: list[tuple[str, np.dtype]]) -> bytes:
    meta = pb.Metadata(meta_id=meta_id, scope=scope, name=name)
    for field_name, dtype in field_sig:
        field = meta.fields.add()
        field.name = field_name
        field.type = DTYPE_TO_PROTO[dtype] 
    return pack_message(meta)


def pack_control(scope: str, name: str) -> bytes:
    control = pb.Control(scope=scope, name=name, action=pb.Action.DELETE)
    return pack_message(control)


def unpack(packed: bytes):
    """Unpack bytes representing zero or more packed messages.
    
    yields all completely parsed items from the `packed` protobuf bytes.
    returns the number of unprocessed bytes.

    Use this as:

    gen = unpacked(bytes)
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


def load_index(
    path: str, 
    scope: str=None, 
    name: str=None
) -> tuple[dict[int, pb.Metadata], dict[int, pb.Entry]]:
    """Load the index, optionally filtering by scope and/or name.

    Returns:
      metas: meta_id => pb.Metadata
      entries: entry_id => pb.Entry
    """
    index_path = index_file(path)
    fh = get_log_handle(index_path, "rb")
    pack = fh.read()
    fh.close()

    def filter(iscope, iname):
        return (scope is None or scope == iscope) and (name is None or name == iname)

    metas = {}   # meta_id => pb.Metadata
    grouped_entries = {} # meta_id => list[pb.Entry]
    entries = {} # entry_id => pb.Entry
    for item in unpack(pack):
        match item:
            case pb.Metadata(meta_id=meta_id, scope=iscope, name=iname):
                if filter(iscope, iname):
                    metas[meta_id] = item
            case pb.Entry(meta_id=meta_id):
                if meta_id in metas:
                    tmp = grouped_entries.setdefault(meta_id, [])
                    tmp.append(item)
            case pb.Control(scope=iscope, name=iname):
                if filter(iscope, iname):
                    meta_id = metadata_id(iscope, iname)
                    metas.pop(meta_id, None)
                    grouped_entries.pop(meta_id, None)
    entries = {ent.entry_id: ent for l in grouped_entries.values() for ent in l}
    return metas, entries


def load_data(
    fh, 
    entries: list[pb.Entry],
) -> dict[int, list[pb.Data]]:   # entry_id => list[pb.Data]
    """Load pb.Data from data file corresponding to entries."""
    entries = sorted(entries, key=lambda ent: ent.beg_offset)
    data_pack = []
    entry_map = {}

    for ent in entries:
        fh.seek(ent.beg_offset, os.SEEK_SET)
        pack = fh.read(ent.end_offset - ent.beg_offset)
        data_pack.append(pack)
    data_pack = b''.join(data_pack)
    datas = list(unpack(data_pack))

    for data in datas:
        dl = entry_map.setdefault(data.entry_id, [])
        dl.append(data)

    # for meta_id, entry_list in entries.items():
        # for ent in entry_list:
            # assert ent.entry_id in entry_map

    return entry_map


@dataclass(frozen=True)
class DataKey:
    meta_id: int
    scope: str
    name: str
    index: int


def data_to_cds(
    metadata: dict[int, pb.Metadata], # meta_id => pb.Metadata
    entries: list[pb.Entry],
    datas: dict[int, list[pb.Data]],  # entry_id => list[pb.Data]
) -> dict[DataKey, 'cds_data']: 
    collate = {} # (meta_id, index) => cds_data
    for ent in entries:
        meta = metadata.get(ent.meta_id, None)
        if meta is None:
            raise RuntimeError("Missing metadata during conversion")
        for data in datas[ent.entry_id]:
            key = DataKey(meta.meta_id, meta.scope, meta.name, data.index) 
            if key not in collate:
                cds = {
                    f.name: np.array((), dtype=PROTO_TO_DTYPE[f.type]) for f in meta.fields}
                collate[key] = cds
            cds = collate[key]
            for axis, field in zip(data.axes, meta.fields):
                if field.type == pb.FieldType.FLOAT:
                    nums = axis.floats.value
                elif field.type == pb.FieldType.INT:
                    nums = axis.ints.value
                cds[field.name] = np.append(cds[field.name], nums)
    return collate

def fetch_cds_data(
    fh,  
    metadata: dict[int, pb.Metadata],
    entries: list[pb.Entry],
) -> dict[tuple[int, int], 'cds_data']:
    datas = load_data(fh, entries)
    return data_to_cds(metadata, entries, datas)


def num_point_data(point):
    values = point.values[0]
    data_name = values.WhichOneof('data') 
    if data_name == 'floats':
        return len(values.floats.value)
    elif data_name == 'ints':
        return len(values.ints.value)

def concat(arrays: list[Any], axis: int):
    match arrays[0]:
        case jax.numpy.Array:
            return jax.numpy.concat(arrays, axis=axis)
        case numpy.ndarray:
            return numpy.concat(arrays, axis=axis)
        case other:
            raise RuntimeError(f"Unsupported array type: {type(other)}")

def get_numpy(data):
    try:
        data = data.detach().numpy()
    except BaseException:
        pass
    try:
        data = np.array(data)

    except BaseException as ex:
        raise RuntimeError(
            f'exception {ex}:\n'
            f'Could not convert data into np.ndarray using either:\n'
            f'data.detach().numpy() or np.array(data).  '
            f'Got type(data) = {type(data)}')

    # For the moment, converting everything to float32 
    data = data.astype(np.float32)
    """
    if data.dtype == np.int64:
        data = data.astype(np.int32)
    elif data.dtype == np.float64:
        data = data.astype(np.float32)
    """
    return data

