import numpy as np
from . import data_pb2 as pb
import pdb

def pack_message(message):
    """
    Create a delimited protobuf message as bytes
    """
    content = message.SerializeToString()
    length_code = len(content).to_bytes(4, 'big')
    if isinstance(message, pb.MetaData):
        kind_code = b'\x00'
    elif isinstance(message, pb.Points):
        kind_code = b'\x01'
    return kind_code + length_code + content 

def unpack_messages(messages):
    """
    Unpack bytes representing zero or more packed messages
    """
    items = []
    off = 0
    end = len(messages)
    while off != end:
        kind_code = messages[off:off+1]
        length_code = messages[off+1:off+5]
        kind = int.from_bytes(kind_code, 'big')
        length = int.from_bytes(length_code, 'big')
        content = messages[off+5:off+5+length]
        if kind == 0:
            item = pb.MetaData()
        elif kind == 1:
            item = pb.Points()
        else:
            raise RuntimeError(f'Unknown kind {kind}, length {length}')
        item.ParseFromString(content)
        off += 5 + length
        items.append(item)
    return items

def points_to_cds(points, group): 
    selected = [p for p in points if p.meta_id == group.id]
    selected = sorted(selected, key=lambda p: p.seqnum)
    cds = { d.name: [] for d in group.data }
    inames = [ d.name for d in group.data if d.is_integer ]
    fnames = [ d.name for d in group.data if not d.is_integer ]
    names = inames + fnames
    num_columns = len(names)
    num_points = len(points)
    grid = np.array([[*p.idata, *p.fdata] for p in selected]).transpose()
    cds = dict(zip(names, grid))
    return cds 

