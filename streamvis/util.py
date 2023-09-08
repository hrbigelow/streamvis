import numpy as np
import random
from . import data_pb2 as pb
import pdb

def separate_messages(messages):
    """
    Separate the messages into an array of Point and PointGroup messages
    """
    groups = []
    points = []
    for item in messages:
        if isinstance(item, pb.Group):
            groups.append(item)
        elif isinstance(item, pb.Points):
            points.append(item)
        else:
            raise RuntimeError(f'Received unknown message type {type(item)}')
    return groups, points 

def pack_message(message):
    """
    Create a delimited protobuf message as bytes
    """
    content = message.SerializeToString()
    length_code = len(content).to_bytes(4, 'big')
    if isinstance(message, pb.Group):
        kind_code = b'\x00'
    elif isinstance(message, pb.Points):
        kind_code = b'\x01'
    return kind_code + length_code + content 

def pack_messages(messages):
    """
    Create a delimited packed string from protobuf messages
    """
    groups, points = separate_messages(messages)
    group_packed = b''.join(pack_message(g) for g in groups)
    points_packed = b''.join(pack_message(p) for p in points)
    return group_packed + points_packed

def unpack(packed):
    """
    Unpack bytes representing zero or more packed messages
    """
    items = []
    off = 0
    end = len(packed)
    while off != end:
        kind_code = packed[off:off+1]
        length_code = packed[off+1:off+5]
        kind = int.from_bytes(kind_code, 'big')
        length = int.from_bytes(length_code, 'big')
        content = packed[off+5:off+5+length]
        if len(content) != length:
            break
        if kind == 0:
            item = pb.Group()
        elif kind == 1:
            item = pb.Points()
        else:
            raise RuntimeError(f'Unknown kind {kind}, length {length}')
        item.ParseFromString(content)
        off += 5 + length
        items.append(item)
    return items, len(packed[off:])

def validate(points, group):
    if points.group_id != group.id:
        raise RuntimeError(f'validate: group.id doesn\'t match points.group_id')
    if len(points.values) != len(group.fields):
        return False
    fields = [ 'floats', 'ints' ]
    sizes = set()
    for v, f in zip(points.values, group.fields):
        if f.type == pb.FieldType.FLOAT:
            sizes.add(len(v.floats.value))
        elif f.type == pb.FieldType.INT:
            sizes.add(len(v.ints.value))
    return len(sizes) == 1

def points_to_cds(points_list, group): 
    """
    Convert an array of pb.Points objects to CDS data
    CDS data is a map of field names to point values: { 'x': [...], 'y': [...], ... }
    """
    selected = [p for p in points_list if p.group_id == group.id]
    selected = sorted(selected, key=lambda p: p.batch)
    proto_to_numpy = { pb.FieldType.INT: np.int32, pb.FieldType.FLOAT: np.float32 }
    cds = { f.name: np.array((), dtype=proto_to_numpy[f.type]) for f in group.fields }
    print(f'starting convert for group {group.scope} {group.name} {group.index}')
    for points in selected:
        for value, field in zip(points.values, group.fields):
            if field.type == pb.FieldType.FLOAT:
                nums = value.floats.value
            elif field.type == pb.FieldType.INT:
                nums = value.ints.value
            cds[field.name] = np.append(cds[field.name], nums)
    print('ending convert')
    return cds 

def values_tuples(gid_beg, group_id, points, sig):
    """
    Gets the points values as a list of tuples, suitable for insert
    into a relational table
    """
    vals = []
    for value, (_, typ) in zip(points.values, sig):
        if typ == pb.FieldType.FLOAT:
            vals.append(value.floats.value)
        elif typ == pb.FieldType.INT:
            vals.append(value.ints.value)
    return [(gid, group_id, *v) for gid, v in enumerate(zip(*vals), gid_beg)]

def make_group(scope, name, index, /, **field_types):
    """
    Construct a pb.Group instance
    """
    group = pb.Group(id=random.randint(0, 2**32), scope=scope, name=name, index=index)
    numpy_to_proto = { 
            np.dtype('int32'): pb.FieldType.INT, 
            np.dtype('float32'): pb.FieldType.FLOAT 
            }
    for field_name, dtype in field_types.items():
        field = group.fields.add()
        field.name = field_name
        field.type = numpy_to_proto[dtype] 
    return group

def make_point(group, batch):
    """
    Construct a pb.Points instance
    group: pb.Group
    batch: integer
    data: field name => list of values
    """
    points = pb.Points(group_id=group.id, batch=batch)
    for _ in range(len(group.fields)):
        points.values.add() 
    return points


def validate(group, /, data):
    # validate field names and data types against group
    numpy_to_proto = { 
            np.dtype('int32'): pb.FieldType.INT, 
            np.dtype('float32'): pb.FieldType.FLOAT 
            }
    if len(data) != len(group.fields):
        raise RuntimeError(f'field name mismatch.\n{group=}\n{data.keys()=}')
    for field in group.fields:
        if field.name not in data:
            raise RuntimeError(
                f'field {field.name} listed in Group but not in data\n')
        if numpy_to_proto[data[field.name].dtype] != field.type:
            raise RuntimeError(
                f'field {field.name} has type {pb.FieldType.Name(field.type)} but '
                f'data dtype was {data[field.name].dtype}')

def add_to_point(group, point, /, **data):
    """
    Adds data to the point, checking data.keys
    """
    validate(group, data)
    for value, field in zip(point.values, group.fields):
        nums = data[field.name]
        if field.type == pb.FieldType.INT:
            value.ints.value.extend(nums)
        elif field.type == pb.FieldType.FLOAT:
            value.floats.value.extend(nums)

def num_point_data(point):
    values = point.values[0]
    data_name = values.WhichOneof('data') 
    if data_name == 'floats':
        return len(values.floats.value)
    elif data_name == 'ints':
        return len(values.ints.value)

def get_sql_type(field_type):
    return pb.FieldType.Name(field_type)

def get_numpy(data):
    """
    """
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

    if data.dtype == np.int64:
        data = data.astype(np.int32)
    elif data.dtype == np.float64:
        data = data.astype(np.float32)
    return data

