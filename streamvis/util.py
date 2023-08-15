import numpy as np
from . import data_pb2 as pb

def separate_messages(messages):
    """
    Separate the messages into an array of Point and PointGroup messages
    """
    groups = []
    points = []
    for item in messages:
        if isinstance(item, pb.PointGroup):
            groups.append(item)
        elif isinstance(item, pb.Points):
            points.extend(item.p)
        else:
            raise RuntimeError(f'Received unknown message type {type(item)}')
    return groups, points 

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

def pack_messages(messages):
    """
    Create a delimited packed string from protobuf messages
    """
    groups, points = separate_messages(messages)
    group_packed = ''.join(pack_message(g) for g in groups)
    points_packed = pack_message(pb.Points(p=points))
    return group_packed + points_packed

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

def points_to_cds(points, point_group): 
    """
    Convert an array of pb.Point objects to CDS data
    """
    selected = [p for p in points if p.group_id == point_group.id]
    selected = sorted(selected, key=lambda p: p.seqnum)
    cds = { d.name: [] for d in point_group.fields }
    inames = [ d.name for d in point_group.fields if d.is_integer ]
    fnames = [ d.name for d in point_group.fields if not d.is_integer ]
    names = inames + fnames
    num_columns = len(names)
    num_points = len(points)
    grid = np.array([[*p.idata, *p.fdata] for p in selected]).transpose()
    cds = dict(zip(names, grid))
    return cds 

def make_point_group(scope, group_name, **field_types):
    """
    Construct a pb.PointGroup instance
    """
    group_id = random.randint(0, 2**32)
    point_group = pb.PointGroup(scope=scope, name=group_name, id=group_id)
    for field_name, is_int in field_types.items():
        field = group.fields.add()
        field.name = field_name
        field.is_integer = is_int
    return group


def make_point(point_group, seqnum, **values):
    """
    Construct a pb.Point instance
    """
    point = pb.Point(group_id=point_group.id, seqnum=seqnum)
    for field in point_group.fields:
        if field.name not in values:
            raise RuntimeError(
                f'DataLogger::write: `values` does not contain field {field.name} '
                f'previously appearing in the point_groups field.  '
                f'Expected fields are {point_group.fields})') 
        if field.is_integer:
            item.idata.append(values[field.name])
        else:
            item.fdata.append(values[field.name])

    if len(values) != len(point_group.fields):
        raise RuntimeError(
            f'DataLogger::write: `values` contained extra entries. '
            f'values: {values}\nvs.'
            f'point_group.fields: {point_group.fields}')

