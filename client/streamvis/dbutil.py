import struct
from dataclasses import dataclass
from typing import Any, Union
import io
import numpy as np
from .v1 import data_pb2 as pb

def slice_in_dim(ary: np.array, _slice: slice | int, dim: int):
    indices = [slice(None)] * ary.ndim
    indices[dim] = _slice 
    return ary[tuple(indices)]

def encode_numeric_array(
    field_handle: str,
    ary: np.array, 
    dim_threshold: int = 5, 
    rtol: float = 1e-10
) -> pb.EncTyp:
    """
    Converts the ary into a normal form using broadcast / increment checks
    """
    if ary.dtype not in (np.dtype('float32'), np.dtype('int32')):
        raise RuntimeError(
                f"Array has dtype {ary.dtype} but expected 'int32' or 'float32'")

    # import pdb
    # pdb.set_trace()
    shape = tuple(ary.shape)
    range_spans = [None] * ary.ndim

    for d in range(ary.ndim):
        if ary.shape[d] < dim_threshold:
            continue
        diffs = slice_in_dim(ary, slice(0, -1), d) - slice_in_dim(ary, slice(1, None), d)
        lo, hi = diffs.min(), diffs.max()
        if (hi - lo) <= rtol * max(np.abs(lo), np.abs(hi)):
            spans = slice_in_dim(ary, -1, d) - slice_in_dim(ary, 0, d)
            span = spans.flatten()[0].item()
            range_spans[d] = span 

    indices = tuple(slice(None) if s is None else 0 for s in range_spans)
    slice_data = ary[indices]

    msg = pb.EncTyp(field_handle=field_handle, base=slice_data.tobytes(), shape=shape)
    if ary.dtype == np.dtype('int32'):
        range_spans = tuple(pb.OptionalInt(value=sp) for sp in range_spans)
        msg.int_spans.values.extend(range_spans)
    else:
        range_spans = tuple(pb.OptionalFloat(value=sp) for sp in range_spans)
        msg.float_spans.values.extend(range_spans)
    return msg

def encode_bool_array(
    field_handle: str,
    ary: np.ndarray
) -> pb.EncTyp: 
    if not np.issubdtype(ary.dtype, np.bool):
        raise RuntimeError(f"Array has dtype {ary.dtype}, but expected 'bool'")
    bcast = [None] * ary.ndim
    for d in range(ary.ndim):
        slices = tuple(slice(0,1) if i == d else slice(None) for i in range(ary.ndim))
        bcast[d] = np.all(ary[slices] == ary).item()
    indices = tuple(0 if bc else slice(None) for bc in bcast)
    slice_data = ary[indices]
    msg = pb.EncTyp(field_handle=field_handle, base=slice_data.tobytes(), shape=ary.shape)
    msg.bool_bcast.values.extend(bcast)
    return msg


def encode_string_array(
    field_handle: str,
    ary: np.ndarray
) -> pb.EncTyp: 
    if not np.issubdtype(ary.dtype, np.str_):
        raise RuntimeError(f"Array has dtype {ary.dtype}, but expected 'str_'")
    bcast = [None] * ary.ndim
    for d in range(ary.ndim):
        slices = tuple(slice(0,1) if i == d else slice(None) for i in range(ary.ndim))
        bcast[d] = np.all(ary[slices] == ary).item()
    indices = tuple(0 if bc else slice(None) for bc in bcast)
    slice_data = ary[indices].astype('S')
    header = struct.pack('<i', slice_data.dtype.itemsize)
    encoded = header + slice_data.tobytes()
    msg = pb.EncTyp(field_handle=field_handle, base=encoded, shape=ary.shape)
    if np.issubdtype(ary.dtype, np.str_):
        msg.string_bcast.values.extend(bcast)
    else:
        msg.bool_bcast.values.extend(bcast)
    return msg

def encode_array(field_handle: str, ary: np.ndarray) -> pb.EncTyp:
    if ary.dtype in (np.dtype('int32'), np.dtype('float32')):
        return encode_numeric_array(field_handle, ary)
    elif ary.dtype == np.dtype('bool'):
        return encode_bool_array(field_handle, ary)
    elif np.issubdtype(ary.dtype, np.dtype('str_')):
        return encode_string_array(field_handle, ary)
    else:
        raise RuntimeError(
                f"ary must have a dtype of int32, float32, bool, or str_. Got {ary.dtype}")

def decode_numeric_array(enc: pb.EncTyp) -> np.array:
    """
    Converts the normalized data back into a plain np.array.  The original shape
    of the array is preserved, to satisfy the invariant:

    ary == decode_array(encode_array(ary))
    """
    set_field = enc.WhichOneof('spans')
    if set_field not in ('int_spans', 'float_spans'):
        raise RuntimeError(
            f"The active spans field is {set_field}, but expected"
            f"'int_spans' or 'float_spans'")

    if set_field == 'int_spans':
        range_spans = enc.int_spans.values
        base = np.frombuffer(enc.base, dtype=np.int32)
    else:
        range_spans = enc.float_spans.values
        base = np.frombuffer(enc.base, dtype=np.float32)

    N = len(enc.shape)
    shape = tuple(1 if sp.HasField('value') else sz for sz, sp in zip(enc.shape, range_spans))
    base = base.reshape(*shape)
    ranges = []
    for i, (sz, sp) in enumerate(zip(enc.shape, range_spans)):
        if not sp.HasField('value'):
            continue
        rng = np.linspace(0, sp.value, sz)
        rng = np.expand_dims(rng, axis=tuple(j for j in range(N) if j != i))
        ranges.append(rng)
    terms = np.broadcast_arrays(base, *ranges)
    # print(tuple(r.shape for r in terms))
    return np.add.reduce(terms).astype(base.dtype)

def decode_bool_array(enc: pb.EncTyp) -> np.array:
    """
    Converts the normalized data back into a plain np.array.
    """
    set_field = enc.WhichOneof('spans')
    if set_field != 'bool_bcast':
        raise RuntimeError(
            f"The active spans field is {set_field}, but expected bool_bcast")
    bcast_flags = enc.bool_bcast.values
    base = np.frombuffer(enc.base, dtype=np.bool)

    shape = tuple(1 if fl else sz for sz, fl in zip(enc.shape, bcast_flags))
    base = base.reshape(*shape)
    return np.broadcast_to(base, enc.shape)

def decode_string_array(enc: pb.EncTyp) -> np.array:
    """
    Converts the normalized data back into a plain np.array.
    """
    set_field = enc.WhichOneof('spans')
    if set_field != 'string_bcast':
        raise RuntimeError(
            f"The active spans field is {set_field}, but expected string_bcast")
    bcast_flags = enc.string_bcast.values
    itemsize = struct.unpack("<i", enc.base[:4])[0]
    base = np.frombuffer(enc.base[4:], dtype=np.dtype(('S', itemsize)))

    shape = tuple(1 if fl else sz for sz, fl in zip(enc.shape, bcast_flags))
    base = base.reshape(*shape).astype('U')
    return np.broadcast_to(base, enc.shape)


def decode_array(enc: pb.EncTyp) -> np.array:
    set_field = enc.WhichOneof('spans')
    match set_field:
        case 'int_spans' | 'float_spans':
            return decode_numeric_array(enc)
        case 'bool_bcast':
            return decode_bool_array(enc)
        case 'string_bcast':
            return decode_string_array(enc)
        case default:
            raise RuntimeError(f"Unknown span type: {set_field}")

def decode_array_flat(enc: pb.EncTyp) -> np.array:
    return decode_array(enc).flatten()

def make_field_value(field: pb.Field, val: Any) -> pb.FieldValue:
    attr = pb.FieldValue(handle=field.handle)
    match field.data_type:
        case pb.FIELD_DATA_TYPE_INT: 
            if not isinstance(val, int):
                raise RuntimeError(
                    f"value `{val}` given for field `{field.name}` was {type(val)} but expected int")
            attr.int_val = val
        case pb.FIELD_DATA_TYPE_FLOAT:
            if not isinstance(val, float):
                raise RuntimeError(
                    f"value `{val}` given for field `{field.name}` was {type(val)} but expected float")
            attr.float_val = val
        case pb.FIELD_DATA_TYPE_STRING:
            if not isinstance(val, str):
                raise RuntimeError(
                    f"value `{val}` given for field `{field.name}` was {type(val)} but expected str")
            attr.string_val = val
        case pb.FIELD_DATA_TYPE_BOOL:
            if not isinstance(val, str):
                raise RuntimeError(
                    f"value `{val}` given for field `{field.name}` was {type(val)} but expected str")
            attr.bool_val = val
        case pb.FIELD_DATA_TYPE_UNSPECIFIED:
            raise RuntimeError(f"database field `{field.name}` has undefined data type")
    return attr

# Store in little endian
SIG_TO_DTYPE = {
    pb.FIELD_DATA_TYPE_INT: np.dtype('<i4'),
    pb.FIELD_DATA_TYPE_FLOAT: np.dtype('<f4'),
    pb.FIELD_DATA_TYPE_BOOL: np.dtype('bool'),
    pb.FIELD_DATA_TYPE_STRING: np.dtype('str_')
}

DTYPE_TO_SIG = {
    np.dtype('<i4'): pb.FIELD_DATA_TYPE_INT,
    np.dtype('<f4'): pb.FIELD_DATA_TYPE_FLOAT,
    np.dtype('bool'): pb.FIELD_DATA_TYPE_BOOL,
    np.dtype('str_'): pb.FIELD_DATA_TYPE_STRING
}

# Use strings so as not to import unnecessary frameworks
ArrayLike = Union[np.ndarray, 'jax.Array', 'torch.Tensor']

def convert_to_array(val) -> Any:
    """
    Convert the argument to an array, preserving the type and location of
    any existing value if it is already and array
    """
    if isinstance(val, (int, float, str, bool, list, tuple)):
        return np.array(val)
    try:
        _ = get_array_type(val) # if succeeds, it is one of 
        return val 
    except ValueError:
        raise ValueError("Cannot convert val of type {type(val)} into an array type")

def get_array_type(ary):
    mod = type(ary).__module__
    if mod.startswith('jax'):
        return 'jax'
    elif mod.startswith('torch'):
        return 'torch'
    elif mod.startswith('numpy'):
        return 'numpy'
    elif mod == 'builtins':
        return 'POD'
    else:
        raise ValueError(f"Unknown array type: {mod}")

def expand_to_ndim(ary: ArrayLike, ndim: int) -> ArrayLike:
    shape = array_shape(ary)
    if ndim == len(shape):
        return ary

    assert len(shape) <= ndim, "Cannot expand ary of shape {shape} to {ndim} dims"
    new_shape = (1,) * (ndim - len(shape)) + shape
    return ary.reshape(*new_shape)

def concat_arrays(arrays: list[Any]):
    if len(arrays) == 0:
        raise ValueError("empty list")

    array_type = get_array_type(arrays[0])

    if array_type == 'jax':
        import jax.numpy as jnp
        return jnp.concatenate(arrays)
    elif array_type == 'torch':
        import torch
        return torch.cat(arrays)
    elif array_type == 'numpy':
        import numpy as np
        return np.concatenate(arrays)

def array_shape(ary) -> tuple[int]:
    return tuple(ary.shape)

def get_element_type(ary) -> int:
    array_type = get_array_type(ary)

    if array_type == 'jax':
        import jax.numpy as jnp
        if jnp.issubdtype(ary.dtype, jnp.integer):
            return pb.FIELD_DATA_TYPE_INT
        elif jnp.issubdtype(ary.dtype, jnp.floating):
            return pb.FIELD_DATA_TYPE_FLOAT
        elif jnp.issubdtype(ary.dtype, jnp.bool):
            return pb.FIELD_DATA_TYPE_BOOL
        else:
            raise ValueError("Invalid dtype for jax array: {ary.dtype}")

    elif array_type == 'torch':
        import torch
        if ary.dtype in {torch.int8, torch.int16, torch.int32, torch.int64, torch.uint32}:
            return pb.FIELD_DATA_TYPE_INT
        elif ary.dtype in {torch.float16, torch.float32, torch.float64}:
            return pb.FIELD_DATA_TYPE_FLOAT
        elif ary.dtype == torch.bool:
            return pb.FIELD_DATA_TYPE_BOOL
        else:
            raise ValueError("Invalid dtype for torch tensor: {ary.dtype}")

    elif array_type == 'numpy':
        import numpy as np
        if np.issubdtype(ary.dtype, np.integer):
            return pb.FIELD_DATA_TYPE_INT
        elif np.issubdtype(ary.dtype, np.floating):
            return pb.FIELD_DATA_TYPE_FLOAT
        elif np.issubdtype(ary.dtype, np.bool):
            return pb.FIELD_DATA_TYPE_BOOL
        elif np.issubdtype(ary.dtype, np.str_):
            return pb.FIELD_DATA_TYPE_STRING
        else:
            raise ValueError("Invalid dtype for numpy array: {ary.dtype}")

def stack_array_list(array_type: str, arrays: list[ArrayLike]) -> ArrayLike:
    match array_type:
        case 'jax':
            import jax.numpy as jnp
            return jnp.stack(arrays)
        case 'numpy':
            return np.stack(arrays)
        case 'torch':
            import torch
            return torch.stack(arrays)
        case 'POD':
            raise RuntimeError(f"Got POD type.  This shouldn't happen")
        case _:
            raise RuntimeError(f"Invalid array_type: {array_type}")
        

def convert_and_downcast(ary) -> np.ndarray:
    array_type = get_array_type(ary)
    match array_type:
        case 'jax':
            ary = np.array(ary)
        case 'torch':
            ary = ary.numpy(force=True)
        case 'numpy':
            pass
        case default:
            raise RuntimeError(f"Invalid array_type: {array_type}")

    if ary.dtype == np.float64:
        return ary.astype(np.float32)
    elif ary.dtype == np.int64:
        return ary.astype(np.int32)
    else:
        return ary

@dataclass
class SeriesValues:
    fields: tuple[ArrayLike, ...]
    broadcast_shape: tuple[int]

    def num_points(self):
        return np.prod(self.broadcast_shape + (1,)).item()

    def shape(self):
        return self.broadcast_shape

    def to_exported(self) -> tuple[np.ndarray]:
        fields = tuple(convert_and_downcast(field) for field in self.fields)
        return np.broadcast_arrays(*fields)

    def __post_init__(self):
        ndim = len(self.broadcast_shape)
        self.fields = tuple(expand_to_ndim(f, ndim) for f in self.fields)


def stack_series_values(
    series_values_list: list[SeriesValues]
) -> SeriesValues:
    fields_list = tuple(sv.fields for sv in series_values_list) # series, field
    by_field = tuple(zip(*fields_list)) # field, series
    array_types = tuple(get_array_type(f[0]) for f in by_field)

    stacked_fields = []
    shapes = []
    for array_type, field in zip(array_types, by_field):
        stacked = stack_array_list(array_type, field)
        stacked_fields.append(stacked)
        shapes.append(array_shape(stacked))

    try:
        bcast_shape = np.broadcast_shapes(*shapes)
    except ValueError as ve:
        raise RuntimeError(f"Couldn't find broadcast shape for shapes: {shapes}") from ve

    return SeriesValues(tuple(stacked_fields), bcast_shape)


