import struct
from dataclasses import dataclass
from typing import Any, Union
import io
import numpy as np
from .v1 import data_pb2 as pb

@dataclass
class DiffArray:
    base: int
    size: int
    diff: np.ndarray 

    def __post_init__(self):
        self.diff = np.array(self.diff)

    @classmethod
    def from_array(cls, ary: np.ndarray) -> 'DiffArray':
        base = int(ary[0])
        size = ary.size
        d = np.diff(ary)

        # find smallest repeat
        L = len(d)
        if L == 0:
            m = 0
        else:
            pi = np.zeros(L, dtype=np.int32)
            k = 0
            for i in range(1, L):
                while k > 0 and d[i] != d[k]:
                    k = pi[k-1]
                if d[i] == d[k]:
                    k += 1
                pi[i] = k
            m = L - int(pi[-1])

        return cls(base, size, d[:m])

    @property
    def array(self) -> np.ndarray:
        buf = np.empty(self.size, dtype=np.int32)
        buf[0] = self.base
        for i in range(1, self.size):
            buf[i] = buf[i-1] + self.diff[(i-1) % self.diff.size]
        return buf

    @classmethod
    def from_msg(cls, msg: pb.DiffArray) -> 'DiffArray':
        return cls(msg.base, msg.size, msg.diff)

    def to_msg(self) -> pb.DiffArray:
        return pb.DiffArray(base=self.base, diff=self.diff.tolist(), size=self.size)

def encode_array(field_handle: str, ary: np.array) -> pb.FullEncTyp:
    """
    Convert an np.array to the FullEncTyp
    """
    data_type = get_element_type(ary)
    if data_type == pb.FIELD_DATA_TYPE_FLOAT:
        enc = pb.EncTyp(data_type=data_type, floats=ary.ravel().tolist())

    elif data_type == pb.FIELD_DATA_TYPE_INT:
        diff_ary = DiffArray.from_array(ary.ravel())
        enc = pb.EncTyp(data_type=data_type, diff_array=diff_ary.to_msg())

    elif data_type == pb.FIELD_DATA_TYPE_TEXT:
        inds = np.empty(ary.size, np.int32)
        words = {}
        for i, word in enumerate(ary.ravel()):
            inds[i] = words.setdefault(word, len(words))
        diff_ary = DiffArray.from_array(inds)
        base_words = np.empty(len(words), ary.dtype)
        for word, idx in words.items():
            base_words[idx] = word

        enc = pb.EncTyp(
            data_type=data_type,
            texts=base_words.tolist(),
            diff_array=diff_ary.to_msg())

    elif data_type == pb.FIELD_DATA_TYPE_BOOL:
        inds = np.empty(ary.size, np.int32)
        bools = {}
        for i, flag in enumerate(ary.ravel()):
            inds[i] = bools.setdefault(flag, len(bools))
        diff_ary = DiffArray.from_array(inds)
        if len(diff_ary.diff) * 4 < ary.size:
            # use diff encoding
            base_bools = np.empty(len(bools), ary.dtype)
            for flag, idx in bools.items():
                base_bools[idx] = flag 
            enc = pb.EncTyp(
                data_type=data_type,
                bools=base_bools,
                diff_array=diff_ary.to_msg())
        else:
            enc = pb.EncTyp(data_type=data_type, bools=ary.ravel().tolist())
    else:
        raise RuntimeError(
            f"data type {ary.dtype} not supported. "  
            f"Must be one of {', '.join(str(k) for k in DTYPE_TO_SIG.keys())}")

    return pb.FullEncTyp(field_handle=field_handle, enc=enc)

def decode_array(enc: pb.EncTyp) -> np.array:
    if enc.data_type == pb.FIELD_DATA_TYPE_INT:
        diff_ary = DiffArray.from_msg(enc.diff_array)
        return diff_ary.array

    elif enc.data_type == pb.FIELD_DATA_TYPE_FLOAT:
        return np.array(enc.floats)

    elif enc.data_type == pb.FIELD_DATA_TYPE_TEXT:
        diff_ary = DiffArray.from_msg(enc.diff_array)
        base = np.array(enc.texts)
        texts = np.empty_like(base, shape=(diff_ary.size,))
        for i, idx in enumerate(diff_ary.array):
            texts[i] = base[idx]
        return texts

    elif enc.data_type == pb.FIELD_DATA_TYPE_BOOL:
        if enc.HasField('diff_array'):
            diff_ary = DiffArray.from_msg(enc.diff_array)
            base = np.array(enc.bools)
            bools = np.empty_like(base, shape=(diff_ary.size,))
            for i, idx in enumerate(diff_ary.array):
                bools[i] = base[idx]
            return bools
        else:
            return np.array(enc.bools)
    else:
        raise RuntimeError(f"Unknown enc.data_type: {enc.data_type}")


def decode_runchunk(rc: pb.RunChunks) -> tuple[np.array]:
    chunks = [] # chunks[chunk][coord] = np.array
    offsets = []
    if len(rc.chunks) == 0:
        return tuple()
    first_chunk = rc.chunks[0]
    offsets = [(0,) * len(first_chunk.enc_vals)]
    for chunk in rc.chunks: 
        arrays = tuple(decode_array(enc) for enc in chunk.enc_vals)
        chunks.append(arrays)
        new_offsets = tuple(off + ary.size for off, ary in zip(offsets[-1], arrays))
        offsets.append(new_offsets)

    bufs = tuple(np.empty((sz,), ary.dtype) for sz, ary in zip(offsets[-1], chunks[-1]))
    for ch, arrs in enumerate(chunks):
        begs, ends = offsets[ch], offsets[ch+1]
        for co, arr in enumerate(arrs):
            beg, end = begs[co], ends[co] 
            bufs[co][beg:end] = arr 
    return bufs


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
        case pb.FIELD_DATA_TYPE_TEXT:
            if not isinstance(val, str):
                raise RuntimeError(
                    f"value `{val}` given for field `{field.name}` was {type(val)} but expected str")
            attr.text_val = val
        case pb.FIELD_DATA_TYPE_BOOL:
            if not isinstance(val, bool):
                raise RuntimeError(
                    f"value `{val}` given for field `{field.name}` was {type(val)} but expected bool")
            attr.bool_val = val
        case pb.FIELD_DATA_TYPE_UNSPECIFIED:
            raise RuntimeError(f"database field `{field.name}` has undefined data type")
    return attr

# Store in little endian
SIG_TO_DTYPE = {
    pb.FIELD_DATA_TYPE_INT: np.dtype('<i4'),
    pb.FIELD_DATA_TYPE_FLOAT: np.dtype('<f4'),
    pb.FIELD_DATA_TYPE_BOOL: np.dtype('bool'),
    pb.FIELD_DATA_TYPE_TEXT: np.dtype('str_')
}

DTYPE_TO_SIG = {
    np.dtype('<i4'): pb.FIELD_DATA_TYPE_INT,
    np.dtype('<f4'): pb.FIELD_DATA_TYPE_FLOAT,
    np.dtype('bool'): pb.FIELD_DATA_TYPE_BOOL,
    np.dtype('str_'): pb.FIELD_DATA_TYPE_TEXT
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
            return pb.FIELD_DATA_TYPE_TEXT
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

    """
    def __repr__(self):
        fmts = []
        for f in self.fields:
            shape = ' '.join(str(dim) for dim in array_shape(f))
            dtype = SIG_TO_DTYPE[get_element_type(f)]
            fmt = f"{dtype}[{shape}]"
            fmts.append(fmt)
        return (
                f"{' '.join(fmts)}, "
                f"broadcast_shape: {' '.join(str(dim) for dim in self.broadcast_shape)}"
                )
    """
    def __repr__(self):
        return "\n".join(str(f) for f in self.fields)


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

