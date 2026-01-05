from dataclasses import dataclass
import numpy as np

@dataclass
class Float32Data:
    # See db/schema.sql float32_data type
    base: np.array
    shape: tuple[int]
    range_spans: tuple[float]

@dataclass
class Int32Data:
    # See db/schema.sql int32_data type
    base: np.array
    shape: tuple[int]
    range_spans: tuple[int]


def slice_in_dim(ary: np.array, _slice: slice | int, dim: int):
    indices = [slice(None)] * ary.ndim
    indices[dim] = _slice 
    return ary[tuple(indices)]

def encode_array(
    data: np.array, 
    dim_threshold: int = 5, 
    rtol: float = 1e-10
) -> Float32Data|Int32Data:
    """
    Converts the data into a normal form using broadcast / increment checks
    """
    ctor = { np.dtype("float32"): Float32Data, np.dtype("int32"): Int32Data }
    if data.dtype not in ctor:
        raise RuntimeError(f"data must be either float32 or int32 type.  got {data.dtype}")

    shape = tuple(data.shape)
    range_spans = [None] * data.ndim

    for d in range(data.ndim):
        if data.shape[d] < dim_threshold:
            continue
        diffs = slice_in_dim(data, slice(0, -1), d) - slice_in_dim(data, slice(1, None), d)
        lo, hi = diffs.min(), diffs.max()
        mid = (hi + lo) / 2.0
        if hi == lo or (hi - lo) / mid < rtol:
            spans = slice_in_dim(data, -1, d) - slice_in_dim(data, 0, d)
            range_spans[d] = spans.mean().item()

    range_spans = tuple(range_spans)
    indices = tuple(slice(None) if s is None else 0 for s in range_spans)
    slice_data = data[indices]
    
    return ctor[data.dtype](slice_data.flatten(), shape, range_spans)

def decode_array(data: Float32Data | Int32Data) -> np.array:
    """
    Converts the normalized data back into a plain np.array.  The original shape
    of the array is preserved, to satisfy the invariant:

    ary == decode_array(encode_array(ary))
    """
    N = len(data.shape)
    shape = tuple(sz if sp is None else 1 for sz, sp in zip(data.shape, data.range_spans))
    base = data.base.reshape(*shape)
    ranges = []
    for i, (sz, sp) in enumerate(zip(data.shape, data.range_spans)):
        if sp is None:
            continue
        rng = np.linspace(0, sp, sz)
        rng = np.expand_dims(rng, axis=tuple(j for j in range(N) if j != i))
        ranges.append(rng)
    terms = np.broadcast_arrays(base, *ranges)
    print(tuple(r.shape for r in terms))
    return np.add.reduce(terms).astype(base.dtype)

