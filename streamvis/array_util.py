import numpy as np

def slice_min(data, slice_dim):
    """
    Find the min along the slice_dim
    Returns a tensor of shape data.shape[slice_dim] 
    """
    ans = data
    for d in range(data.ndim):
        if d == slice_dim:
            continue
        ans = ans.min(dim=0)[0]
    return ans 

def slice_max(data, slice_dim):
    ans = data
    for d in range(data.ndim):
        if d == slice_dim:
            continue
        ans = ans.max(dim=0)[0]
    return ans 

def make_grid(data, grid_dim, spatial_dim, ncols, pad_factor=1.2):
    """
    data: tensor with data.shape[spatial_dim] == 2 
    Transform data by spacing it out evenly across an nrows x ncols grid
    according to the index of grid_dim
    spatial_dim: the spatial dimension being augmented
    return: gridded data
    """
    if grid_dim >= data.ndim:
        raise RuntimeError(f'grid_dim = {grid_dim} must be < data.ndim (= {data.ndim})')
    if spatial_dim >= data.ndim or data.shape[spatial_dim] != 2:
        raise RuntimeError(
            f'spatial_dim = {spatial_dim} but data.shape[{spatial_dim}] either doesn\'t '
            f'exist or doesn\'t equal 2')
    
    strides = (slice_max(data, spatial_dim) - slice_min(data, spatial_dim)) * pad_factor
    strides[1] *= -1 # flip y, so the grid is left-to-right, top-to-bottom
    G = data.shape[grid_dim]

    cols = (t.arange(G) % ncols)
    rows = (t.arange(G) // ncols)

    # G, 2
    bcast = [ 1 if i not in (grid_dim, spatial_dim) else d for i, d in enumerate(data.shape) ]
    grid = t.dstack((cols, rows)) * strides
    grid_data = data + grid.reshape(*bcast)
    return grid_data

def dim_to_data(data, dim):
    """
    Increase the size of the last dimension by 1, by injecting dim index value
    into the tensor cell value.
    """
    if dim > data.ndim - 1:
        raise RuntimeError(
                f'dim_to_data: Got dim = {dim}, data.ndim = {data.ndim}. '
                f'dim must be < data.ndim-1')
    D = data.shape[dim]
    bcast = tuple(D if i == dim else 1 for i in range(data.ndim))
    vals = t.arange(D).reshape(*bcast).expand(*data.shape[:-1], 1)
    return t.cat((data, vals), data.ndim-1)


def to_list_of_list(data, point_dim):
    """
    Return a list of lists of dimension (N, data.shape[point_dim])
    """
    if point_dim >= data.ndim:
        raise RuntimeError(
            f'to_list_of_list: got point_dim={point_dim}, '
            f'not valid for data.ndim = {data.ndim}')
    point_dim_size = data.shape[point_dim]
    return data.moveaxis(point_dim, -1).reshape(-1, point_dim_size).aslist()

def to_dict(data, key_dim, val_dims=(), key_string='xy'):
    """
    - data has an unspecified shape but contains key_dim, val_dims.
    - data is first permuted to shape (key_dim, *other_dims, *val_dims)
    - other_dims are collapsed, and each slice along key_dim is packaged into
        C, *val_dims list of lists.
    Returns: { key_string[key_dim] => list: C x *val_dims }
    """
    if not all(d < data.ndim for d in val_dims + (key_dim,)):
        raise RuntimeError(
            f'One of key_dim or val_dims is out of bounds.  data.ndim = {data.ndim} '
            f'but key_dim = {key_dim}, val_dims = {val_dims}')
    if data.shape[key_dim] != len(key_string):
        raise RuntimeError(
            f'data.shape[key_dim] must equal lengh of key_string.  Got '
            f'data.shape[key_dim] = {data.shape[key_dim]}, key_string = {key_string}')
    if key_dim in val_dims:
        raise RuntimeError(
            f'key_dim must not be in val_dims. Got key_dim = {key_dim}, '
            f'val_dims = {val_dims}')

    other_dims = tuple(d for d in range(data.ndim) if d not in val_dims + (key_dim,))
    # permute key_dim to start, val_dims to end
    data = data.permute(key_dim, *other_dims, *val_dims).flatten(1, len(other_dims))
    dmap = dict(zip(key_string, data.tolist()))
    return dmap


