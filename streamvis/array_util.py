import numpy as np

def make_grid(data, spatial_dim, grid_dim, ncols, pad_factor=1.2):
    """
    data: tensor of arbitrary shape
    Transform data by spacing it out evenly across an nrows x ncols grid
    according to the index of grid_dim
    spatial_dim: the first two elements of this dimension will be augmented by the
                 grid values
    returns: the modified data, with the same shape
    """
    if grid_dim not in range(data.ndim):
        raise RuntimeError(f'grid_dim = {grid_dim} inconsistent with data.ndim={data.ndim}')
    if spatial_dim not in range(data.ndim):
        raise RuntimeError(f'spatial_dim = {spatial_dim}, but data.ndim = {data.ndim}')
   
    spatial_data = np.take(data, indices=range(2), axis=spatial_dim)
    dims=tuple(i for i in range(data.ndim) if i != spatial_dim)
    strides = (spatial_data.max(axis=dims) - spatial_data.min(axis=dims)) * pad_factor
    strides[1] *= -1 # flip y, so the grid is left-to-right, top-to-bottom
    G = data.shape[grid_dim]
    S = data.shape[spatial_dim]

    cols = (np.arange(G) % ncols)
    rows = (np.arange(G) // ncols)

    # G, 2
    bcast = [1] * data.ndim
    bcast[grid_dim] = G
    bcast[spatial_dim] = 2
    grid = np.dstack((cols, rows)) * strides
    
    extra_data = np.take(data, indices=range(2, S), axis=spatial_dim)
    spatial_data = spatial_data + grid.reshape(*bcast)
    grid_data = np.concatenate((spatial_data, extra_data), spatial_dim)

    return grid_data

def dim_to_data(data, source_dim, augment_dim, rng=None):
    """
    Increase the size of augment_dim by 1 by injecting the value of the index
    in source_dim into the newly created tensor cell value.
    If rng is given, the value of the index is affinely interpolated to rng.
    """
    if augment_dim > data.ndim - 1:
        raise RuntimeError(
                f'dim_to_data: Got dim = {augment_dim}, data.ndim = {data.ndim}. '
                f'dim must be < data.ndim-1')
    # import pdb
    # pdb.set_trace()
    D = data.shape[source_dim]
    bshape = [1] * data.ndim
    bshape[source_dim] = D
    eshape = list(data.shape)
    eshape[augment_dim] = 1
    if rng is None:
        seed_vals = np.arange(D)
    else:
        seed_vals = np.linspace(*rng, D)
    vals = seed_vals.reshape(*bshape)
    vals = np.broadcast_to(vals, eshape)
    return np.concatenate((data, vals), augment_dim)

def to_list_of_list(data, point_dim):
    """
    Return a list of lists of dimension (N, data.shape[point_dim])
    """
    if point_dim >= data.ndim:
        raise RuntimeError(
            f'to_list_of_list: got point_dim={point_dim}, '
            f'not valid for data.ndim = {data.ndim}')
    point_dim_size = data.shape[point_dim]
    return np.moveaxis(data, point_dim, -1).reshape(-1, point_dim_size).tolist()

def axes_to_front(ary, *front_axes):
    """
    Permute the array, moving axes to the front
    """
    axes = list(front_axes)
    axes += [i for i in range(ary.ndim) if i not in front_axes]
    front_shape = (ary.shape[a] for a in front_axes)
    ary = np.transpose(ary, axes).reshape(*front_shape, -1)
    return ary

