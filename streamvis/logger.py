import numpy as np
from tensorflow.io.gfile import GFile
import random
import time
import signal
from . import util
import pdb

class DataLogger:
    """
    Create one instance of this in the producer script, to send data to
    a bokeh server.
    """
    def __init__(self, scope):
        self.configured_plots = set()
        self.scope = scope
        self.groups = []
        self.points = []
        self.batch = 0
        self.elem_count = 0
        random.seed(time.time())

    def init(self, path, buffer_max_elem=1000):
        """
        Initialize logger to log data to the given path.
        path:  filesystem path or gs:// resource
        buffer_max_elem:  number of data items to buffer
        """
        self.fh = GFile(path, 'ab')
        self.buffer_max_elem = buffer_max_elem

    def find_group(self, group_name, index):
        return next((i for i, g in enumerate(self.groups) if g.name == group_name
            and g.index == index), -1)

    @staticmethod
    def upscale_inputs(data):
        def up2(k, v):
            if v.ndim == 0:
                return v[None,None]
            elif v.ndim == 1:
                return v[None,:]
            elif v.ndim == 2:
                return v
            else:
                raise RuntimeError(f'{k} has rank {v.ndim} value. Only 0, 1, or 2D allowed')
        keys = data.keys()
        vals = np.broadcast_arrays(*[up2(k, v) for k, v in data.items()])
        return dict(zip(keys, vals))

    def write(self, group_name, index=None, /, **data):
        """
        Writes new data, possibly creating one or more Group items
        group_name:  the `name` field of the Group(s) created
        index: the `index` field of the Group created
        data: map of field_name => data, where data can be one of:

        - scalar:  data = single data point coordinate.  
        - 1d tensor:  data[p] coordinate for data point p
        - 2d tensor:  data[i, p] coordinate for group index i and data point p
        """
        # validate index and data
        try:
            data = { k: util.get_numpy(v) for k, v in data.items() }
        except RuntimeError as ex:
            raise RuntimeError(
                f'{group_name=}, could not convert `data` to numpy arrays:\n{data=}')

        try:
            data = self.upscale_inputs(data)
        except RuntimeError as ex:
            raise RuntimeError(f'{group_name=}, got exception {ex}')

        leading_dim = next(v.shape[0] for v in data.values())
        if index is None:
            index = tuple(range(leading_dim))
        elif isinstance(index, int):
            index = (index,)
        if len(index) != leading_dim:
            raise RuntimeError(
                f'logger.write: {group_name=} {index=}.  `index` incompatible with '
                f'maximal leading data dimension {leading_dim}\n'
                f'index must be None (automatically inferred)\n'
                f'an integer if leading_dim == 1, or a tuple of integers of size\n'
                f'leading_dim')

        # create all non-existent group objects
        field_types = { k: v.dtype for k, v in data.items() }
        for i in index:
            if self.find_group(group_name, i) == -1:
                new_group = util.make_group(self.scope, group_name, i, **field_types)
                self.fh.write(util.pack_message(new_group))
                self.groups.append(new_group)
                self.points.append(None)

        for i in index:
            obj_idx = self.find_group(group_name, i)
            gobj = self.groups[obj_idx]
            pobj = self.points[obj_idx]
            slice_data = { k: v[i] for k, v in data.items() }
            if pobj is None:
                pobj = util.make_point(gobj, self.batch)
                self.batch += 1
                self.points[obj_idx] = pobj
            util.add_to_point(gobj, pobj, **slice_data)

        self.elem_count += sum(v.size for v in data.values())
        if self.elem_count >= self.buffer_max_elem:
            self.flush_buffer()

    def flush_buffer(self): 
        packed = util.pack_messages([p for p in self.points if p is not None])
        self.fh.write(packed)
        self.fh.flush()
        self.points = [None] * len(self.groups)
        self.elem_count = 0

    def shutdown(self):
        """
        Call shutdown in a SIGINT or SIGTERM signal handler in your main application
        for a clean exit.  Unfortunately, this is not always possible, for example
        when running in Google Colab.
        """
        self.fh.close()

