import numpy as np
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
    def __init__(self, scope: str):
        """
        scope: a string which defines the scope in the logical point grouping
        """
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
        self.fh = util.get_log_handle(path, 'ab')
        self.buffer_max_elem = buffer_max_elem

    def find_group(self, group_name, index):
        return next((i for i, g in enumerate(self.groups) if g.name == group_name
            and g.index == index), -1)

    @staticmethod
    def upscale_inputs(data):
        """
        Reshape all data to have shape: (index,point) 
        Shape transformations will be:
        () -> (1, 1)
        (point,) -> (1, point)
        (index,point) -> (index,point)
        """
        def up2(k, v):
            if v.ndim == 0:
                return v[None,None]
            elif v.ndim == 1:
                return v[None,:]
                # return v[:,None]
            elif v.ndim == 2:
                return v
            else:
                raise RuntimeError(
                    f'Datum {k} had shape {v.shape}.  Only rank 0, 1, or 2 data are '
                    f'allowed')

        keys, vals = list(zip(*data.items()))
        vals = [up2(k, v) for k, v in data.items()]
        try:
            vals = np.broadcast_arrays(*vals)
        except BaseException:
            raise RuntimeError(
                f'Data shapes aren\'t broadcastable: ' +
                ', '.join(f'{k}: {v.shape}' for k, v in data.items()))

        return dict(zip(keys, vals))

    def write(self, group_name, /, **data):
        """Writes new data, possibly creating one or more Group items.

        Inputs:
        group_name:  
          the `name` field of the (scope, name, index) tuple that will be associated
          with these points.
        data: 
          map of field_name => item, with the following logic.

        1. all data items (whether rank 0, 1, or 2) are implicitly broadcasted 
           with shape (1,1).  The final shape denotes (index, point)

        2. points are then written to (group_name, index)

        The common idioms for writing series of points:

        x[point], y[index, point]
        """
        # validate index and data
        for k, v in data.items():
            try:
                v = util.get_numpy(v)
                data[k] = v
            except RuntimeError as ex:
                raise RuntimeError(
                    f'{group_name=}, could not convert data key `{k}` to '
                    f'numpy arrays:\n{v=}\n{ex}')

        try:
            data = self.upscale_inputs(data)
        except BaseException as ex:
            raise RuntimeError(f'{group_name=}, got exception {ex}')

        item = next(iter(data.values()))
        indices = range(item.shape[0])
        
        # create all non-existent group objects
        field_types = { k: v.dtype for k, v in data.items() }
        for gi in indices:
            if self.find_group(group_name, gi) == -1:
                new_group = util.make_group(self.scope, group_name, gi, **field_types)
                self.fh.write(util.pack_message(new_group))
                self.groups.append(new_group)
                self.points.append(None)

        for gi in indices:
            obj_idx = self.find_group(group_name, gi)
            gobj = self.groups[obj_idx]
            pobj = self.points[obj_idx]
            slice_data = { k: v[gi] for k, v in data.items() }
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

