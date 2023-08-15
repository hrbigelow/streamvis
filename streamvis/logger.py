import numpy as np
from dataclasses import dataclass
from google.cloud import storage
import random
import time
import signal
import fcntl
from bokeh import palettes
from . import array_util, util
from . import data_pb2 as pb

@dataclass
class GridSpec:
    dim: int
    num_columns: int
    padding_factor: float = 1.2

class NonInterrupt:
    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self.signal_received = False
        self.old_handler = signal.signal(signal.SIGINT, self.handler)
                
    def handler(self, sig, frame):
        self.signal_received = (sig, frame)
        print(f'Finishing uninterruptible {self.name}')
        # logging.debug('SIGINT received. Delaying KeyboardInterrupt.')
    
    def __exit__(self, type, value, traceback):
        signal.signal(signal.SIGINT, self.old_handler)
        if self.signal_received:
            self.old_handler(*self.signal_received)


class DataLogger:
    """
    Create one instance of this in the producer script, to send data to
    a bokeh server.
    """
    def __init__(self, scope):
        self.configured_plots = set()
        self.scope = scope
        self.metadata = {}
        self.seqnum = {}
        random.seed(time.time())

    def init_gcs(self, bucket_name, blob_name, buffer_items=100):
        """
        Initialize logger to log data to the given GCS blob
        buffer_items:  number of data items to buffer
        bucket_name:  GCS bucket name for writing all data
        blob_name:  GCS blob name for writing all data
        """
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        self.blob = bucket.blob(blob_name)
        if not self.blob.exists():
            self.blob.upload_from_string('')
        suffix = hex(random.randint(0, 0x1000000))[2:]
        self.tmp_blob = bucket.blob(blob_name + '-' + suffix) 
        self.buffer_items = buffer_items
        self.points = pb.Points()

    def _append_gcs(self, content):
        """
        Appends content to the GCS blob
        content: protobuf message 
        """
        if self.tmp_blob.exists():
            self.tmp_blob.delete()
        self.tmp_blob.upload_from_string(content)
        self.blob.compose([self.blob, self.tmp_blob])

    def _write_message(self, message):
        """
        do a buffered write of the message.
        If message is pb.Point, it is buffered in self.points
        If it is pb.MetaData, it is written immediately
        """
        if isinstance(message, pb.MetaData):
            packed = util.pack_message(message)
            self._append_gcs(packed)
        elif isinstance(message, pb.Point):
            self.points.p.append(message)
            if len(self.points.p) >= self.buffer_items:
                packed = util.pack_message(self.points)
                self._append_gcs(packed)
                self.points = pb.Points()
        else:
            raise RuntimeError(
                    'DataLogger::write accepts only MetaData or Point.  Received '
                    f'{type(message)}')

    def write(self, meta_name, **values):
        """
        Writes new data, possibly creating a new 
        """
        if meta_name not in self.metadata:
            meta = pb.MetaData()
            meta.id = random.randint(0, 2**32)
            meta.scope = self.scope
            meta.name = meta_name
            for name, val in values.items():
                if not isinstance(val, (int, float)):
                    raise RuntimeError(
                        'DataLogger::write: `values` contains a non-{int,float} '
                        f'value: {values}')
                datum = meta.data.add()
                datum.name = name
                datum.is_integer = isinstance(val, int)

            self.metadata[meta_name] = meta
            self.seqnum[meta_name] = 0
            self._write_message(meta)

        meta = self.metadata[meta_name]
        item = pb.Point()
        item.meta_id = meta.id
        item.seqnum = self.seqnum[meta_name]
        self.seqnum[meta_name] += 1
        for datum in meta.data:
            if datum.name not in values:
                raise RuntimeError(
                    f'DataLogger::write: `values` does not contain datum {datum.name} '
                    f'previously appearing in the metadata field.  '
                    f'Expected data are {meta.data})') 
            if datum.is_integer:
                item.idata.append(values[datum.name])
            else:
                item.fdata.append(values[datum.name])

        if len(values) != len(meta.data):
            raise RuntimeError(
                f'DataLogger::write: `values` contained extra entries. '
                f'values: {values}\nvs.'
                f'meta.data: {meta.data}')

        self._write_message(item)


    def shutdown(self):
        """
        Call shutdown in a SIGINT or SIGTERM signal handler in your main application
        for a clean exit 
        """
        # Any GCS resources need flushing or cleanup?
        pass

    @staticmethod
    def get_numpy(data):
        try:
            data = data.detach().numpy().astype(np.float32)
        except BaseException:
            pass
        try:
            data = np.array(data, dtype=np.float32)
        except BaseException as ex:
            raise RuntimeError(
                f'exception {ex}:\n'
                f'Could not convert data into np.ndarray using either:\n'
                f'data.detach().numpy() or np.array(data).  '
                f'Got type(data) = {type(data)}')
        return data

    @staticmethod
    def _maybe_apply_grid(data, grid, spatial_dim):
        if grid is not None:
            if grid.dim == spatial_dim:
                raise RuntimeError(
                    f'Error: Got spatial_dim = {spatial_dim} and '
                    f'grid.dim = {grid.dim}.  grid cannot be along '
                    f'spatial dimension')
            if grid.dim not in range(data.ndim):
                raise RuntimeError(
                    f'Error: Got grid.dim = {grid_spce.dim} which is not a valid '
                    f'dimension of data with {data.ndim} dimensions')
            data = array_util.make_grid(data, spatial_dim, grid.dim, grid.num_columns, 
                    grid.padding_factor)
        return data

    def scatter(self, plot_name, data, append, palette='Viridis256', fig_kwargs={}):
        """
        Produce a scatter plot of `data`, which can be one of these shapes:
        [points, xy]         
        [points, xyc]
        [color, points, xy]

        'xy' is a size-2 dimension with x,y coordinates.
        'xyc' indicates x,y and color values.

        """
        data = self.get_numpy(data)
        init_cfg = dict(glyph_kind='scatter', palette=palette, fig_kwargs=fig_kwargs)
        append_dim = 1 if append else None
        nd_columns = 'xy' if data.shape[-1] == 2 else 'xyz'
        cds_opts = dict(append_dim=append_dim, nd_columns=nd_columns, zmode=None)
        self._send(plot_name, data, init_cfg, cds_opts)

    def scatter_grid(self, plot_name, data, append, palette='Viridis256', grid_columns=5,
            grid_spacing=0.1, fig_kwargs={}):
        """
        Produce a scatter plot of `data`, which can be one of these shapes:
        [grid, points, xy]
        [grid, points, xyc]
        [grid, color, points, xy]

        If ColorSpec is None, or ColorSpec with dim is provided, then
        data.shape[spatial_dim] = 2 (x,y) 
        
        Otherwise, ColorSpec with no dim was provided.  In this case,
        data.shape[spatial_dim] = 3 (x,y,color)
        """
        data = self.get_numpy(data)

        if data.ndim not in (3, 4):
            raise RuntimeError(f'scatter_grid: data must have 3 or 4 dimensions')
        if data.shape[1] == 3 or data.ndim == 4:
            if palette is None:
                raise RuntimeError(
                    f'scatter_grid: got {palette=} but {data.shape=}. '
                    f'data has color information but no palette specified')
        if data.shape[-1] == 2 and data.ndim == 3:
            palette = None

        init_cfg = dict(glyph_kind='scatter', palette=palette, fig_kwargs=fig_kwargs)
        append_dim = 1 if append else None
        nd_columns = 'xy' if data.shape[-1] == 2 else 'xyz'
        cds_opts = dict(append_dim=append_dim, nd_columns=nd_columns, zmode=None)

        if data.ndim == 4: # color is dimension 1
            data = array_util.dim_to_data(data, 1, 3, (0, 1))
            # merge dimensions 1 and 2
            shape = list(data.shape)
            shape[1] *= shape[2]
            shape.pop(2)
            # [grid, points, spatial]
            data = data.reshape(*shape)

        spatial_dim = data.ndim - 1
        data = array_util.make_grid(data, spatial_dim, 0, grid_columns, grid_spacing)
        data = data.reshape(-1, data.shape[-1])

        # bokeh scatter expects shape [xy, points]
        data = data.transpose(1, 0)
        self._send(plot_name, data, init_cfg, cds_opts)

    def tandem_lines(self, plot_name, data, palette=None, fig_kwargs={}):
        """
        plot_name: the plot to plot this data
        data: [lines, new_points, xy]  (xy = 2)
        
        Adds `num_points` new points each to `lines` lines.  Each point is defined by
        its `xy` coordinates.

        Expect all calls for `plot_name` to have the same num_lines, but may have
        differing num_new_points.
        """
        data = self.get_numpy(data)
        if data.ndim != 3 or data.shape[2] != 2:
            raise RuntimeError(
                f'tandem_lines requires data shape [lines, new_points, xy]. '
                f'Received {data.shape=}')

        # The multi_line plot expects 
        init_cfg = dict(glyph_kind='multi_line', palette=palette, fig_kwargs=fig_kwargs)
        zmode = None if palette is None else 'linecolor'
        
        # bokeh multi_line requires shape [xy, lines, new_points]
        data = data.transpose(2,0,1)
        append_dim = 2 # new_points dimension
        cds_opts = dict(append_dim=append_dim, nd_columns='xy', zmode=zmode)
        self._send(plot_name, data, init_cfg, cds_opts)

    def multi_lines(self, plot_name, data, line_dims, spatial_dim, append, color=None,
            grid=None, fig_kwargs={}):
        """
        Plot multiple lines from data
        - line_dims: int or tuple of ints indexing the separate lines
        - spatial_dim: int indexing the x,y or x,y,z values
        - z must be present if color=ColorSpec(palette, None), otherwise absent
        """
        data = self.get_numpy(data)

        if isinstance(line_dims, int):
            line_dims = (line_dims,)

        init_cfg = dict(glyph_kind='multi_line', fig_kwargs=fig_kwargs)
        append_dim = 2 if append else None
        cds_opts = dict(append_dim=append_dim, zmode=None)

        data = self._maybe_apply_color(data, color, spatial_dim, init_cfg, cds_opts)
        data = self._maybe_apply_grid(data, grid, spatial_dim)

        # permute to: spatial_dim, *line_dims, other
        data = array_util.axes_to_front(data, spatial_dim, *line_dims)
        # collapse line_dims
        num_spatial = data.shape[0]
        num_lines = np.prod(data.shape[1:1+len(line_dims)])
        data = data.reshape(num_spatial, num_lines, -1)
        data = data.tolist()
        self._send(plot_name, data, init_cfg, cds_opts)

