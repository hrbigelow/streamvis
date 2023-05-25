import numpy as np
from dataclasses import dataclass
import signal
import fcntl
import pickle
from bokeh import palettes
from . import array_util, util

@dataclass
class GridSpec:
    dim: int
    num_columns: int
    padding_factor: float = 1.2

class ColorSpec:
    def __init__(self, palette='Viridis', dim=None):
        if palette not in palettes.__palettes__:
            raise RuntimeError(
                f'Invalid ColorSpec:  got palette {palette}.  palette must be one of '
                f'the Bokeh palette identifiers in bokeh.palettes.__palettes__')
        self.palette = palette
        if dim is not None and not isinstance(dim, int):
            raise RuntimeError(
                f'Invalid ColorSpec:  got dim = {dim}.  Must be None or an integer')
        self.dim = dim 

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
    def __init__(self, run_name):
        self.configured_plots = set()
        self.pub = None
        self.run_name = run_name
        self.write_log_fh = None

    def init_pubsub(self, project_id, topic_id):
        from google.cloud import pubsub_v1
        self.pub = pubsub_v1.PublisherClient(
                publisher_options = pubsub_v1.types.PublisherOptions(
                    enable_message_ordering=True
                    ))
        if not util.topic_exists(self.pub, project_id, topic_id):
            raise RuntimeError(
                f'Cannot initialize client since topic {topic_id} does not exist. '
                f'Launch streamvis_server first, and provide the topic it returns')
        self.topic_path = self.pub.topic_path(project_id, topic_id)

    def init_write_log(self, write_log_path):
        try:
            self.write_log_fh = open(write_log_path, 'ab')
        except OSError as ex:
            raise RuntimeError(f'Could not open {write_log_path=} for writing: {ex}')

    def shutdown(self):
        """
        Call shutdown in a SIGINT or SIGTERM signal handler in your main application
        for a clean exit 
        """
        if self.write_log_fh is not None:
            print(f'\nClosing streamvis log file {self.write_log_fh.name}')
            self.write_log_fh.close()

    def _publish(self, plot_name, action, data):
        assert plot_name is not None, '_publish: plot_name is None'
        data = pickle.dumps(data)
        # with NonInterrupt('_publish'):
        # TODO: since the logger may run in a spawned process, the NonInterrupt
        # mechanism (which relies on signal) is not allowed.
        if self.pub is not None:
            attrs = dict(run=self.run_name, plot_name=plot_name, action=action)
            future = self.pub.publish(
                    topic=self.topic_path, 
                    data=data, 
                    ordering_key=self.run_name,
                    **attrs)
            future.result()

        if self.write_log_fh is not None:
            # TODO: context manager
            fcntl.flock(self.write_log_fh, fcntl.LOCK_EX)
            log_entry = util.LogEntry(self.run_name, action, plot_name, data)
            pickle.dump(log_entry, self.write_log_fh)
            fcntl.flock(self.write_log_fh, fcntl.LOCK_UN)

    def _send(self, plot_name, data, init_cfg, cds_opts):
        if plot_name not in self.configured_plots:
            init_data = { **init_cfg, 'cds_opts': cds_opts}
            self._publish(plot_name, 'init', init_data)
            self.configured_plots.add(plot_name)
        self._publish(plot_name, 'add-data', data)

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
    def _maybe_apply_color(data, color, spatial_dim, init_cfg, update_cfg):
        if color is None:
            init_cfg['palette'] = None
            update_cfg['nd_columns'] = 'xy'
        else:
            init_cfg['palette'] = color.palette
            update_cfg['nd_columns'] = 'xyz'
            if color.dim is not None:
                if color.dim == spatial_dim:
                    raise RuntimeError(
                        f'color.dim, if provided, must not be equal to spatial_dim.  '
                        f'Both were equal to {color.dim}')
                if color.dim not in range(data.ndim):
                    raise RuntimeError(
                        f'color.dim = {color.dim} but data.ndim = {data.ndim}')

                data = array_util.dim_to_data(data, color.dim, spatial_dim, (0,1))
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


    def scatter(self, plot_name, data, spatial_dim, append, color=None, grid=None,
            fig_kwargs={}):
        """
        Produce a scatter plot of `data`.
        If ColorSpec is None, or ColorSpec with dim is provided, then
        data.shape[spatial_dim] must be 2, and is interpreted as x,y
        
        Otherwise, ColorSpec with no dim was provided.  In this case,
        data.shape[spatial_dim] must be 3, and is interpreted as x,y,color

        If GridSpec is provided, the data will be spread out into a grid by slicing
        along the grid.dim dimension and wrapping the slices into a grid of
        grid.num_columns grid items.
        """
        data = self.get_numpy(data)
        
        init_cfg = dict(glyph_kind='scatter', fig_kwargs=fig_kwargs)
        cds_opts = dict(append_dim=(1 if append else -1), zmode=None)

        data = self._maybe_apply_color(data, color, spatial_dim, init_cfg, cds_opts)
        data = self._maybe_apply_grid(data, grid, spatial_dim)

        data = array_util.axes_to_front(data, spatial_dim)
        data = data.tolist()
        self._send(plot_name, data, init_cfg, cds_opts)

    def tandem_lines(self, plot_name, x, ys, palette=None, fig_kwargs={}):
        """
        Plot streaming data as a set of tandem lines, all sharing the same x axis.
        x: a scalar value
        ys: array of L elements y1,...,yl.
        L: number of lines being plotted
        """
        init_cfg = dict(glyph_kind='multi_line', palette=palette, fig_kwargs=fig_kwargs)

        ys = self.get_numpy(ys)
        xs = np.full(ys.shape[0], x)
        data = np.stack((xs, ys), axis=0)

        # data: 2, L, 1
        data = data.reshape(*data.shape, 1)
        data = data.tolist()
        zmode = None if palette is None else 'linecolor'
        cds_opts = dict(append_dim=2, nd_columns='xy', zmode=zmode)
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
        append_dim = 2 if append else -1
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

