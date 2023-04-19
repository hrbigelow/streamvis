import numpy as np
import requests
from bokeh import palettes
from dataclasses import dataclass
from . import array_util

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

class Client:
    """
    Create one instance of this in the producer script, to send data to
    a bokeh server.
    """
    def __init__(self, rest_uri, run_name):
        self.uri = f'http://{rest_uri}/{run_name}'
        self.layout_plots = set()
        self.configured_plots = set()

    def clear(self):
        requests.delete(f'{self.uri}')

    def set_layout(self, grid_map):
        """
        Specify the layout of plots on the page.
        grid_map is a map of plot_name => (top, left, height, width)
        """
        if not (isinstance(grid_map, dict) and
                all(isinstance(v, tuple) and len(v) == 4 for v in grid_map.values())):
            raise RuntimeError(
                f'set_layout: grid_map must be a map of: '
                f'plot_name => (beg_row, beg_col, end_row, end_col)')
        for plot_name, coords in grid_map.items():
            requests.post(f'{self.uri}/layout/{plot_name}', json=coords)
            self.layout_plots.add(plot_name)

    def _post(self, plot_name, data, init_cfg, update_cfg):
        requests.post(f'{self.uri}/data/{plot_name}', json=data)
        if plot_name not in self.configured_plots:
            requests.post(f'{self.uri}/init_config/{plot_name}', json=init_cfg)
            requests.post(f'{self.uri}/update_config/{plot_name}', json=update_cfg)
            self.configured_plots.add(plot_name)

    @staticmethod
    def get_numpy(data):
        try:
            data = data.detach().numpy()
        except BaseException:
            pass
        try:
            data = np.array(data)
        except BaseException as ex:
            raise RuntimeError(
                f'exception {ex}:\n'
                f'Could not convert data into np.ndarray using either:\n'
                f'data.detach().numpy() or np.array(data).  '
                f'Got type(data) = {type(data)}')
        return data

    def _check_name(self, plot_name):
        if plot_name not in self.layout_plots:
            plots = '\n'.join(f'   {p}' for p in self.layout_plots)
            raise RuntimeError(
                f'Plot \'{plot_name}\' has not been registered in layout.\n'
                f'Plots registered in layout are:\n{plots}'
                )

    @staticmethod
    def _maybe_apply_color(data, color, spatial_dim, init_cfg, update_cfg):
        if color is None:
            init_cfg['with_color'] = False
            init_cfg['palette'] = None
            update_cfg['nd_columns'] = 'xy'
        else:
            init_cfg['with_color'] = True
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
        self._check_name(plot_name)

        data = self.get_numpy(data)
        
        init_cfg = dict(kind='scatter', fig_kwargs=fig_kwargs)
        update_cfg = dict(append_dim=(1 if append else -1), zmode=None)

        data = self._maybe_apply_color(data, color, spatial_dim, init_cfg, update_cfg)
        data = self._maybe_apply_grid(data, grid, spatial_dim)

        data = array_util.axes_to_front(data, spatial_dim)
        data = data.tolist()
        self._post(plot_name, data, init_cfg, update_cfg)

    def tandem_lines(self, plot_name, x, ys, palette=None, fig_kwargs={}):
        """
        Plot streaming data as a set of tandem lines, all sharing the same x axis.
        x: a scalar value
        ys: array of K elements y1,...,yk
        """
        self._check_name(plot_name)

        init_cfg = dict(kind='multi_line', with_color=False, palette=palette,
                fig_kwargs=fig_kwargs)
        if palette is not None:
            init_cfg['with_color'] = True

        ys = self.get_numpy(ys)
        xs = np.full(ys.shape[0], x)
        data = np.expand_dims(np.stack((xs, ys), axis=0), -1)
        data = data.tolist()
        zmode = None if palette is None else 'linecolor'
        update_cfg = dict(append_dim=2, nd_columns='xy', zmode=zmode)
        self._post(plot_name, data, init_cfg, update_cfg)

    def multi_lines(self, plot_name, data, line_dims, spatial_dim, append, color=None,
            grid=None, fig_kwargs={}):
        """
        Plot multiple lines from data
        - line_dims: int or tuple of ints indexing the separate lines
        - spatial_dim: int indexing the x,y or x,y,z values
        - z must be present if color=ColorSpec(palette, None), otherwise absent
        """
        self._check_name(plot_name)
        data = self.get_numpy(data)

        if isinstance(line_dims, int):
            line_dims = (line_dims,)

        init_cfg = dict(kind='multi_line', fig_kwargs=fig_kwargs)
        append_dim = 2 if append else -1
        update_cfg = dict(append_dim=append_dim, zmode=None)

        data = self._maybe_apply_color(data, color, spatial_dim, init_cfg, update_cfg)
        data = self._maybe_apply_grid(data, grid, spatial_dim)

        # permute to: spatial_dim, *line_dims, other
        data = array_util.axes_to_front(data, spatial_dim, *line_dims)
        # collapse line_dims
        num_spatial = data.shape[0]
        num_lines = np.prod(data.shape[1:1+len(line_dims)])
        data = data.reshape(num_spatial, num_lines, -1)
        data = data.tolist()
        self._post(plot_name, data, init_cfg, update_cfg)

