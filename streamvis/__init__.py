from bokeh.io import curdoc
from bokeh.layouts import column
from bokeh.models import GridBox
from bokeh.plotting.figure import Figure
from functools import partial
from copy import deepcopy
import requests
from . import plots

class Server:
    def __init__(self, doc, run_state, run_name):
        """
        run_name: a name to scope this run
        """
        self.run_state = run_state
        self.run_name = run_name
        self.doc = doc 
        self.column = column()
        self.doc.add_root(self.column)
        self.plot_config = {} 

    def get_figure(self, cds_name):
        return self.doc.select({ 'type': Figure, 'name': cds_name })

    def get_state(self):
        return self.run_state.get(self.run_name, None)

    def init_callback(self, layout, cfg):
        """
        Called when new cfg data is available
        """
        # cfg may be left over from a previously aborted run, so not
        # congruent with the current layout.  in this case, it is ignored.
        # during a client run, the client ensures that all POSTs to cfg
        # endpoint are keys present in layout
        if any(k not in layout for k in cfg.keys()):
            return

        grid = []
        for cds_name, plot_cfg in cfg.items():
            if len(plot_cfg) == 0:
                fig = self.get_figure(cds_name)
            else:
                fig = plots.make_figure(cds_name, plot_cfg)
                self.plot_config[cds_name] = plot_cfg

            coords = layout[cds_name]
            grid.append((fig, *coords))
        plot = GridBox(children=grid)
        self.column.children.clear()
        self.column.children.append(plot)

    def update_callback(self, all_data):
        for cds_name, data in all_data.items():
            cds = self.doc.get_model_by_name(cds_name)
            if cds is None:
                continue
            plot_cfg = self.plot_config[cds_name]
            plots.update_data(data, cds, plot_cfg)

    def work_callback(self):
        state = self.get_state()
        if state is None or state.cfg is None:
            return

        if len(state.cfg) != 0:
            layout = state.layout
            wrap = partial(self.init_callback, deepcopy(layout), deepcopy(state.cfg))
            self.doc.add_next_tick_callback(wrap)
            state.cfg.clear()
            return

        if state.data is None:
            return
        elif all(len(v) == 0 for v in state.data.values()):
            # no new data to process
            return
        else:
            wrap = partial(self.update_callback, deepcopy(state.data))
            self.doc.add_next_tick_callback(wrap)
            state.data.clear()

    def start(self):
        """
        Call this in the bokeh server code at the end of the script.
        This starts the receiver listening for data updates from the
        sender.
        """
        self.doc.add_periodic_callback(self.work_callback, 500)

class Client:
    """
    Create one instance of this in the producer script, to send data to
    a bokeh server.
    """
    def __init__(self, rest_uri, run_name):
        self.uri = f'http://{rest_uri}/{run_name}'
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

    def _post(self, plot_name, data, cfg):
        requests.post(f'{self.uri}/data/{plot_name}', json=data)
        if plot_name not in self.configured_plots:
            requests.post(f'{self.uri}/cfg/{plot_name}', json=cfg)
            self.configured_plots.add(plot_name)

    def scatter(self, plot_name, data, spatial_dim, color_mode, palette='Viridis', 
            append=True, fig_kwargs={}):
        cfg = dict(item_shape='bs', color_mode=color_mode, palette=palette,
                append=append, kind='scatter', fig_kwargs=fig_kwargs)
        self._post(plot_name, data, cfg)

    def tandem_lines(self, plot_name, data, fig_kwargs={}):
        cfg = dict(item_shape='m', append=True, kind='multi_line', fig_kwargs=fig_kwargs)
        self._post(plot_name, data, cfg)

