import numpy as np
from bokeh.layouts import column, row
from . import plots

class PlotPage:
    """
    Represents a browser page
    """
    def __init__(self, state_machine, doc, *plot_names):
        self.state_machine = state_machine
        self.doc = doc
        self.plots = plot_names
        self.versions = [0] * len(plot_names)
        self.container = None # will be either a bokeh.layout.row or column
        self.coords = None
        self.nbox = None
        self.box_elems = None
        self.widths = None
        self.heights = None

    def set_layout(self, row_mode, box_elems, box_part, plot_part):
        """
        """
        self.row_mode = row_mode
        self.box_elems = box_elems

        denom = sum(box_part)
        box_norm = []
        for part, nelem in zip(box_part, box_elems):
            box_norm.extend([part / denom] * nelem)

        assert sum(self.box_elems) == len(self.plots)
        assert len(box_part) == len(box_elems)
        assert len(plot_part) == len(self.plots)
        
        self.nbox = len(box_elems)
        cumul = [0] + [ sum(box_elems[:i+1]) for i in range(self.nbox) ]

        def _sl(lens, i):
            return slice(cumul[i], cumul[i+1])

        slices = [ plot_part[_sl(box_elems, i)] for i in range(self.nbox) ]
        plot_norm = [ v / sum(sl) for sl in slices for v in sl ]

        # self.coords[i] = (box_index, elem_index) for plot i
        self.coords = []
        for box, sz in enumerate(box_elems):
            self.coords.extend([(box, elem) for elem in range(sz)])

        if self.row_mode:
            self.widths = box_norm
            self.heights = plot_norm
        else:
            self.widths = plot_norm
            self.heights = box_norm

    def build_page(self, state):
        """
        Build the page for the first time
        """
        self.container = column() if self.row_mode else row() 

        for index in range(len(self.plots)):
            plot = self.plots[index]
            box_index, _ = self.coords[index]
            if box_index >= len(self.container.children):
                box = row() if self.row_mode else column()
                self.container.children.append(box)
            else:
                box = self.container.children[box_index]
            plot_state = state[plot]
            fig = plots.make_figure(plot, **plot_state.fig_kwargs)
            box.children.append(fig)
            self.versions[index] = plot_state.version

    def replace_plot(self, index, fig):
        """
        Assuming the layout is initialized, replace the plot 
        """
        box_index, elem_index = self.coords[index]
        box = self.container.children[box_index]
        box.children[elem_index] = fig

    def get_data_source(self, index):
        box_index, elem_index = self.coords[index]
        fig = self.container.children[box_index].children[elem_index]
        return fig.renderers[0].data_source

    def schedule_callback(self):
        self.doc.add_next_tick_callback(self.update_callback)

    def update_callback(self):
        """
        Scheduled as a next-tick callback when server state is updated
        """
        with self.state_machine.get_state(blocking=False) as state:
            if state is None:
                return
            if not all(plot in state for plot in self.plots):
                # state is incomplete
                return
            if self.container is None:
                self.build_page(state)
            
            # update any figures if out of date version
            for ind, plot in enumerate(self.plots):
                plot_state = state[plot]
                if self.versions[ind] != plot_state.version:
                    fig = plots.make_figure(plot, **plot_state.fig_kwargs)
                    # does this work?
                    self.replace_plot(ind, fig)
                    self.versions[ind] = plot_state.version

                # update cds
                cds = self.get_data_source(ind)
                zmode = plot_state.cds_opts['zmode']
                nd_columns = plot_state.cds_opts['nd_columns']
                ary = plot_state.nddata
                cdata = dict(zip(nd_columns, ary.tolist()))
                if zmode == 'linecolor':
                    k = ary.shape[1]
                    cdata['z'] = np.linspace(0, 1, k).tolist()
                cds.data = cdata

