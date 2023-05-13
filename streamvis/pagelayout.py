import numpy as np
from bokeh.layouts import column, row
from . import plots


class PageLayout:
    """
    Represents a browser page
    """
    def __init__(self, state_machine, doc):
        self.state_machine = state_machine
        self.doc = doc
        self.session_id = doc.session_context.id
        self.doc.on_session_destroyed(self.destroy)
        self.coords = None
        self.nbox = None
        self.box_elems = None
        self.widths = None
        self.heights = None
        self.page_built = False

    def destroy(self, session_context):
        del self.state_machine.pages[self.session_id]

    def _set_layout(self, plots, box_elems, box_part, plot_part):
        """
        box_elems:  box_elems[i] = number of plots in box i
        box_part:   box_part[i] = relative stacking size of box i
        plot_part:  plot_part[i] = relative size of plot i
        """
        self.plots = plots
        self.versions = [-1] * len(plots)
        self.box_elems = box_elems

        denom = sum(box_part)
        box_norm = []
        for part, nelem in zip(box_part, box_elems):
            box_norm.extend([part / denom] * nelem)

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
            self.widths = plot_norm
            self.heights = box_norm
        else:
            self.widths = box_norm
            self.heights = plot_norm

    def process_request(self, request):
        """
        API:
        rows:   semi-colon separated list of csv names lists
        cols:   semi-colon separate list of csv names lists
        
        width:  csv numbers list
        height: csv numbers list

        Exactly one of `rows` or `cols` must be given.  Both `width` and `height` are
        optional.
        """
        args = request.arguments

        with self.state_machine.get_state() as state:
            known_plots = state.keys()

        def maybe_get(args, param):
            val = args.pop(param, None)
            return val if val is None else val[0].decode()

        def parse_grid(param, grid, plots, box_elems):
            if grid == '':
                raise RuntimeError(f'Got empty {param} value') 
            blocks = grid.split(';')
            for block in blocks:
                items = block.split(',')
                for plot in items:
                    if plot not in known_plots:
                        raise RuntimeError(
                            f'In {param}={grid}, plot \'{plot}\' is not among the '
                            f'known plots, or has no data yet.  Known plots are:\n'
                            f'{", ".join(known_plots)}')
                    plots.append(plot)
                box_elems.append(len(items))

        def parse_csv(param, arg, target_nelems):
            # Expect arg to be a csv numbers with target_nelems 
            if arg is None:
                return [1] * target_nelems
            try:
                num_list = list(map(float, arg.split(',')))
            except ValueError:
                raise RuntimeError(
                    f'{param} value \'{arg}\' is not a valid csv list of numbers')
            if any(num <= 0 for num in num_list):
                raise RuntimeError(
                    f'{param} value \'{arg}\' are not all positive numbers')
            if len(num_list) != target_nelems:
                raise RuntimeError(
                    f'Received {len(num_list)} values but expected {target_nelems}. '
                    f'Context: {param}={arg}')
            return num_list 

        rows = maybe_get(args, 'rows')
        cols = maybe_get(args, 'cols')
        if (rows is None) == (cols is None):
            raise RuntimeError(
                f'Exactly one of `rows` or `cols` query parameter must be given')

        plots = [] 
        box_elems = [] # 
        box_part = []  # box stacking dimension proportion 
        plot_part = [] # plot packing dimension proportions

        width_arg = maybe_get(args, 'width')
        height_arg = maybe_get(args, 'height')

        if rows is not None:
            self.row_mode = True
            parse_grid('rows', rows, plots, box_elems)
            plot_part = parse_csv('width', width_arg, len(plots))
            box_part = parse_csv('height', height_arg, len(box_elems))
        else:
            self.row_mode = False
            parse_grid('cols', cols, plots, box_elems)
            plot_part = parse_csv('height', height_arg, len(plots))
            box_part = parse_csv('width', width_arg, len(box_elems))
            
        self._set_layout(plots, box_elems, box_part, plot_part)

    def get_figsize(self, index):
        width = int(self.widths[index] * self.page_width)
        height = int(self.heights[index] * self.page_height)
        return dict(width=width, height=height)

    def set_pagesize(self, width, height):
        self.page_width = width
        self.page_height = height

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
            box = self.container.children[box_index]
            ps = state[plot]
            fig_kwargs = ps.fig_kwargs
            fig_kwargs.update(self.get_figsize(index))
            fig = plots.make_figure(plot, ps.glyph_kind, ps.palette, fig_kwargs)
            box.children.append(fig)
            # print(f'in build_page, appended {fig=}, {fig.height=}, {fig.width=}, {fig.title=}')
            self.versions[index] = ps.version
        self.doc.add_root(self.container)
        self.page_built = True

    def replace_plot(self, index, fig):
        """
        Assuming the layout is initialized, replace the plot 
        """
        # print(f'replace_plot: {index=}, {fig=}')
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
            if not self.page_built:
                self.build_page(state)
            
            # update any figures if out of date version
            for index, plot in enumerate(self.plots):
                ps = state[plot]
                if self.versions[index] != ps.version:
                    fig_kwargs = ps.fig_kwargs
                    fig_kwargs.update(self.get_figsize(index))
                    fig = plots.make_figure(plot, ps.glyph_kind, ps.palette, fig_kwargs)
                    # does this work?
                    self.replace_plot(index, fig)
                    self.versions[index] = ps.version

                # update cds
                cds = self.get_data_source(index)
                zmode = ps.cds_opts['zmode']
                nd_columns = ps.cds_opts['nd_columns']
                ary = ps.nddata
                cdata = dict(zip(nd_columns, ary.tolist()))
                if zmode == 'linecolor':
                    k = ary.shape[1]
                    cdata['z'] = np.linspace(0, 1, k).tolist()
                cds.data = cdata

