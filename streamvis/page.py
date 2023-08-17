import numpy as np
import re
from bokeh.layouts import column, row
from bokeh.models.dom import HTML
from bokeh.models import Div, ColumnDataSource
from bokeh.plotting import figure
from streamvis import util

class IndexPage:
    """
    An index page, providing links to each available plot
    """
    def __init__(self, server, doc):
        self.server = server
        self.doc = doc
        self.session_id = doc.session_context.id
        self.doc.on_session_destroyed(self.destroy)

    def destroy(self, session_context):
        self.server.delete_page(self.session_id)

    def build(self):
        self.container = row()
        text = '<h2>Streamvis Server Index Page</h2>'
        self.container.children.append(column([Div(text=text)]))
        inner = '<br>'.join(plot for plot in self.server.schema.keys())
        html = f'<p>{inner}</p>'
        self.container.children[0].children[0] = Div(text=html)
        self.doc.add_root(self.container)

    def schedule_callback(self):
        self.doc.add_next_tick_callback(self.update_callback)

    def update_callback(self):
        # no-op because the schema doesn't change
        pass

class PageLayout:
    """
    Represents a browser page
    """
    def __init__(self, server, doc):
        self.server = server
        # index into the server message log
        self.read_pos = 0
        self.doc = doc
        self.session_id = doc.session_context.id
        self.doc.on_session_destroyed(self.destroy)
        self.coords = None
        self.nbox = None
        self.box_elems = None
        self.widths = None
        self.heights = None
        self.server_points_pos = 0 

    def destroy(self, session_context):
        self.server.delete_page(self.session_id)

    def _set_layout(self, plots, box_elems, box_part, plot_part):
        """
        The overall page layout is a list of 'boxes', with each box containing
        one or more plots.  The boxes are either all Bokeh row or column objects.
        Accordingly, each plot is addressed by a (box, elem) tuple indicating which
        box it is in, and its position within the box.

        Computes height and width fractions for all plots and sets the properties.
        `widths` and `heights`.  Once overall page width and height are known, these
        fractions can be used to resolve the individual plot dimensions in pixels.

        box_elems:  box_elems[i] = number of plots in box i
        box_part:   box_part[i] = relative stacking size of box i
        plot_part:  plot_part[i] = relative size of plot i

        """
        self.plot_names = plots
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

        This function only accesses the server schema, not the data state
        """
        args = request.arguments
        known_plots = self.server.schema.keys()

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
                            f'In {param}={grid}, plot \'{plot}\' is not in the schema. '
                            f'Schema contains plots {", ".join(known_plots)}')
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
        self.build()

    def get_figsize(self, index):
        width = int(self.widths[index] * self.page_width)
        height = int(self.heights[index] * self.page_height)
        return dict(width=width, height=height)

    def set_pagesize(self, width, height):
        self.page_width = width
        self.page_height = height

    def build(self):
        """
        Build the page for the first time
        """
        self.container = column() if self.row_mode else row() 
        schema = self.server.schema

        for index, plot_name in enumerate(self.plot_names):
            box_index, _ = self.coords[index]
            if box_index >= len(self.container.children):
                box = row() if self.row_mode else column()
                self.container.children.append(box)
            box = self.container.children[box_index]
            fig_kwargs = schema[plot_name].get('kwargs', {})
            fig_kwargs.update(self.get_figsize(index))
            fig = figure(name=plot_name, **fig_kwargs)
            box.children.append(fig)
            # print(f'in build, appended {fig=}, {fig.height=}, {fig.width=}, {fig.title=}')
        self.doc.add_root(self.container)
        print('finished building page')

    def schedule_callback(self):
        print(f'in page {self.session_id} scheduling update callback')
        self.doc.add_next_tick_callback(self.update_callback)

    @staticmethod
    def matching_groups(plot_schema, groups):
        matched = []
        for g in groups:
            if (re.match(plot_schema['scope_pattern'], g.scope) and
                    re.match(plot_schema['group_pattern'], g.name)):
                matched.append(g)
        return matched

    def update_callback(self):
        """
        Scheduled as a next-tick callback when server state is updated
        """
        # print(f'in page {self.session_id} before reserving state')
        with self.server.get_state(blocking=False) as state:
            # print(f'in page {self.session_id} update_callback, reserved server state')
            if self.server_points_pos == len(state['points']):
                return
            
            # update any figures if out of date version
            new_points = state['points'][self.server_points_pos:]
            all_data_groups = state['groups']
            for fig in self.doc.select(selector={'type': figure}):
                plot_schema = self.server.schema[fig.name]
                glyph_kwargs = plot_schema.get('glyph_kwargs', {})
                data_groups = self.matching_groups(plot_schema, all_data_groups)
                for group in data_groups:
                    glyphs = fig.select({'name': str(group.id)})
                    if len(glyphs) == 0:
                        cols = plot_schema['columns']
                        cds = ColumnDataSource({c: [] for c in cols})
                        fig.line(*cols, source=cds, name=str(group.id), **glyph_kwargs)
                    glyph = fig.select({'name': str(group.id)})[0]
                    new_cds_data = util.points_to_cds(new_points, group)
                    glyph.data_source.stream(new_cds_data)
            self.server_points_pos = len(state['points'])

