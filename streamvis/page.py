from dataclasses import dataclass
import numpy as np
import re
from bokeh.layouts import column, row
from bokeh.models.dom import HTML
from bokeh.models import ColumnDataSource, Legend
from bokeh.plotting import figure
from bokeh import palettes

from streamvis import util

class Plot:
    name: str
    schema: Dict
    name_pat: re.Pattern
    figure: 'figure'

    def __init__(self, name: str, global_schema: Dict):
        self.name = name
        self.schema = global_schema.get(name)
        if self.schema is None:
            raise RuntimeError(
                f"No name '{name}' found in global_schema. "
                f"Available names: {', '.join(name for name in global_schema.keys())}")
        pat = self.schema.get(name_pattern)
        if pat is None:
            raise RuntimeError(
                f"Plot schema '{name}' didn't contain a 'name_pattern' field")
        self.name_pat = re.compile(pat)

class Session:
    id: int
    plots: List[Plot] 
    groups: Dict[pb.Group, List[pb.Point]]
    scope_pat: re.Pattern
    fh: Optional[_io._IOBase, 'GFile']

    def __init__(self, id: int, log_file: str):
        self.id = id
        self.plots = []
        self.groups = {}
        self.scope_pat = None 
        self.fh = util.get_log_handle(log_file, 'rb')

class PageLayout:
    """Represents a browser page."""
    def __init__(self, server, doc):
        self.server = server
        # index into the server message log
        self.doc = doc
        self.session = Session(doc.session_context.id, server.log_file)
        self.doc.on_session_destroyed(self.destroy)
        self.coords = None
        self.nbox = None
        self.box_elems = None
        self.widths = None
        self.heights = None

    def destroy(self, session_context):
        del self.server.pages[self.session_id]

    def _set_layout(self, box_elems, box_part, plot_part):
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
        scopes: regex pattern of scopes to include 
        rows:   semi-colon separated list of csv names lists
        cols:   semi-colon separated list of csv names lists
        
        width:  csv numbers list
        height: csv numbers list

        Exactly one of `rows` or `cols` must be given.  Both `width` and `height` are
        optional.

        scopes is optional.  if absent, defaults to '.+'

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
        scope_pat = maybe_get(args, "scopes")
        if scope_pat is None:
            scope_pat = ".*"
        try:
            self.session.scope_pat = re.compile(scope_pat)
        except re.PatternError as ex:
            raise RuntimeError(f"scopes argument '{scope_pat}' is not a valid regex")

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
            
        for plot_name in plots:
            self.session.plots.append(Plot(plot_name, self.server.schema))

        self._set_layout(box_elems, box_part, plot_part)

    def get_figsize(self, index):
        width = int(self.widths[index] * self.page_width)
        height = int(self.heights[index] * self.page_height)
        return dict(width=width, height=height)

    def set_pagesize(self, width, height):
        self.page_width = width
        self.page_height = height

    def start(self):
        self.doc.add_next_tick_callback(self.build_page_cb)

    def build_page_cb(self):
        """Build the page for the first time.  

        Must be scheduled as next-tick callback
        """
        self.container = column() if self.row_mode else row() 

        for index, plot in enumerate(self.session.plots):
            box_index, _ = self.coords[index]
            if box_index >= len(self.container.children):
                box = row() if self.row_mode else column()
                self.container.children.append(box)
            box = self.container.children[box_index]
            fig_kwargs = plot.schema.get('figure_kwargs', {})
            # fig_kwargs.update(self.get_figsize(index))
            size_opts = self.get_figsize(index)
            fig = figure(name=plot.name, output_backend='webgl', **size_opts)
            if 'legend' in fig_kwargs:
                legend = Legend(**fig_kwargs['legend'])
                fig.add_layout(legend)
            fig.title.update(**fig_kwargs.get('title', {}))
            fig.xaxis.update(**fig_kwargs.get('xaxis', {}))
            fig.yaxis.update(**fig_kwargs.get('yaxis', {}))
            box.children.append(fig)
            plot.figure = fig
            # print(f'in build, appended {fig=}, {fig.height=}, {fig.width=}, {fig.title=}')
        self.doc.add_root(self.container)
        self.doc.add_periodic_callback(self.refresh_data,
                                       self.server.refresh_seconds)

    @staticmethod
    def color(plot_schema, scope_name_index, index):
        """
        Assign a color to a point group based on Yaml color[formula] value

        Inputs:
          plot_schema: 
            the section of the schema yaml file for this plot
          scope_name_index: 
            a monotonic integer index assigned to (scope, name) pairs as they appear
            in the log file.  See server.Server::scope_name_index
          index:
            integer assigned to each data point.  See logger.DataLogger::write  
        """
        cdef = plot_schema.get('color', {})
        cdef.setdefault('palette', 'Viridis8')
        cdef.setdefault('num_colors', 10)
        cdef.setdefault('num_indices', 1)
        cdef.setdefault('num_groups', 1)
        cdef.setdefault('formula', 'name_index')

        if cdef['formula'] == 'name_index':
            palette_index = scope_name_index * cdef['num_indices'] + index
        elif cdef['formula'] == 'index_name':
            palette_index = index * cdef['min_groups'] + scope_name_index
        elif cdef['formula'] == 'name':
            palette_index = scope_name_index
        elif cdef['formula'] == 'index':
            palette_index = index

        pal = palettes.__dict__[cdef['palette']]
        pal = palettes.interp_palette(pal, cdef['num_colors'])
        palette_index = palette_index % cdef['num_colors']
        return pal[palette_index]

    def maybe_add_glyph(self, plot: Plot, group: pb.Group):
        # add a glyph for the group if it doesn't exist
        if len(plot.figure.select({"name": str(group.id)})) > 0:
            return

        cols = plot.schema['columns']
        cds = ColumnDataSource({c: [] for c in cols})

        scope_name_index = self.scope_name_index(plot.name, group)
        color = PageLayout.color(plot.schema, scope_name_index, group.index)
        label = f"{group.scope}-{group.name}-{group.index}"
        glyph_kind = plot.schema.get("glyph_kind")
        glyph_kwargs = plot.schema.get("glyph_kwargs", {})

        if glyph_kind == "line":
            plot.figure.line(*cols, source=cds, name=str(group.id), color=color,
                             legend_label=label, **glyph_kwargs)
        elif glyph_kind == "scatter":
            plot.figure.circle(*cols, name=str(group.id), source=cds, color=color,
                               legend_label=label, **glyph_kwargs) 
        else:
            raise RuntimeError(f"Unsupported glyph_kind: {glyph_kind}")


    def refresh_data(self):
        """Read a chunk of log file and incorporate it into the session."""

        session = self.session
        packed = session.fh.read(self.chunk_size)
        gen = util.unpack(packed)
        while True:
            try:
                item = next(gen)
                if isinstance(item, pb.Group):
                    if (session.scope_pat.fullmatch(item.scope)
                        and session.name_pat.fullmatch(item.name)):
                    session.groups[item] = []
                elif isinstance(item, pb.Point):
                    if item.group_id not in session.groups:
                        continue
                    session.groups[item.group_id].append(item)
                elif isinstance(item, pb.Action):
                    for group in list(session.groups.values()):
                        if group.scope == item.scope:
                            del session.groups[group.id]
            except StopIteration as exc:
                remain = exc.value
                if remain:
                    session.fh.seek(-remain)

            cds_map = {}
            for group in session.groups:
                cds_data = util.points_to_cds_data(group, session.groups[group])
                cds_map[group] = cds_data
                session.groups[group].clear()

        def update_cb():
            # remove unbacked renderers
            for plot in session.plots:
                for r in plot.figure.select(GlyphRenderer):
                    if r.name not in session.groups:
                        plot.figure.renderers.remove(r)

            # refresh plots by group
            for group, cds_data in cds_map:
                for plot in session.plots:
                    if not plot.name_pat.fullmatch(group.name):
                        continue
                    self.maybe_add_glyph(plot, group)
                    for glyph in plot.figure.select({"name": str(group.id)}):
                        glyph.data_source.stream(cds_data)

        self.plot.page.doc.add_next_tick_callback(update_cb)

