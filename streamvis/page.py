import asyncio
from typing import Dict, List, Union, Tuple
from dataclasses import dataclass
import threading
import numpy as np
import re
import _io
from bokeh.layouts import column, row
from bokeh.document import without_document_lock
from bokeh.models.dom import HTML
from bokeh.models import ColumnDataSource, Legend
from bokeh.models.renderers.glyph_renderer import GlyphRenderer
from bokeh.plotting import figure
from bokeh import palettes
from . import data_pb2 as pb
from . import util
from .base import BasePage


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
        pat = self.schema.get("name_pattern")
        if pat is None:
            raise RuntimeError(
                f"Plot schema '{name}' didn't contain a 'name_pattern' field")
        self.name_pat = re.compile(pat)

    def color(self, index: int, num_colors: int):
        color_opts = self.schema.get("color", {})
        palette_name = color_opts.get("palette", "Viridis8")
        pal = palettes.__dict__[palette_name]
        pal = palettes.interp_palette(pal, num_colors)
        return pal[index]

    def key(self, group: pb.Group) -> Tuple[...]:
        """Compute a key for this group.  Will be used to index a palette."""
        color_opts = self.schema.get("color", {})
        sig = color_opts.get("key_fun", "sni")
        d = {"s": group.scope, "n": group.name, "i": group.index}
        return tuple(d[ch] for ch in sig)

class Session:
    id: int
    plots: List[Plot] 
    groups: Dict[int, pb.Group]
    points: Dict[int, List[pb.Points]]
    scope_pat: re.Pattern
    name_pat: re.Pattern
    fh: Union[_io._IOBase, 'GFile']

    def __init__(self, id: int, log_file: str):
        self.id = id
        self.plots = []
        self.groups = {}
        self.points = {}
        self.scope_pat = None 
        self.name_pat = None
        self.fh = util.get_log_handle(log_file, 'rb')

    def glyph_index_map(self, plot: Plot) -> Dict['renderer', int]:
        keys = set()
        gi_map = {}
        for r in list(plot.figure.renderers):
            group = self.groups[int(r.name)]
            key = plot.key(group)
            keys.add(key)
        key_ord = {k: i for i, k in enumerate(sorted(keys))}
        for r in list(plot.figure.renderers):
            group = self.groups[int(r.name)]
            key = plot.key(group)
            gi_map[r] = key_ord[key]
        return gi_map

class PageLayout(BasePage):
    """Represents a browser page."""
    def __init__(self, server, doc):
        super().__init__(server, doc)
        self.session = Session(doc.session_context.id, server.log_file)
        self.update_lock = threading.Lock()
        self.coords = None
        self.nbox = None
        self.box_elems = None
        self.widths = None
        self.heights = None

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
        names:  regex pattern of names to include
        rows:   semi-colon separated list of csv names lists
        cols:   semi-colon separated list of csv names lists
        
        width:  csv numbers list
        height: csv numbers list

        Exactly one of `rows` or `cols` must be given.  Both `width` and `height` are
        optional.

        scopes is optional.  if absent, defaults to '.+'
        names is optiona.  if absent, defaults to '.+'

        This function only accesses the server schema, not the data state
        """
        args = request.arguments
        known_plots = self.server.schema.keys()

        def maybe_get(args, param, default=None):
            val = args.pop(param, None)
            return default if val is None else val[0].decode()

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
        scope_pat = maybe_get(args, "scopes", ".+")
        try:
            self.session.scope_pat = re.compile(scope_pat)
        except re.PatternError as ex:
            raise RuntimeError(f"scopes argument '{scope_pat}' is not a valid regex")
        name_pat = maybe_get(args, "names", ".+")
        try:
            self.session.name_pat = re.compile(name_pat)
        except re.PatternError as ex:
            raise RuntimeError(f"names argument '{name_pat}' is not a valid regex")

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

    def build_page_cb(self, done: asyncio.Future):
        """Build the page for the first time.  

        Must be scheduled as next-tick callback
        """
        self.container = column() if self.row_mode else row() 

        for index, plot in enumerate(self.session.plots):
            # print(f"{self.session.id}: {plot.name}")
            box_index, _ = self.coords[index]
            if box_index >= len(self.container.children):
                box = row() if self.row_mode else column()
                self.container.children.append(box)
            box = self.container.children[box_index]
            fig_kwargs = plot.schema.get('figure_kwargs', {})
            title_kwargs = fig_kwargs.get("title", {})
            xaxis_kwargs = fig_kwargs.get("xaxis", {})
            yaxis_kwargs = fig_kwargs.get("yaxis", {})
            top_kwargs = { k: v for k, v in fig_kwargs.items() 
                          if k not in ("title", "xaxis", "yaxis")}
            # fig_kwargs.update(self.get_figsize(index))
            size_opts = self.get_figsize(index)
            fig = figure(name=plot.name, output_backend='webgl', **size_opts, **top_kwargs)
            if 'legend' in fig_kwargs:
                legend = Legend(**fig_kwargs['legend'])
                fig.add_layout(legend)
            fig.title.update(**title_kwargs)
            fig.xaxis.update(**xaxis_kwargs)
            fig.yaxis.update(**yaxis_kwargs)
            box.children.append(fig)
            plot.figure = fig
            # print(f'in build, appended {fig=}, {fig.height=}, {fig.width=}, {fig.title=}')
        self.doc.add_root(self.container)
        done.set_result(f"built {self.session.id}")

    def maybe_add_glyph(self, plot: Plot, group: pb.Group):
        # add a glyph for the group if it doesn't exist
        if len(plot.figure.select({"name": str(group.id)})) > 0:
            return
        all_groups = [self.session.groups[int(r.name)] for r in plot.figure.renderers]
        scope_name_index = len(set((g.scope, g.name) for g in all_groups))

        cols = plot.schema['columns']
        cds = ColumnDataSource({c: [] for c in cols})

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

    # @without_document_lock
    async def refresh_data(self):
        """Read a chunk of log file and incorporate it into the session."""
        session = self.session
        packed = session.fh.read(self.server.fetch_bytes)
        gen = util.unpack(packed)
        while True:
            try:
                item = next(gen)
                if isinstance(item, pb.Group):
                    if (session.scope_pat.fullmatch(item.scope)
                        and session.name_pat.fullmatch(item.name)
                        and any (plot.name_pat.fullmatch(item.name) for plot in session.plots)):
                        session.groups[item.id] = item
                        session.points[item.id] = []
                elif isinstance(item, pb.Points):
                    if item.group_id not in session.groups:
                        continue
                    session.points[item.group_id].append(item)
                elif isinstance(item, pb.Control):
                    if item.action == pb.Action.DELETE:
                        for group in list(session.groups.values()):
                            if group.scope == item.scope and group.name == item.name:
                                del session.groups[group.id]
                                del session.points[group.id]
                    else:
                        raise RuntimeError(f"Unknown action type: {pb.Action.Name(item.action)}")
                else:
                    raise RuntimeError(f"Got unknown protobuf item type: {type(item)}")

            except StopIteration as exc:
                remain = exc.value
                if remain:
                    session.fh.seek(-remain)
                break

        cds_map = {} # str(group_id) -> (group, cds_data)
        for group_id, points in session.points.items():
            group = session.groups[group_id]
            cds_data = util.points_to_cds_data(group, points)
            cds_map[str(group_id)] = (group, cds_data)
            session.points[group_id].clear()

        return cds_map

    def send_patch_cb(self, cds_map, fut):
        # remove unbacked renderers
        for plot in self.session.plots:
            for r in list(plot.figure.renderers):
                if r.name in cds_map:
                    continue
                if not self.doc.session_context or self.doc.session_context.destroyed:
                    break
                plot.figure.renderers.remove(r)

        # refresh plots by group
        for group, cds_data in cds_map.values():
            if not self.doc.session_context or self.doc.session_context.destroyed:
                break
            for plot in self.session.plots:
                if not plot.name_pat.fullmatch(group.name):
                    continue
                self.maybe_add_glyph(plot, group)
                for r in plot.figure.select({"name": str(group.id)}):
                    r.data_source.stream(cds_data)

        for plot in self.session.plots:
            gi_map = self.session.glyph_index_map(plot)
            num_colors = max(gi_map.values()) + 1
            for r, idx in gi_map.items():
                r.glyph.line_color = plot.color(idx, num_colors)

        fut.set_result(None)
