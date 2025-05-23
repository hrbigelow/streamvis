import asyncio
import copy
from typing import Dict, List, Union, Tuple
from dataclasses import dataclass
import threading
import numpy as np
import re
import _io
from bokeh.layouts import column, row
from bokeh.document import without_document_lock
from bokeh.models.dom import HTML
from bokeh.models import ColumnDataSource, Legend, LegendItem
from bokeh.models.renderers.glyph_renderer import GlyphRenderer
from bokeh.plotting import figure
from bokeh import palettes
from . import data_pb2 as pb
from . import util
from .base import BasePage, GlyphUpdate


class Plot:
    name: str
    schema: Dict
    figure: 'figure'
    group_name_pat: re.Pattern

    def __init__(self, name: str, plot_schema: Dict, group_name_pat: re.Pattern):
        self.name = name
        self.schema = plot_schema 
        self.group_name_pat = group_name_pat


    def color(self, index: int, num_colors: int):
        color_opts = self.schema.get("color", {})
        palette_name = color_opts.get("palette", "Viridis8")
        pal = palettes.__dict__[palette_name]
        pal = palettes.interp_palette(pal, num_colors+2)
        return pal[index+1]

    def label(self, meta: pb.Metadata, index: int) -> str:
        """Compute a label for this group.  Will be used to index a palette."""
        color_opts = self.schema.get("color", {})
        sig = color_opts.get("key_fun", "sni")
        d = {"s": meta.scope, "n": meta.name, "i": index}
        return "-".join(str(d[ch]) for ch in sig)


class Session:
    id: int
    metadata: Dict[int, pb.Metadata]
    plots: List[Plot] 
    scope_pat: re.Pattern
    index_fh: Union[_io._IOBase, 'GFile']
    data_fh: Union[_io._IOBase, 'GFile']

    def __init__(self, id: int, log_file: str):
        self.id = id
        self.metadata = {}
        self.plots = []
        self.scope_pat = None 
        self.index_fh = util.get_log_handle(util.index_file(log_file), "rb")
        self.data_fh = util.get_log_handle(log_file, "rb")

    @staticmethod
    def split_glyph_id(glyph_id):
        return tuple(int(f) for f in glyph_id.split(","))

    def glyph_index_map(
        self, plot: Plot
    ) -> Tuple[List[str], Dict[str, List['renderer']]]:
        """Return.

        label_ord: label -> order index
        label_to_rend_map: label -> List[renderer]
        """
        labels = set()
        label_to_rend_map = {}
        for r in list(plot.figure.renderers):
            meta_id, index = self.split_glyph_id(r.name)
            meta = self.metadata[meta_id]
            label = plot.label(meta, index)
            labels.add(label)
        label_ord = {k: i for i, k in enumerate(sorted(labels))}
        for r in list(plot.figure.renderers):
            meta_id, index = self.split_glyph_id(r.name)
            meta = self.metadata[meta_id]
            label = plot.label(meta, index)
            rs = label_to_rend_map.setdefault(label, [])
            rs.append(r)
        return label_ord, label_to_rend_map


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
        rows:   semi-colon separated list of csv plot names lists
        cols:   semi-colon separated list of csv plot names lists
        names:  regex pattern of names to include (use one query param per plot name,
                in same order as the names in rows or cols is given)
        xlog:   if present, set x axis to log-scale
        ylog:   if present, set y axis to log-scale
        
        width:  csv numbers list
        height: csv numbers list

        Exactly one of `rows` or `cols` must be given.  Both `width` and `height` are
        optional.

        scopes is optional.  if absent, defaults to '.+'
        names is optional.  if absent, defaults to '.+'

        This function only accesses the server schema, not the data state
        """
        # import pdb
        # pdb.set_trace()
        args = request.arguments
        known_plots = self.server.schema.keys()

        def get_decode(args, param):
            vals = args.get(param)
            if vals is None:
                return None
            return tuple(v.decode() for v in vals)

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

        scope_pats = get_decode(args, "scopes") 
        if scope_pats is None:
            scope_pats = (".*",)
        if len(scope_pats) != 1:
            raise RuntimeError(f"scopes argument must be provided exactly once")
        try:
            scope_pat = scope_pats[0]
            self.session.scope_pat = re.compile(scope_pat)
        except re.PatternError as ex:
            raise RuntimeError(f"scopes argument '{scope_pat}' is not a valid regex")

        rows = get_decode(args, "rows")
        cols = get_decode(args, "cols")
        if rows is not None and cols is None and len(rows) == 1:
            rows = rows[0]
        elif cols is not None and rows is None and len(cols) == 1:
            cols = cols[0]
        else:
            raise RuntimeError(
                f"Either `rows` or `cols` query parameter must be given exactly once")

        name_pats = get_decode(args, "names")

        plots = [] 
        box_elems = [] # 
        box_part = []  # box stacking dimension proportion 
        plot_part = [] # plot packing dimension proportions

        width_arg = get_decode(args, 'width')
        height_arg = get_decode(args, 'height')

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
            
        if len(plots) != len(name_pats):
            raise RuntimeError(
                f"Must provide same number of names as plots in `cols` or `rows`.  "
                f"Received {len(plots)} plots and {len(name_pats)} names query parameters")

        axes_arg = get_decode(args, "axes")
        if axes_arg is None:
            axes = ("lin",) * len(plots) 
        elif len(axes_arg) == 1:
            axes = axes_arg[0].split(",")
        else:
            raise RuntimeError(f"`axes` query parameter must be provided at most once")
        if (len(axes) != len(plots) 
            or any(mode not in ("lin", "xlog", "ylog", "xylog") for mode in axes)):
            raise RuntimeError(
                f"`axes` must be comma-separated list of modes, one for each plot.  "
                f"Each mode should be one of 'lin', 'xlog', 'ylog', 'xylog'.  "
                f"Received {axes_arg=}")
        axes_mode_to_kwargs = {
            "lin": { "x_axis_type": "linear", "y_axis_type": "linear" },
            "xlog": { "x_axis_type": "log", "y_axis_type": "linear" },
            "ylog": { "x_axis_type": "linear", "y_axis_type": "log" },
            "xylog": { "x_axis_type": "log", "y_axis_type": "log" },
        }

        for plot_name, name_pat, mode in zip(plots, name_pats, axes):
            plot_schema = copy.deepcopy(self.server.schema.get(plot_name))
            if plot_schema is None:
                raise RuntimeError(
                    f"No name '{name}' found in global_schema. "
                    f"Available names: {', '.join(name for name in self.server.schema)}")
            try:
                group_name_pat = re.compile(name_pat)
            except re.PatternError as ex:
                raise RuntimeError(f"names argument '{name_pat}' is not a valid regex")

            args_update = axes_mode_to_kwargs.get(mode)
            figure_kwargs = plot_schema.setdefault("figure_kwargs", {})
            figure_kwargs.update(**args_update)
            self.session.plots.append(Plot(plot_name, plot_schema, group_name_pat))

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
            legend_kwargs = fig_kwargs.get("legend", {})
            legend = Legend(**legend_kwargs)
            fig.add_layout(legend)
            fig.title.update(**title_kwargs)
            fig.xaxis.update(**xaxis_kwargs)
            fig.yaxis.update(**yaxis_kwargs)
            box.children.append(fig)
            plot.figure = fig
            # print(f'in build, appended {fig=}, {fig.height=}, {fig.width=}, {fig.title=}')
        self.doc.add_root(self.container)
        done.set_result(f"built {self.session.id}")

    def maybe_add_glyph(self, plot: Plot, glyph_id: str):
        # add a glyph for the group if it doesn't exist
        if len(plot.figure.select({"name": glyph_id})) > 0:
            return

        cols = plot.schema['columns']
        cds = ColumnDataSource({c: [] for c in cols})
        glyph_kind = plot.schema.get("glyph_kind")
        glyph_kwargs = plot.schema.get("glyph_kwargs", {})

        color = "white"
        if glyph_kind == "line":
            plot.figure.line(*cols, source=cds, name=glyph_id, color=color, **glyph_kwargs)
        elif glyph_kind == "scatter":
            plot.figure.circle(*cols, name=glyph_id, source=cds, color=color, **glyph_kwargs) 
        else:
            raise RuntimeError(f"Unsupported glyph_kind: {glyph_kind}")

    async def refresh_data(self):
        index_pack = self.session.index_fh.read(self.server.fetch_bytes)
        index_gen = util.unpack(index_pack)
        grouped_entries = {} # meta_id => list[Entry]
        glyph_update = GlyphUpdate()
        glyph_update.cds_data = tuple({} for _ in range(len(self.session.plots)))

        def filter(scope, name):
            return (self.session.scope_pat.search(scope)
                    and any (p.group_name_pat.search(name) for p in self.session.plots))

        while True:
            try:
                index_item = next(index_gen)
                match index_item:
                    case pb.Metadata(meta_id=meta_id, scope=scope, name=name):
                        if filter(scope, name):
                            self.session.metadata[meta_id] = index_item
                    case pb.Entry(meta_id=meta_id):
                        if meta_id in self.session.metadata:
                            l = grouped_entries.setdefault(meta_id, [])
                            l.append(index_item)
                    case pb.Control(scope=scope, name=name):
                        if filter(scope, name):
                            meta_id = util.metadata_id(scope, name)
                            glyph_update.deleted_meta_ids.add(meta_id)
                            self.session.metadata.pop(meta_id, None)
                            grouped_entries.pop(meta_id, None)
            except StopIteration as exc:
                remain = exc.value
                if remain:
                    self.session.index_fh.seek(-remain)
                break
        
        entries = [ent for v in grouped_entries.values() for ent in v]
        cds_data = util.fetch_cds_data(self.session.data_fh, self.session.metadata, entries)
        
        # collate cds data into individual plots
        for plot_update, plot in zip(glyph_update.cds_data, self.session.plots):
            for key, data in cds_data.items():
                if plot.group_name_pat.search(key.name):
                    glyph_id = f"{key.meta_id},{key.index}"
                    plot_update[glyph_id] = data

        return glyph_update

    def send_patch_cb(self, glyph_update: GlyphUpdate, fut):
        # total_elems = sum(ary.size for ent in cds_map.values() for ary in ent[1].values())
        # print(f"send_patch_cb called with {len(cds_map)} groups, {total_elems} points")
        # remove unbacked renderers

        for plot in self.session.plots:
            for r in list(plot.figure.renderers):
                meta_id = int(r.name.split(',')[0])
                if meta_id in glyph_update.deleted_meta_ids:
                    plot.figure.renderers.remove(r)

        # refresh plots by group
        for cds_updates, plot in zip(glyph_update.cds_data, self.session.plots):
            for glyph_id, data in cds_updates.items():
                self.maybe_add_glyph(plot, glyph_id)
                for r in plot.figure.select({"name": glyph_id}):
                    r.data_source.stream(data)

        for plot in self.session.plots:
            label_ord, label_to_rend_map = self.session.glyph_index_map(plot)
            num_colors = len(label_ord)
            for label, rs in label_to_rend_map.items():
                idx = label_ord[label]
                for r in rs:
                    r.glyph.line_color = plot.color(idx, num_colors)

            legend = plot.figure.legend[0]
            existing_label_ord = {item.label.value: item.index for item in legend.items}
            if label_ord == existing_label_ord:
                continue
            legend.items.clear()
            for label, rs in label_to_rend_map.items():
                idx = label_ord[label]
                legend_item = LegendItem(index=idx, label=label, renderers=rs)
                legend.items.append(legend_item)

        fut.set_result(None)
