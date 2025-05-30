import asyncio
import copy
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass
import threading
import numpy as np
import re
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, Legend, LegendItem
from bokeh.model.model import Model
from bokeh.plotting import figure
from bokeh.application.application import SessionContext
from bokeh import palettes
import grpc
from grpc import aio
from . import data_pb2 as pb
from . import data_pb2_grpc as pb_grpc
from . import util
from .base import BasePage

def parse_csv(param, arg, target_nelems):
    # returns a num_list representing the csv param
    if arg is None:
        return [1] * target_nelems
    try:
        num_list = [float(v) for v in arg.split(",")]
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


def parse_grid(known_plots, param, grid, plots, box_elems):
    # modifies plots and box_elems
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


def get_decode(args, param):
    vals = args.get(param)
    if vals is None:
        return None
    return tuple(v.decode() for v in vals)


class Plot:
    name: str
    schema: Dict
    figure: 'figure'
    width_frac: float
    height_frac: float
    group_name_pat: re.Pattern

    def __init__(
        self, 
        name: str, 
        plot_schema: Dict, 
        width_frac: float,
        height_frac: float,
        group_name_pat: re.Pattern
    ):
        self.name = name
        self.schema = plot_schema 
        self.width_frac = width_frac
        self.height_frac = height_frac
        self.group_name_pat = group_name_pat

    def color(self, index: int, num_colors: int):
        color_opts = self.schema.get("color", {})
        palette_name = color_opts.get("palette", "Viridis8")
        pal = palettes.__dict__[palette_name]
        pal = palettes.interp_palette(pal, num_colors+2)
        return pal[index+1]

    def label(self, scope: pb.Scope, name: pb.Name, index: int) -> str:
        """Compute a label for this group.  Will be used to index a palette."""
        color_opts = self.schema.get("color", {})
        sig = color_opts.get("key_fun", "sni")
        d = {"s": scope.scope, "n": name.name, "i": index}
        return "-".join(str(d[ch]) for ch in sig)

    def scale_to_pagesize(self, page_width: float, page_height: float):
        width, height = self.get_scaled_size(page_width, page_height)
        self.figure.width = width
        self.figure.height = height

    def get_scaled_size(self, page_width: float, page_height: float):
        width = int(self.width_frac * page_width)
        height = int(self.height_frac * page_height)
        return width, height


class Session:
    index: util.Index
    chan: grpc.aio._channel.Channel
    uri: str
    plots: List[Plot] 
    scope_filter: re.Pattern        # global pattern for all plots on the page
    name_filters: tuple[re.Pattern] # one pattern per plot

    def __init__(
        self, 
        grpc_uri: str, 
        scope_filter: re.Pattern, 
        name_filters: tuple[re.Pattern]
    ):
        self.index = util.Index.from_filters(scope_filter, name_filters)
        self.chan = grpc.insecure_channel(grpc_uri)
        self.plots = []
        self.scope_filter = scope_filter
        self.name_filters = name_filters

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
            name_id, index = self.split_glyph_id(r.name)
            name = self.index.names[name_id]
            scope = self.index.scopes[name.scope_id]
            label = plot.label(scope, name, index)
            labels.add(label)
        label_ord = {k: i for i, k in enumerate(sorted(labels))}
        for r in list(plot.figure.renderers):
            name_id, index = self.split_glyph_id(r.name)
            name = self.index.names[name_id]
            scope = self.index.scopes[name.scope_id]
            label = plot.label(scope, name, index)
            rs = label_to_rend_map.setdefault(label, [])
            rs.append(r)
        return label_ord, label_to_rend_map


class PageLayout(BasePage):
    """Represents a browser page."""
    def __init__(self, server):
        super().__init__(server)

    def _set_layout(self, box_elems, box_part, plot_part, args) -> dict[str, Any]:
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
        denom = sum(box_part)
        box_norm = []
        for part, nelem in zip(box_part, box_elems):
            box_norm.extend([part / denom] * nelem)

        args["box-norm"] = box_norm

        nbox = len(box_elems)
        cumul = [0] + [ sum(box_elems[:i+1]) for i in range(nbox) ]

        def _sl(lens, i):
            return slice(cumul[i], cumul[i+1])

        slices = [ plot_part[_sl(box_elems, i)] for i in range(nbox) ]
        plot_norm = [ v / sum(sl) for sl in slices for v in sl ]

        args["coords"] = []
        for box, sz in enumerate(box_elems):
            args["coords"].extend([(box, elem) for elem in range(sz)])

        if args["row-mode"]:
            args["widths"] = plot_norm
            args["heights"] = box_norm
        else:
            args["widths"] = box_norm
            args["heights"] = plot_norm

    def process_request(self, request) -> dict[str, Any]:
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
        out_args = {}
        args = request.arguments
        known_plots = self.server.schema.keys()

        scope_pats = get_decode(args, "scopes") 
        if scope_pats is None:
            scope_pats = (".*",)
        if len(scope_pats) != 1:
            raise RuntimeError(f"scopes argument must be provided exactly once")
        try:
            scope_pat = re.compile(scope_pats[0])
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
            out_args["row-mode"] = True
            parse_grid(known_plots, 'rows', rows, plots, box_elems)
            plot_part = parse_csv('width', width_arg, len(plots))
            box_part = parse_csv('height', height_arg, len(box_elems))
        else:
            out_args["row-mode"] = False
            parse_grid(known_plots, 'cols', cols, plots, box_elems)
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
            "lin":  dict(x_axis_type="linear", y_axis_type="linear"),
            "xlog": dict(x_axis_type="log",    y_axis_type="linear"),
            "ylog": dict(x_axis_type="linear", y_axis_type="log"),
            "xylog":dict(x_axis_type="log",    y_axis_type="log"),
        }
        try:
            name_pats = tuple(re.compile(np) for np in name_pats)
        except re.PatternError as ex:
            raise RuntimeError(
                    f"Error compiling one or more of names arguments: "
                    f"{name_pats}: {ex}")

        self.session = Session(self.server.grpc_uri, scope_pat, name_pats)

        self._set_layout(box_elems, box_part, plot_part, out_args)
        width_fracs = out_args["widths"]
        height_fracs = out_args["heights"]
        z = zip(plots, name_pats, axes, width_fracs, height_fracs)

        for plot_name, name_pat, mode, width_frac, height_frac in z:
            plot_schema = copy.deepcopy(self.server.schema.get(plot_name))
            if plot_schema is None:
                raise RuntimeError(
                    f"No name '{name}' found in global_schema. "
                    f"Available names: {', '.join(name for name in self.server.schema)}")

            args_update = axes_mode_to_kwargs.get(mode)
            figure_kwargs = plot_schema.setdefault("figure_kwargs", {})
            figure_kwargs.update(**args_update)
            plot = Plot(plot_name, plot_schema, width_frac, height_frac, name_pat) 
            self.session.plots.append(plot)

        print(f"process_request returning: {out_args}")
        return out_args 
        

    def set_pagesize(self, width, height):
        self.page_width = width
        self.page_height = height

    def build_page(self, ctx: SessionContext, page_width: int, page_height: int) -> Model:
        """Build actual page content after screen size is known."""
        row_mode = ctx.token_payload.get("row-mode")
        coords = ctx.token_payload.get("coords")
        model = column() if row_mode else row() 

        for index, plot in enumerate(self.session.plots):
            # print(f"{self.session.id}: {plot.name}")
            box_index, _ = coords[index]
            if box_index >= len(model.children):
                box = row() if row_mode else column()
                model.children.append(box)
            box = model.children[box_index]
            fig_kwargs = plot.schema.get('figure_kwargs', {})
            title_kwargs = fig_kwargs.get("title", {})
            xaxis_kwargs = fig_kwargs.get("xaxis", {})
            yaxis_kwargs = fig_kwargs.get("yaxis", {})
            top_kwargs = { k: v for k, v in fig_kwargs.items() 
                          if k not in ("title", "xaxis", "yaxis")}

            width, height = plot.get_scaled_size(page_width, page_height)
            fig = figure(name=plot.name, output_backend='webgl', 
                    width=width, height=height,
                    **top_kwargs)
            legend_kwargs = fig_kwargs.get("legend", {})
            legend = Legend(**legend_kwargs)
            fig.add_layout(legend)
            fig.title.update(**title_kwargs)
            fig.xaxis.update(**xaxis_kwargs)
            fig.yaxis.update(**yaxis_kwargs)
            box.children.append(fig)
            plot.figure = fig
            # print(f'in build, appended {fig=}, {fig.height=}, {fig.width=}, {fig.title=}')
        return model

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
        index = self.session.index
        pb_index = index.export()
        stub = pb_grpc.RecordServiceStub(self.session.chan)
        datas = []
        for record in stub.QueryRecords(pb_index):
            match record.type:
                case pb.INDEX:
                    index = util.Index.from_message(record.index)
                case pb.DATA:
                    datas.append(record.data)
        cds_map = util.data_to_cds(index, datas)
        self.session.index = index
        return cds_map

    def send_patch_cb(self, cds_map: dict[util.DataKey, 'cds'], fut):
        for plot in self.session.plots:
            for rend in list(plot.figure.renderers):
                name_id = int(rend.name.split(',')[0])
                if name_id not in self.session.index.names: 
                    plot.figure.renderers.remove(rend)

        for key, cds in cds_map.items():
            for plot in self.session.plots:
                if plot.group_name_pat.search(key.name):
                    glyph_id = f"{key.name_id},{key.index}" 
                    self.maybe_add_glyph(plot, glyph_id)
                    for rend in plot.figure.select({"name": glyph_id}):
                        rend.data_source.stream(cds)

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
