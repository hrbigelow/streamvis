from typing import Any
from dataclasses import dataclass, field
from functools import reduce
import asyncio
import math
import re
import copy
import grpc
from streamvis.v1 import data_pb2_grpc as pb_grpc
from streamvis.v1 import data_pb2 as pb
import numpy as np
from bokeh.models import ColumnDataSource, Legend, LegendItem, Slider, CategoricalSlider, Div
from bokeh.application.application import SessionContext, Document
from bokeh.model.model import Model
from bokeh.layouts import column, row
from bokeh.models.ranges import Range1d, DataRange1d
from bokeh.models import glyphs
from bokeh import palettes
from bokeh.plotting import figure
from . import util

EMPTY_VAL = "EMPTY"

@dataclass(frozen=True, order=True)
class GlyphKey:
    data_key: util.DataKey
    level_fields: tuple[str]
    level_values: tuple[Any]

    @property
    def id(self):
        levels = tuple(f"{f}:{v:5.3f}" for f, v in zip(self.level_fields, self.level_values))
        return ",".join(str(k) for k in (self.data_key.name_id, self.data_key.index, *levels))

    @staticmethod
    def split_id(glyph_id: str):
        name_id, index, *levels = glyph_id.split(",")
        return (int(name_id), int(index), *levels)


class Plot:
    name: str
    doc: Document 
    plot_schema: dict 
    model: Model  # the top-level container, either is figure or contains figure 
    figure: Model # the figure contained in this plot
    width_frac: float
    height_frac: float
    name_pat: re.Pattern 
    full_sources: dict[str, ColumnDataSource]
    plot_sources: dict[str, ColumnDataSource]
    filter_columns: tuple[str]
    ignore_columns: tuple[str]
    sliders: dict # filter_col => CategoricalSlider 
    slider_values: dict # filter_col => filter values
    margin: int # number of pixels of margin

    def __init__(
        self, 
        name: str, 
        doc: Document,
        plot_schema: dict, 
        axis_mode: str,
        width_frac: float,
        height_frac: float,
        name_pat: str,
        ignore_columns: tuple[str],
        color_key: str,
    ):
        axes_mode_to_kwargs = {
            "lin":  dict(x_axis_type="linear", y_axis_type="linear"),
            "xlog": dict(x_axis_type="log",    y_axis_type="linear"),
            "ylog": dict(x_axis_type="linear", y_axis_type="log"),
            "xylog":dict(x_axis_type="log",    y_axis_type="log"),
        }
        args_update = axes_mode_to_kwargs.get(axis_mode)
        plot_schema = copy.deepcopy(plot_schema)
        figure_kwargs = plot_schema.setdefault("figure_kwargs", {})
        figure_kwargs.update(**args_update)
        if color_key is not None:
            plot_schema["color"]["key_fun"] = color_key

        self.name = name
        self.doc = doc
        self.plot_schema = plot_schema 
        self.width_frac = width_frac
        self.height_frac = height_frac
        self.name_pat = re.compile(name_pat)
        self.full_columns = self.plot_schema['columns']
        self.filter_columns = tuple()
        self.ignore_columns = ignore_columns 
        self.sliders = {} 
        self.slider_values = {}
        self.full_sources = {}
        self.plot_sources = {}
        self.sync_plot_source_cb = None
        self.margin = 10

    @property
    def plot_columns(self):
        return tuple(c for c in self.full_columns if c not in self.filter_columns)

    def build(self, session_opts: dict, page_width: int, page_height: int) -> Model:
        """Build the figure and optional widgets.  session_opts is from ctx.token_payload."""
        fig_kwargs = self.plot_schema.get('figure_kwargs', {})
        top_kwargs = { k: v for k, v in fig_kwargs.items() 
                      if k not in ("title", "xaxis", "yaxis")}

        w, h = self.get_scaled_size(page_width, page_height)
        fig = figure(name=self.name, output_backend='webgl', width=w, height=h, **top_kwargs)
        legend = Legend(**fig_kwargs.get("legend", {}))
        fig.add_layout(legend)
        if "title" in fig_kwargs:
            fig_kwargs["title"]["text"] = session_opts["title"]
        if "xaxis" in fig_kwargs:
            fig_kwargs["xaxis"]["axis_label"] = session_opts["xaxis"]
        if "yaxis" in fig_kwargs:
            fig_kwargs["yaxis"]["axis_label"] = session_opts["yaxis"]

        fig.title.update(**fig_kwargs.get("title", {}))
        fig.xaxis.update(**fig_kwargs.get("xaxis", {}))
        fig.yaxis.update(**fig_kwargs.get("yaxis", {}))
        self.figure = fig

        self.filter_columns = session_opts["filter_columns"]
        self.ignore_columns = session_opts["ignore_columns"]

        controls = []
        for col in self.filter_columns:
            slider = Slider(start=0, end=1, step=1, value=0,
                            sizing_mode="stretch_width", height=30)
                    
            # slider = CategoricalSlider(
                    # value=EMPTY_VAL, categories=[EMPTY_VAL],
                    # height=30, 
                    # sizing_mode="stretch_width",
                    # )
            cb = lambda _, old, new, col=col: self.on_slider_change_cb(col, old, new)
            slider.on_change("value", cb)

            self.sliders[col] = slider
            self.slider_values[col] = tuple() 

            label = Div(
                    text=str(col),
                    styles={
                        "text-align": "center", 
                        "font-weight": "bold", 
                        "margin-bottom": "2px"
                        },
                    sizing_mode="stretch_width",
                    )
            controls.append(column(label, slider, sizing_mode="stretch_width"))
        sliders_row = row(*controls, sizing_mode="stretch_width", spacing=20)
        self.model = column(fig, sliders_row, sizing_mode="stretch_width")
        return self.model


    @staticmethod
    def get_name_id(glyph_id: str):
        name_id, *_ = GlyphKey.split_id(glyph_id)
        return name_id

    @staticmethod
    def to_label(label_key: tuple[Any]) -> str:
        return "-".join(str(v) for v in label_key)

    @property
    def is_filtered(self):
        return len(self.filter_columns) != 0

    def color(self, index: int, num_colors: int):
        color_opts = self.plot_schema.get("color", {})
        palette_name = color_opts.get("palette", "Viridis8")
        pal = palettes.__dict__[palette_name]
        pal = palettes.interp_palette(pal, num_colors)
        return pal[index]

    def label_key(self, scope: pb.Scope, name: pb.Name, index: int, levels: tuple[Any]) -> tuple[Any]:
        """Compute the label key, defining the ordering for labels."""
        color_opts = self.plot_schema.get("color", {})
        sig = color_opts.get("key_fun", "sni")
        d = {"s": scope.scope, "n": name.name, "i": index}
        return tuple(d[ch] for ch in sig) + levels

    def label(self, scope: pb.Scope, name: pb.Name, index: int, levels: tuple[Any]) -> str:
        """Compute a label for this group.  Will be used to index a palette."""
        key = self.label_key(scope, name, index, levels)
        return self.to_label(key)

    def scale_to_pagesize(self, page_width: float, page_height: float):
        width, height = self.get_scaled_size(page_width, page_height)
        self.figure.width = width
        self.figure.height = height

    def get_scaled_size(self, page_width: float, page_height: float):
        if self.is_filtered:
            one_col = self.filter_columns[0]
            page_height -= self.sliders[one_col].height + 20 

        width = int(self.width_frac * page_width) - (self.margin * 2)
        height = int(self.height_frac * page_height) - (self.margin * 2)
        return width, height

    def key_belongs(self, key: util.DataKey) -> bool:
        return self.name_pat.search(key.name)

    def add_data(self, key: util.DataKey, cds_data: dict[str, np.array]):
        """
        Split out cds_data according to the glyph_columns.
        """
        glyph_columns = tuple(c for c in sorted(cds_data.keys())
                              if c not in self.ignore_columns
                              and c not in self.filter_columns
                              and c not in self.plot_columns)

        if len(glyph_columns) == 0:
            glyph_key = GlyphKey(key, tuple(), tuple())
            self.add_glyph_data(glyph_key, cds_data)
            return

        block = np.stack(tuple(cds_data[c] for c in glyph_columns), axis=1)
        slices = np.unique(block, axis=0)
        for slc in slices:
            glyph_key = GlyphKey(key, glyph_columns, tuple(slc.tolist()))
            inds = np.all(block == slc, axis=1).nonzero()[0]
            sub_cds_data = {k: v[inds] for k, v in cds_data.items() if k not in glyph_columns}
            self.add_glyph_data(glyph_key, sub_cds_data)


    def add_glyph_data(self, glyph_key: GlyphKey, cds_data: dict[str, np.array]):
        """
        Adds data to the plot.
        Each distinct key maps to exactly one glyph.
        If the glyph exists, data is appended
        """
        # print(f"add_glyph_data: {glyph_key}")
        if any(col not in cds_data for col in self.plot_columns):
            raise RuntimeError(
                    f"Plot `{self.name}` takes columns {set(self.plot_columns)} "
                    f"but received {set(cds_data)}")

        if glyph_key.id not in self.full_sources:
            empty_data = {k: np.zeros_like(v, shape=(0,)) for k, v in cds_data.items()}
            self.full_sources[glyph_key] = cds = ColumnDataSource(empty_data)
            if self.is_filtered:
                empty_plot_data = {k: np.zeros_like(v, shape=(0,)) for k, v in
                        cds_data.items() if k in self.plot_columns}
                self.plot_sources[glyph_key] = ColumnDataSource(empty_plot_data)
            else:
                self.plot_sources[glyph_key] = cds

            glyph_kind = self.plot_schema.get("glyph_kind")
            glyph_kwargs = self.plot_schema.get("glyph_kwargs", {})
            plot_cds = self.plot_sources[glyph_key]
            color = "white"
            if glyph_kind == "line":
                self.figure.line(
                    *self.plot_columns, source=plot_cds, name=glyph_key.id, color=color, **glyph_kwargs)
            elif glyph_kind == "scatter":
                self.figure.circle(
                    *self.plot_columns, name=glyph_key.id, source=plot_cds, 
                    color=color, **glyph_kwargs)
            else:
                raise RuntimeError(f"Unsupported glyph_kind: {glyph_kind}")

        self.full_sources[glyph_key].stream(cds_data)

    @property
    def name_ids(self):
        return set(g.data_key.name_id for g in self.full_sources)

    def filter_values(self, filter_column: str) -> tuple[int]:
        # the set of values determined from
        if not self.is_filtered:
            return None
        vals = set()
        for src in self.full_sources.values():
            if filter_column not in src.data:
                continue
            vals.update(np.unique(src.data[filter_column]).tolist())
        return tuple(sorted(vals))
        
    def filter_value(self, filter_column: str):
        slider = self.sliders[filter_column]
        try:
            index = int(slider.value)
        except ValueError as ve:
            raise RuntimeError(f"filter_value {filter_column}: {ve}") from ve
        return self.filter_values(filter_column)[index]

    def sync_plot_to_data(self):
        # print("sync_plot_to_data")
        """Synchronize plot state to new data.""" 
        if not self.is_filtered:
            return
        self.sync_sliders()
        self.sync_plot_source(fix_ranges=False)

    def _sync_slider(self, filter_column: str):
        """Synchronize the slider state to the filter_values."""
        if not self.is_filtered:
            return

        slider = self.sliders[filter_column]
        new_categories = tuple(str(v) for v in self.filter_values(filter_column))
        if slider.end == len(new_categories):
            return

        at_max_val = (slider.value == slider.end)
        slider.end = max(len(new_categories) - 1, 1) # slider must be non-empty
        self.slider_values[filter_column] = new_categories 

        # print(f"Assigned slider {filter_column} num_categories: {len(new_categories)}") 
        if slider.end == 1:
            slider.value = 0 
        if at_max_val:
            slider.value = slider.end

        
    def _sync_slider_old(self, filter_column: str):
        """Synchronize the slider state to the filter_values."""
        if not self.is_filtered:
            return

        slider = self.sliders[filter_column]
        new_categories = [str(v) for v in self.filter_values(filter_column)]
        if len(range(slider.start, slider.end, slider.step)) == len(new_categories):
            return

        if slider.categories == new_categories:
            return

        at_max_val = (slider.value == slider.categories[-1])
        slider.categories = new_categories

        # print(f"Assigned slider {filter_column} slider.categories = {new_categories}") 
        if len(slider.categories) == 0:
            slider.categories = [EMPTY_VAL]
            slider.value = EMPTY_VAL
        if at_max_val:
            slider.value = slider.categories[-1]
        # print(f"sync_slider: {len(slider.categories)} categories, value={self.slider.value}")

    def sync_sliders(self):
        for filter_column in self.filter_columns:
            self._sync_slider(filter_column)


    def sync_plot_source(self, fix_ranges=False):
        """Updates plot_sources from full_sources.

        May be called either because the filter state changed or there was new data.
        If filter state changed, should be called with fix_ranges=True
        """
        # print("sync_plot_source")
        if not self.is_filtered:
            return


        for glyph_id, full_cds in self.full_sources.items():
            vals = tuple(full_cds.data[c] == self.filter_value(c) 
                         for c in self.filter_columns if c in full_cds.data)
            filter_vals = {c: self.filter_value(c) 
                           for c in self.filter_columns if c in full_cds.data}
            # print(f"sync_plot_source: filtering on settings: {filter_vals}")
            if len(vals) == 0:
                plot_vals = full_cds.data.copy()
            else:
                mask = reduce(np.logical_and, vals)
                plot_vals = {k: v[mask] for k, v in full_cds.data.items() if k not in self.filter_columns} 

            self.plot_sources[glyph_id].data = plot_vals 

        if fix_ranges:
            self._fix_ranges()
        else:
            self._unfix_ranges()
        self.sync_plot_source_cb = None
            

    def _fix_ranges(self):
        xr, yr = self.figure.x_range, self.figure.y_range
        if any(math.isnan(val) for val in (xr.start, xr.end, yr.start, yr.end)):
            return
        if isinstance(xr, DataRange1d):
            self.figure.x_range = Range1d(start=xr.start, end=xr.end)
        if isinstance(yr, DataRange1d):
            self.figure.y_range = Range1d(start=yr.start, end=yr.end)

    def _unfix_ranges(self):
        xr, yr = self.figure.x_range, self.figure.y_range
        if isinstance(xr, Range1d):
            self.figure.x_range = DataRange1d(start=xr.start, end=xr.end)
        if isinstance(yr, Range1d):
            self.figure.y_range = DataRange1d(start=yr.start, end=yr.end)


    def on_slider_change_cb(self, filter_column, old, new):
        slider = self.sliders.get(filter_column)
        if slider is None:
            print(f"No slider exists for {filter_column}")
            return

        # print(f"on_slider_change_cb: {filter_column} {old} {new}, {type(old)=} {type(new)=}")
        # slider.value = new
        # if self.sync_plot_source_cb is not None:
         #    self.doc.remove_next_tick_callback(self.sync_plot_source_cb)
        self.sync_plot_source_cb = self.doc.add_next_tick_callback(
            lambda: self.sync_plot_source(fix_ranges=True)
        )

    def remove_name_id(self, name_id: int):
        remove = tuple(g for g in self.full_sources if g.data_key.name_id == name_id)
        for g in remove:
            del self.full_sources[g]
            del self.plot_sources[g]
        for rend in list(self.figure.renderers):
            if rend.name in remove:
                self.figure.renderers.remove(rend)

@dataclass
class GrpcClientState:
    scopes: dict[int, pb.Scope] = field(default_factory=dict)
    names: dict[int, pb.Name] = field(default_factory=dict)
    file_offset: int = field(default_factory=int)
            

class Session:
    schema: dict
    chan: grpc.Channel
    stub: pb_grpc.ServiceStub 
    uri: str
    plots: list[Plot] 
    scope_filter: re.Pattern        # global pattern for all plots on the page
    name_filter: re.Pattern
    grpc_state: GrpcClientState

    def __init__(
        self, 
        schema: dict,
        grpc_uri: str, 
        session_context: SessionContext,
        scope_filter: str, 
        name_filters: tuple[str],
        refresh_seconds: float,
    ):
        self.schema = schema
        self.refresh_seconds = refresh_seconds
        self.chan = grpc.insecure_channel(grpc_uri)
        self.stub = pb_grpc.ServiceStub(self.chan)
        self.plots = []
        self.session_context = session_context
        self.scope_filter = scope_filter
        self.name_filter = "|".join(name_filters)
        self.grpc_state = GrpcClientState()

        req_args = session_context.token_payload
        plots = req_args["plots"]
        axes_modes = req_args["axes-modes"]
        width_fracs = req_args["widths"]
        height_fracs = req_args["heights"]
        self.window = req_args["window"] # whether to use window smoothing
        self.stride = req_args["stride"]
        color_keys = req_args["color_keys"]
        ignore_columns = req_args["ignore_columns"]

        # hack
        z = zip(plots, name_filters, axes_modes, width_fracs, height_fracs, color_keys)
        doc = self.session_context._document

        for plot_name, name_pat, axes_mode, width_frac, height_frac, color_key in z:
            plot_schema = self.schema.get(plot_name)
            default_schema = self.schema.get("DEFAULTS", {})
            util.fill_defaults(default_schema, plot_schema)

            if plot_schema is None:
                raise RuntimeError(
                    f"No name '{plot_name}' found in global_schema. "
                    f"Available names: {', '.join(name for name in self.schema)}")
            plot = Plot(plot_name, doc, plot_schema, axes_mode, width_frac,
                        height_frac, name_pat, ignore_columns, color_key) 
            self.plots.append(plot)


    @staticmethod
    def split_glyph_id(glyph_id):
        return tuple(int(f) for f in glyph_id.split(","))

    def glyph_index_map(
        self, plot: Plot
    ) -> tuple[list[str], dict[str, list['renderer']]]:
        """Return.

        label_ord: label -> order index
        label_to_rend_map: label -> list[renderer]
        """
        label_keys = set()
        label_to_rend_map = {}
        for r in list(plot.figure.renderers):
            # name_id, index, *levels = self.split_glyph_id(r.name)
            name_id, index, *levels = GlyphKey.split_id(r.name) 
            name = self.grpc_state.names[name_id]
            scope = self.grpc_state.scopes[name.scope_id]
            label_key = plot.label_key(scope, name, index, tuple(levels))
            label_keys.add(label_key)
        for r in list(plot.figure.renderers):
            # name_id, index, *levels = self.split_glyph_id(r.name)
            name_id, index, *levels = GlyphKey.split_id(r.name) 
            name = self.grpc_state.names[name_id]
            scope = self.grpc_state.scopes[name.scope_id]
            label = plot.label(scope, name, index, tuple(levels))
            rs = label_to_rend_map.setdefault(label, [])
            rs.append(r)

        label_ord = {plot.to_label(k): i for i, k in enumerate(sorted(label_keys))}
        return label_ord, label_to_rend_map

    # prepare a 
    def prepare_request(self) -> pb.DataRequest:
        sampling = None
        if self.window is not None and self.stride is not None:
            sampling = pb.Sampling(
                window_size=self.window,
                reduction=pb.Reduction.REDUCTION_MEAN,
                stride=self.stride
                )

        return pb.DataRequest(
            scope_pattern=self.scope_filter,
            name_pattern=self.name_filter,
            file_offset=self.grpc_state.file_offset,
            sampling=sampling
        )

    def process_response(self, result: pb.RecordResult):
        self.grpc_state.scopes = result.scopes
        self.grpc_state.names = result.names
        self.grpc_state.file_offset = result.file_offset

    async def refresh_data(self):
        req = self.prepare_request()
        record_result, cds_map = util.get_new_data(req, self.stub)
        self.process_response(record_result)
        return cds_map

    def send_patch_cb(self, cds_map: dict[util.DataKey, 'cds'], fut):
        if len(cds_map) == 0:
            fut.set_result(None)
            return

        # for key, cds in cds_map.items():
            # shapes = {k: v.size for k, v in cds.items()}
            # print(f"send_patch_cb: {key}: {shapes}")

        for plot in self.plots:
            for name_id in plot.name_ids:
                if name_id not in self.grpc_state.names: 
                    plot.remove_name_id(name_id)
            plot.sync_sliders()

        for plot in self.plots:
            plot_updated = False
            for key, cds in reversed(cds_map.items()):
                if plot.key_belongs(key):
                    plot.add_data(key, cds)
                    plot_updated = True
            if plot_updated:
                plot.sync_plot_to_data()

        for plot in self.plots:
            label_ord, label_to_rend_map = self.glyph_index_map(plot)
            num_colors = len(label_ord)
            for label, rs in label_to_rend_map.items():
                idx = label_ord[label]
                for r in rs:
                    if hasattr(r.glyph, "line_color"):
                        r.glyph.line_color = plot.color(idx, num_colors)
                    if hasattr(r.glyph, "fill_color"):
                        r.glyph.fill_color = plot.color(idx, num_colors)

            legend = plot.figure.legend[0]
            existing_label_ord = {item.label.value: item.index for item in legend.items}
            if label_ord == existing_label_ord:
                # all labels are identical, nothing to update
                continue
            legend.items.clear()
            for label, idx in sorted(label_ord.items(), key=lambda kv: kv[1]):
                rs = label_to_rend_map[label]
                legend_item = LegendItem(index=idx, label=label, renderers=rs)
                legend.items.append(legend_item)

        fut.set_result(None)

    async def __aenter__(self):
        self._task_group = tg = asyncio.TaskGroup()
        await tg.__aenter__()
        self.refresh_task = tg.create_task(self.refresh())

    async def __aexit__(self, *args):
        self.refresh_task.cancel()
        try:
            await self._task_group.__aexit__(*args)
        except asyncio.CancelledError:
            pass

    async def refresh(self):
        # Trying to call doc.add_next_tick_callback from ctx.with_locked_document
        # seems to cause deadlock.
        # await self.on_change_called.wait()
        while True:
            try:
                cds_map = await self.refresh_data()
                # print(f"in refresh: {cds_map=}")
                done = asyncio.Future()
                # This is necessary because session destruction happens *before*
                # on_session_destroyed callback is called
                if self.session_context.destroyed:
                    break
                doc = self.session_context._document
                doc.add_next_tick_callback(lambda: self.send_patch_cb(cds_map, done))
                await done
                # await ctx.with_locked_document(patch_fn)
                await asyncio.sleep(self.refresh_seconds)
            except asyncio.CancelledError:
                break

