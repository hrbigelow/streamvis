from typing import Any
import asyncio
import math
import re
import copy
import grpc
from . import data_pb2_grpc as pb_grpc
from . import data_pb2 as pb
import numpy as np
from bokeh.models import ColumnDataSource, Legend, LegendItem, CategoricalSlider
from bokeh.application.application import SessionContext, Document
from bokeh.model.model import Model
from bokeh.layouts import column
from bokeh.models.ranges import Range1d, DataRange1d
from bokeh.models import glyphs
from bokeh import palettes
from bokeh.plotting import figure
from . import data_pb2 as pb
from . import util

EMPTY_VAL = "EMPTY"

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
    filter_column: str | None
    margin: int # number of pixels of margin

    def __init__(
        self, 
        name: str, 
        doc: Document,
        plot_schema: dict, 
        axis_mode: str,
        width_frac: float,
        height_frac: float,
        name_pat: str 
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

        self.name = name
        self.doc = doc
        self.plot_schema = plot_schema 
        self.width_frac = width_frac
        self.height_frac = height_frac
        self.name_pat = re.compile(name_pat)
        self.full_columns = self.plot_schema['columns']
        self.filter_column = None
        self.full_sources = {}
        self.plot_sources = {}
        self.sync_plot_source_cb = None
        self.margin = 10

    @property
    def plot_columns(self):
        return tuple(c for c in self.full_columns if c != self.filter_column)

    def build(self, session_opts: dict, page_width: int, page_height: int) -> Model:
        """Build the figure and optional widgets.  session_opts is from ctx.token_payload."""
        fig_kwargs = self.plot_schema.get('figure_kwargs', {})
        top_kwargs = { k: v for k, v in fig_kwargs.items() 
                      if k not in ("title", "xaxis", "yaxis")}

        w, h = self.get_scaled_size(page_width, page_height)
        fig = figure(name=self.name, output_backend='webgl', width=w, height=h, **top_kwargs)
        legend = Legend(**fig_kwargs.get("legend", {}))
        fig.add_layout(legend)
        fig.title.update(**fig_kwargs.get("title", {}))
        fig.xaxis.update(**fig_kwargs.get("xaxis", {}))
        fig.yaxis.update(**fig_kwargs.get("yaxis", {}))
        self.figure = fig

        filter_opts = self.plot_schema.get("filter_opts")
        if filter_opts is not None:
            self.filter_column = filter_opts.get("column")
            self.slider = CategoricalSlider(
                    value=EMPTY_VAL, categories=[EMPTY_VAL],
                    height=30, sizing_mode="stretch_width",
                    styles={"width": "50%", "margin": "0 auto"})
            self.slider.on_change("value", self.on_slider_change_cb)
            self.model = column(fig, self.slider)
        else:
            self.model = fig
        return self.model


    @staticmethod
    def make_glyph_id(name_id: int, index: int):
        return f"{name_id},{index}"

    @staticmethod
    def get_name_id(glyph_id: str):
        return int(glyph_id.split(",")[0])

    @staticmethod
    def to_label(label_key: tuple[Any]) -> str:
        return "-".join(str(v) for v in label_key)

    @property
    def is_filtered(self):
        return self.filter_column is not None

    def color(self, index: int, num_colors: int):
        color_opts = self.plot_schema.get("color", {})
        palette_name = color_opts.get("palette", "Viridis8")
        pal = palettes.__dict__[palette_name]
        pal = palettes.interp_palette(pal, num_colors)
        return pal[index]

    def label_key(self, scope: pb.Scope, name: pb.Name, index: int) -> tuple[Any]:
        """Compute the label key, defining the ordering for labels."""
        color_opts = self.plot_schema.get("color", {})
        sig = color_opts.get("key_fun", "sni")
        d = {"s": scope.scope, "n": name.name, "i": index}
        return tuple(d[ch] for ch in sig)

    def label(self, scope: pb.Scope, name: pb.Name, index: int) -> str:
        """Compute a label for this group.  Will be used to index a palette."""
        key = self.label_key(scope, name, index)
        return self.to_label(key)

    def scale_to_pagesize(self, page_width: float, page_height: float):
        width, height = self.get_scaled_size(page_width, page_height)
        self.figure.width = width
        self.figure.height = height

    def get_scaled_size(self, page_width: float, page_height: float):
        if self.is_filtered:
            page_height -= (self.slider.height + 20)

        width = int(self.width_frac * page_width) - (self.margin * 2)
        height = int(self.height_frac * page_height) - (self.margin * 2)
        return width, height

    def key_belongs(self, key: util.DataKey) -> bool:
        return self.name_pat.search(key.name)

    def add_data(self, key: util.DataKey, cds_data: dict[str, np.array]):
        """Adds data to the plot."""
        if set(cds_data) != set(self.full_columns):
            raise RuntimeError(
                    f"Plot `{self.name}` takes columns {set(self.full_columns)} "
                    f"but received {set(cds_data)}")
        glyph_id = self.make_glyph_id(key.name_id, key.index)
        if glyph_id not in self.full_sources:
            empty_data = {k: np.zeros_like(v, shape=(0,)) for k, v in cds_data.items()}
            self.full_sources[glyph_id] = cds = ColumnDataSource(empty_data)
            if self.is_filtered:
                empty_plot_data = {k: np.zeros_like(v, shape=(0,)) for k, v in
                        cds_data.items() if k in self.plot_columns}
                self.plot_sources[glyph_id] = ColumnDataSource(empty_plot_data)
            else:
                self.plot_sources[glyph_id] = cds

            glyph_kind = self.plot_schema.get("glyph_kind")
            glyph_kwargs = self.plot_schema.get("glyph_kwargs", {})
            plot_cds = self.plot_sources[glyph_id]
            color = "white"
            if glyph_kind == "line":
                self.figure.line(
                    *self.plot_columns, source=plot_cds, name=glyph_id, color=color, **glyph_kwargs)
            elif glyph_kind == "scatter":
                self.figure.circle(
                    *self.plot_columns, name=glyph_id, source=plot_cds, 
                    color=color, **glyph_kwargs)
            else:
                raise RuntimeError(f"Unsupported glyph_kind: {glyph_kind}")

        cds = self.full_sources[glyph_id]
        cds.stream(cds_data)

    @property
    def name_ids(self):
        return set(self.get_name_id(g) for g in self.full_sources)

    @property
    def filter_values(self) -> tuple[int]:
        # the set of values determined from
        if not self.is_filtered:
            return None
        vals = set()
        for src in self.full_sources.values():
            vals.update(np.unique(src.data[self.filter_column]))
        return tuple(sorted(vals))
        
    @property
    def filter_value(self):
        index = self.slider.categories.index(self.slider.value)
        return self.filter_values[index]

    def sync_plot_to_data(self):
        # print("sync_plot_to_data")
        """Synchronize plot state to new data.""" 
        if not self.is_filtered:
            return
        self.sync_slider()
        self.sync_plot_source(fix_ranges=False)
        
    def sync_slider(self):
        if not self.is_filtered:
            return

        """Synchronize the slider state to the filter_values."""
        new_categories = [str(v) for v in self.filter_values]
        if self.slider.categories == new_categories:
            return

        at_max_val = (self.slider.value == self.slider.categories[-1])
        self.slider.categories = new_categories
        if len(self.slider.categories) == 0:
            self.slider.categories.append(EMPTY_VAL)
            self.slider.value = EMPTY_VAL
        if at_max_val:
            self.slider.value = self.slider.categories[-1]
        # print(f"sync_slider: {len(self.slider.categories)} categories, value={self.slider.value}")


    def sync_plot_source(self, fix_ranges=False):
        """Updates plot_sources from full_sources.

        May be called either because the filter state changed or there was new data.
        If filter state changed, should be called with fix_ranges=True
        """
        # print("sync_plot_source")
        if not self.is_filtered:
            return

        for glyph_id, full_cds in self.full_sources.items():
            all_data = full_cds.data
            plot_cds = self.plot_sources[glyph_id]
            mask = (all_data[self.filter_column] == self.filter_value)
            plot_vals = {k: v[mask] for k, v in all_data.items() if k != self.filter_column} 
            plot_cds.data = plot_vals 

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


    def on_slider_change_cb(self, attr, old, new):
        self.slider.value = new
        if self.sync_plot_source_cb is not None:
            self.doc.remove_next_tick_callback(self.sync_plot_source_cb)
        self.sync_plot_source_cb = self.doc.add_next_tick_callback(
            lambda: self.sync_plot_source(fix_ranges=True)
        )

    def on_slider_change_cb_old(self, attr, old, new):
        self.slider.value = new
        self.sync_plot_source(fix_ranges=True)

    def remove_name_id(self, name_id: int):
        remove = tuple(g for g in self.full_sources if self.get_name_id(g) == name_id)
        for g in remove:
            del self.full_sources[g]
            del self.plot_sources[g]
        for rend in list(self.figure.renderers):
            if rend.name in remove:
                self.figure.renderers.remove(rend)
            

class Session:
    index: util.Index
    schema: dict
    chan: grpc.Channel
    stub: pb_grpc.RecordServiceStub 
    uri: str
    plots: list[Plot] 
    scope_filter: re.Pattern        # global pattern for all plots on the page

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
        self.index = util.Index.from_filters(scope_filter, name_filters)
        self.chan = grpc.insecure_channel(grpc_uri)
        self.stub = pb_grpc.RecordServiceStub(self.chan)
        self.plots = []
        self.session_context = session_context
        self.scope_filter = re.compile(scope_filter)

        req_args = session_context.token_payload
        plots = req_args["plots"]
        axes_modes = req_args["axes-modes"]
        width_fracs = req_args["widths"]
        height_fracs = req_args["heights"]

        z = zip(plots, name_filters, axes_modes, width_fracs, height_fracs)
        doc = self.session_context._document

        for plot_name, name_pat, axes_mode, width_frac, height_frac in z:
            plot_schema = self.schema.get(plot_name)
            default_schema = self.schema.get("DEFAULTS", {})
            util.fill_defaults(default_schema, plot_schema)

            if plot_schema is None:
                raise RuntimeError(
                    f"No name '{plot_name}' found in global_schema. "
                    f"Available names: {', '.join(name for name in self.schema)}")
            plot = Plot(plot_name, doc, plot_schema, axes_mode, width_frac, height_frac, name_pat) 
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
            name_id, index = self.split_glyph_id(r.name)
            name = self.index.names[name_id]
            scope = self.index.scopes[name.scope_id]
            label_key = plot.label_key(scope, name, index)
            label_keys.add(label_key)
        for r in list(plot.figure.renderers):
            name_id, index = self.split_glyph_id(r.name)
            name = self.index.names[name_id]
            scope = self.index.scopes[name.scope_id]
            label = plot.label(scope, name, index)
            rs = label_to_rend_map.setdefault(label, [])
            rs.append(r)

        label_ord = {plot.to_label(k): i for i, k in enumerate(sorted(label_keys))}
        return label_ord, label_to_rend_map

    async def refresh_data(self):
        self.index, cds_map = util.get_new_data(self.index, self.stub)
        return cds_map

    def send_patch_cb(self, cds_map: dict[util.DataKey, 'cds'], fut):
        if len(cds_map) == 0:
            fut.set_result(None)
            return

        for plot in self.plots:
            for name_id in plot.name_ids:
                if name_id not in self.index.names: 
                    plot.remove_name_id(name_id)
            plot.sync_slider()

        for plot in self.plots:
            plot_updated = False
            for key, cds in cds_map.items():
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

