import numpy as np
import asyncio
import textwrap
import traceback
import dateparser
import mplcursors
import time
from typing import Optional, Any, Callable
from functools import reduce
from dataclasses import dataclass, field
from collections.abc import Iterable
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import colorsys
from matplotlib.lines import Line2D
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.widgets import Slider, RangeSlider
import pandas as pd
from enum import Enum
import hydra
from hydra.utils import instantiate
from hydra.core.config_store import ConfigStore
from omegaconf import DictConfig, OmegaConf
from .v1 import data_pb2 as pb
from .v1.data_pb2_grpc import ServiceStub
from . import rpc_client
from . import dbutil

class PlotType(Enum):
    SCATTER = 'scatter'
    LINE = 'line'

@dataclass
class AttrFilter:
    name: str|None = None
    inc_missing: bool = False
    lo: float|int|None = None
    hi: float|int|None = None
    vals: list[float|int|bool|str] = field(default_factory=list)

@dataclass
class ColumnFilter:
    name: str|None = None
    lo: float|int|None = None
    hi: float|int|None = None
    vals: list[float|int|bool|str] = field(default_factory=list)

    @property
    def is_range_filter(self):
        return self.lo is not None or self.hi is not None

    @property
    def is_value_filter(self):
        return len(self.vals) > 0


@dataclass
class PlotOpts:
    """
    Specifies how series data (a set of points of with the same structure) is to be
    plotted.  Some rules: 
    1. series (and series2) each get their own DataFrame
    2. both DataFrames are augmented with the run attributes
    3. all non-spatial axes (non x, y, y2) to bind the same columns for every DataFrame
    """
    ty: PlotType 

    fig_width: int
    fig_height: int
    dpi: int
    legend_at: str

    x: str # x-axis field name (must be present in series and series2 if provided)
    y: str # y-axis field name (must be present in series)
    y2: str | None # y-axis field name (bound to series2 if provided, otherwise bound to series)

    # fields to assign the main color group
    color_main: list[str] = field(default_factory=list) 

    # fields for the sub-color group (shades of the main color)
    color_sub: list[str] = field(default_factory=list)

    # field for determining the ordering of points within a glyph.
    # if None, defaults to the x field
    order: str = None

    label: list[str] = field(default_factory=list) # label field_name

    # additional fields besides color_main, color_sub, label for grouping
    # points into glyphs
    add_group: list[str] = field(default_factory=list) 

    slider: list[str] = field(default_factory=list) # slider field_name
    tooltip: list[str] = field(default_factory=list) # tooltip field_names

    tags: list[str] = field(default_factory=list)
    neg_tags: list[str] = field(default_factory=list)
    match_all: bool = False
    neg_match_all: bool = False

    # used for a window-averaged query
    use_win_query: bool = False
    win_size: int = None
    stride: int = None
    win_groups: list[str] = field(default_factory=list)

    f1: AttrFilter = None
    f2: AttrFilter = None
    f3: AttrFilter = None
    f4: AttrFilter = None
    f5: AttrFilter = None

    after: Optional[str] = None
    until: Optional[str] = None

    c1: ColumnFilter = None
    c2: ColumnFilter = None
    c3: ColumnFilter = None
    c4: ColumnFilter = None
    c5: ColumnFilter = None

    series_fields = set()

    def _normalize_prop(self, prop: str):
        propval = getattr(self, prop)
        if propval is None:
            return

        def maybe_convert(propval):
            if propval.startswith("s:"):
                self.series_fields.add(propval[2:])
                return propval[2:]
            return propval

        if isinstance(propval, str):
            propval = maybe_convert(propval)
            setattr(self, prop, propval)
        else:
            propval = [maybe_convert(pv) for pv in propval]
            setattr(self, prop, propval)

    def __post_init__(self):
        self.ty = PlotType(self.ty)
        if self.after is not None:
            self.after = dateparser.parse(self.after, settings={"RETURN_AS_TIMEZONE_AWARE": True})
        if self.until is not None:
            self.until = dateparser.parse(self.until, settings={"RETURN_AS_TIMEZONE_AWARE": True})

        if self.order is None:
            self.order = self.x

        for p in ("x", "y", "y2", "add_group", "order", "color_main", "color_sub",
                  "label", "slider", "tooltip"): 
            self._normalize_prop(p)



    @property
    def field_names(self):
        s = set((self.x, self.y, *self.color_main, *self.color_sub, 
                 *self.add_group, *self.label, self.order, *self.slider))
        s.discard(None)
        return list(s)

    @property
    def attr_fields(self):
        return tuple(af for af in self.field_names if af not in self.series_fields)

    @property
    def glyph_fields(self):
        return set((*self.add_group, *self.color_main, *self.color_sub, *self.label))

    @property
    def filters(self):
        s = (self.f1, self.f2, self.f3, self.f4, self.f5)
        return tuple(f for f in s if f is not None) 

    @property
    def cfilters(self):
        s = (self.c1, self.c2, self.c3, self.c4, self.c5)
        return tuple(c for c in s if c is not None)


class DataFrameWrapper:
    def __init__(self, df: pd.DataFrame, *group_cols: str):
        self.fields = group_cols 
        self.ncols = len(self.fields)

        match self.ncols:
            case 0: 
                self.df = df
            case 1: 
                self.df = df.groupby(group_cols[0], dropna=False)
            case default: 
                self.df = df.groupby(list(self.fields), dropna=False)

    def __iter__(self):
        if self.ncols == 0:
            yield tuple(), self.df
            return

        for key, frame in self.df:
            if isinstance(key, str):
                yield (key,), frame
            else:
                yield key, frame

    @property
    def groups(self) -> list[Any]:
        match self.ncols:
            case 0:
                return []
            case 1:
                return [(g,) for g in self.df.groups.keys()]
            case default:
                return tuple(self.df.groups.keys())

class CategoricalSlider:
    def __init__(self, ax: plt.Axes, label: str):
        self.ax = ax
        self.label = label
        self.w = None 
        self.values = None 
        self.callback = None

    def update(self, values: np.array):
        if np.array_equal(self.values, values):
            return

        self.ax.clear()
        self.values = values
        prev_val = 0 if self.w is None else self.w.val
        
        self.w = Slider(self.ax, self.label, valmin=0, valmax=values.size-1, valstep=1)
        self.w.set_val(np.clip(prev_val, self.w.valmin, self.w.valmax))

        if self.callback is not None:
            self.w.on_changed(self.callback)

    def on_changed(self, func: Callable):
        def func_wrap(val: float):
            sval = str(self.values[int(val)])
            self.w.valtext.set_text(sval)
            func(val)
        self.callback = func_wrap

    def contains(self, val: Any) -> bool:
        return self.catval == val

    @property
    def catval(self):
        if self.w is None:
            return None
        return self.values[int(self.w.val)]

class CategoricalRangeSlider:
    pass


STARTED_AT = '_run_started_at'
RUN_HANDLE = '_run_handle'

class PlotManager:
    def __init__(self, opts: PlotOpts):
        self.opts = opts
        self.data_req: pb.QueryRunDataRequest = None
        self.all_fields: dict[str, pb.Field] = None
        self.df: pd.DataFrame = None

        self.runs = {} # handle => pb.Run
        self.glyphs = {} # glyph_id => plt.Artist 
        self.filters = {} # name => plt.Widget 
        self.visibility_event = asyncio.Event()
        self._cursor = None

        self.legend_opts = dict(
            title=textwrap.fill(', '.join(opts.label), width=80), 
            loc=self.opts.legend_at, 
            fontsize=8, 
            title_fontsize=6)

    def new_dataframe(self, data=None):
        pbmap = { 
                   pb.FIELD_DATA_TYPE_INT: "Int64",
                   pb.FIELD_DATA_TYPE_FLOAT: "Float64",
                   pb.FIELD_DATA_TYPE_TEXT: "string",
                   pb.FIELD_DATA_TYPE_BOOL: "boolean" }

        series_types = { name: pbmap[f.data_type] for name, f in self.all_fields.items() }
        series_types[STARTED_AT] = "Int64"
        series_types[RUN_HANDLE] = "category"

        if data is None:
            data = {}

        row_count = 0
        for val in data.values():
            match val:
                case np.ndarray() | list():
                    row_count = len(val)
                    break
        master_index = pd.RangeIndex(row_count)

        cols = { 
                name: pd.Series(data=data.get(name), index=master_index, dtype=stype)
                for name, stype in series_types.items() }
        return pd.DataFrame(cols)

    def to_dataframes(
        self,
        stub: ServiceStub, 
    ) -> list[pd.DataFrame]:
        run_req = pb.ListRunsRequest(run_filter=self.data_req.run_filter)
        runs = { run.handle: run for run in stub.ListRuns(run_req) }
        frames = []

        for data in stub.QueryRunData(self.data_req):
            run = runs.get(data.run_handle)
            if run is None:
                raise RuntimeError(f"Couldn't get run metadata")

            arrays = dbutil.decode_runchunk(data)
            d = dict(zip(self.opts.series_fields, arrays))
            attrs = { n: rpc_client.get_oneof(run.attrs[n]) for n in self.opts.attr_fields }
            d[STARTED_AT] = run.started_at.seconds
            d[RUN_HANDLE] = run.handle
            d.update(attrs)
            df = self.new_dataframe(d)
            frames.append(df)

        return frames

    def prepare(self, stub: ServiceStub):
        o = self.opts
        req = rpc_client.get_query_run_data_request(
            stub, o.series_fields, o.tags, o.match_all, o.neg_tags, o.neg_match_all,
            o.after, o.until)

        for f in self.opts.filters:
            if f.name is None:
                continue
            af = rpc_client.get_attribute_filter(stub, f.name, f.lo, f.hi, f.vals, f.inc_missing)
            req.run_filter.attribute_filters.append(af)

        self.data_req = req
        self.all_fields = { f.name: f for f in rpc_client.list_fields(stub) }

        self.df = self.new_dataframe()

        xdesc = self.all_fields[self.opts.x].description
        ydesc = self.all_fields[self.opts.y].description

        plt.ion()
        self.fig, self.ax = plt.subplots(
            figsize=(self.opts.fig_width, self.opts.fig_height), 
            dpi=self.opts.dpi)

        self.ax.set_xlabel(xdesc, fontsize=10)
        self.ax.set_ylabel(ydesc, fontsize=10)

        for s in self.opts.slider:
            self.add_slider(s)

        # plt.tight_layout()
        self.fig.canvas.draw()
        self.fig.canvas.flush_events()

    def add_slider(self, field_name: str):
        slider_ax = self.fig.add_axes([0.15, 0.02, 0.7, 0.03])
        self.filters[field_name] = w = CategoricalSlider(slider_ax, field_name) 
        w.on_changed(self.on_slider_changed)

    async def refresh_data(self, stub: ServiceStub):
        # start_time = time.perf_counter()
        pbruns = rpc_client.list_runs(stub, self.data_req.run_filter)
        old_runs = self.runs
        self.runs = { r.handle: r for r in pbruns }
        stale_handles = [
            h for h, r in old_runs.items() 
            if h not in self.runs 
            or self.runs[h].started_at.seconds > r.started_at.seconds
        ]

        self.df = self.df[~self.df[RUN_HANDLE].isin(stale_handles)]
        new_dfs = self.to_dataframes(stub)
        pre_chunk_id = self.data_req.begin_chunk_id
        self.data_req.begin_chunk_id = cur_chunk_id = rpc_client.get_end_chunk_id(stub) 
        
        self.data_changed = (pre_chunk_id != cur_chunk_id or len(stale_handles) > 0)

        if len(new_dfs) > 0: 
            self.df = pd.concat([self.df, *new_dfs], ignore_index=True)
            self.df[RUN_HANDLE] = self.df[RUN_HANDLE].astype('category')
            # self.df.sort_values(by=STARTED_AT, inplace=True)

        for cf in self.opts.cfilters:
            if cf.name is None:
                continue
            if cf.is_range_filter:
                self.df = self.df[self.df[cf.name].between(cf.lo, cf.hi)]
            else:
                self.df = self.df[self.df[cf.name].isin(cf.vals)]

        self.df.sort_values(
            by=[STARTED_AT, *self.opts.glyph_fields, self.opts.order],
            ascending=True, inplace=True)

        self.group_df = DataFrameWrapper(self.df, RUN_HANDLE, *self.opts.glyph_fields)

        for field_name, w in self.filters.items():
            values = self.df[field_name].unique()
            values.sort()
            w.update(values)

        if self.data_changed:
            self._refresh_glyphs()

        # elapsed = time.perf_counter() - start_time
        # print(f"refresh_data took {elapsed:.4f} s")

    def _refresh_glyphs(self):
        # update
        color_df = DataFrameWrapper(self.df, *self.opts.color_main, *self.opts.color_sub)
        base_inds = tuple(self.group_df.fields.index(f) for f in self.opts.color_main)
        sub_inds = tuple(self.group_df.fields.index(f) for f in self.opts.color_sub)

        cgroups = {}
        nbase = len(self.opts.color_main)
        for cg in color_df.groups:
            base, sub = cg[:nbase], cg[nbase:]
            cgroups.setdefault(base, list()).append(sub)
        base_groups = list(sorted(cgroups.keys()))

        def get_glyph_color(glyph_id):
            base_gr = tuple(glyph_id[i] for i in base_inds)
            sub_gr = tuple(glyph_id[i] for i in sub_inds)
            base_idx = base_groups.index(base_gr)
            sub_idx = cgroups[base_gr].index(sub_gr)
            num_sub = len(cgroups[base_gr])
            return get_color(base_idx, sub_idx, num_sub)

        legend_label_inds = tuple(self.group_df.fields.index(l) for l in self.opts.label)
        tooltip_label_inds = tuple(self.group_df.fields.index(t) for t in self.opts.tooltip)
        legend_labels = {} # label => glyph 

        if self._cursor is not None:
            self._cursor.remove()

        for glyph_id, data in self.group_df:
            if glyph_id not in self.glyphs:
                color = get_glyph_color(glyph_id)
                tooltip_label = ' '.join(glyph_id[l] for l in tooltip_label_inds)
                self.glyphs[glyph_id], = self.ax.plot(
                    0, 0, label=str(tooltip_label), color=color, linewidth=1)
            self.glyphs[glyph_id].set_data(data[self.opts.x], data[self.opts.y])
            legend_label = tuple(glyph_id[l] for l in legend_label_inds)
            legend_labels[legend_label] = self.glyphs[glyph_id]

        self._cursor = mplcursors.cursor(self.ax, hover=True)
        
        def on_add(sel):
            label_text = sel.artist.get_label()
            sel.annotation.set_text(label_text)

        self._cursor.connect("add", on_add)

        if len(legend_labels) == 0:
            labels, glyphs = [], []
        else:
            def sort_with_na(kv):
                return tuple((isinstance(x, pd.api.typing.NAType), x) for x in kv[0])
                    
            labels, glyphs = list(zip(*sorted(legend_labels.items(), key=sort_with_na)))

        self.ax.legend(handles=glyphs, labels=labels, **self.legend_opts)
        self.ax.relim()
        self.ax.autoscale_view()
        self.fig.canvas.draw()


    def _update_visibility(self):
        # print(f"_update_visibility")
        all_groups = self.group_df.groups
        fields = self.group_df.fields
        field_map = { v: i for i, v in enumerate(fields) }

        for group, glyph in self.glyphs.items():
            show = True
            for filt in self.filters.values():
                val = group[field_map[filt.label]]
                show = show and filt.contains(val)
            glyph.set_visible(show)

        by_label = { g.get_label(): g for g in self.glyphs.values() if g.get_visibility() }
        labels, glyphs = list(zip(*by_label.items()))

        self.ax.legend(handles=glyphs, labels=labels, **self.legend_opts)
        self.ax.relim()
        self.ax.autoscale_view()
        self.fig.canvas.draw()


    async def refresh_glyph_visibility(self):
        while True:
            await self.visibility_event.wait()
            self.visibility_event.clear()
            self._update_visibility()

    async def refresh_task(self, stub: ServiceStub, refresh_every: int=5):
        while True:
            await self.refresh_data(stub)
            await asyncio.sleep(refresh_every)

    async def gui_loop(self):
        while True:
            # print(f"flush_events")
            self.fig.canvas.flush_events()
            await asyncio.sleep(0.1)

    def on_slider_changed(self, val: float):
        self.visibility_event.set()

    async def start(self, stub):
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self.gui_loop())
                tg.create_task(self.refresh_task(stub))
                tg.create_task(self.refresh_glyph_visibility())
        except ExceptionGroup as eg:
            traceback.print_exception(eg)

def get_color(base_idx: int, sub_idx: int, num_sub: int) -> str:
    colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
    base = colors[base_idx % len(colors)]
    if sub_idx is None:
        return base

    fac = np.linspace(0.75, 1.25, num_sub)
    col = colorsys.rgb_to_hls(*mcolors.to_rgb(base))
    return colorsys.hls_to_rgb(col[0], np.clip(fac[sub_idx] * col[1], 0, 1), col[2])


@hydra.main(config_path="./opts", config_name="plot", version_base="1.2")
def main(cfg: DictConfig):
    asyncio.run(amain(cfg))


async def amain(cfg: DictConfig):
    opts = instantiate(cfg, _convert_='all') # _convert_ needed for lists
    stub = rpc_client.get_service_stub()
    mgr = PlotManager(opts)
    mgr.prepare(stub)
    await mgr.start(stub) 

if __name__ == "__main__":
    main()
