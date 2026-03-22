import numpy as np
import asyncio
import textwrap
import traceback
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
from datetime import datetime
import hydra
from hydra.utils import instantiate
from hydra.core.config_store import ConfigStore
from omegaconf import DictConfig, OmegaConf
from .v1 import data_pb2 as pb
from .v1.data_pb2_grpc import ServiceStub
from . import rpc_client
from . import dbutil


class DataFrameWrapper:
    def __init__(self, df: pd.DataFrame, *group_cols: str):
        self.fields = group_cols 
        self.ncols = len(self.fields)

        match self.ncols:
            case 0: 
                self.df = df
            case 1: 
                self.df = df.groupby(group_cols[0])
            case default: 
                self.df = df.groupby(list(self.fields))

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
                return (self.df.groups.keys())

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
    ty: PlotType 
    series: str

    fig_width: int
    fig_height: int
    dpi: int
    legend_at: str

    x: str # x-axis field name
    y: str # y-axis field name
    c: list[str] = field(default_factory=list) # color field_name
    g: list[str] = field(default_factory=list) # group field_name
    o: list[str] = field(default_factory=list) # order field_name
    s: list[str] = field(default_factory=list) # slider field_names

    tags: list[str] = field(default_factory=list)
    match_all: bool = False

    f1: AttrFilter = None
    f2: AttrFilter = None
    f3: AttrFilter = None
    f4: AttrFilter = None
    f5: AttrFilter = None

    min_started_at: Optional[str] = None
    max_started_at: Optional[str] = None

    c1: ColumnFilter = None
    c2: ColumnFilter = None
    c3: ColumnFilter = None
    c4: ColumnFilter = None
    c5: ColumnFilter = None

    def __post_init__(self):
        self.ty = PlotType(self.ty)
        if self.min_started_at is not None:
            self.min_started_at = datetime.fromisoformat(self.min_started_at)
        if self.max_started_at is not None:
            self.max_started_at = datetime.fromisoformat(self.max_started_at)

    @property
    def axis_to_fname(self):
        m = { 'x': self.x, 'y': self.y, 'c': self.c, 'g': self.g, 'o': self.o, 's': self.s }
        return { k: v for k, v in m.items() if v is not None }

    @property
    def field_names(self):
        s = set((self.x, self.y, *self.c, *self.g, *self.o, *self.s))
        s.discard(None)
        return list(s)

    @property
    def filters(self):
        s = (self.f1, self.f2, self.f3, self.f4, self.f5)
        return tuple(f for f in s if f is not None) 

    @property
    def cfilters(self):
        s = (self.c1, self.c2, self.c3, self.c4, self.c5)
        return tuple(c for c in s if c is not None)

STARTED_AT = '_run_started_at'
RUN_HANDLE = '_run_handle'

class PlotManager:
    def __init__(self, opts: PlotOpts):
        self.opts = opts
        self.data_req: pb.QueryRunDataRequest = None
        self.data_info: rpc_client.QueryRunInfo = None
        self.df: pd.DataFrame = None


        self.runs = {} # handle => pb.Run
        self.glyphs = {} # glyph_id => plt.Artist 
        self.filters = {} # name => plt.Widget 
        self.visibility_event = asyncio.Event()


    def prepare(self, stub: ServiceStub):
        o = self.opts
        req, info = rpc_client.get_query_run_data_request(
            stub, o.series, o.field_names, o.tags, o.match_all, o.min_started_at, o.max_started_at)

        for f in self.opts.filters:
            if f.name is None:
                continue
            af = rpc_client.get_attribute_filter(stub, f.name, f.lo, f.hi, f.vals, f.inc_missing)
            req.run_filter.attribute_filters.append(af)

        self.data_req = req
        self.data_info = info 
        self.df = pd.DataFrame(columns=(STARTED_AT, RUN_HANDLE, *self.data_info.field_names))
        self.df[RUN_HANDLE] = self.df[RUN_HANDLE].astype('category')

        xdesc = info.field_name_map[self.opts.x]
        ydesc = info.field_name_map[self.opts.y]

        plt.ion()
        self.fig, self.ax = plt.subplots(
            figsize=(self.opts.fig_width, self.opts.fig_height), 
            dpi=self.opts.dpi)

        self.ax.set_xlabel(xdesc, fontsize=10)
        self.ax.set_ylabel(ydesc, fontsize=10)

        for s in self.opts.s:
            self.add_slider(s)

        # plt.tight_layout()
        self.fig.canvas.draw()
        self.fig.canvas.flush_events()

    def add_slider(self, field_name: str):
        slider_ax = self.fig.add_axes([0.15, 0.02, 0.7, 0.03])
        self.filters[field_name] = w = CategoricalSlider(slider_ax, field_name) 
        w.on_changed(self.on_slider_changed)

    async def refresh_data(self, stub: ServiceStub):
        print("refresh_data")
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

        new_dfs = to_dataframes(stub, self.data_req, self.data_info.field_names)
        pre_chunk_id = self.data_req.begin_chunk_id
        self.data_req.begin_chunk_id = cur_chunk_id = rpc_client.get_end_chunk_id(stub) 
        
        self.data_changed = (pre_chunk_id != cur_chunk_id or len(stale_handles) > 0)

        if len(new_dfs) > 0: 
            self.df = pd.concat([self.df, *new_dfs], ignore_index=True)
            self.df[RUN_HANDLE] = self.df[RUN_HANDLE].astype('category')
            self.df.sort_values(by=STARTED_AT, inplace=True)

        for cf in self.opts.cfilters:
            if cf.name is None:
                continue
            if cf.is_range_filter:
                self.df = self.df[self.df[cf.name].between(cf.lo, cf.hi)]
            else:
                self.df = self.df[self.df[cf.name].isin(cf.vals)]

        if all(field in self.df for field in self.opts.o):
            self.df.sort_values(by=self.opts.o, inplace=True)

        self.group_df = DataFrameWrapper(self.df, RUN_HANDLE, *self.opts.g)

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
        print("_refresh_glyphs")
        plot_groups = list(self.glyphs.keys())
        color_df = DataFrameWrapper(self.df, *self.opts.c)
        color_inds = tuple(self.group_df.fields.index(f) for f in color_df.fields)

        for glyph_id, data in self.group_df:
            if glyph_id not in plot_groups:
                color_group = tuple(glyph_id[c] for c in color_inds) 
                color = get_color(color_group, color_df.groups)
                self.glyphs[glyph_id], = self.ax.plot(
                    0, 0,
                    label=str(glyph_id), 
                    color=color, 
                    linewidth=1)
            self.glyphs[glyph_id].set_data(data[self.opts.x], data[self.opts.y])

        self.ax.relim()
        self.ax.autoscale_view()
        self.fig.canvas.draw()


    def _update_visibility(self):
        print(f"_update_visibility")
        all_groups = self.group_df.groups
        fields = self.group_df.fields
        field_map = { v: i for i, v in enumerate(fields) }

        for group, glyph in self.glyphs.items():
            show = True
            for filt in self.filters.values():
                val = group[field_map[filt.label]]
                show = show and filt.contains(val)
            glyph.set_visible(show)

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

def to_dataframes(
    stub: ServiceStub, 
    req: pb.QueryRunDataRequest,
    field_names: list[str],
) -> list[pd.DataFrame]:
    run_req = pb.ListRunsRequest(run_filter=req.run_filter)
    runs = { run.handle: run for run in stub.ListRuns(run_req) }

    def _gen():
        for data in rpc_client.get_data(stub, req):
            run = runs.get(data.run_handle)
            if run is None:
                raise RuntimeError(f"Couldn't get run metadata")
            arrays = tuple(dbutil.decode_array_flat(enc) for enc in data.enc_vals)
            d = dict(zip(field_names, arrays))
            d[STARTED_AT] = run.started_at.seconds
            d[RUN_HANDLE] = run.handle
            df = pd.DataFrame(d)
            yield df

    return list(_gen())

def get_color(color_group: tuple, color_groups: list[tuple]) -> str:
    colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
    cols = tuple(tuple(sorted(set(c))) for c in zip(*color_groups))
    colmaps = tuple({v: i for i, v in enumerate(col)} for col in cols)
    cinds = tuple(cm[col] for cm, col in zip(colmaps, color_group)) 

    N = len(cinds)
    if N == 0:
        return None
    if N == 1:
        base = colors[cinds[0] % len(colors)]
        return base
    elif N == 2:
        mults = np.linspace(-0.3, 0.3, len(colmaps[1]))
        factor = 1.0 + mults[cinds[1]]
        base = colors[cinds[0] % len(colors)]
        c = base
        c = colorsys.rgb_to_hls(*mcolors.to_rgb(c))
        return colorsys.hls_to_rgb(c[0], max(0, min(1, factor * c[1])), c[2])


def line_plot(
    fig: Figure,
    ax: Axes,
    df: pd.DataFrame,
    axis_fname: dict[str, str],
    fname_desc: dict[str, str],
    opts: PlotOpts, 
) -> None:
    if len(df) == 0:
        print(f"No data to display")
        return
    ax.clear()

    x_fname = axis_fname['x']
    y_fname = axis_fname['y']
    group_fnames = [RUN_HANDLE, RUN_HANDLE] + axis_fname.get('g')
    color_fnames = [RUN_HANDLE, RUN_HANDLE] + axis_fname.get('c')

    color_df = df.groupby(color_fnames)
    color_groups = list(x[1:] for x in color_df.groups.keys())

    if len(group_fnames) > 0:
        ax.legend(
            title=textwrap.fill(', '.join(group_fnames[2:]), width=80),
            loc=opts.legend_at, 
            fontsize=8,
            title_fontsize=6
        )
    plt.tight_layout()
    plt.show()
    fig.canvas.draw()
    fig.canvas.flush_events()
    # plt.draw()
    # plt.pause(0.1)



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
