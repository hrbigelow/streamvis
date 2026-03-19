import numpy as np
import asyncio
import textwrap
from typing import Optional
from dataclasses import dataclass, field
from collections.abc import Iterable
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import colorsys
from matplotlib.lines import Line2D
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.widgets import Slider
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

    @property
    def is_range_filter(self):
        return self.lo is not None or self.hi is not None

    @property
    def is_value_filter(self):
        return len(self.vals) > 0

    def __post_init__(self):
        if self.name is not None and self.is_range_filter == self.is_value_filter:
            raise RuntimeError(f"must set either vals or (lo and/or hi)")

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

    fig_width: int = None
    fig_height: int = None

    dpi: int = None
    legend_at: str = None

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
    def field_to_axis(self):
        m = { 'x': self.x, 'y': self.y, 'c': self.c, 'g': self.g, 'o': self.o, 's': self.s }
        return { v: k for k, v in m.items() if v is not None }

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

def color2d(i: int, j: int, jmax: int):
    pass

def empty_dataframe(field_names: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=(STARTED_AT, RUN_HANDLE, *field_names))

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
        yield empty_dataframe(field_names) 

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



    for color_group, color_data in df.groupby(color_fnames):
        for glyph_group, glyph_data in color_data.groupby(group_fnames):
            color = get_color(color_group[2:], color_groups)
            label = str(glyph_group[2:])
            line, = ax.plot(
                glyph_data[x_fname], 
                glyph_data[y_fname], 
                label=label,
                color=color,
                linewidth=1)

    ax.set_xlabel(fname_desc[x_fname], fontsize=10)
    ax.set_ylabel(fname_desc[y_fname], fontsize=10)

    if len(group_fnames) > 0:
        ax.legend(
            title=textwrap.fill(', '.join(group_fnames[2:]), width=80),
            loc=opts.legend_at, 
            fontsize=8,
            title_fontsize=6
        )
    plt.tight_layout()
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
    info = rpc_client.get_data_columns(stub, opts.series, opts.field_names)
    field_map = {}
    for field in rpc_client.list_fields(stub):
        field_map[field.name] = field
    
    axes = []
    req = pb.QueryRunDataRequest()
    req.coord_handles.extend((c.coord_handle for c in info.coords))
    req.attr_handles.extend((a.handle for a in info.attrs))

    rf = req.run_filter
    tf = rf.tag_filter

    tf.tags.extend(opts.tags)
    tf.match_all = opts.match_all
    if opts.min_started_at is not None:
        rf.min_started_at = opts.min_started_at
    if opts.max_started_at is not None:
        rf.max_started_at = opts.max_started_at

    for filt in opts.filters:
        if filt.name is None:
            continue
        field = field_map.get(filt.name)
        if field is None:
            raise RuntimeError(
                f"Filter field name {name} is not a valid Field. "
                f"Use streamvis.v1.Service.ListFields RPC endpoint to see valid fields")

        af = pb.AttributeFilter(field_handle=field.handle,
                                include_missing=filt.inc_missing)
        match field.data_type:
            case pb.FieldDataType.FIELD_DATA_TYPE_INT:
                if filt.is_range_filter:
                    af.int_range.imin = filt.lo
                    af.int_range.imax = filt.hi
                else:
                    af.int_list.vals.extend(filt.vals)
            case pb.FieldDataType.FIELD_DATA_TYPE_FLOAT:
                if not filt.is_range_filter:
                    raise RuntimeError(
                        f"filter {filt.name} is a Float filter. "
                        f"only a range style is supported")
                af.float_range.vals.extend(filt.vals)
            case pb.FieldDataType.FIELD_DATA_TYPE_BOOL:
                if not filt.is_value_filter:
                    raise RuntimeError(
                        f"filter {filt.name} is a Bool filter. "
                        f"only a value style is supported")
                af.bool_list.vals.extend(filt.vals)
            case pb.FieldDataType.FIELD_DATA_TYPE_STRING:
                if not filt.is_value_filter:
                    raise RuntimeError(
                        f"filter {filt.name} is a String filter. "
                        f"only a value style is supported")
                af.string_list.vals.extend(filt.vals)

        rf.attribute_filters.append(af)


    fig, ax = plt.subplots(figsize=(opts.fig_width, opts.fig_height), dpi=opts.dpi)
    plt.ion()
    plt.show()

    runs = {}
    df = empty_dataframe(info.field_names) 

    while True:
        # filter out any stale data
        pbruns = stub.ListRuns(pb.ListRunsRequest(run_filter=req.run_filter))
        new_runs = { r.handle: r for r in pbruns }
        stale_handles = [h for h, r in runs.items() if h not in new_runs or
                         new_runs[h].started_at.seconds > r.started_at.seconds]
        df = df[~df[RUN_HANDLE].isin(stale_handles)]
        runs = new_runs
        
        new_dfs = to_dataframes(stub, req, info.field_names)
        req.begin_chunk_id = rpc_client.get_end_chunk_id(stub) 

        df = pd.concat([df, *new_dfs], ignore_index=True)
        df.sort_values(by=STARTED_AT, inplace=True)

        for cf in opts.cfilters:
            if cf.name is None:
                continue
            if cf.is_range_filter:
                df = df[df[cf.name].between(cf.lo, cf.hi)]
            else:
                df = df[df[cf.name].isin(cf.vals)]

        if all(field in df for field in opts.o):
            df.sort_values(by=opts.o, inplace=True)

        line_plot(fig, ax, df, opts.axis_to_fname, info.field_name_map, opts)
        await asyncio.sleep(5)


if __name__ == "__main__":
    main()
