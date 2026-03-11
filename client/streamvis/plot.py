import asyncio
from typing import Optional
from dataclasses import dataclass, field
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.axes import Axes
from matplotlib.figure import Figure
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
class PlotOpts:
    ty: PlotType 
    series: str
    x: str # x-axis field name
    y: str # y-axis field name
    c: Optional[str] = None # color field_name
    g: list[str] = field(default_factory=list) # group field_name
    o: list[str] = field(default_factory=list) # order field_name

    tags: list[str] = field(default_factory=list)
    match_all: bool = False

    f1: AttrFilter = None
    f2: AttrFilter = None
    f3: AttrFilter = None
    f4: AttrFilter = None
    f5: AttrFilter = None

    min_started_at: Optional[str] = None
    max_started_at: Optional[str] = None

    def __post_init__(self):
        self.ty = PlotType(self.ty)
        if self.min_started_at is not None:
            self.min_started_at = datetime.fromisoformat(self.min_started_at)
        if self.max_started_at is not None:
            self.max_started_at = datetime.fromisoformat(self.max_started_at)

    @property
    def field_to_axis(self):
        m = { 'x': self.x, 'y': self.y, 'c': self.c, 'g': self.g, 'o': self.o }
        return { v: k for k, v in m.items() if v is not None }

    @property
    def axis_to_fname(self):
        m = { 'x': self.x, 'y': self.y, 'c': self.c, 'g': self.g, 'o': self.o }
        return { k: v for k, v in m.items() if v is not None }

    @property
    def field_names(self):
        s = set((self.x, self.y, self.c, *self.g, *self.o))
        s.discard(None)
        return list(s)

    @property
    def filters(self):
        s = (self.f1, self.f2, self.f3, self.f4, self.f5)
        return tuple(f for f in s if f is not None) 

def as_dataframe(
    stub: ServiceStub, 
    req: pb.QueryRunDataRequest,
    field_names: list[str],
) -> pd.DataFrame:
    def _gen():
        for data in rpc_client.get_data(stub, req):
            arrays = tuple(dbutil.decode_array(enc) for enc in data.enc_vals)
            df = pd.DataFrame(dict(zip(field_names, arrays)))
            yield df
        yield pd.DataFrame(columns=field_names) # sentinel
    return pd.concat(_gen())


def line_plot(
    fig: Figure,
    ax: Axes,
    df: pd.DataFrame,
    axis_fname: dict[str, str],
    fname_desc: dict[str, str],
) -> None:
    if len(df) == 0:
        print(f"No data to display")
        return
    ax.clear()

    x_fname = axis_fname['x']
    y_fname = axis_fname['y']
    group_fnames = axis_fname.get('g')
    if len(group_fnames) > 0:
        for group, data in df.groupby(group_fnames):
            ax.plot(data[x_fname], data[y_fname], label=str(group))
    else:
        ax.plot(df[x_fname], df[y_fname])

    ax.set_xlabel(fname_desc[x_fname], fontsize=16)
    ax.set_ylabel(fname_desc[y_fname], fontsize=20)

    if len(group_fnames) > 0:
        ax.legend(
            title=','.join(group_fnames), 
            loc="upper right", 
            fontsize=16,
            title_fontsize=18,
            markerscale=1.5,
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
                pass
            case pb.FieldDataType.FIELD_DATA_TYPE_STRING:
                pass

        rf.attribute_filters.append(af)


    fig, ax = plt.subplots(figsize=(15,10))
    plt.ion()
    plt.show()

    while True:
        df = as_dataframe(stub, req, info.field_names)

        if all(field in df for field in opts.o):
            df.sort_values(by=opts.o, inplace=True)

        line_plot(fig, ax, df, opts.axis_to_fname, info.field_name_map)
        await asyncio.sleep(5)


if __name__ == "__main__":
    main()
