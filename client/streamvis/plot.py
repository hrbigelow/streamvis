from typing import Optional
from dataclasses import dataclass, field
import matplotlib.pyplot as plt
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
class PlotOpts:
    ty: PlotType 
    series: str
    x: str # x-axis field name
    y: str # y-axis field name
    c: Optional[str] = None # color field_name
    g: Optional[str] = None # group field_name
    o: Optional[str] = None # order field_name

    tags: list[str] = field(default_factory=list)
    match_all: bool = False

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
        s = set((self.x, self.y, self.c, self.g, self.o))
        s.discard(None)
        return list(s)

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
    df: pd.DataFrame,
    axis_fname: dict[str, str],
    fname_desc: dict[str, str],
) -> None:
    fig, ax = plt.subplots(figsize=(15,10))
    if len(df) == 0:
        print(f"No data to display")
        return

    x_fname = axis_fname['x']
    y_fname = axis_fname['y']
    group_fname = axis_fname.get('g')
    if group_fname is not None and group_fname in df:
        for group, data in df.groupby(group_fname):
            ax.plot(data[x_fname], data[y_fname], label=str(group))
    else:
        ax.plot(df[x_fname], df[y_fname])

    ax.set_xlabel(fname_desc[x_fname], fontsize=16)
    ax.set_ylabel(fname_desc[y_fname], fontsize=20)

    if group_fname is not None:
        ax.legend(title=group_fname, loc="upper right", 
                  fontsize=16,
                  title_fontsize=18,
                  markerscale=1.5,
        )
    plt.tight_layout()
    plt.show()


@hydra.main(config_path="./opts", config_name="plot", version_base="1.2")
def main(cfg: DictConfig):
    opts = instantiate(cfg)
    stub = rpc_client.get_service_stub()
    info = rpc_client.get_data_columns(stub, opts.series, opts.field_names)
    
    axes = []
    req = pb.QueryRunDataRequest()
    req.coord_handles.extend((c.coord_handle for c in info.coords))
    req.attr_handles.extend((a.handle for a in info.attrs))
    req.run_filter.tag_filter.tags.extend(opts.tags)
    req.run_filter.tag_filter.match_all = opts.match_all
    req.run_filter.min_started_at = opts.min_started_at
    req.run_filter.max_started_at = opts.max_started_at

    df = as_dataframe(stub, req, info.field_names)

    if opts.o in df:
        df.sort_values(by=opts.o, inplace=True)

    line_plot(df, opts.axis_to_fname, info.field_name_map)


if __name__ == "__main__":
    main()




