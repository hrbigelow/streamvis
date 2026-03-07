from typing import Optional
from dataclasses import dataclass, field
import matplotlib.pyplot as plt
import pandas as pd
from enum import Enum
import datetime
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
        m = { 'x': self.x, 'y': self.y, 'c': self.c, 'g': self.g }
        return { v: k for k, v in m.items() if v is not None }


def as_dataframe(
    stub: ServiceStub, 
    req: pb.QueryRunDataRequest,
    axis_names: list[str],
) -> pd.DataFrame:
    def _gen():
        for data in rpc_client.get_data(stub, req):
            arrays = tuple(dbutil.decode_array(enc) for enc in data.enc_vals)
            df = pd.DataFrame(dict(zip(axis_names, arrays)))
            yield df
    return pd.concat(_gen())


def line_plot(
    df: pd.DataFrame,
    axis_label_map: dict[str, str],
) -> None:
    fig, ax = plt.subplots()
    if 'g' in df:
        for group, data in df.groupby('g'):
            ax.plot(data['x'], data['y'], label=str(group))
    else:
        ax.plot(df['x'], df['y'])

    ax.set_xlabel(axis_label_map['x'])
    ax.set_ylabel(axis_label_map['y'])

    plt.show()


@hydra.main(config_path="./opts", config_name="plot", version_base="1.2")
def main(cfg: DictConfig):
    opts = instantiate(cfg)
    stub = rpc_client.get_service_stub()
    info = rpc_client.get_data_columns(stub, opts.series, opts.field_to_axis.keys())
    
    axes = []
    req = pb.QueryRunDataRequest()
    req.coord_handles.extend((c.coord_handle for c in info.coords))
    req.attr_handles.extend((a.handle for a in info.attrs))
    req.run_filter.tag_filter.tags.extend(opts.tags)
    req.run_filter.tag_filter.match_all = opts.match_all

    axis_order = [opts.field_to_axis[fname] for fname in info.names]

    df = as_dataframe(stub, req, axis_order)
    if 'o' in df:
        df.sort_values(by='o', inplace=True)

    axis_label_map = { axis: info.name_map[fname] for fname, axis in opts.field_to_axis.items() }
    line_plot(df, axis_label_map)


if __name__ == "__main__":
    main()




