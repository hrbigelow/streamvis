from typing import Optional
from dataclasses import dataclass, field
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

    def get_request(self, stub: ServiceStub) -> pb.QueryRunDataRequest:
        series_msg = None
        for s in rpc_client.list_series(stub):
            if s.name == self.series:
                series_msg = s
                break
        if series_msg is None:
            raise RuntimeError(f"Series {self.series} does not exist")

        all_fields = { f.name: f for f in rpc_client.list_fields(stub) }
        all_coords = { c.name: c for c in series_msg.coords }
        msg = pb.QueryRunDataRequest()
        for field_name in (self.x, self.y, self.c, self.g):
            if field_name is None:
                continue
            if field_name in all_coords:
                coord = all_coords[field_name]
                msg.coord_handles.append(coord.coord_handle)
            elif field_name in all_fields:
                field = all_fields[field_name]
                msg.attr_handles.append(field.handle)
            else:
                raise RuntimeError(f"field '{field}' not found in database")
        msg.run_filter.tag_filter.tags.extend(self.tags)
        msg.run_filter.tag_filter.match_all = self.match_all

        return msg


@hydra.main(config_path="./opts", config_name="plot", version_base="1.2")
def main(cfg: DictConfig):
    opts = instantiate(cfg)
    stub = rpc_client.get_service_stub()
    req = opts.get_request(stub)
    for data in rpc_client.get_data(stub, req):
        for enc in data.enc_vals:
            ary = dbutil.decode_numeric_array(enc)
            print(f"{data.run_handle}, {ary.dtype}:{ary.shape}")



if __name__ == "__main__":
    main()




