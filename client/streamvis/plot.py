from typing import Optional
from dataclasses import dataclass, field
from enum import Enum
import hydra
from hydra.utils import instantiate
from hydra.core.config_store import ConfigStore
from omegaconf import DictConfig, OmegaConf
from streamvis.v1 import data_pb2 as pb

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

	def get_request(self) -> pb.QueryRunDataRequest:
		pass

# cs = ConfigStore.instance()
# cs.store(name="config", node=PlotOpts)

@hydra.main(config_path="./opts", config_name="plot", version_base="1.2")
def main(cfg: DictConfig):
	opts = instantiate(cfg)



if __name__ == "__main__":
	main()



	
