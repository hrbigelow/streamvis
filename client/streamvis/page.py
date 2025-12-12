import re
import json
from json.decoder import JSONDecodeError
from typing import Any
from bokeh.layouts import column, row
from bokeh.model.model import Model
from bokeh.application.application import SessionContext
import grpc
from grpc import aio
from . import util
from .base import BasePage
from .session import Session, SessionConfig, LazyStub

def parse_csv(param, arg, target_nelems):
    # returns a num_list representing the csv param
    if arg is None:
        return [1] * target_nelems
    try:
        num_list = [float(v) for v in arg.split(",")]
    except ValueError:
        raise RuntimeError(
            f'{param} value \'{arg}\' is not a valid csv list of numbers')
    if any(num <= 0 for num in num_list):
        raise RuntimeError(
            f'{param} value \'{arg}\' are not all positive numbers')
    if len(num_list) != target_nelems:
        raise RuntimeError(
            f'Received {len(num_list)} values but expected {target_nelems}. '
            f'Context: {param}={arg}')
    return num_list 


def parse_grid(known_plots, param, grid, plots, box_elems):
    # modifies plots and box_elems
    if grid == '':
        raise RuntimeError(f'Got empty {param} value') 
    blocks = grid.split(';')
    for block in blocks:
        items = block.split(',')
        for plot in items:
            if plot not in known_plots:
                raise RuntimeError(
                    f'In {param}={grid}, plot \'{plot}\' is not in the schema. '
                    f'Schema contains plots {", ".join(known_plots)}')
            plots.append(plot)
        box_elems.append(len(items))


def get_decode(args, param, default=None):
    vals = args.get(param)
    if vals is None:
        return default 
    return tuple(v.decode() for v in vals)

def unique_ordered(elems: list[Any]):
    out = []
    seen = set()
    for el in elems:
        if el in seen:
            continue
        out.append(el)
        seen.add(el)
    return tuple(out)



class PageLayout(BasePage):
    """Represents a browser page."""
    def __init__(self, grpc_uri, refresh_seconds: float):
        super().__init__()
        self.lazy_stub = LazyStub(grpc_uri) 
        self.refresh_seconds = refresh_seconds

    async def initialize_grpc(self):
        await self.lazy_stub.initialize()

    async def on_session_created(self, ctx: SessionContext) -> None:
        try:
            cfg = SessionConfig.model_validate(ctx.token_payload["page_config"])
        except ValidationError as e:
            raise RuntimeError(f"Couldn't validate request: {e}")
        ctx.custom_state = Session(self.lazy_stub, cfg, ctx, self.refresh_seconds)
        await ctx.custom_state.__aenter__()

    async def on_session_destroyed(self, ctx: SessionContext) -> None:
        await ctx.custom_state.__aexit__(None, None, None)

    def process_request(self, request) -> dict[str, Any]:
        try:
            json_str = request.arguments.get("query", "")[0]
            page_config = json.loads(json_str)
            return {"page_config": page_config}

        except JSONDecodeError as e:
            raise RuntimeError(f"Couldn't process request: {e}")

    def build_page(self, session: Session, page_width: int, page_height: int) -> Model:
        """Build actual page content after screen size is known."""
        return session.plot.build(page_width, page_height)

