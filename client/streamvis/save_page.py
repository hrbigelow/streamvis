import hydra
import asyncio
from omegaconf import DictConfig
from hydra.utils import instantiate
from dataclasses import dataclass

from bokeh.io import curdoc
from bokeh.embed import file_html
from bokeh.application.application import Document
from bokeh.model.model import Model
from bokeh.resources import INLINE

from .session import Session, Plot
from .script import fetch_with_patterns 

@dataclass
class SavePage:
    out_file: str
    grpc_uri: str
    scope_pattern: str
    name_pattern: str
    axis_mode: str         # lin, xlog, ylog, xylog
    key_fun: str         # s, n, i, sn, sni, etc
    window_size: int
    stride: int
    page_width: int
    page_height: int
    schema: dict

@dataclass
class FakeSessionContext:
    token_payload: dict
    _document: Document


@hydra.main(config_path="configs", config_name="save_page.yaml", version_base="1.2")
def main(cfg: DictConfig) -> None:
    asyncio.run(amain(cfg))

async def amain(cfg: DictConfig) -> None:
    opts: SavePage = instantiate(cfg)
    data = fetch_with_patterns(
        opts.grpc_uri, opts.scope_pattern, opts.name_pattern, opts.window_size, opts.stride, False
    )

    plot_type = next(iter(opts.schema.keys()))
    ctx = FakeSessionContext(
        token_payload={
            "plots": [plot_type],
            "axes-modes": [opts.axis_mode],
            "widths": [1],
            "heights": [1],
            "window": opts.window_size,
            "stride": opts.stride,
            "color_keys": [opts.key_fun],
            "row-mode": True,
            "coords": [(0, 0)],
        },
        _document=Document()
    )
    sess = Session(opts.schema, opts.grpc_uri, ctx, opts.scope_pattern, [opts.name_pattern], 1)
    plot = sess.plots[0]
    plot_model = plot.build(ctx, opts.page_width, opts.page_height)

    cds_map = await sess.refresh_data()
    done = asyncio.Future()
    sess.send_patch_cb(cds_map, done)
    await done

    html = file_html(plot_model, INLINE, "snapshot")
    with open(opts.out_file, "w") as fh:
        fh.write(html)


if __name__ == "__main__":
    main()


