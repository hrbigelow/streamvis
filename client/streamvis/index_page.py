import asyncio
from .base import BasePage
from . import util
from bokeh.models import Div
from bokeh.layouts import column, row

class IndexPage(BasePage):
    """An index page, providing links to each available plot."""
    def __init__(self, server):
        super().__init__(server)

    def modify_document(self, doc):
        """Must be scheduled as next tick callback."""
        self.container = row()
        text = '<h2>Streamvis Server Index Page</h2>'
        self.container.children.append(column([Div(text=text)]))
        inner = '<br>'.join(plot for plot in self.server.schema.keys())
        html = f'<p>{inner}</p>'
        self.container.children[0].children[0] = Div(text=html)
        doc.add_root(self.container)

    def send_patch_cb(self, cds_map: dict[util.DataKey, 'cds'], fut: asyncio.Future):
        fut.set_result(None)

    async def refresh_data(self):
        return None

