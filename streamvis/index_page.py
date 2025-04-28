from .base import BasePage
from bokeh.models import Div
from bokeh.layouts import column, row

class IndexPage(BasePage):
    """An index page, providing links to each available plot."""
    def __init__(self, server, doc):
        super().__init__(server, doc)
        self.session_id = doc.session_context.id

    def build_page_cb(self):
        """Must be scheduled as next tick callback."""
        self.container = row()
        text = '<h2>Streamvis Server Index Page</h2>'
        self.container.children.append(column([Div(text=text)]))
        inner = '<br>'.join(plot for plot in self.server.schema.keys())
        html = f'<p>{inner}</p>'
        self.container.children[0].children[0] = Div(text=html)
        self.doc.add_root(self.container)

        with self.server.page_lock.block():
            self.server.pages[self.session_id] = self

