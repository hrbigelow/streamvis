from bokeh.models import Div

class IndexPage:
    """An index page, providing links to each available plot."""
    def __init__(self, server, doc):
        self.server = server
        self.doc = doc
        self.session_id = doc.session_context.id
        self.doc.on_session_destroyed(self.destroy)

    def destroy(self, session_context):
        self.server.delete_page(self.session_id)

    def build_callback(self):
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

    def schedule_callback(self):
        self.doc.add_next_tick_callback(self.update)

    def update(self):
        # no-op because the schema doesn't change
        pass

