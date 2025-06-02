import asyncio
from bokeh.application.handlers import Handler
from bokeh.application.application import SessionContext, Document
from bokeh.models import Div, ColumnDataSource
from bokeh.events import DocumentReady
from bokeh.models.callbacks import CustomJS
from .session import Session

class BasePage(Handler):
    def __init__(self, server):
        super().__init__()
        self.server = server

    async def on_session_created(self, ctx: SessionContext) -> None:
        scope_pat = ctx.token_payload.get("scope-pat")
        name_pats = ctx.token_payload.get("name-pats")
        ctx.custom_state = Session(
                self.server.schema, self.server.grpc_uri, ctx, scope_pat, name_pats,
                self.server.refresh_seconds)
        await ctx.custom_state.__aenter__()
        print(f"finished on_session_created, session id: {ctx.id}")

    async def on_session_destroyed(self, ctx: SessionContext) -> None:
        await ctx.custom_state.__aexit__(None, None, None)
        # print("on_session_destroyed")

    def modify_document(self, doc: Document):

        screen_size = ColumnDataSource(data=dict(width=[0], height=[0]))
        js_callback = CustomJS(args=dict(cds=screen_size), code="""
            function resizeHandler() {
                const width = window.innerWidth;
                const height = window.innerHeight;
                // this must be a full re-assignment to trigger a change
                cds.data = { width: [width], height: [height] };
                cds.change.emit();
                console.log(`window resized to: ${width}, ${height}`);
            }
            window.addEventListener('resize', resizeHandler);
            resizeHandler();
        """
        )
        session_state = doc.session_context.custom_state

        def on_change_fun(attr, old, new):
            print(f"in on_change_fun with {attr}, {old}, {new}")
            width = new["width"][0]
            height = new["height"][0]
            for plot in session_state.plots:
                plot.scale_to_pagesize(width, height)

        doc.add_root(screen_size)
        model = self.build_page(doc.session_context, 1000, 600)
        doc.add_root(model)
        doc.js_on_event(DocumentReady, js_callback)
        screen_size.on_change("data", on_change_fun)

