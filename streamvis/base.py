import asyncio
from bokeh.application.handlers import Handler
from bokeh.application.application import SessionContext, Document
from bokeh.models import Div, ColumnDataSource
from bokeh.events import DocumentReady
from bokeh.models.callbacks import CustomJS
from abc import abstractmethod

class BasePage(Handler):
    def __init__(self, server):
        super().__init__()
        self.server = server
        self.screen_size = ColumnDataSource(data=dict(width=[0], height=[0]))

    async def on_session_created(self, session_context: SessionContext) -> None:
        self.on_change_called = asyncio.Event()
        self._task_group = tg = asyncio.TaskGroup()
        await self._task_group.__aenter__()
        self.task = tg.create_task(self._start(session_context))
        print("finished on_session_created")

    async def on_session_destroyed(self, session_context: SessionContext) -> None:
        self.task.cancel()
        try:
            await self._task_group.__aexit__(None, None, None)
        except asyncio.CancelledError:
            pass
        # print("on_session_destroyed")

    async def _start(self, ctx: SessionContext):
        # Trying to call doc.add_next_tick_callback from ctx.with_locked_document
        # seems to cause deadlock.
        await self.on_change_called.wait()
        while True:
            try:
                cds_map = await self.refresh_data()
                # patch_fn = partial(patch, cds_map)
                # print(f"before ctx.with_locked...{len(cds_map)=}")
                done = asyncio.Future()
                # This is necessary because session destruction happens *before*
                # on_session_destroyed callback is called
                if ctx.destroyed:
                    break
                doc = ctx._document
                doc.add_next_tick_callback(lambda: self.send_patch_cb(cds_map, done))
                await done
                # await ctx.with_locked_document(patch_fn)
                await asyncio.sleep(self.server.refresh_seconds)
                print(f"self.screen_size.data: {self.screen_size.data}")
            except asyncio.CancelledError:
                break

    def modify_document(self, doc: Document):

        js_callback = CustomJS(args=dict(cds=self.screen_size), code="""
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

        def on_change_fun(attr, old, new):
            print(f"in on_change_fun with {attr}, {old}, {new}")
            width = new["width"][0]
            height = new["height"][0]
            doc.clear()
            model = self.build_page(doc.session_context, width, height)
            doc.add_root(model)
            doc.add_root(self.screen_size) # to enable future resizes
            self.on_change_called.set()

        doc.add_root(self.screen_size)
        doc.js_on_event(DocumentReady, js_callback)
        self.screen_size.on_change("data", on_change_fun)

