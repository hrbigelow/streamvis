import asyncio
from bokeh.application.handlers import Handler
from bokeh.application.application import SessionContext

class BasePage(Handler):
    def __init__(self, server):
        super().__init__()
        self.server = server

    async def on_session_created(self, session_context: SessionContext) -> None:
        self._task_group = tg = asyncio.TaskGroup()
        await self._task_group.__aenter__()
        self.task = tg.create_task(self._start(session_context))
        # print("on_session_created")

    async def on_session_destroyed(self, session_context: SessionContext) -> None:
        self.task.cancel()
        try:
            await self._task_group.__aexit__(None, None, None)
        except asyncio.CancelledError:
            pass
        # print("on_session_destroyed")

    async def _start(self, ctx):
        # Trying to call doc.add_next_tick_callback from ctx.with_locked_document
        # seems to cause deadlock.
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
            except asyncio.CancelledError:
                break
