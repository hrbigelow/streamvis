import asyncio
from bokeh.application.handlers import Handler
from bokeh.application.application import SessionContext
from functools import partial
from abc import abstractmethod
from tornado.ioloop import IOLoop

class BasePage(Handler):
    def __init__(self, server):
        super().__init__()
        self.server = server

    async def on_session_created(self, session_context: SessionContext) -> None:
        self._task_group = asyncio.TaskGroup()
        await self._task_group.__aenter__()
        self._task_group.create_task(self._start(session_context))

    async def on_session_destroyed(self, session_context: SessionContext) -> None:
        await self._task_group.__aexit__(None, None, None)

    async def _start(self, ctx):
        # def patch(cds_map, doc):
            # done = asyncio.Future()
            # doc.add_next_tick_callback(lambda: self.send_patch_cb(cds_map, done))
            # print("in patch: returning done future...")
            # return done

        while True:
            doc = ctx._document
            try:
                cds_map = await self.refresh_data()
                # patch_fn = partial(patch, cds_map)
                # print(f"before ctx.with_locked...{len(cds_map)=}")
                done = asyncio.Future()
                doc.add_next_tick_callback(lambda: self.send_patch_cb(cds_map, done))
                await done
                # await ctx.with_locked_document(patch_fn)
                # print("after ctx.with_locked...")
                await asyncio.sleep(self.server.refresh_seconds)
            except asyncio.CancelledError:
                break
