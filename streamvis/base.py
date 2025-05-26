import asyncio
from functools import partial
from abc import ABC, abstractmethod
from tornado.ioloop import IOLoop

class GlyphUpdate:
    # used to delete any glyphs whose name contains meta_id
    deleted_meta_ids: set[int] = set()
    # one entry per plot.  inner dict key is glyph_id
    cds_data: tuple[dict[str, 'cds_data'], ...] = None 

class BasePage(ABC):
    def __init__(self, server, doc):
        self.server = server
        self.doc = doc
        self.doc.on_session_destroyed(self.destroy)

    def destroy(self, session_context):
        # print(f"Destroy page: {session_context.id}")
        del self.server.pages[session_context.id]
        IOLoop.current().add_callback(self._cleanup)

    async def _cleanup(self):
        # with self.server.session_lock:
        self.refresh_task.cancel() # will this even work?
        try:
            await self.refresh_task
        except asyncio.CancelledError:
            print(f"Page done.  Server now has {len(self.server.pages)} pages.")
            pass

    async def _start(self):
        # A short-lived coro since this delegates to Bokeh
        built = asyncio.Future()
        self.doc.add_next_tick_callback(partial(self.build_page_cb, built))
        result = await built
        # print(f"Result from build_page_cb: {result}")
        while self.doc.session_context and not self.doc.session_context.destroyed:
            try:
                cds_map = await self.refresh_data()
            except asyncio.CancelledError:
                pass
            except Exception as ex:
                print(f"got exception from refresh_data: {ex}.  finishing coro.")
                break
            try:
                patch_done = asyncio.Future()
                self.doc.add_next_tick_callback(
                    lambda: self.send_patch_cb(cds_map, patch_done))
                await patch_done
                await asyncio.sleep(self.server.refresh_seconds)
            except asyncio.CancelledError:
                pass
                # print("refresh data cancelled")
            except Exception as ex:
                print(f"got exception from send_patch_cb: {ex}.  finishing coro.")
                break

    def start(self):
        self.refresh_task = asyncio.create_task(self._start())

    @abstractmethod
    def build_page_cb(self, done: asyncio.Future):
        """A callback for building the page.

        Must be called as: 
        done: asyncio.Future
        doc.add_next_tick_callback(functools.partial(build_page_cb, done))
        You may await the future to coordinate with other coroutines
        """
        ...


