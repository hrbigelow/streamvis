import asyncio
from functools import partial
from abc import ABC, abstractmethod

class BasePage(ABC):
    def __init__(self, server, doc):
        self.server = server
        self.doc = doc
        self.doc.on_session_destroyed(self.destroy)

    def destroy(self, session_context):
        del self.server.pages[session_context.id]

    async def start(self):
        built = asyncio.Future()
        self.doc.add_next_tick_callback(partial(self.build_page_cb, built))
        await built
        while True:
            self.refresh_data()
            await asyncio.sleep(self.server.refresh_seconds)


    @abstractmethod
    def build_page_cb(self, done: asyncio.Future):
        """A callback for building the page.

        Must be called as: 
        done: asyncio.Future
        doc.add_next_tick_callback(functools.partial(build_page_cb, done))
        You may await the future to coordinate with other coroutines
        """
        ...


