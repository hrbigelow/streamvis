from abc import ABC, abstractmethod

class BasePage(ABC):
    def __init__(self, server, doc):
        self.server = server
        self.doc = doc
        self.doc.on_session_destroyed(self.destroy)

    def destroy(self, session_context):
        del self.server.pages[session_context.id]

    def start(self):
        self.doc.add_next_tick_callback(self.build_page_cb)

    @abstractmethod
    def build_page_cb(self):
        ...


