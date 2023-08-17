import asyncio
import tornado
import threading
import signal
import sys
import os
import yaml
from contextlib import contextmanager
from tensorflow.io.gfile import GFile
from google.cloud import storage
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.application.handlers import Handler
from tornado.ioloop import IOLoop
from bokeh.server.server import Server as BokehServer
from streamvis import util
from streamvis.page import IndexPage, PageLayout

class LockManager:
    def __init__(self):
        self.lock = threading.Lock()

    def __enter__(self):
        self.lock.acquire()
        return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.release()

class CleanupHandler(Handler):
    def __init__(self, sv_server):
        super().__init__()
        self.sv_server = sv_server

    def modify_document(self, doc):
        pass

    def on_server_unloaded(self, server_context):
        self.sv_server.shutdown()

    
class Server:
    def __init__(self, data_fetch_period=5.0, page_update_period=1.0):
        self.update_lock = threading.Lock()
        self.schema = {}
        # groups and points are append-only logs, collective the server data 'state'
        self.state = dict(groups=[], points=[])

        # should this (and the blob?) be protected? 
        self.blob_offset = 0
        self.page_lock = LockManager()
        self.pages = {} # map of session_id -> {page.PageLayout or page.IndexPage}
        # pretected by page_lock.  this is set when the server receives new data
        # and needs to update all pages
        self.data_update_pending = False 
        self.data_fetch_period = data_fetch_period # seconds 
        self.page_update_period = page_update_period

    def load_schema(self, schema_file):
        """
        Parses and sets the schema, determines how plots are created from data
        """
        try:
            with open(schema_file, 'r') as fh: 
                schema = yaml.safe_load(fh)
        except Exception as ex:
            raise RuntimeError(
                    f'Server could not open or parse schema file {schema_file}. '
                    f'Exception was: {ex}')
        self.schema = schema

    def init(self, path):
        """
        Initialize the data source
        """
        self.path = path

    def shutdown(self):
        """
        Cleanup actions.  What is needed for GCS?
        """
        pass

    @contextmanager
    def get_state(self, blocking=False):
        locked = self.update_lock.acquire(blocking=blocking)
        try:
            if locked:
                yield self.state
            else:
                yield None
        finally:
            if locked:
                self.update_lock.release()

    async def update_pages(self):
        """
        A callback that triggers page updating if there is new data to
        incorporate into plots
        """
        while True:
            await asyncio.sleep(self.page_update_period)
            with self.page_lock:
                if self.data_update_pending:
                    for page in self.pages.values():
                        page.schedule_callback()
                self.data_update_pending = False

    def fetch_new_data(self):
        """
        Read any new data from the blob, updating the current position
        """
        fh = GFile(self.path, 'rb')
        fh.seek(self.blob_offset)
        packed = fh.read()
        fh.close()
        if len(packed) == 0:
            return

        # TODO: protect this with lock?
        self.blob_offset += len(packed)
        try:
            messages = util.unpack(packed)
            new_groups, new_points = util.separate_messages(messages)
        except Exception as ex:
            raise RuntimeError(
                    f'Could not unpack messages from GCS log file {self.path}. '
                    f'Got exception: {ex}')

        with self.get_state(blocking=True) as state:
            # print(f'adding {len(new_points)} new data to server')
            state['groups'].extend(new_groups)
            state['points'].extend(new_points)

        with self.page_lock:
            self.data_update_pending = True

    async def gcs_callback(self):
        while True:
            await asyncio.sleep(self.data_fetch_period)
            self.fetch_new_data()

    def add_page(self, doc):
        """
        Add a page object to the state machine
        REST API arguments:
        plots: comma-separate names of plots
        mode: 'row' or 'column'; what a box represents in the layout
        box_elems: comma-separated list of the number of plots each box contains
        box_part: comma-separated list of column width (or row height) proportions
        plot_part: comma-separate list of plot width (row mode) or height (column mode)
        """
        req = doc.session_context.request
        session_id = doc.session_context.id
        print(f'got {session_id=}')

        if len(req.arguments) == 0:
            page = IndexPage(self, doc)
            page.build()
        else:
            page = PageLayout(self, doc)
            page.set_pagesize(1800, 900)
            page.process_request(req)
            page.schedule_callback()

        with self.page_lock:
            self.pages[session_id] = page
            print(f'server has {len(self.pages)} pages')

    def delete_page(self, session_id):
        with self.page_lock:
            del self.pages[session_id]
            print(f'deleted page {session_id}.  server now has {len(self.pages)} pages.')

def make_server(port, schema_file, path):  
    """
    port: localhost port to run this server on
    schema_file:  YAML schema file defining plots
    path:  filesystem path, s3:// or gs:// path
    """
    sv_server = Server()
    sv_server.load_schema(schema_file)
    sv_server.init(path)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop_wrap = tornado.ioloop.IOLoop.current()

    loop.create_task(sv_server.update_pages())
    loop.create_task(sv_server.gcs_callback())

    def shutdown_handler(signum, frame):
        print(f'Server received {signal.Signals(signum).name}')
        sv_server.shutdown()

    signal.signal(signal.SIGQUIT, shutdown_handler)
    signal.signal(signal.SIGHUP, shutdown_handler) 

    handler = FunctionHandler(sv_server.add_page)
    cleanup = CleanupHandler(sv_server)
    bokeh_app = Application(handler, cleanup)
    bsrv = BokehServer({'/': bokeh_app}, port=port, io_loop=loop_wrap)

    print(f'Web server is running on http://localhost:{port}')
    bsrv.run_until_shutdown()

def run():
    import fire
    fire.Fire(make_server)
    
if __name__ == '__main__':
    run()

