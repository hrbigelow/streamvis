import asyncio
import tornado
import threading
import sys
import os
import fcntl
import pickle
import uuid
from contextlib import contextmanager
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.application.handlers import Handler
from tornado.ioloop import IOLoop
from bokeh.server.server import Server as BokehServer
from streamvis import util, plotstate, plotpage

class CleanupHandler(Handler):
    def __init__(self, sv_server):
        super().__init__()
        self.sv_server = sv_server

    def modify_document(self, doc):
        pass

    def on_server_unloaded(self, server_context):
        self.sv_server.shutdown()
    
class Server:
    def __init__(self, run_name):
        """
        run_name: a name to scope this run
        """
        self.run_state = { run_name: {} }
        self.run_name = run_name
        self.update_lock = threading.Lock()
        self.is_pubsub = False
        self.write_log_fh = None
        self.read_log_fh = None
        self.pages = []

    def init_write_log(self, write_log_path):
        try:
            self.write_log_fh = open(write_log_path, 'ab')
        except OSError as ex:
            raise RuntimeError(f'Could not open {write_log_path=} for writing: {ex}')

    """
    The server uses either a log file or a pubsub subscription (exactly one of
    these).  The semantics are as follows:

    resource           init_read_log    init_pubsub 
    data               log_file         topic    
    handle             filehandle       subscription
    location           seek to start    seek to start

    The semantics differ in that once streamvis reads a message on pubsub, it is
    acknowledged and then goes away.  Whereas, when streamvis reads an entry in the
    log file, the message remains, and could be read again by another streamvis
    server instance.

    In both cases, streamvis manages the handle - creating it on startup, and
    destroying it on shutdown.  It also seeks the handle to the beginning of the
    data.
    """

    def init_read_log(self, read_log_path):
        """
        Open `read_log_path` for binary reading, creating the file if not exists.
        """
        if os.path.exists(read_log_path):
            try:
                self.read_log_fh = open(read_log_path, 'rb')
                print(f'Opened log file \'{read_log_path}\' for reading.')
            except OSError as ex:
                raise RuntimeError(
                    f'{read_log_path=} could not be opened for reading: {ex}')
            return
        try:
            open(read_log_path, 'a').close()
            self.read_log_fh = open(read_log_path, 'rb')
            print(f'Created new log file \'{read_log_path}\' opened for reading.')
            print('Launch a client process to write to this file')
        except OSError as ex:
            raise RuntimeError(
                f'{read_log_path=} did not exist and couldn\'t be created: {ex}')

    def init_pubsub(self, project_id, topic_id): 
        """
        Set up the server to use Pub/Sub.  
        """
        print('Starting streamvis server...', flush=True)
        from google.cloud import pubsub_v1
        self.is_pubsub = True
        self.project_id = project_id
        subscription_id = 'streamvis-{}'.format(str(uuid.uuid4())[:8])

        # the subscriber client object must exist during the subscription
        self.sub_client = pubsub_v1.SubscriberClient()
        self.topic_path = self.sub_client.topic_path(project_id, topic_id)
        with pubsub_v1.PublisherClient() as client:
            if not util.topic_exists(client, project_id, topic_id):
                raise RuntimeError(
                    f'Topic {self.topic_path} did not exist.  '
                    f'Create it separately with:\n'
                    f'gcloud pubsub topics create {topic_id}')

        self.sub_path = self.sub_client.subscription_path(project_id, subscription_id)
        req = dict(name=self.sub_path, topic=self.topic_path)
        self.sub_client.create_subscription(request=req)

        # the future is not needed since we wait indefinitely
        sub = self.sub_client.subscribe(self.sub_path, callback=self.pull_callback)
        req = dict(subscription=self.sub_path, time='1970-01-01T00:00:00.00Z')
        self.sub_client.seek(req)
        print(f'Created subscription: {subscription_id}')

    def shutdown(self):
        if self.is_pubsub:
            from google.cloud import pubsub_v1
            self.sub_client.delete_subscription(request={'subscription': self.sub_path})
            print(f'Deleted subscription {self.sub_path}')

        if self.read_log_fh is not None:
            self.read_log_fh.close()
        if self.write_log_fh is not None:
            print(f'Wrote {self.write_log_fh.tell()} bytes to write_log file '
                    f'{self.write_log_fh.name}')
            self.write_log_fh.close()

    @contextmanager
    def get_state(self, blocking=False):
        locked = self.update_lock.acquire(blocking=blocking)
        try:
            if locked:
                yield self.run_state.get(self.run_name, None)
            else:
                yield None
        finally:
            if locked:
                self.update_lock.release()

    """
    Server runs in either 'file' mode or 'pubsub' mode.  

    file mode: logfile_callback updates state - called from Tornado IOLoop
    pubsub mode: pubsub_callback updates state - called from pubsub separate thread
    """
    async def logfile_callback(self):
        # don't want to block
        def update(state):
            # runs while state is locked
            while True:
                try:
                    log_entry = pickle.load(self.read_log_fh)
                    plot_name = log_entry.plot_name
                    plot_state = state.setdefault(plot_name, plotstate.PlotState(plot_name))
                    plot_state.update(log_entry)
                except EOFError:
                    break
                except Exception as ex:
                    print(f'Could not process log_entry from log file: {ex}',
                            file=sys.stderr)
                    sys.exit(1)

        while True:
            await asyncio.sleep(1)
            with self.get_state(blocking=False) as locked_state:
                if locked_state is None:
                    continue
                fcntl.flock(self.read_log_fh, fcntl.LOCK_EX)
                update(locked_state)
                fcntl.flock(self.read_log_fh, fcntl.LOCK_UN)

            for page in self.pages:
                page.schedule_callback()

    def pubsub_callback(self, message):
        """
        """
        message.ack()
        try:
            log_entry = util.LogEntry.from_pubsub_message(message)
        except Exception as ex:
            print('Could not create log_entry from pubsub message: {ex}', file=sys.stderr)

        if self.write_log_fh is not None:
            pickle.dump(log_entry, self.write_log_fh)

        with self.get_state(blocking=True) as state:
            cds = log_entry.attributes.get('cds')
            plot_state = state.setdefault(cds, plotstate.PlotState(cds))
            plot_state.update(log_entry)

        for page in self.pages:
            page.schedule_callback()

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

        def parse(req_args):
            dec = lambda k: req_args[k][0].decode()
            intsp = lambda l: list(map(int, l.split(',')))
            final = dict(
                    mode = dec('mode'),
                    plots = dec('plots').split(','),
                    box_elems = intsp(dec('box_elems')),
                    box_part = intsp(dec('box_part')),
                    plot_part = intsp(dec('plot_part'))
                    )

            return type('__Args', (), final)

        args = parse(req.arguments)
        page = plotpage.PlotPage(self, doc, *args.plots)
        page.set_layout(args.mode, args.box_elems, args.box_part, args.plot_part)
        self.pages.append(page)

def make_server(port, run_name, project, topic, read_log_path, write_log_path):
    """
    port: webserver port
    run_name: arbitrary name for this
    project: Google Cloud Platform project id with Pub/Sub API enabled
    topic: Pub/Sub topic for client/server communication.  Topic will be
           deleted and re-created if already exists.  Will be deleted upon
           server shutdown
    """
    sv_server = Server(run_name)
    if project is None and read_log_path is None:
        raise RuntimeError(
            f'No information source provided.  Either provide `project` and `topic` '
            f'or `read_log_path`')
    if (project is None) != (topic is None):
        raise RuntimeError(
            f'`project` and `topic` must be provided together or both absent')
    if write_log_path and read_log_path:
        raise RuntimeError(
            f'`write_log_path` and `read_log_path` cannot both be provided. '
            f'Received {write_log_path=}, {read_log_path=}')

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop_wrap = tornado.ioloop.IOLoop.current()

    if project is not None:
        sv_server.init_pubsub(project, topic)

    if read_log_path is not None:
        sv_server.init_read_log(read_log_path)
        loop.create_task(sv_server.logfile_callback())

    if write_log_path is not None:
        sv_server.init_write_log(write_log_path)

    handler = FunctionHandler(sv_server.add_page)
    cleanup = CleanupHandler(sv_server)
    bokeh_app = Application(handler, cleanup)
    bsrv = BokehServer({'/': bokeh_app}, port=port, io_loop=loop_wrap)

    print(f'Web server is running on http://localhost:{port}')
    bsrv.run_until_shutdown()
    # IOLoop.current().start()

def run():
    import fire
    def file(port: int, run_name: str, log_file_path: str):
        """
        Visualize data from `log_file_path`

        :param port: webserver port
        :param run_name: unused
        :param log_file_path: Path to existing local log file containing data to be
                              visualized.  File may be produced by a previous server run 
                              in `pubsub` mode, or produced by a previous run of the 
                              streamvis client.
        """
        return make_server(port, run_name, None, None, log_file_path, None)

    def pubsub(port: int, run_name: str, project: str, topic: str, log_file: str =None):
        """
        Visualize data from pubsub subscription

        :param port: webserver port
        :param run_name: unused
        :param project: GCP project_id of existing project with Pub/Sub API enabled
        :param topic: GCP Pub/Sub topic id.  Will be re-created/deleted by this process
        :param log_file: path to local file to log all received data if provided
        """
        return make_server(port, run_name, project, topic, None, log_file)

    cmds = dict(file=file, pubsub=pubsub)
    fire.Fire(cmds)

if __name__ == '__main__':
    run()
