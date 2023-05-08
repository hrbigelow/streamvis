import threading
import sys
import os
import fcntl
import numpy as np
import json
import pickle
import uuid
from dataclasses import dataclass, field
from contextlib import contextmanager
from bokeh.models import GridBox
from bokeh.layouts import column
from bokeh.plotting import figure
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.application.handlers import Handler
from tornado.ioloop import IOLoop
from bokeh.server.server import Server as BokehServer
from . import plots, util

@dataclass
class RunState:
    # cds => data to put into the ColumnDataSource object
    new_data: dict = field(default_factory=dict)

    # cds => plot configuration data for the init_callback
    init_cfg: dict = field(default_factory=dict)

    # cds => plot configuration data for the update_callback
    update_cfg: dict = field(default_factory=dict)

    # cds => (row_start, col_start, row_end, col_end)
    layout: dict = field(default_factory=dict)

    # cds => ndarray data
    nddata: dict = field(default_factory=dict)

    @staticmethod
    def _samekeys(*maps):
        first, *rest = maps
        return all(set(first.keys()) == set(r.keys()) for r in rest)

    def init_ready(self):
        return (self._samekeys(self.init_cfg, self.update_cfg, self.layout) and
                len(self.layout) > 0)

    def update_ready(self):
        return (self._samekeys(self.new_data, self.update_cfg) and
                len(self.new_data) > 0)

    def update(self, log_entry):
        """
        Update state from the log entry
        """
        if log_entry.action == 'clear':
            print(f'clearing state')
            self.init_cfg.clear()
            self.update_cfg.clear()
            self.layout.clear()
            self.new_data.clear()
            self.nddata.clear()
        elif log_entry.action == 'layout':
            self.layout[log_entry.cds] = log_entry.data 
        elif log_entry.action == 'init':
            self.init_cfg[log_entry.cds] = log_entry.data
        elif log_entry.action == 'update':
            self.update_cfg[log_entry.cds] = log_entry.data 
        elif log_entry.action == 'add-data':
            ary = self.new_data.setdefault(log_entry.cds, [])
        else: 
            raise RuntimeError(f'Unknown action in log_entry: {log_entry.action}')

    def __repr__(self):
        return (
                f'init_cfg: {self.init_cfg}\n'
                f'update_cfg: {self.update_cfg}\n'
                f'layout: {self.layout}\n'
                )

class Cleanup(Handler):
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
        # TODO: Semantics of using pubsub are different
        self.run_state = { run_name: RunState() }
        self.run_name = run_name
        self.update_lock = threading.Lock()
        self.is_pubsub = False
        self.write_log_fh = None
        self.read_log_fh = None

    def init_write_log(self, write_log_path):
        try:
            self.write_log_fh = open(write_log_path, 'ab')
        except OSError as ex:
            raise RuntimeError(f'Could not open {write_log_path=} for writing: {ex}')

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
        print('Starting...', flush=True)
        from google.cloud import pubsub_v1
        self.is_pubsub = True
        self.project_id = project_id
        subscription_id = 'subscription-streamvis-{}'.format(str(uuid.uuid4())[:8])

        # we can get away without managing the publisher client

        # the subscriber client object must exist during the subscription
        self.sub_client = pubsub_v1.SubscriberClient()
        self.topic_path = self.sub_client.topic_path(project_id, topic_id)
        with pubsub_v1.PublisherClient() as client:
            if util.topic_exists(client, project_id, topic_id):
                topic_req = {'topic': self.topic_path}
                for sub in client.list_topic_subscriptions(request=topic_req):
                    sub_req = {'subscription': sub}
                    self.sub_client.delete_subscription(request=sub_req)
                client.delete_topic(request = topic_req)
            client.create_topic(request = {'name': self.topic_path} )

        self.sub_path = self.sub_client.subscription_path(project_id, subscription_id)
        req = dict(name=self.sub_path, topic=self.topic_path)
        self.sub_client.create_subscription(request=req)

        # TODO: validate project_id

        # the future is not needed since we wait indefinitely
        _ = self.sub_client.subscribe(self.sub_path, callback=self.pull_callback)
        print(f'Created topic: {topic_id}')
        print(f'Created subscription: {subscription_id}')

    def shutdown(self):
        if self.is_pubsub:
            print('Deleting topic and subscription')
            from google.cloud import pubsub_v1
            with pubsub_v1.PublisherClient() as pub:
                pub.delete_topic(request={'topic': self.topic_path})
            self.sub_client.delete_subscription(request={'subscription': self.sub_path})
        if self.read_log_fh is not None:
            self.read_log_fh.close()
        if self.write_log_fh is not None:
            print(f'Wrote {self.write_log_fh.tell()} bytes to write_log file '
                    f'{self.write_log_fh.name}')
            self.write_log_fh.close()

    def pull_callback(self, message):
        # runs in side thread managed by PubSub, so blocking call to get_state is okay
        # run is unused currently (will be needed to scope individual runs)
        # acknowledge even though we don't yet know if processing will be successful
        # because there is no fallback anyway
        message.ack()
        log_entry = util.LogEntry.from_pubsub_message(message)
        if not log_entry.valid:
            return

        if self.write_log_fh is not None:
            pickle.dump(log_entry, self.write_log_fh)

        with self.get_state(blocking=True) as state:
            state.update(log_entry)

    def get_figure(self, cds_name):
        return self.doc.select({ 'type': figure, 'name': cds_name })

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

    def add_new_data(self, nddata, cds, new_data, append_dim, **kwargs):
        """
        Incorporate new data into the nddata store
        data: N, *item_shape; N is a number of new streaming data points
        append_dim: the dimension of data to append along, or -1 if replacing
        """
        if append_dim == -1:
            # if not appending, skip old updates accumulated in REST endpoint
            data = data[-1:]

        for item in data:
            new_nd = np.array(item)

            if cds not in nddata:
                empty_shape = list(new_nd.shape)
                empty_shape[append_dim] = 0
                nddata[cds] = np.empty(empty_shape)
            cur_nd = nddata[cds]

            if append_dim != -1:
                # print(f'shapes for {cds}: cur: {cur_nd.shape}, new: {new_nd.shape}')
                cur_nd = np.concatenate((cur_nd, new_nd), axis=append_dim)
                nddata[cds] = cur_nd
            else:
                nddata[cds] = new_nd 

    def update_cds(self, nddata, cds_name, nd_columns, zmode, **kwargs):
        """
        Transfer the nddata into the cds
        zmode: an identifier instructing how to populate the z column if needed
        """
        cds = self.doc.get_model_by_name(cds_name)
        if cds is None:
            return
        ary = nddata[cds_name]
        cdata = dict(zip(nd_columns, ary.tolist()))
        if zmode == 'linecolor':
            k = ary.shape[1]
            cdata['z'] = np.linspace(0, 1, k).tolist()
        cds.data = cdata
        # print(f'cds_name={cds_name}, nd_columns={nd_columns}, zmode={zmode}, cdata:\n',
                # ",".join(f'{k}: {len(v)}' for k, v in cdata.items()))

    def init_callback(self):
        """
        Creates new Bokeh plot objects using the state init_cfg and layout fields.
        """
        with self.get_state(blocking=False) as state:
            grid = []
            for cds_name, cfg in state.init_cfg.items():
                if len(cfg) == 0:
                    fig = self.get_figure(cds_name)
                else:
                    fig = plots.make_figure(cds_name, **cfg)

                coords = state.layout[cds_name]
                grid.append((fig, *coords))
            plot = GridBox(children=grid)
            self.column.children.clear()
            self.column.children.append(plot)
            state.init_cfg.clear()

    def update_callback(self):
        """
        Updates ColumnDataSources using state
        """
        with self.get_state(blocking=False) as state:
            # print(f'in update with keys {state.new_data.keys()}')
            for cds, new_data in state.new_data.items():
                update_cfg = state.update_cfg[cds]
                # adds this data to the nddata store
                self.add_new_data(state.nddata, cds, new_data, **update_cfg)

                # synchs the cds's contents from the nddata store
                self.update_cds(state.nddata, cds, **update_cfg)
            state.new_data.clear()

    def work_callback(self):
        """
        """
        with self.get_state(blocking=False) as state:
            if not self.is_pubsub:
                fcntl.flock(self.read_log_fh, fcntl.LOCK_EX)
                while True:
                    try:
                        log_entry = pickle.load(self.read_log_fh)
                        state.update(log_entry)
                    except EOFError:
                        break
                fcntl.flock(self.read_log_fh, fcntl.LOCK_UN)

            # hack to make sure we're ready to init
            if state.init_ready():
                self.doc.add_next_tick_callback(self.init_callback)
                return

            if state.update_ready():
                self.doc.add_next_tick_callback(self.update_callback)
                return

    def start(self, doc):
        """
        Called once for each page refresh, as part of the application handlers
        """
        with self.get_state(blocking=True) as state:
            print(f'Current state:\n{state}')
        self.doc = doc 
        self.column = column()
        self.doc.add_root(self.column)
        self.doc.add_periodic_callback(self.work_callback, 1000)

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

    if project is not None:
        sv_server.init_pubsub(project, topic)

    if read_log_path is not None:
        sv_server.init_read_log(read_log_path)

    if write_log_path is not None:
        sv_server.init_write_log(write_log_path)

    handler = FunctionHandler(sv_server.start)
    cleanup = Cleanup(sv_server)
    bokeh_app = Application(handler, cleanup)
    bsrv = BokehServer({'/': bokeh_app}, port=port, io_loop=IOLoop.current())

    print(f'Web server is running on http://localhost:{port}')
    bsrv.run_until_shutdown()
    # IOLoop.current().start()

def run():
    import fire
    def readlog(port: int, run_name: str, log_file_path: str):
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

    def subscribe(port: int, run_name: str, project: str, topic: str, log_file: str =None):
        """
        Visualize data from pubsub subscription

        :param port: webserver port
        :param run_name: unused
        :param project: GCP project_id of existing project with Pub/Sub API enabled
        :param topic: GCP Pub/Sub topic id.  Will be re-created/deleted by this process
        :param log_file: path to local file to log all received data if provided
        """
        return make_server(port, run_name, project, topic, None, None, None)

    cmds = dict(file=readlog, pubsub=subscribe)
    fire.Fire(cmds)

