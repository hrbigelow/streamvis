import threading
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
from tornado.ioloop import IOLoop
from bokeh.server.server import Server as BokehServer
from . import plots

@dataclass
class RunState:
    # cds => data to put into the ColumnDataSource object
    data: dict = field(default_factory=dict)

    # cds => plot configuration data for the init_callback
    init_cfg: dict = field(default_factory=dict)

    # cds => plot configuration data for the update_callback
    update_cfg: dict = field(default_factory=dict)

    # cds => (row_start, col_start, row_end, col_end)
    layout: dict = field(default_factory=dict)

    def __repr__(self):
        return (
                f'init_cfg: {self.init_cfg}\n'
                f'update_cfg: {self.update_cfg}\n'
                f'layout: {self.layout}\n'
                )


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

        # cds => ndarray.  The ndarray contains the full contents to be mirrored to
        # the cds.
        self.nddata = {}

    def init_pubsub(self, project_id, topic_id=None): 
        """
        Set up the server to use Pub/Sub.  If topic_id is provided,
        Returns the created topic_id
        """
        print('Starting...', flush=True)
        from google.cloud import pubsub_v1
        from concurrent.futures import TimeoutError
        self.is_pubsub = True
        self.project_id = project_id
        if topic_id is None:
            topic_id = 'topic-streamvis-{}'.format(str(uuid.uuid4())[:8])
        subscription_id = 'subscription-streamvis-{}'.format(str(uuid.uuid4())[:8])

        # we can get away without managing the publisher client

        # the subscriber client object must exist during the subscription
        self.sub_client = pubsub_v1.SubscriberClient()
        self.topic_path = self.sub_client.topic_path(project_id, topic_id)
        with pubsub_v1.PublisherClient() as client:
            client.create_topic(request = {'name': self.topic_path})

        self.sub_path = self.sub_client.subscription_path(project_id, subscription_id)
        req = dict(name=self.sub_path, topic=self.topic_path)
        self.sub_client.create_subscription(request=req)

        # TODO: validate project_id

        # the future is not needed since we wait indefinitely
        _ = self.sub_client.subscribe(self.sub_path, callback=self.pull_callback)

        print(f'Created topic: {topic_id}')
        print(f'Created subscription: {subscription_id}')

    def shutdown(self):
        # TODO: how to deploy this?
        if self.is_pubsub:
            with pubsub_v1.PublisherClient() as pub:
                pub.delete_topic(request={'topic': self.topic_path})
            self.sub_client.delete_subscription(request={'subscription': self.sub_path})

    def pull_callback(self, message):
        # runs in side thread managed by PubSub, so blocking call to get_state is okay
        # run is unused currently (will be needed to scope individual runs)
        # acknowledge even though we don't yet know if processing will be successful
        # because there is no fallback anyway
        message.ack()
        attrs = {}
        try:
            for key in message.attributes:
                val = message.attributes[key]
                attrs[key] = val
        except Exception as e:
            print(f'Got exception during pull_callback: {e} (ignoring)')
            return
        try:
            data = pickle.loads(message.data)
        except Exception as e:
            print(f'pull_callback: exception when unpickling data: {e}\n'
                    f'Context: {attrs}\n'
                    f'len(data) = {len(message.data)}.\n')
            return

        if any(k not in attrs for k in ('run', 'field', 'cds')):
            print(f'pull_callback: message is missing one or more attributes: '
                    f'\'run\', \'field\', or \'cds\'. '
                    f'Found {attrs} (ignoring)')
            return
        # print(f'boof: {message.size}, {field}, {cds}', flush=True)
        field = attrs['field']
        cds = attrs['cds']

        with self.get_state(blocking=True) as state:
            if field == 'init':
                state.init_cfg[cds] = data
            elif field == 'update':
                state.update_cfg[cds] = data 
            elif field == 'layout':
                state.layout[cds] = data 
            else: # field == 'data'
                ary = state.data.setdefault(cds, [])
                ary.append(data)

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

    def add_new_data(self, cds_name, data, append_dim, **kwargs):
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

            if cds_name not in self.nddata:
                empty_shape = list(new_nd.shape)
                empty_shape[append_dim] = 0
                self.nddata[cds_name] = np.empty(empty_shape)
            cur_nd = self.nddata[cds_name]

            if append_dim != -1:
                # print(f'shapes for {cds_name}: cur: {cur_nd.shape}, new: {new_nd.shape}')
                cur_nd = np.concatenate((cur_nd, new_nd), axis=append_dim)
                self.nddata[cds_name] = cur_nd
            else:
                self.nddata[cds_name] = new_nd 

    def update_cds(self, cds_name, nd_columns, zmode, **kwargs):
        """
        Transfer the nddata into the cds
        zmode: an identifier instructing how to populate the z column if needed
        """
        cds = self.doc.get_model_by_name(cds_name)
        if cds is None:
            return
        ary = self.nddata[cds_name]
        cdata = dict(zip(nd_columns, ary.tolist()))
        if zmode == 'linecolor':
            k = ary.shape[1]
            cdata['z'] = np.linspace(0, 1, k).tolist()
        cds.data = cdata
        # print(f'cds_name={cds_name}, nd_columns={nd_columns}, zmode={zmode}, cdata:\n',
                # ",".join(f'{k}: {len(v)}' for k, v in cdata.items()))

    def init_callback(self):
        """
        Called when new cfg data is available
        """
        # cfg may be left over from a previously aborted run, so not
        # congruent with the current layout.  in this case, it is ignored.
        # during a client run, the client ensures that all POSTs to cfg
        # endpoint are keys present in layout
        with self.get_state(blocking=False) as state:
            if state is None:
                return
            
            # print(f'in init with state = \n{state}\n')
            if any(k not in state.layout for k in state.init_cfg.keys()):
                return

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
        with self.get_state(blocking=False) as state:
            if state is None:
                return
            # print(f'in update with keys {state.data.keys()}')
            for cds_name, data in state.data.items():
                update_cfg = state.update_cfg[cds_name]
                self.add_new_data(cds_name, data, **update_cfg)
                self.update_cds(cds_name, **update_cfg)
            state.data.clear()

    def work_callback(self):
        with self.get_state(blocking=False) as state:
            # print(f'in work with state = \n{state}\n')
            if state is None or state.init_cfg is None:
                return

            if len(state.init_cfg) != 0:
                self.doc.add_next_tick_callback(self.init_callback)
                return

            if state.data is None:
                return

            elif all(len(v) == 0 for v in state.data.values()):
                # no new data to process
                return
            else:
                self.doc.add_next_tick_callback(self.update_callback)

    def start(self, doc):
        """
        Call this in the bokeh server code at the end of the script.
        This starts the receiver listening for data updates from the
        sender.
        """
        with self.get_state(blocking=True) as state:
            print(f'Current state:\n{state}')
        self.doc = doc 
        self.column = column()
        self.doc.add_root(self.column)
        self.doc.add_periodic_callback(self.work_callback, 1000)

def make_server(bokeh_port, project_id, run_name, topic_id=None):
    sv_server = Server(run_name)
    sv_server.init_pubsub(project_id, topic_id)

    # def app_function(doc):
        # print(f'doc request: {doc.session_context.request}')
        # sv_server.start(doc)

    handler = FunctionHandler(sv_server.start)
    bokeh_app = Application(handler)
    bsrv = BokehServer({'/': bokeh_app}, port=bokeh_port, io_loop=IOLoop.current())
    # rest_app = endpoint.make_app(state)
    # rest_app.listen(rest_port)
    print(f'Web server is running on http://localhost:{bokeh_port}')
    # print(f'Rest endpoint is listening on http://localhost:{rest_port}')
    IOLoop.current().start()

def run():
    import fire
    fire.Fire(make_server)

