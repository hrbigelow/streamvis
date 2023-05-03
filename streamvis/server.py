import numpy as np
from bokeh.models import GridBox
from bokeh.layouts import column
from bokeh.plotting import figure
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from tornado.ioloop import IOLoop
from bokeh.server.server import Server as BokehServer
from . import plots, endpoint

class Server:
    def __init__(self, doc, run_state, run_name):
        """
        run_state: an empty map to be shared with a REST endpoint.
                   will populate as (run => data)
        run_name: a name to scope this run
        """
        self.run_state = run_state
        self.run_name = run_name
        self.doc = doc 
        self.column = column()
        self.doc.add_root(self.column)

        # cds => ndarray.  The ndarray contains the full contents to be mirrored to
        # the cds.
        self.nddata = {}

    def get_figure(self, cds_name):
        return self.doc.select({ 'type': figure, 'name': cds_name })

    def get_state(self):
        return self.run_state.get(self.run_name, None)

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
        state = self.get_state()
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
        state = self.get_state()
        # print(f'in update with keys {state.data.keys()}')
        for cds_name, data in state.data.items():
            update_cfg = state.update_cfg[cds_name]
            self.add_new_data(cds_name, data, **update_cfg)
            self.update_cds(cds_name, **update_cfg)
        state.data.clear()

    def work_callback(self):
        state = self.get_state()
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

    def start(self):
        """
        Call this in the bokeh server code at the end of the script.
        This starts the receiver listening for data updates from the
        sender.
        """
        self.doc.add_periodic_callback(self.work_callback, 1000)

def make_server(rest_port, bokeh_port, run_name):
    state = {} # run -> RunState

    def app_function(doc):
        # print(f'doc request: {doc.session_context.request}')
        sv_server = Server(doc, state, run_name)
        sv_server.start()

    handler = FunctionHandler(app_function)
    bokeh_app = Application(handler)
    bsrv = BokehServer({'/': bokeh_app}, port=bokeh_port, io_loop=IOLoop.current())
    rest_app = endpoint.make_app(state)
    rest_app.listen(rest_port)
    print(f'Web server is running on http://localhost:{bokeh_port}')
    print(f'Rest endpoint is listening on http://localhost:{rest_port}')
    IOLoop.current().start()

def run():
    import fire
    fire.Fire(make_server)

