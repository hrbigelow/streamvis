from time import sleep
from tornado.ioloop import IOLoop
from bokeh.io import curdoc
from bokeh.layouts import column
from functools import partial
from threading import Thread
import logging
import requests

class Server:
    def __init__(self, run_name, rest_host, rest_port, init_page, update_data):
        """
        run_name: a name to scope this run
        rest_host: host of the REST endpoint
        rest_port: port of the REST endpoint
        init_page: function called as init(schema) if there is a signal to
                   reconfigure the page.  should return either an instance or
                   tuple of instances of 

        update_data: function called as update_data(self.doc, new_data)
                     each time there is new data received
        """
        self.run = run_name
        self.data_uri = f'http://{rest_host}:{rest_port}/data'
        self.ctrl_uri = f'http://{rest_host}:{rest_port}/ctrl'
        self.doc = curdoc()
        self.column = column()
        self.doc.add_root(self.column)
        self.init_page = init_page
        self.update_data = update_data
        self.page_initialized = False

    @staticmethod
    def run_data_empty(run_data):
        return run_data is None or all(len(v) == 0 for v in run_data.values())

    def init_callback(self, run_data):
        """
        Called if self.ctrl_uri contains a refresh signal 
        Updates the document column with new Bokeh models which are
        produced by self.init_page
        """
        schema = {}
        for cds, ent in run_data.items():
            schema[cds] = list(next(iter(ent.values())))
        figs = self.init_page(schema)
        self.column.children.clear()
        if not isinstance(figs, tuple):
            figs = (figs,)
        self.column.children.extend(figs)
        self.page_initialized = True

    def update_callback(self, run_data):
        self.update_data(self.doc, run_data)
        for cds, entry in run_data.items():
            for step in entry.keys():
                requests.delete(f'{self.data_uri}/{self.run}/{cds}/{step}')

    def work(self):
        refresh = requests.get(f'{self.ctrl_uri}/{self.run}')
        if refresh.json() == 'refresh':
            requests.delete(f'{self.ctrl_uri}/{self.run}')
            self.page_initialized = False

        resp = requests.get(f'{self.data_uri}/{self.run}')
        run_data = resp.json()

        if not self.page_initialized:
            if run_data is None or any(len(v) == 0 for v in run_data.values()):
                return
            self.doc.add_next_tick_callback(partial(self.init_callback, run_data))
        else:
            self.doc.add_next_tick_callback(partial(self.update_callback, run_data))

    def start(self):
        """
        Call this in the bokeh server code at the end of the script.
        This starts the receiver listening for data updates from the
        sender.
        """
        curdoc().add_periodic_callback(self.work, 100)

class Client:
    """
    Create one instance of this in the producer script, to send data to
    a bokeh server.
    """
    def __init__(self, host, port, run_name):
        self.data_uri = f'http://{host}:{port}/data'
        self.ctrl_uri = f'http://{host}:{port}/ctrl'
        self.run = run_name

    def clear(self):
        requests.delete(f'{self.data_uri}/{self.run}')
        requests.delete(f'{self.ctrl_uri}/{self.run}')

    def init(self, *cds_names):
        """
        Create an empty container of cds_names 
        """
        requests.post(f'{self.data_uri}/{self.run}', json=cds_names)
        requests.post(f'{self.ctrl_uri}/{self.run}', json='refresh')

    def update(self, cds_name, step, data):
        """
        Sends data to the server.  
        The receiver maintains nested map of step => (key => data).
        sending a (step, key) pair more than once overwrites the existing data.
        """
        # print('data: ', data)
        requests.patch(f'{self.data_uri}/{self.run}/{cds_name}/{step}', json=data)

    def updatel(self, cds_name, step, data):
        """
        Same as send, but wraps any non-list data item as a one-element list
        """
        data = { k: v if isinstance(v, list) else [v] for k, v in data.items() }
        return self.update(cds_name, step, data)


