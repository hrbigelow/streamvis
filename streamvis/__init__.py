from time import sleep
from tornado.ioloop import IOLoop
from functools import partial
import logging
import requests

class Server:
    def __init__(self, doc, run_name, rest_host, rest_port, init_func, update_func):
        """
        doc: the bokeh Document object
        run_name: a name to scope this run
        rest_host: host of the REST endpoint
        rest_port: port of the REST endpoint
        init_func: function called as init(doc, config) once after the first 
        update_func: function called as update(self.doc, new_data)
                     each time there is new data received
        """
        self.run = run_name
        self.uri = f'http://{rest_host}:{rest_port}'
        self.doc = doc
        self.next_step = 0
        self.init = init_func
        self.update_data = update_func

    def __call__(self):
        self.doc.add_next_tick_callback(self.init_page)
        self.doc.add_periodic_callback(self.run_update, 1000)

    def init_page(self):
        """ Run this once, in a next_tick callback """

        while True:
            resp = requests.get(f'{self.uri}/update/{self.run}/{self.next_step}')
            new_data = resp.json()
            if new_data is not None and len(new_data) > 0:
                break
            sleep(1)

        first_step = next(k for k in sorted(new_data.keys(), key=int))
        step_data = new_data[first_step]
        schema = { k: list(v.keys()) for k, v in step_data.items() }
        self.init(self.doc, schema)

    def run_update(self):
        resp = requests.get(f'{self.uri}/update/{self.run}/{self.next_step}')
        new_data = resp.json()
        if new_data is None or len(new_data) == 0:
            return

        self.update_data(self.doc, new_data)
        self.next_step = max((int(k) + 1 for k in new_data.keys()))

    def start(self):
        """
        Call this in the bokeh server code at the end of the script.
        This starts the receiver listening for data updates from the
        sender.
        """
        # logging.getLogger('tornado.access').setLevel(logging.ERROR)
        IOLoop.current().spawn_callback(self)

class Client:
    """
    Create one instance of this in the producer script, to send data to
    a bokeh server.
    """
    def __init__(self, host, port, run_name):
        self.uri = f'http://{host}:{port}'
        self.run = run_name

    def clear(self):
        requests.post(f'{self.uri}/clear/{self.run}')

    def update(self, step, key, data):
        """
        Sends data to the server.  
        The receiver maintains nested map of step => (key => data).
        sending a (step, key) pair more than once overwrites the existing data.
        """
        # print('data: ', data)
        requests.post(f'{self.uri}/update/{self.run}/{step}/{key}', json=data)

    def updatel(self, step, key, data):
        """
        Same as send, but wraps any non-list data item as a one-element list
        """
        data = { k: v if isinstance(v, list) else [v] for k, v in data.items() }
        return self.update(step, key, data)

