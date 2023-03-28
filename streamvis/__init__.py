from time import sleep
from tornado.ioloop import IOLoop
from functools import partial
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
        config_data = None
        while config_data is None:
            resp = requests.get(f'{self.uri}/init/{self.run}')
            config_data = resp.json()
            sleep(1)

        # print(f'in __call__ with config_data={config_data}')
        wrapped = partial(self.init, self.doc, config_data)
        self.doc.add_next_tick_callback(wrapped)
        self.doc.add_periodic_callback(self.run_update, 500)

    def run_update(self):
        resp = requests.get(f'{self.uri}/update/{self.run}/{self.next_step}')
        new_data = resp.json()
        if len(new_data) == 0:
            return
        self.update_data(self.doc, new_data)
        self.next_step = max((int(k) + 1 for k in new_data.keys()))

    def start(self):
        """
        Call this in the bokeh server code at the end of the script.
        This starts the receiver listening for data updates from the
        sender.
        """
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
        requests.post(f'{self.uri}/{self.run}/clear')

    def init(self, data):
        """
        Called once by the sender to supply configuration information to
        the receiver init function
        """
        requests.post(f'{self.uri}/init/{self.run}', json=data)

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

