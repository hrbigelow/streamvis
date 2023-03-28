from time import sleep
from tornado.ioloop import IOLoop
from functools import partial
import requests

class Server:
    def __init__(self, doc, init_key, init_uri, update_uri, init_func, update_func):
        """
        doc: the bokeh Document object
        init_uri: REST endpoint for receiving configuration info (used once)
        update_uri: REST endpoint for receiving more data 
        init_func: function called as init(doc, config) once after the first 
        update_func: function called as update(self.doc, new_data)
                     each time there is new data received
        """
        self.init_key = init_key
        self.init_uri = init_uri
        self.update_uri = update_uri
        self.doc = doc
        self.next_step = 0
        self.init = init_func
        self.update_data = update_func

    def __call__(self):
        config_data = None
        while config_data is None:
            resp = requests.get(f'{self.init_uri}/{self.init_key}')
            config_data = resp.json()
            sleep(1)

        # print(f'in __call__ with config_data={config_data}')
        wrapped = partial(self.init, self.doc, config_data)
        self.doc.add_next_tick_callback(wrapped)
        self.doc.add_periodic_callback(self.run_update, 3000)

    def run_update(self):
        resp = requests.get(f'{self.update_uri}/{self.next_step}')
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
    def __init__(self, init_uri, update_uri):
        self.init_uri = init_uri
        self.update_uri = update_uri

    def clear(self):
        requests.post(f'{self.update_uri}/clear')

    def init(self, key, data):
        """
        Called once by the sender to supply configuration information to
        the receiver init function
        """
        requests.post(f'{self.init_uri}/{key}', json=data)

    def send(self, step, key, data):
        """
        Sends data to the server.  
        The receiver maintains nested map of step => (key => data).
        sending a (step, key) pair more than once overwrites the existing data.
        """
        # print('data: ', data)
        requests.post(f'{self.update_uri}/{step}', json={key: data})

    def sendl(self, step, key, data):
        """
        Same as send, but wraps any non-list data item as a one-element list
        """
        data = { k: v if isinstance(v, list) else [v] for k, v in data.items() }
        return self.send(step, key, data)

