import tornado.ioloop
import tornado.web
import json
import sys

class ClearData(tornado.web.RequestHandler):
    def initialize(self, data, config):
        self.data = data
        self.config = config

    def post(self):
        self.data.clear()
        self.config.clear()
        self.set_status(200)

class StepData(tornado.web.RequestHandler):
    def initialize(self, data):
        self.data = data
    
    def get(self, step):
        step = int(step)
        result = {} 
        for step, item in enumerate(self.data[step:], step):
            result.update({ step: item })
        self.write(json.dumps(result))
        self.set_status(200)
    
    def post(self, step):
        step = int(step)
        while len(self.data) <= step:
            self.data.append(None)
        if self.data[step] is None:
            self.data[step] = {}
        entry = self.data[step]
        entry.update(json.loads(self.request.body))
        self.set_status(200)
        # print(f'in post: self.data={self.data}, step={step}')

class InitData(tornado.web.RequestHandler):
    def initialize(self, cfg):
        self.cfg = cfg

    def get(self, key):
        data = self.cfg.get(key, None)
        self.write(json.dumps(data))
        self.set_status(200)

    def post(self, key):
        data = json.loads(self.request.body)
        self.cfg[key] = data
        self.set_status(200)

def make_app():
    data = [] 
    config = {}
    return tornado.web.Application([
        (r"/step", StepData, dict(data=data)),
        (r"/step/([0-9]+)", StepData, dict(data=data)),
        (r"/step/clear", ClearData, dict(data=data, config=config)),
        (r"/init/(\w+)", InitData, dict(cfg=config))
    ])


"""
GET /step/{step}  - retrieve data associated with {step} or greater
POST /step/{step} - expect a map in the body, update the entry 
GET /init/{key}   - retrieve the entry associated with {key}, or None if not exists
POST /init/{key}  - add or update entry associated with {key}
"""

if __name__ == "__main__":
    port = int(sys.argv[1])
    app = make_app()
    app.listen(port)
    print("Server is running on http://localhost:8888")
    tornado.ioloop.IOLoop.current().start()


