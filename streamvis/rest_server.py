import tornado.ioloop
import tornado.web
import json
import sys

class ClearData(tornado.web.RequestHandler):
    def initialize(self, data, config):
        self.data = data
        self.config = config

    def post(self, run_name):
        self.data.pop(run_name, None)
        self.config.pop(run_name, None)
        self.set_status(200)

class StepData(tornado.web.RequestHandler):
    def initialize(self, data):
        self.data = data
    
    def get(self, run_name, start_step):
        start_step = int(start_step)
        if run_name not in self.data:
            self.write('null')
            self.set_status(200)
            return

        run_data = self.data[run_name]
        result = {} 
        for step, item in enumerate(run_data[start_step:], start_step):
            result.update({ step: item })
        self.write(json.dumps(result))
        self.set_status(200)
    
    def post(self, run_name, step, key):
        step = int(step)
        run_data = self.data.setdefault(run_name, [])
        while len(run_data) <= step:
            run_data.append(None)
        if run_data[step] is None:
            run_data[step] = {}
        run_data[step][key] = json.loads(self.request.body)
        self.set_status(200)

def make_app():
    data = {}   # (run => [])
    config = {} # (run => [])
    return tornado.web.Application([
        (r"/update/(\w+)/([0-9]+)", StepData, dict(data=data)),
        (r"/update/(\w+)/([0-9]+)/(\w+)", StepData, dict(data=data)),
        (r"/clear/(\w+)", ClearData, dict(data=data, config=config)),
    ])


"""
GET /update/{step}  - returns a JSON map of { step: <entry> } structure, for all steps
                      greater than or equal to step
                    
POST /update/{step} - expect a map in the request body.   update or augment the entry 
                      associated with step
"""

def main():
    port = int(sys.argv[1])
    app = make_app()
    app.listen(port)
    print(f'Server is running on http://localhost:{port}')
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()
