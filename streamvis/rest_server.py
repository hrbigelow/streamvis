import tornado.ioloop
import tornado.web
import json
import sys

class RunHandler(tornado.web.RequestHandler):
    """
    GET {run}?min_step=10 - retrieve run data for step 10 onwards
    POST {run} - set run resource
    """
    def initialize(self, data):
        self.data = data
    
    def get(self, run):
        self.write(json.dumps(self.data.get(run, None)))
        self.set_status(200)

    def post(self, run):
        cdss = json.loads(self.request.body)
        if isinstance(cdss, list) and all(isinstance(cds, str) for cds in cdss):
            self.data[run] = { cds: {} for cds in cdss }
            self.set_status(200)
        else:
            self.set_status(400)

    def delete(self, run):
        if run in self.data:
            del self.data[run]
            self.set_status(200)

class CDSHandler(tornado.web.RequestHandler):

    def initialize(self, data):
        self.data = data

    def get(self, run, cds):
        if run in self.data and cds in self.data[run]:
            self.write(json.dumps(self.data[run][cds]))
        else:
            self.write(json.dumps(None))
        self.set_status(200)

    def delete(self, run, cds):
        """
        DELETE {run}/{cds} clears data at path run/cds/
        Always succeeds, even if the resource doesn't exist
        """
        if run in self.data and cds in self.data[run]:
            del self.data[run][cds]
        self.set_status(200)

class StepHandler(tornado.web.RequestHandler):

    def initialize(self, data):
        self.data = data

    def get(self, run, cds, step):
        if run in self.data and cds in self.data[run] and step in self.data[run][cds]:
            self.write(json.dumps(self.data[run][cds][step]))
        else:
            self.write(json.dumps(None))
        self.set_status(200)
    
    def delete(self, run, cds, step):
        """
        DELETE {run}/{cds}/{step} clears data at path run/cds/step
        """
        if run in self.data and cds in self.data[run] and step in self.data[run][cds]:
            del self.data[run][cds][step]
            self.set_status(200)
        else:
            self.set_status(400)

    def patch(self, run, cds, step):
        # PATCH {run}/{cds}/{step} - add a (step => data) entry within the run/cds path
        if run not in self.data or cds not in self.data[run]:
            self.set_status(400)
        else:
            entry = self.data[run][cds]
            entry[step] = json.loads(self.request.body)
            self.set_status(200)

def make_app():
    data = {}   # (run => {})
    return tornado.web.Application([
        (r"/(\w+)", RunHandler, dict(data=data)),
        (r"/(\w+)/(\w+)", CDSHandler, dict(data=data)),
        (r"/(\w+)/(\w+)/([0-9]+)", StepHandler, dict(data=data)),
    ])

def main():
    port = int(sys.argv[1])
    app = make_app()
    app.listen(port)
    print(f'Server is running on http://localhost:{port}')
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()
