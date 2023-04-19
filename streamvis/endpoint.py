import json
from tornado.web import RequestHandler, Application
from dataclasses import dataclass, field

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


class CdsHandler(RequestHandler):

    def initialize(self, state):
        self.state = state 

    def post(self, run_name, mode, cds):
        
        run = self.state.setdefault(run_name, RunState())
        data = json.loads(self.request.body) 

        if mode == 'data':
            ary = run.data.setdefault(cds, [])
            ary.append(data)
        elif mode == 'init_config':
            run.init_cfg[cds] = data
        elif mode == 'update_config':
            run.update_cfg[cds] = data
        elif mode == 'layout':
            run.layout[cds] = data
        else:
            raise RuntimeError('impossible error')
        self.set_status(200)

class TopHandler(RequestHandler):

    def initialize(self, state):
        self.state = state

    def delete(self, run_name):
        if run_name in self.state:
            del self.state[run_name]
        self.set_status(200)

def make_app(state):
    return Application([
        (r"/(\w+)/(data|init_config|update_config|layout)/(\w+)", 
            CdsHandler, dict(state=state)),
        (r"/(\w+)", TopHandler, dict(state=state))
    ])

