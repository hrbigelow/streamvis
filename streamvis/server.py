from bokeh.models import GridBox
from bokeh.layouts import column
from bokeh.plotting import figure
from . import plots

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

    def get_figure(self, cds_name):
        return self.doc.select({ 'type': figure, 'name': cds_name })

    def get_state(self):
        return self.run_state.get(self.run_name, None)

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
            cds = self.doc.get_model_by_name(cds_name)
            if cds is None:
                continue
            update_cfg = state.update_cfg[cds_name]
            plots.update_data(data, cds, **update_cfg)
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

