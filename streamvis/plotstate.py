import numpy as np

class PlotState:
    """
    State of a plot as accumulated from the log messages
    """
    def __init__(self, name):
        self.name = name
        self.version = 0
        self.glyph_kind = None
        self.palette = None
        self.fig_kwargs = {}
        self.cds_opts = {}
        self.nddata = None

    def update(self, log_evt):
        """
        Updates the plot state from the log_evt
        Incorporate new data into the nddata store
        data: *item_shape; 
        append_dim: the dimension of data to append along, or -1 if replacing
        append_dim must identify one of the dimensions of item_shape
        """
        if log_evt.action == 'init':
            self.version += 1
            self.glyph_kind = log_evt.data['glyph_kind']
            self.palette = log_evt.data['palette']
            self.fig_kwargs = log_evt.data['fig_kwargs']
            self.cds_opts = log_evt.data['cds_opts']
            self.nddata = None

        elif log_evt.action == 'add-data':
            append_dim = self.cds_opts['append_dim']
            new_nd = np.array(log_evt.data)
            if not -1 <= append_dim < new_nd.ndim:
                raise RuntimeError(
                    f'PlotState::update: {append_dim=} not in bounds of log_evt.data '
                    f'shape {new_nd.shape}')

            if self.nddata is None:
                empty_shape = list(new_nd.shape)
                if append_dim != -1:
                    empty_shape[append_dim] = 0
                self.nddata = np.empty(empty_shape)
            cur_nd = self.nddata

            if append_dim == -1:
                self.nddata = new_nd 
            else:
                cur_nd = np.concatenate((cur_nd, new_nd), axis=append_dim)
                self.nddata = cur_nd


