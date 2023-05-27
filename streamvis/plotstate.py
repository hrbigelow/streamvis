import numpy as np

class PlotState:
    """
    State of a plot as accumulated from the log messages.
    TODO: elucidate 
    """
    def __init__(self, name):
        self.name = name
        self.version = 0
        self.glyph_kind = None
        self.palette = None
        self.fig_kwargs = {}
        self.cds_opts = {}
        self.nddata = None
        self.initialized = False

    def update(self, log_entry):
        """
        Updates the plot state from the log_entry
        Incorporate new data into the nddata store
        data: *item_shape; 
        append_dim: the dimension of data to append along, or -1 if replacing
        append_dim must identify one of the dimensions of item_shape
        """
        if log_entry.action == 'init':
            self.version += 1
            self.glyph_kind = log_entry.config['glyph_kind']
            self.palette = log_entry.config['palette']
            self.fig_kwargs = log_entry.config['fig_kwargs']
            self.cds_opts = log_entry.config['cds_opts']
            self.nddata = None
            self.initialized = True

        elif log_entry.action == 'add-data':
            if not self.initialized:
                raise RuntimeError(
                    f'PlotState::update: plot {self.name} received an \'add-data\' '
                    f'message before an \'init\' message.')
            append_dim = self.cds_opts['append_dim']
            new_data = log_entry.tensor_data

            if self.nddata is None or append_dim is None:
                self.nddata = new_data
            elif append_dim in range(new_data.ndim):
                self.nddata = np.concatenate((self.nddata, new_data), axis=append_dim)
            else:
                raise RuntimeError(
                    f'PlotState::update: plot {self.name} {append_dim=} not in bounds of '
                    f'log_entry.tensor_data shape {new_data.shape}')


