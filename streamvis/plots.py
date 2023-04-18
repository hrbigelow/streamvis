import numpy as np
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource
from bokeh.transform import linear_cmap
from . import array_util 

def make_figure(cds_name, kind, with_color, palette, fig_kwargs):
    """
    create a figure according to configuration information 
    cds_name: the name of a ColumnDataSource instantiated for this plot
    cfg: a map of configuration information
    returns: a bokeh.plotting.figure instance 
    """
    # print(f'make_figure: {cds_name}, kind={kind}, with_color={with_color}')

    fig = figure(title=cds_name, **fig_kwargs)
    col_map = dict(x=[], y=[], z=[]) if with_color else dict(x=[], y=[])
    cds = ColumnDataSource(col_map, name=cds_name)

    vis_kwargs = { 'source': cds }
    if with_color:
        cmap = linear_cmap('z', palette=palette, low=0, high=1)
        if kind == 'multi_line':
            vis_kwargs['line_color'] = cmap 
        else:
            vis_kwargs['color'] = cmap

    # print(f'vis_kwargs = {vis_kwargs}')
    if kind == 'scatter':
        fig.scatter(x='x', y='y', **vis_kwargs)
    elif kind == 'line':
        fig.line(x='x', y='y', **vis_kwargs)
    elif kind == 'multi_line':
        fig.multi_line(xs='x', ys='y', **vis_kwargs)
    return fig

"""
raw_data is a list of items.  plot_cfg['item_shape'] describes the shape of each
item.  An item is a (possibly nested) list of numbers.  

the item_shape is a string of one-letter-codes, one for each dimension of nesting,
with the following meanings:

b: a batch of independent data points.
s: dimension of size 2 representing x,y values
m: dimension of size k+1 representing x,y1,y2,...,yk values
"""

def update_data(raw_data, cds, item_shape, append, **kwargs):
    """
    Incorporate `data` into cds, according to rules specified by cfg
    raw_data: list of items
    """
    # import pdb
    # pdb.set_trace()
    if item_shape == 'bs':
        pre_data = np.array(raw_data)
        if append:
            data = pre_data.reshape(-1, pre_data.shape[-1])
        else:
            data = pre_data[-1,:,:]

        data = data.transpose().tolist()
        cdata = dict(zip(cds.column_names, data))
        if append: 
            cds.stream(cdata)
        else:
            cds.data = cdata
    elif item_shape == 'm':
        data = np.array(raw_data).transpose()
        K = data.shape[0]-1 # number of lines
        xdata = np.repeat(data[0:1, :], K, 0).tolist()
        ydata = data[1:,:].tolist()
        cds_vals = [cds.data[k] for k in 'xy']
        if len(cds_vals[0]) == 0:
            # first call
            cdata = dict(x=xdata, y=ydata)
            if 'z' in cds.column_names:
                cdata['z'] = np.linspace(0, 1, K).tolist()
            cds.data = cdata

        else:
            new_vals = [xdata, ydata]
            for cds_val, new_val in zip(cds_vals, new_vals):
                for cds_elem, new_elem in zip(cds_val, new_val):
                    cds_elem.extend(new_elem)
            cdata = dict(x=cds_vals[0], y=cds_vals[1])
            if 'z' in cds.data:
                cdata['z'] = cds.data['z']
            cds.data = cdata

