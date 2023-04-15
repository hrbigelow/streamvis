import numpy as np
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource
# from bokeh.palettes import TolRainbow, interp_palette
# from bokeh.core.enums import Dimensions
from . import array_util 

def make_figure(cds_name, cfg):
    """
    create a figure according to configuration information 
    cds_name: the name of a ColumnDataSource instantiated for this plot
    cfg: a map of configuration information
    returns: a bokeh.plotting.figure instance 
    """
    kind = cfg['kind']
    fig_kwargs = cfg['fig_kwargs']
    cmap_range = cfg.get('cmap_range', None)

    fig = figure(title=cds_name, **fig_kwargs)
    colnames = 'xy' if cmap_range is None else 'xyz'
    col_map = { col: [] for col in colnames }
    cds = ColumnDataSource(col_map, name=cds_name)

    vis_kwargs = { 'source': cds }
    if cmap_range is not None:
        low, high = cmap_range
        cmap = linear_cmap('z', palette='Viridis256', low=low, high=high)
        if kind == 'multi_line':
            vis_kwargs['line_color'] = cmap 
        else:
            vis_kwargs['color'] = cmap

    if kind == 'scatter':
        fig.scatter(x='x', y='y', **fig_kwargs, **vis_kwargs)
    elif kind == 'line':
        fig.line(x='x', y='y', **fig_kwargs, **vis_kwargs)
    elif kind == 'multi_line':
        fig.multi_line(xs='x', ys='y', **fig_kwargs, **vis_kwargs)
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

def update_data(raw_data, cds, plot_cfg):
    """
    Incorporate `data` into cds, according to rules specified by plot_cfg
    """
    pre_data = np.array(raw_data)
    item_shape = plot_cfg['item_shape']
    append = plot_cfg['append']
    if item_shape == 'bs':
        data = pre_data.reshape(-1, pre_data.shape[-1])
        data = data.transpose().tolist()
        cdata = dict(zip(cds.column_names, data))
        # print(f'cdata = {cdata}')
        if append: 
            cds.stream(cdata)
        else:
            cds.data.update(cdata)
    elif item_shape == 'm':
        data = np.array(raw_data).transpose()
        xdata = np.repeat(data[0:1, :], data.shape[0]-1, 0).tolist()
        ydata = data[1:,:].tolist()
        cds_vals = [cds.data[k] for k in cds.column_names]
        new_vals = [xdata, ydata]
        if len(cds_vals[0]) == 0:
            # first call
            cds.data.update(dict(zip(cds.column_names, new_vals)))
        else:
            for cds_val, new_val in zip(cds_vals, new_vals):
                for cds_elem, new_elem in zip(cds_val, new_val):
                    cds_elem.extend(new_elem)
            cds.data.update(dict(zip(cds.column_names, cds_vals)))

def line_plot(schema, key, xcol_name, yaxis, **line_kwargs):
    cols = { v: [] for v in schema[key] }
    fig = figure(title=key, width=750, height=500, x_axis_label=xcol_name,
            y_axis_label=key, resizable=Dimensions.both)

    cit = iter(interp_palette(TolRainbow[20], len(cols)))
    cds = ColumnDataSource(cols, name=key)
    for y in cols.keys():
        if y != xcol_name:
            fig.line(x=xcol_name, y=y, legend_label=y, source=cds,
                    line_color=next(cit),
                    **line_kwargs)
    fig.legend.location = 'top_right'
    return fig

def vbar(schema, key, xcol_name):
    fig = figure(title=key, height=500, resizable=Dimensions.both)
    counts = schema[key]
    cols = { v: [] for v in schema[key] } 
    ycol_name = next(k for k in schema[key] if k != xcol_name)
    cds = ColumnDataSource(cols, name=key)
    fig.vbar(x=xcol_name, top=ycol_name, width=0.1, source=cds)
    return fig

