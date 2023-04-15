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
        fig.scatter(x='x', y='y', **fig_opts, **vis_kwargs)
    elif kind == 'line':
        fig.line(x='x', y='y', **fig_opts, **vis_kwargs)
    elif kind == 'multi_line':
        fig.multi_line(xs='x', ys='y', **fig_opts, **vis_kwargs)
    return fig

def update_data(raw_data, cds, plot_cfg):
    """
    Incorporate `data` into cds, according to rules specified by plot_cfg
    raw_data: [ item, item, ... ] where item is [ col1_data, col2_data, ... ] 
    The colX_data are assumed to correspond with cds.column_names
    """
    append = plot_cfg['append']

    # do we actually need kind?
    kind = plot_cfg['kind']

    if append:
        data = np.array(raw_data, dtype=object).transpose()
        data = dict(zip(cds.column_names, data))
        cds.stream(data)
    else:
        data = dict(zip(cds.column_names, raw_data[-1]))
        cds.data.update(data)

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

