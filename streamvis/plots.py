from bokeh.plotting import figure
from bokeh.models import ColumnDataSource
from bokeh.transform import linear_cmap

def make_figure(cds_name, kind, with_color, palette, fig_kwargs, **unused):
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

