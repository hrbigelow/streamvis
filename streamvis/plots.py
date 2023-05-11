from bokeh.plotting import figure
from bokeh.models import ColumnDataSource
from bokeh.transform import linear_cmap

# TODO fix fig_kwargs scoping
def make_figure(name, kind, palette, fig_kwargs):
    """
    create a figure according to configuration information 
    name: the name of a ColumnDataSource instantiated for this plot
    cfg: a map of configuration information
    returns: a bokeh.plotting.figure instance 
    """
    # print(f'make_figure: {name=}, {kind=}, {palette=}, {fig_kwargs=}')
    fig = figure(title=name, **fig_kwargs)
    col_map = dict(x=[], y=[], z=[]) if palette else dict(x=[], y=[])
    cds = ColumnDataSource(col_map, name=name)

    vis_kwargs = { 'source': cds }
    if palette is not None:
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

