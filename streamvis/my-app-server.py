import fire
from bokeh.io import curdoc
from bokeh.plotting import figure
from bokeh.layouts import column
from bokeh.models import ColumnDataSource
from streamvis import Server 

def init_page(doc, schema):
    """
    streamvis calls this function once when the first data arrives
    doc: bokeh.io.Document object
    schema: structure extracted from first step data
    """

    cols = { k: [] for k in schema['main_plot'] }
    cds = ColumnDataSource(cols, name='main_plot')
    fig = figure(width=1500, height=600)
    
    for y in schema['main_plot']:
        if y != 'x':
            fig.line(x='x', y=y, source=cds)
    doc.add_root(fig)

def update_page(doc, new_data):
    """
    Every three seconds, streamvis checks if new data has been POSTed to the
    REST service.  If any is available, streamvis calls this function with
    that new_data

    doc: bokeh.io.Document object
    new_data: data sent from the client
    """
    # retrieve the ColumnDataSource by name given in the init_page function
    cds = doc.get_model_by_name('main_plot')
    for step in sorted(new_data.keys(), key=int):
        step_data = new_data[step]

        # 'main_plot' corresponds with the 'key' argument in the client sendl
        # call.
        if 'main_plot' in step_data:
            cds.stream(step_data['main_plot'])

def main(rest_host, rest_port, run_name):
    """
    rest_host: REST service host
    rest_port: REST service port
    run_name: name to scope this run 
    """
    doc = curdoc()
    server = Server(doc, run_name, rest_host, rest_port, init_page, update_page) 
    server.start()

# This is naked here since script is launched using 'bokeh serve'
fire.Fire(main)

