# streamvis - interactive visualizations of streaming data with Bokeh

This is a small repo that allows you to design custom visualizations using the Bokeh
library, and define how they update with periodic arrivals of new data.

![Demo](data/demo.gif)

# Install

    pip install git+https://github.com/hrbigelow/streamvis.git

# Setup

```bash
# launch the server 
streamvis_server --rest_port 8080 --bokeh_port 5006 --run_name myapp &

# start your data-producing application
python my-app-client.py localhost:8080 myapp &

# watch and interact with your data at localhost:5006
```

# Overview

Streamvis allows you to design a browser based dashboard of interactive
visualizations (using the [bokeh](https://github.com/bokeh/bokeh) package) which
automatically update and re-configure as new data is produced from a separate
process; for example, a process training a machine learning model that produces
various metrics at each gradient descent step.

Streamvis provides a Client which POSTs your data to the REST endpoint provided by
`streamvis_server`, which then creates visualizations from this data.

In your client app (for example, `my-app-client.py`):

```python
from streamvis import Client

# point your client to the running sv_rest_server service
client = Client(rest_uri, run_name)

# clear the data on the REST server associated with run_name
client.clear()

# specifies rectangular packing layout of plots
grid_map = dict(
        top_plot=(0,0,1,2), # (top,left,height,width)
        low_left_plot=(1,0,2,1),
        low_right_plot=(1,1,2,2)
        )

# set the physical layout in the page for your plots
client.set_layout(grid_map)

# Generate some data 
N = 1000
left_data = np.random.randn(N, 2)

for step in range(10000):
    sleep(0.2)
    top_data = [
            step,
            math.sin(1 + step / 10),
            0.5 * math.sin(1.5 + step / 20),
            1.5 * math.sin(2 + step / 15) 
            ]

    left_data = left_data + np.random.randn(N, 2) * 0.1

    client.tandem_lines('top_plot', top_data) 
    client.scatter('low_left_plot', left_data.tolist(), spatial_dim=1, append=False)
```

You can then access the visualizations in your browser at `localhost:5006` for
example.  The visualizations are both interactive (allowing you to zoom in/out, etc),
and automatically update as new data arrives.

