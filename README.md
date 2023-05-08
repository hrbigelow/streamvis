# streamvis - interactive visualizations of streaming data with Bokeh

Streamvis allows you to design custom visualizations using the Bokeh
library, and define how they update with periodic arrivals of new data.

# Install

    pip install git+https://github.com/hrbigelow/streamvis.git

# Setup

Streamvis provides a server and a `DataLogger` class to be used with a data-producing
application.  The server consumes data streamed from the application, and provides
the interactive visualization of that data.  The server + application pair can be run
in two modes.  If you can run both on machines with access to the same filesystem,
launch as follows:

```bash
# launch the server, using log file to recieve data from client
streamvis_server file PORT RUN_NAME LOG_FILE_PATH

# start your data-producing application, logging the data only
streamvis_test_app file RUN_NAME LOG_FILE
```

If you need to run the app on a separate machine, you must use Google Pub/Sub to
send data from the app to the server.  For this, you create a Google Cloud project
and enable the Pub/Sub API for the project.

```bash
# launch the server, using Google Pub/Sub subscription (published from app)
streamvis_server pubsub PORT RUN_NAME PROJECT TOPIC [--log_file]

# start your data-producing application, using Google Pub/Sub to publish the data
# optionally log the data as well
streamvis_test_app publish RUN_NAME PROJECT TOPIC [--log_file]
```

When the server and app are run in `pubsub` mode, they communicate exclusively
through Google Pub/Sub.  The app is the publisher, and the Streamvis server is the
subscriber.  The server manages the topic and subscription resources during its
lifetime.

In your app, it is also possible to configure your `DataLogger` instance to log the
data to a file in addition to publishing to the topic.  If you opt to log the data,
you can later retrieve the file, and start a new server in `file` mode to consume and
visualize the log file data.  This provides safety against a server crash while your
app is producing data.

The server can also log data even when in `pubsub` mode.  This log will be identical
to the client log file but with the added convenience of being on the machine running
the server.  Then, in the event of a crash (of either the app, or the server), the
log file is conveniently located.

# Example Application

Streamvis ships with a simple example aplication based on
[test_app.py](streamvis/test_app.py) which is installed as a script called
`streamvis_test_app` mentioned above.  Briefly, the app produces data as it runs.  On
startup, it should instantiate a `DataLogger` instance and intialize it
appropriately.  Then, as data is produced, the logger calls one of the provided API
functions for logging the additional data with various visualization instructions.



```python
from streamvis import DataLogger, ColorSpec, GridSpec

# arbitrary run name for scoping (currently unused)
run_name = 'myapp'
logger = DataLogger(run_name)

# ID of your Google Cloud Platform project with Pub/Sub API enabled
project = 'ml-services-385715' 

# Topic of your choice 
# must be valid topic name, see:
# https://cloud.google.com/pubsub/docs/create-topic#resource_names
topic = 'mytopic'

if using_pubsub:
    logger.init_pubsub(project, topic)

if using_logfile:
    logger.init_write_log(write_log_path)

# specifies rectangular packing layout of plots
grid_map = dict(
        top_left=(0,0,1,1), # (top,left,height,width)
        top_right=(0,1,1,1),
        bottom_left=(1,0,1,1),
        bottom_right=(1,1,1,1)
        )

# set the physical layout in the page for your plots
logger.set_layout(grid_map)

N = 50
L = 20
left_data = np.random.randn(N, 2)

for step in range(10000):
    sleep(1.0)
    top_data = [
            math.sin(1 + step / 10),
            0.5 * math.sin(1.5 + step / 20),
            1.5 * math.sin(2 + step / 15) 
            ]


    left_data = left_data + np.random.randn(N, 2) * 0.1
    layer_mult = np.linspace(0, 10, L)
    data_rank3 = np.random.randn(L,N,2) * layer_mult.reshape(L,1,1)

    logger.tandem_lines('top_left', step, top_data, palette='Viridis256') 

    # Distribute the L dimension along grid cells
    logger.scatter(plot_name='top_right', data=data_rank3, spatial_dim=2,
            append=False, grid=GridSpec(0, 5, 1.2))

    # Colorize the L dimension
    logger.scatter(plot_name='bottom_left', data=data_rank3, spatial_dim=2,
            append=False, color=ColorSpec('Viridis256', 0))

    # data4 = np.random.randn(N,3)
    data4 = np.random.uniform(size=(N,3))

    # Assign color within the spatial_dim
    logger.scatter(plot_name='bottom_right', data=data4, spatial_dim=1,
            append=False, color=ColorSpec('Viridis256'))
```

Here is an example from the
[simple-diffusion](https://github.com/hrbigelow/simple-diffusion/blob/master/swissroll.py)
repository.  Six plots are shown, laid out in a 2x3 grid, including colored scatter
plots and two line plots.  A full video of 1000 training steps can be found
[here](https://mlcrumbs.com/video/swissroll.mp4).

![dashboard](data/dashboard.png)

Above is shown a snapshot of the visualization at `localhost:5006` for example.
The points move as the model trains, and you can zoom in or out interactively for
individual plots.

