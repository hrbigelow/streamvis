# streamvis - interactive visualizations of streaming data with Bokeh

# Install

    pip install git+https://github.com/hrbigelow/streamvis.git

## Quick Start: file mode

File mode is used when server and application access the same filesystem.

```bash
# launch the server, using log file to recieve data from client
streamvis_server file PORT RUN_NAME LOG_FILE

# start your data-producing application, logging the data only
streamvis_test_app file RUN_NAME LOG_FILE
```

## (not so) Quick Start: pubsub mode

Pubsub mode enables running the application remotely from the server.


```bash
# launch the server, subscribing to Google Pub/Sub topic 
# optionally logs the data to --log_file
streamvis_server pubsub PORT RUN_NAME PROJECT TOPIC [--log_file]

# start your data-producing application, publishing to topic
# optionally logs the data to --log_file 
streamvis_test_app pubsub RUN_NAME PROJECT TOPIC [--log_file]
```  

To use this mode, you must first create a Pub/Sub API-enabled GCP project and Pub/Sub
topic.  See Google's documentation on 
[creating a project](https://developers.google.com/workspace/guides/create-project), 
[enabling APIs](https://developers.google.com/workspace/guides/enable-apis), and 
[creating a topic](https://cloud.google.com/pubsub/docs/create-topic#create_a_topic).

# Introduction

Streamvis provides interactive visualizations for data that is periodically produced
from your application as it is running.  You create a `streamvis.logger.DataLogger`
instance in your application, then call its plotting API functions to log data in
different plot formats to a named plot of your choice.  

Using the server, you can visualize the data interactively.  The content of the plots
will automatically update as new data is logged.

## Design multi-plot page layouts

If you have several different plots, you may want to view subsets of them in
different layouts that you don't know ahead of time.  The `streamvis_server` lets you
do this using parameters to specify layouts organized as rows or columns.

### Row-based layout

```
localhost:5006/?rows=A,B;C&width=1,2,1&height=1,2

+----------+----+
|     A    | B  |
+----------+----+
|               |
|       C       |
|               |
+---------------+

Query parameters:
rows:   (required) semi-colon separated plot rows.  each plot row is a csv string list 
width:  (optional) csv number list of relative plot widths
height: (optional) csv number list of row heights

Detail
rows=A,B;C   # Top row contains plots A and B.  Bottom row is plot C
width=1,2,1  # Plots A and B are 1/3 and 2/3 of page width.  Plot C is full page width
height=1,2   # rows are 1/3 and 2/3 of page height, respectively 
```

For row mode layout, the `rows` parameter is required.  The `width` and `height`
parameters default to a list of 1's, which means each plot in a row will take an
equal share of width, and each row in the page an equal share of height.

### Column-based layout

```
localhost:5006/?cols=A,B;C&width=2,1&height=1,2,1

+-----+---------+
|  A  |         |
+-----+         |
|     |    C    |
|  B  |         |
|     |         |
+-----+---------+

Query parameters
cols:   (required) semi-colon separated plot columns.  each plot column is a csv string list
width:  (optional) csv number list of column widths
height: (optional) csv number list of relative plot heights

Detail
cols=A,B;C    # Left column contains plots A and B, right column contains plot C
width=2,1     # Left column is 1/3 of page width, right column is 2/3
height=1,2,1  # Plots A and B take up 1/3 and 2/3 of page height.  Plot C is full page height
```

## Detail 

Communication between the `DataLogger` and `streamvis_server` may be either in `file`
mode or `pubsub` mode.  The `pubsub` mode requires a Google Cloud project with
Pub/Sub API enabled.  It is harder to set up but allows you to run your application
on the cloud while visualizing the data locally.  For example, you can run your
application in a Google Colab, Kaggle Notebook, or Google Cloud compute instance, but
run the `streamvis_server` on your laptop.

When the server and app are run in `pubsub` mode, they communicate through Google
Pub/Sub.  The app is the publisher, and the Streamvis server is the subscriber.  The
server creates and deletes a Pub/Sub subscription resource during its lifetime.
However, you must provide the Pub/Sub API-enabled project, and create a topic.

In your app, it is also possible to configure your `DataLogger` instance to log the
data to a file in addition to publishing to the topic.  If you opt to log the data,
you can later retrieve the file, and start a new server in `file` mode to consume and
visualize the log file data.  This provides safety against a server crash while your
app is producing data.

The server can also log data even when in `pubsub` mode.  This log will be identical
to the client log file but with the added convenience of being on the machine running
the server.  Then, in the event of a crash (of either the app, or the server), the
log file is conveniently located.

## Append-only logging

When the `DataLogger` or the server write to a log file, it is always append-only.
Neither one will truncate an existing log file.  This design is in line with the
semantics of logging.  Semantically, the `DataLogger::clear` function call produces a
message to empty the server state.  This effectively removes any data that has
accumulated up until that point in the log.    

# Example Application

Streamvis ships with a simple example aplication based on
[test_app.py](streamvis/test_app.py) which is installed as a script called
`streamvis_test_app` mentioned above.  Briefly, the app produces data as it runs.  On
startup, it should instantiate a `DataLogger` instance and intialize it
appropriately.  Then, as data is produced, the logger calls one of the provided API
functions for logging the additional data with various visualization instructions.



```python
from streamvis import DataLogger, GridSpec

# arbitrary run name for scoping (currently unused)
run_name = 'myapp'
logger = DataLogger(run_name)

# ID of your Google Cloud Platform project with Pub/Sub API enabled
project = 'ml-services-385715' 

# identifies your pre-existing Pub/Sub topic 
topic = 'mytopic'

if using_pubsub:
    logger.init_pubsub(project, topic)

if using_logfile:
    logger.init_write_log(write_log_path)

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
            append=False))

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

