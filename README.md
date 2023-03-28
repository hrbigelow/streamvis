# streamvis - interactive visualizations of streaming data with Bokeh

This is a small repo that allows you to design custom visualizations using the Bokeh
library, and define how they update with periodic arrivals of new data.

![Demo](data/demo.gif)

# Install

    pip install git+https://github.com/hrbigelow/streamvis.git

# Setup

```bash
# launch the REST server (test with rest-server-test.sh)
# or, launch on a separate machine

PKG_DIR=$(pip show streamvis | grep Location | cut -f 2 -d ' ')
REST_HOST=localhost
REST_PORT=8080
APP_NAME=demo

streamvis_rest $REST_PORT &

# launch the bokeh server
bokeh serve ${PKG_DIR}/my-app-server.py --args $REST_HOST $REST_PORT $APP_NAME &

# start your data-producing application
python ${PKG_DIR}/my-app-client.py $REST_HOST $REST_PORT $APP_NAME &

# watch your data at localhost:5006/my-app-server
```

# Overview

Streamvis allows you to design a browser based dashboard of interactive
visualizations (using the [bokeh](https://github.com/bokeh/bokeh) package) which
automatically update as new data is produced from a separate process; for example, a
process training a machine learning model that produces various metrics at each
gradient descent step.

Streamvis provides a Client and Server class which communicate through a REST
endpoint (also provided).  The client can send your metrics to the REST endpoint via
a POST request.  The server sends GET requests to the same endpoint and updates the
interactive visualizations.  Because of this decoupling, the client and server may
reside on separate machines.

As the author, you are responsible for writing two functions which will run inside
the Bokeh server: `init_page` and `update_page`.  `init_page` is called once when the
Bokeh server starts.  It defines the structure of the page, which visualizations are
in it and how they are laid out, and the structure of the data sources.  The
`update_page` function updates the contents of these data sources with new data as
it is produced by your client app.

In your client app (the script training your machine learning model, for example),
you call Client.init with any app-specific configuration data that the structure of
the page depends on.  Then, within the training loop, you call Client.send (or
Client.sendl) to periodically send metrics for a given training step.

You can then access the visualizations in your browser at
`localhost:5006/my-app-server` for example.  The visualizations are both interactive
(allowing you to zoom in/out, select points, and generally any interactions that
Bokeh allows), but also will automatically update as new data arrives.

This is much like a TensorBoard session or a `wandb` setup, except that you have more
control over how to set up the visualizations.

# Design

The overall design of this setup is:

Client App ------> REST Endpoint -------> Bokeh Server

Client App:
  - create a `streamvis.Client` instance
  - call `Client.init(app_name, page_config_data)` once
  - periodically call `Client.send(step, key, data)`. updates the `{key: data}` entry map

Bokeh Server (see my-app-server.py example)
  - write an `init_page(doc, page_config_data)` function to build the page
  - write an `update_page(doc, entry)` to process new entries


Streamvis lets you define your own data layer, and adapter to associate and update
that data into Bokeh ColumnDataSources through custom `update_page` that you write.
The only structure it imposes is that each entry is assigned to an integer `step`
value, and the entry must be a `{key: data}` map.  The Streamvis Server object (which
runs in Bokeh server) internally maintains a `next_step` value and updates it to the
highest value seen plus one.  At regular intervals, it will request more data from
the REST server and process it as it arrives.

For example, a `client.init` call:

```python
page_config = ['y1', 'y2', 'y3']
client = streamvis.Client(init_url, update_url)
client.init('my_app', page_config) 
```

Once this call is made in the client, the `streamvis.Server` instance on the server
passes this information into your custom `init_page` function to build the Bokeh
`Document` object.

Then, the client periodically calls `send(step, key, data)` with new pieces of data.
After three calls, the data on the REST server might look like:

```json
{
  "0": {
    "main_plot": {
      "x": [
        0
      ],
      "y1": [
        0.8414709848078965
      ],
      "y2": [
        0.4987474933020272
      ],
      "y3": [
        1.3639461402385225
      ]
    }
  },
  "1": {
    "main_plot": {
      "x": [
        1
      ],
      "y1": [
        0.8912073600614354
      ],
      "y2": [
        0.4998918820946785
      ],
      "y3": [
        1.3193324064263425
      ]
    }
  }
}
```

On the Bokeh server, your `update_page` function is periodically called (default once
per second) on any step data that arrived since the last call, and incorporates it
into the ColumnDataSources instantiated in the `init_page` function.

Some features include:

* The Bokeh host and your data-producer client process host may be different
* The data model is `{step: {key: data} }`, where `data` can be arbitrary JSON
* You define how your data is used to update ColumnDataSources


