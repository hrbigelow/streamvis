# streamvis - interactive visualizations of streaming data with Bokeh

# Install

    pip install git+https://github.com/hrbigelow/streamvis.git

## Quick Start

```sh
# start the server
# streamvis serve PORT SCHEMA PATH
streamvis serve 5006 data/demo.yaml gs://bucket/path/to/file
streamvis serve 5006 data/demo.yaml s3://bucket/path/to/file
streamvis serve 5006 data/demo.yaml hdfs://bucket/path/to/file
streamvis serve 5006 data/demo.yaml /path/to/file

# run a test data producing demo app 
# streamvis demo SCOPE PATH
streamvis demo run24 gs://bucket/path/to/file

# summarize the data in a log file
# streamvis list PATH
streamvis list gs://bucket/path/to/file
```

Starts the web server on localhost:PORT, using the yaml SCHEMA file to configure how the
data in PATH is plotted.  PATH may be any locator accepted by
[tf.io.gfile.GFile](https://www.tensorflow.org/api_docs/python/tf/io/gfile/GFile).
Visit localhost:PORT to see interactive plots, and watch the data progressively
appear as your data-producing application runs.

The non-local (`gs://` etc) forms of PATH enable you to run your data producing
application and the server on different machines, and communicate through the shared
resource at PATH.  

In your data-producing application:

```python
from streamvis.logger import DataLogger
# `scope` is a name that will be applied to all data points produced by this process
logger = DataLogger(scope='run24')
logger.init(path='gs://bucket/path/to/file', buffer_max_elem=100) 

...
for step in range(100):
    # generate some data and log it. 
    # This is the 0D (scalar) logging
    logger.write('kldiv', x=step, y=some_kldiv_val)
    logger.write('weight_norm', x=step, y=some_norm_val)

# or, log in batches of points (1D logging) 
# accepts Python list, numpy, jax/pytorch/tensorflow tensors
for step in range(100, 200, 10):
    logger.write('kldiv', x=list(range(step, step+10)), y=kldiv_val_list)
    logger.write('weight_norm', x=list(range(step, step+10)), y=norm_val_list)

# or, log to a series of plots
for step in range(200, 300, 10):
    # attn[layer, point], value at `layer` for a particular step
    logger.write('attn_layer', x=list(range(step, step+10)), y=attn)

# buffer is flushed automatically every `buffer_max_elem` data points, but
# you may call this at the end or at an interrupt handler:
logger.flush_buffer()
```

The SCHEMA is in yaml format, for example:

```yaml
loss:
  scope_pattern: run2?
  group_pattern: (kldiv|weight_norm)
  glyph: line
  columns:
    - x
    - y
```

This entry says that a line plot called `loss` will be created.  It will create one
line for each group of (x,y) points logged.  The groups of points included in the
plot are those with `group_name` matching `group_pattern` and `scope` matching
`scope_pattern`.  The scope allows one to log multiple independent runs of your
application to the same log file, and then create plots with specific subsets of
these groups.

In the above example, 

To create a GCS bucket for example, see [creating a
project](https://developers.google.com/workspace/guides/create-project) and [enabling
APIs](https://developers.google.com/workspace/guides/enable-apis)

# Introduction

Streamvis provides interactive visualizations for data that is periodically produced
from your application as it is running.  In your application you create a
`streamvis.logger.DataLogger` instance, then call `write` to write data to PATH. 

PATH is an append-only log.  The Streamvis server reads it on startup, and
periodically reads new data appended to it, updating the visualizations dynamically.
The logger periodically appends data as driven by your application.  The data format
is a set of delimited Protobuf messages described by
[data.proto](streamvis/data.proto).  

There is no visualization-specific data logged to PATH,  However, data points
(`Point` protobuf message) logically refer to one `PointGroup` protobuf via the
`group_id` field.  The SCHEMA provided to the server allows you to define which
`PointGroup`s should be included in a given plot.

You can use the same PATH for multiple runs of your application.  If two runs are
considered logically the same (such as when you are resuming from a training
checkpoint), use the same `scope` when instantiating the `DataLogger`.

## Multiple browser tabs and multi-plot page layouts

If you have several different plots, you may want to view subsets of them in
different browser tabs, and design page layouts that you don't know ahead of time.
The `streamvis_server` lets you do this using URL parameters to specify layouts
organized as rows or columns.

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
