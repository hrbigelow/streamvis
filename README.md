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
resource at PATH.  (To create a GCS bucket for example, see [creating a
project](https://developers.google.com/workspace/guides/create-project) and [enabling
APIs](https://developers.google.com/workspace/guides/enable-apis).)

In your data-producing application you instantiate one `DataLogger` object and call
its `write` method to log any data that you produce.  It is buffered logging, so
there is no need to worry about how frequently you call it.  The `write` method can
be used with unbatched or batched data.  This is merely a convenience for the user.
The batched forms of `write` are logically identical to multiple calls of the
unbatched form.

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

The SCHEMA is in yaml format.  It is still in development, but here is a current demo
with some explanation.

```yaml
kldiv:
  # these two are required field, both must be valid regex for selecting which
  # groups of data will be included in this plot
  scope_pattern: .*
  group_pattern: kldiv
  # optional, these are keyword arguments to be provided as-is to the bokeh.plotting.figure
  # constructor as listed here: 
  # https://docs.bokeh.org/en/latest/docs/reference/plotting/figure.html#figure
  figure_kwargs:
    title: KL Divergence (bits)
    x_axis_label: SGD Steps
    y_axis_label: D[q(x_t|x_<t) || p(x_t|x_<t)]

  # required.  currently only supports the value 'line'
  glyph_kind: line

  # optional.  any keyword arguments accepted by the given glyph, as listed here:
  # https://docs.bokeh.org/en/latest/docs/reference/plotting/figure.html#bokeh.plotting.figure.line
  glyph_kwargs:
    line_color: blue

  # required - an ordered list of data column names.  These names must correspond to
  # the names used in the streamvis.logger.DataLogger.write command, for example:
  # l.write('myplot', 0, x=5, y=[1,3,5]).  The order must correspond with the glyph
  # expects
  columns:
    - x
    - y

cross_entropy:
  scope_pattern: .*
  group_pattern: cross_entropy
  figure_kwargs:
    title: Cross Entropy (bits)
    x_axis_label: SGD Steps
    y_axis_label: cross entropy (bits)
  glyph_kind: line
  glyph_kwargs:
    line_color: red
  columns:
    - x
    - y


enc_attn_entropy:
  scope_pattern: .*
  group_pattern: enc_attn_entropy

  # optional - if provided, must identify a name in bokeh.palettes.__palettes__ as
  # described in https://docs.bokeh.org/en/latest/docs/reference/palettes.html.
  # The individual glyph colors will be assigned by using the PointGroup.index field
  # to index into the given palette.
  palette: Viridis6
  figure_kwargs:
    title: Encoder Self Attention Entropy (bits)
    x_axis_label: SGD Steps
    y_axis_label: H(att_t) / log(num targets)
  glyph_kind: line
  columns:
    - x
    - y
```

In the above schema file, there are three top-level keys: `kldiv`, `cross_entropy`
and `enc_attn_entropy`.  Each of these represents a figure to be rendered by the
Streamvis server.  The `scope_pattern` and `group_pattern` attributes provide regex
expressions for selecting PointGroups in the dataset to graph in the figure.
`figure_kwargs` are arguments that are directly passed to the Bokeh `figure`
constructor.  `glyph_kwargs`, similarly, are arguments that are directly passed to
the glyph constructor.  The `columns` attribute specifies the order that the data
fields from the `Points` will be fed into the Bokeh ColumnDataSource.  It should
correspond semantically to the glyph constructor.  (Currently, Streamvis can only
plot line plots).

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
