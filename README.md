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

# show all scopes in the log file
# streamvis scopes PATH
streamvis scopes gs://bucket/path/to/file
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

The SCHEMA is in yaml format.  It defines how logged data is interpreted by Bokeh to
draw interactive figues.  Some examples can be found in
[data](streamvis/data/aiayn.yaml).

# Introduction

Streamvis provides interactive visualizations for data that is periodically produced
from your application as it is running.  In your application you create a
`streamvis.logger.DataLogger` instance, then call `write` to write data to PATH. 

PATH is an append-only log.  The Streamvis server reads it on startup, and
periodically reads new data appended to it, updating the visualizations dynamically.
The logger periodically appends data as driven by your application.  The data format
is a set of delimited Protobuf messages described by
[data.proto](streamvis/data.proto).  


There is no visualization-specific data logged to PATH.  Data consists of individual
`Point` objects, each `Point` has one or more `Fields`, which is of either a float or
int type.  Also, each `Point` is tagged with a tuple of (scope, name, index) of type
(string, stringm integer).  The `scope` associated with each logged Point is
set when the DataLogger is instantiated with `logger = DataLogger(scope=...)`.  The
name is set with `logger.write(name=..., **data)`.  Finally, the index is set
automatically depending on the broadcasted structure of the data elements, see
`DataLogger::write` for details.

The set of points tagged with a given (scope, name, index) tuple form a glyph.  The
set of glyphs included in a plot is determined by the plot-level yaml field
`name_pattern`, which is set to a regex pattern to select all glyphs whose `name`
matches `name_pattern`.  For example, if this occurs at the top of the yaml file:

```yaml
loss:
  name_pattern: (kldiv|cross_entropy)
```

Then this means all points whose `name` is either `kldiv` or `cross_entropy` (as
logged by `logger.write(name="kldiv", ...)` or `logger.write(name="cross_entropy",
...)` will appear in the plot called `loss`.  This plot can then be viewed using a
URL of:

    http://localhost:5006/?rows=loss

for example.

Multiple glyphs can appear on the same plot.  To specify glyph color, the yaml file
includes a `color` section as follows:

```yaml
loss:
  color:
    formula: name_index # one of: name_index, index_name, name, or index
    palette: Viridis10  # one of the Bokeh palettes, se
    num_colors:         # number of distinct colors to draw from palette
    num_indices:        # used in the `index_name` formula (default 1)
    num_groups:         # used in the `name_index` formula (default 1) 
```


You can use the same PATH for multiple runs of your application.  If two runs are
considered logically the same (such as when you are resuming from a training
checkpoint), use the same `scope` when instantiating the `DataLogger`.

## Some Design Principles

### Separation of data from presentation

The logged data is agnostic to presentation, or membership in any particular
visualization.  The data has a two-level structure - each datum is called a `Point`
and all `Points` are part of exactly one `Group`.  The `Group` has three descriptors
called `scope`, `name`, and `index`.  As mentioned above, `scope` and `name` are used
by the schema to associate `Group`s to each figure.  `index` is used to further group
the data into separate glyphs (currently, lines) within the same figure.

### Easy to combine multiple experiments

At the moment, Streamvis reads from a single log file.  But, this file can include
results from multiple experiments.  Then, multiple schema files can each define
specific subsets of data to visualize.  One possible extension for Streamvis could be
to support multiple log files, however.

### Multiple browser tabs and multi-plot page layouts

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
scopes: (optional, defaults to ".*")  regex to filter scopes
names:  (required) specify this query parameter once per plot mentioned in `cols`.
        This should include a regex to search and match any part of the name field of
        the data.
rows:   (required) semi-colon separated plot rows.  each plot row is a csv string list 
width:  (optional) csv number list of relative plot widths
height: (optional) csv number list of row heights
axes:   (optional) csv list of designators for axis types for each plot.  Each item
        in the list must be one of `lin`, `xlog`, `ylog`, or `xylog`.  defaults to:
        lin,lin,lin,...  (one for each plot)

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
scopes: (optional, defaults to ".*")  regex to filter scopes.  matches anywhere in
        the scope field of the data.
names:  (required) specify this query parameter once per plot mentioned in `cols`.
        This should include a regex to search and match any part of the name field of
        the data.
cols:   (required) semi-colon separated plot columns.  each plot column is a csv string list
width:  (optional) csv number list of column widths
height: (optional) csv number list of relative plot heights
axes:   (optional) csv list of designators for axis types for each plot.  Each item
        in the list must be one of `lin`, `xlog`, `ylog`, or `xylog`.  defaults to:
        lin,lin,lin,...  (one for each plot)

Detail
cols=A,B;C    # Left column contains plots A and B, right column contains plot C
width=2,1     # Left column is 1/3 of page width, right column is 2/3
height=1,2,1  # Plots A and B take up 1/3 and 2/3 of page height.  Plot C is full page height
```

## Python API

```python
from streamvis import script
logfile = "..."
scopes = script.scopes(path=logfile)
names = script.names(path=logfile, scope=scopes[0])

all_data = script.export(path=logfile)
# all_data[(scope, name, index)] = { axis: ndarray }

scope_data = script.export(path=logfile, scope=scopes[0])
# scope_data[(scope, name, index)] = { axis: ndarray }

name_data = script.export(path=logfile, scope=scopes[0], name=names[0])
# name_data[(scope, name, index)] = { axis: ndarray }
```

