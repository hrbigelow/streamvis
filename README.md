# streamvis - interactive visualizations of streaming data with Bokeh

# Introduction

Streamvis logger allows you to log analytic data progressively as your program runs.  This
data then streams automatically into visualizations without any UI intervention.
Visualizations themselves are somewhat interactive, allowing zoom and scroll.  They are
powered by Bokeh and use webGL acceleration - so that scatter plots with many thousands of
points are performant.


# Install

    pip install git+https://github.com/hrbigelow/streamvis.git

## Quick Start

```sh
IP=100.68.200.91
GRPC_PORT=8081
GRPC_URI=$IP:GRPC_PORT
WEB_URI=$IP:8888
DATA_PREFIX=/data/test
SCHEMA_FILE=streamvis.yaml
DEMO_SCOPE=run24
NUM_STEPS=2000

# start the data server
streamvis grpc-serve $DATA_PREFIX $GRPC_PORT

# start the web server
streamvis web-serve $WEB_URI $GRPC_URI $SCHEMA_FILE 

# run a test data producing demo app 
streamvis demo $DEMO_SCOPE $NUM_STEPS

# list scopes logged so far 
streamvis scopes $GRPC_URI  

# list names logged under scope
streamvis names $GRPC_URI $DEMO_SCOPE
```

# Logging Data

There is both a sync and async API for logging data in your application.  Both involve
first instantiating the logger:

```python
logger = DataLogger(scope=scope, delete_existing=True)
logger.init(grpc_uri=uri, flush_every=1.0, tensor_type="numpy")
```

The `flush_every` parameter is only relevant for the async API.  Using this, you would use
its async context manager as follows:

```python
# This starts the flushing task
async with logger:
    await logger.write_config({ "start-time": time.time() })
    ...
    # Choose any name (i.e. "top_left") you would like your data tagged with
    # Choose any column names - here "x" and "y" are used
    await logger.write("top_left", x=xs, y=top_data)

    # Here, "x", "y", and "t" are used.
    xs, ys = points[:,0], points[:,1]
    await logger.write('cloud', x=xs, y=ys, t=step)

# When the logger context manager exits, all remaining queued data is flushed
```

Streamvis is built for efficient logging of data - in particular, it accepts tensor types
of jax, torch and numpy data.  Importantly, the tensor data is not transferred over to CPU
when `logger.write` is called, but rather during the flush.  So, you may freely log
individual SGD steps without causing any GPU<=>CPU synchronization.  The only thing that
forces transfer of tensor data from GPU to CPU is the flush call, at an interval of your
choosing.


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

## Python Local File API

Command-line

```bash
streamvis scopes /data/streamvis.log
fig4-bos-17-coin-sm-c100
fig4-bos-23-coin-sm-c100
fig4-bos-1-coin-sm-c100
...

streamvis names /data/streamvis.log "fig4-bos-1-coin-sm-c100"
xent-process-ent-ratio
sem-cross-entropy
sem-kl-divergence
eval-kl-divergence
...

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

## Python Remote Fetch API

This API does not require you to download the log file.  It is more convenient when the
log file is updated more frequently so that you don't have to repeatedly download a log
file just to receive updates.


```bash
# on the machine hosting the log file and index file
# find the public IP address
g347 $ hostname -I
10.15.36.97 172.17.0.1 100.68.200.91 fd7a:115c:a1e0::5601:c85b
# launch the server
$ python -m streamvis.grpc_server /data/streamvis.log 8081
```
Then, on the machine you want to consume the data:

Command-line:

```bash
$ streamvis scopes 100.68.200.91:8081
fig4-bos-17-coin-sm-c100
fig4-bos-23-coin-sm-c100
fig4-bos-1-coin-sm-c100
...

$ streamvis names 100.68.200.91:8081 fig4-bos-17-coin-sm-c100
xent-process-ent-ratio
sem-cross-entropy
sem-kl-divergence
eval-kl-divergence
...
```

Python:


```python
from streamvis import script

# uri of the remote fetch server (see gfile_server.py)
# uri is public_ip_addr:port
uri="100.68.200.91:8081"

scopes = script.gscopes(uri=uri)
names = script.gnames(uri=uri, scope=scopes[0])

all_data = script.gfetch_sync(uri=uri)
# all_data[(scope, name, index)] = { axis: ndarray }

scope_data = script.gfetch_sync(uri, scopes[0])
# scope_data[(scope, name, index)] = { axis: ndarray }

name_data = script.gfetch_sync(uri, scopes[0], names[0])
# name_data[(scope, name, index)] = { axis: ndarray }

```


