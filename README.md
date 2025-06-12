# Interactive, streaming data WebGL visualizations

# Introduction

Streamvis logger allows you to log analytic data progressively as your program runs.  This
data then streams automatically into visualizations without any UI intervention.
Visualizations themselves are somewhat interactive, allowing zoom and scroll.  They are
powered by Bokeh and use webGL acceleration - so that scatter plots with many thousands of
points are responsive when zooming or panning.

# Design Philosophy

**Flexible Data Collection**:  Streamvis decouples data writing (calls to `write`) from data
grouping.  All data logged are tagged with a `(scope, name, index)` tuple.  Within one
tag, data are in columnar format - i.e. a dictionary of column names and same-length 1D
arrays of numbers.  A call to `write` always produces data for a single `(scope, name)`
setting, but may produce data under multiple `index` values. 

**Deferred Visualization Decisions** - downstream visualizations can specify which `(scope,
name, index)` tuples will be included in the visualization, and what type of visualization
is used (scatter plot or line plot) and how the `(scope, name, index)` tag is interpreted
for the visualization.  This decision can be made any time, while data are in the process
of being logged, or after.

**Efficiency** - Streamvis is built for efficient logging of data - in particular, it
accepts tensor types of jax, torch and numpy data.  The tensor data is not transferred
over to CPU when `logger.write` is called, but rather during the flush.  So, you may
call `write` frequently with small amounts of data, without causing any transfer of data
from GPU to CPU.  The only transfers happen during `flush_buffer()` which happen at
a fixed frequency of your choosing.


# Install

    pip install git+https://github.com/hrbigelow/streamvis.git

## Quick Start

```sh
IP=100.68.200.91
GRPC_PORT=8081
GRPC_URI=$IP:GRPC_PORT
WEB_URI=$IP:8888
DATA_PREFIX=/data/test
SCHEMA_FILE=./data/demo.yaml  # see demo.yaml in this repo
DEMO_SCOPE=run24
NUM_STEPS=2000

# start the data server
streamvis grpc-serve $DATA_PREFIX $GRPC_PORT

# start the web server
streamvis web-serve $WEB_URI $GRPC_URI $SCHEMA_FILE 

# run a test data producing demo app 
streamvis logging-demo $DEMO_SCOPE $NUM_STEPS

# list scopes logged so far 
streamvis scopes $GRPC_URI  

# list names logged under scope
streamvis names $GRPC_URI $DEMO_SCOPE
```

# Logging Data

There is both a sync and async API for logging data in your application.
## Sync Logging API

The synchronous logging API is easiest to use.  The main drawback is that it is harder to
control how often the buffer is flushed.  Also, you must manually call `init_scope` at the
beginning, and `flush_buffer()` periodically.

```bash
streamvis logging-demo $GRPC_URI $SCOPE
```

See [streamvis/demo_sync.py](streamvis/demo_sync.py)

```python
import time
from .logger import DataLogger
from .demo_funcs import Cloud, Sinusoidal

def demo_log_data(grpc_uri, scope, num_steps):
    """Demo of the Synchronous DataLogger."""
    logger = DataLogger(
        scope=scope, 
        grpc_uri=grpc_uri,
        tensor_type="numpy",
        delete_existing=True,
    )

    cloud = Cloud(num_points=10000, num_steps=num_steps)
    sinusoidal = Sinusoidal()

    logger.init_scope()
    logger.write_config({ "start-time": time.time() })

    for step in range(0, num_steps, 10):
        time.sleep(0.1)

        xs, top_data = sinusoidal.step(step)
        logger.write('sinusoidal', x=xs, y=top_data)

        points = cloud.step(step)
        xs, ys = points[:,0], points[:,1]
        logger.write('cloud', x=xs, y=ys, t=step)

        if step % 10 == 0:
            print(f'Logged {step=}')

        if step % 100 == 0:
            logger.flush_buffer()

    # final flush
    logger.flush_buffer()
```

## Async Logging API

The async logging API allows you to specify buffer flush frequency, and you don't have to
call `init_scope()` or `flush_buffer()` at all.


```bash
streamvis logging-demo-async $GRPC_URI $SCOPE
```

See [streamvis/demo_async.py](streamvis/demo_async.py)

```python
import time
from streamvis.logger import AsyncDataLogger
from .demo_funcs import Cloud, Sinusoidal

async def demo_log_data_async(grpc_uri, scope, num_steps):
    """Demo of the AsyncDataLogger."""
    logger = AsyncDataLogger(
        scope=scope, 
        grpc_uri=grpc_uri, 
        tensor_type="numpy", 
        delete_existing=True, 
        flush_every=1.0,
    )

    cloud = Cloud(num_points=10000, num_steps=num_steps)
    sinusoidal = Sinusoidal()

    async with logger:
        logger.write_config({ "start-time": time.time() })

        for step in range(0, num_steps, 10):
            time.sleep(0.1)
            # top_data[group, point], where group is a logical grouping of points that
            # form a line, and point is one of those points
            xs, top_data = sinusoidal.step(step)
            await logger.write('sinusoidal', x=xs, y=top_data)

            points = cloud.step(step)
            xs, ys = points[:,0], points[:,1]
            await logger.write('cloud', x=xs, y=ys, t=step)

            if step % 10 == 0:
                print(f'Logged {step=}')
```

# Fetching logged data 

## Python Fetch API

```python
from streamvis import script
grpc_uri="100.68.200.91:8081"

scopes = script.scopes(uri=grpc_uri)
names = script.names(uri=grpc_uri, scope=scopes[0])

all_data = script.fetch(uri=grpc_uri)
# all_data[(scope, name, index)] = { axis: ndarray }

scope_data = script.fetch(grpc_uri, scopes[0])
# scope_data[(scope, name, index)] = { axis: ndarray }

name_data = script.fetch(uri, scopes[0], names[0])
# name_data[(scope, name, index)] = { axis: ndarray }

# fetch the data that was written using logger.write_config(...)
config_data = script.config(uri, scopes[0]) 
```

# Interactive Visualization with Bokeh Server


## Yaml syntax


# URI API

### Multiple browser tabs and multi-plot page layouts

If you have several different plots, you may want to view subsets of them in
different browser tabs, and design page layouts that you don't know ahead of time.

### Row-based layout

```
$WEB_URI/?rows=A,B;C&width=1,2,1&height=1,2

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


## Python Data API


```bash
$ streamvis scopes $GRPC_URI
fig4-bos-17-coin-sm-c100
fig4-bos-23-coin-sm-c100
fig4-bos-1-coin-sm-c100
...

$ streamvis names $GRPC_URI fig4-bos-17-coin-sm-c100
xent-process-ent-ratio
sem-cross-entropy
sem-kl-divergence
eval-kl-divergence
...
```

