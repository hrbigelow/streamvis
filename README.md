# Interactive, streaming data WebGPU visualizations

# Introduction

Streamvis consists of a python data logger, a web search interface for searching the
database, and a web application supporting interactive visualizations of the logged
data.  The interactive visualizations also auto-refresh as new data is logged.

# Motivation

There is a basic tension between a data logging and a data visualization application.
Any single visualization requires the dataset to be of a consistent shape, but this
shape may not be known when data is logged.  The design choices Streamvis makes are
trying to thread this needle, allowing for the lowest possible barrier for logging
while enforcing a minimal constraint to make downstream visualizations possible.

The two top-level constraints Streamvis logging enforces are:

1) all data is semantically well typed, using user-defined types
2) all logged data is rectangular, with no missing values

The first constraint is maintained by defining the notion of a `Field`, which
consists of a name, data type (one of int, float, string, or bool) and text description.
`Field`s are first-class objects in the database.  Every scalar value of data logged
must be of a particular `Field`.

The second requirement is that, when data are logged, there is always a one-to-one
correspondence between each individual value logged to each `Field`.  For example, if
you are logging stock data to `trade-closing-time` and `sale-price` fields (you can
imagine the proper data types and descriptions that could go with these), the logger
enforces a one-to-one correspondence in values logged.

The second requirement is enforced as follows.  First, another object called a
`Series` must be created.  A `Series` is also a first-class object, and it is a named
collection of `Field`s, which must be pre-existing.  All calls to the `logger.write`
function log data to an existing `Series`, and the function enforces there are no
missing values, and that the same number of values are logged to each `Field` in the
`Series`.

The total set of data logged to one or more `Series` during the lifetime of a logging
script belong to a `Run`, which is automatically created when the logger first calls
`write`.  A `Run` can't be given a name, however, one can attach zero or more
string-valued `tags` to it, as well as define `Attribute` values.  Each `Attribute`
is actually a single global `Field` value associated with the `Run`.  Importantly,
because it is a single value, it logically can be used together with any `Field` in
any `Series` and broadcasted across the data in the run, ensuring the rectangularity
of the collection of data.

## Restrictions

Due to the constraints of `logger.write`, data logged to a given `Series` across all
Runs is guaranteed to be rectangular.  But to ensure this, once a `Series` is
created, the collection of `Fields` in it cannot be changed.  This presents a
difficulty if, after logging data for several runs, the user wants to update their
workflow to include another field.  The only way to do this is to create a new `Series`
with the same set of `Field`s, and the extra one.

## Filtering Data

As mentioned above, each Run may have a set of tags attached to it, as well as a set
of Attribute values.  It also has a 'started_at' timestamp useful for searching.  The
web search interface allows an interactive search across runs based on all of this
information.  Such a search defines a subset of Runs.  From there, a choice of a
`Series` and subset of `Field`s within the `Series`, together with a choice of Attribute
values define a rectangular dataset of `Field`s collectively from both the `Series` and
Attribute values.

Having defined this, it only remains to bind each selected `Field` (whether `Series`
`Field` or Attribute Value) to a given conceptual plot axis.  Plot axes include x-axis,
y-axis, z-axis, color, line-grouping, line-point-order (for line plots), point-size.
Some restrictions apply however.  For spatial axes line-point-order and point-size,
only the float and int types are supported.  For color and line-grouping, int,
string, and bool types are supported.

This is where the flexibility of the approach shines.  One can log in general many
different `Field`s in the form of a rich `Series` plus Attributes, and much later choose
how to plot it.  Also, one need not choose ahead of time which Runs need to be
plotted together with each other.

## Deleting or overwriting a Run

As mentioned above, all data logged during the lifetime of the logger is logged to a
single `Run`.  All Run objects receive a permanent UUID handle.  To this end, the
Streamvis logger provides a `set_run_handle` API function to optionally specify the
UUID to be assigned.  If not set, the system automatically generates a new UUID.

If `set_run_handle` is called, and a Run already exists with that handle, it is
deleted before the logging starts.  This pattern is useful during development to
avoid accumulating data that's just the result of a buggy script.

Notably, the streaming visualization also respects deletion of data, so you can just
leave the window open, and plots will disappear and reappear with your newly logged
data.

So, during the development cycle, the recommended pattern then is to call
`set_run_handle` with the same UUID for each launch of your script.  Any existing run
will be deleted and you can avoid accumulating junk.

Then, when you are ready to deploy additional runs, don't call the API function, and
the system will autogenerate a new UUID.

# Quickstart

1. Install PostgreSQL server and build the database artifacts

```bash
psql -U streamvis -d streamvis -f db/deploy.sql
```

2. Compile the go gRPC binary

```bash
cd streamvis/pier
go build ./cmd/pier

# needed for the gRPC server
export STREAMVIS_DB_URI=postgresql://streamvis:streamvis@localhost/streamvis
./pier -port 8001 &
```

3. Check gRPC endpoints
```bash
$ grpcurl --plaintext localhost:8001 list streamvis.v1.Service
streamvis.v1.Service.AppendToSeries
streamvis.v1.Service.CreateAttribute
streamvis.v1.Service.CreateRun
streamvis.v1.Service.CreateSeries
streamvis.v1.Service.DeleteEmptySeries
streamvis.v1.Service.DeleteRun
streamvis.v1.Service.ListAttributes
streamvis.v1.Service.ListRuns
streamvis.v1.Service.ListSeries
streamvis.v1.Service.ReplaceRun
streamvis.v1.Service.SetRunAttributes

$ grpcurl --plaintext localhost:8001 describe streamvis.v1.Service.CreateAttribute
streamvis.v1.Service.CreateAttribute is a method:
rpc CreateAttribute ( .streamvis.v1.CreateAttributeRequest ) returns ( .streamvis.v1.CreateAttributeResponse );
$ grpcurl --plaintext localhost:8001 describe streamvis.v1.CreateAttributeRequest
streamvis.v1.CreateAttributeRequest is a message:
message CreateAttributeRequest {
  string attr_name = 1;
  string attr_type = 2;
  string attr_desc = 3;
}
```

# Logging workflow

The Streamvis logging API doesn't log data directly with every call to
`logger.write`.  Rather, it is configured to flush the data every `flush_every`
seconds (specified by the user).  To do this, the `DataLogger` class runs a flushing
function in a separate thread, while the `AsyncDataLogger` uses a coroutine.

## Metadata setup

Before any data can be logged, two kinds of metadata must be defined:  `Field`s and
`Series`.  `Field`s are individual named (and typed) fields:

```bash
# streamvis create-field <field-name> <field-type> <field-desc>
streamvis create-field sgd_step int "The SGD gradient step of training"
streamvis create-field noisy_channel_epsilon float "Probability of mutating an emitted symbol"
streamvis create-field with_BOS_token bool "Whether BOS token was used in generation"
streamvis create-field experiment_name string "Name of the overall experiment"

grpcurl --plaintext localhost:8001 streamvis.v1.Service/ListFields
{
  "fieldHandle": "3d0422df-e059-472d-a996-0c423ec5fc59",
  "fieldName": "sgd_step",
  "fieldType": "int",
  "fieldDesc": "The SGD gradient step of training"
}
{
  "fieldHandle": "eaec4fe0-9876-4116-a9ab-883792649a60",
  "fieldName": "noisy_channel_epsilon",
  "fieldType": "float",
  "fieldDesc": "Probability of mutating an emitted symbol"
}
{
  "fieldHandle": "32eb2ef8-a64e-4a4c-906f-f7a6dc8736d7",
  "fieldName": "with_BOS_token",
  "fieldType": "bool",
  "fieldDesc": "Whether BOS token was used in generation"
}
{
  "fieldHandle": "0fa61144-2a6b-4fbd-8910-7ffc107c4b0e",
  "fieldName": "experiment_name",
  "fieldType": "text",
  "fieldDesc": "Name of the overall experiment"
}

```

Note that the `fieldName` strings are used directly in `logger.write`:

```python
def write(self, series_name: str, /, **field_values):
   ...
```

so, it is more convenient to use strings that are legal Python argument identifiers.
But you can define a dictionary with arbitrary string keys and use unpacking as in: 

```python
field_values = { 'experiment-name': 'process', 'sgd-step': 10 }
write('all', **field_values)
```

A `Series` is a named collection of these `Field`s.  The order of `Field`s doesn't
matter.

```bash
# streamvis create-series <series-name> <field-name> <field-name> ... 
streamvis create-series training-analysis sgd-step 
```


## Broadcasting during `write`, and late materialization on CPU

The values of the `field_values` dictionary can be any shapes and types (Python
scalars or lists, numpy, jax, or pytorch tensors) as long as the data type is
consistent with the declared data type of the corresponding `Field` named in the
`field_values` dictionary.  They also must be broadcastable together.  The shapes you
choose are merely for your own convenience in logging the data.  Logically, because
the values are broadcastable, they simply define a set of rectangular data
(one-to-one correspondence between values for each field).

Secondly, no data is actually written at the moment `logger.write` is called.
Instead, it is enqueued, and will be written at the next `flush` (at a user-specified
time interval).  Importantly, any tensor values which reside on GPU are not moved to
CPU until the `flush` call.  Values from multiple write calls for a given flush
interval are first concatenated on GPU and then transferred to CPU as a single
tensor.


## Attribute FAQ

Q: What if I create an attribute, for example named `kl-divergence`, and then later
realize that I need another attribute which also measures KL divergence?  What should
I do?

A: You can always change the attr_name and attr_desc (but not the attr_type) of an
existing attribute.  It is the attr_id and attr_handle which identify the attribute,
and what is used to asssociate the data to that attribute.  So, in this case, you'd
want to specialize the name and description of the existing `kl-divergence` attribute
so as to distinguish it from the additional attribute.

However, if you realize that you've already logged data from separate runs to this
attribute but the semantics of the data were different, then it becomes harder to
remedy the situation.  Basically, it requires some planning to choose and define
attributes wisely, with the anticipation that all data logged will be mapped to a
plot axis by a given attribute (although see the concept of multi-dataset plots).


## Synchronous API

```python
from streamvis import DataLogger

