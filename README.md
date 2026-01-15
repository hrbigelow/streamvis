# Interactive, streaming data WebGPU visualizations

# Introduction

Streamvis is both a data logger, and a web application supporting interactive
visualizations of the logged data.  Both communicate through a postgres-backed gRPC
service.  The web app periodically polls the gRPC service for new data, updating the
plots automatically.

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

Before any data can be logged, two kinds of metadata must be defined:  Fields and
Series.  Fields are individual named (and typed) fields:

```bash
# streamvis create-field <field-name> <field-type> <field-desc>
streamvis create-field sgd-step int "The SGD gradient step of training"
streamvis create-field noisy-channel-epsilon float "Probability of mutating an emitted symbol"
streamvis create-field with-BOS-token bool "Whether BOS token was used in generation"
streamvis create-field experiment-name string "Name of the overall experiment"

grpcurl --plaintext localhost:8001 streamvis.v1.Service/ListFields
{
  "fieldHandle": "3d0422df-e059-472d-a996-0c423ec5fc59",
  "fieldName": "sgd-step",
  "fieldType": "int",
  "fieldDesc": "The SGD gradient step of training"
}
{
  "fieldHandle": "eaec4fe0-9876-4116-a9ab-883792649a60",
  "fieldName": "noisy-channel-epsilon",
  "fieldType": "float",
  "fieldDesc": "Probability of mutating an emitted symbol"
}
{
  "fieldHandle": "32eb2ef8-a64e-4a4c-906f-f7a6dc8736d7",
  "fieldName": "with-BOS-token",
  "fieldType": "bool",
  "fieldDesc": "Whether BOS token was used in generation"
}
{
  "fieldHandle": "0fa61144-2a6b-4fbd-8910-7ffc107c4b0e",
  "fieldName": "experiment-name",
  "fieldType": "text",
  "fieldDesc": "Name of the overall experiment"
}

```

A Series is a named collection of these Fields.  The order of Fields doesn't
matter.

```bash
# streamvis create-series <series-name> <field-name> <field-name> ... 
streamvis create-series training-analysis sgd-step 
```




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







