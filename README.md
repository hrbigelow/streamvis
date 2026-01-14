# Interactive, streaming data WebGPU visualizations

# Introduction

Streamvis is both a data logger, and a web application supporting interactive
visualizations of the logged data.  Both communicate through a postgres-backed gRPC
service.  The web app periodically polls the gRPC service for new data, updating the
plots automatically.

# Runs

The data are organized into "runs".  A run represents all the data that is logged
during the life of your script.  There are two types of data associated with a run:
attributes data, and series data.  The attributes data consist of a set of key-value
pairs.  Each key must be a pre-registered "Attribute" which contains a name, a type
(int, float, text, bool), and an optional description.  These attribute values are
all scalar.  Any subset of the attributes may be associated with a run.

In contrast, 'series' data are a collection of multiple structured values associated
with a run.  
