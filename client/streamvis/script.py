import fire
from typing import Any
import asyncio
import grpc
from grpc import aio
from dataclasses import dataclass
from functools import partial
import sys
import math
import time
import numpy as np
import re
from streamvis import util
from streamvis.logger import DataLogger
from streamvis import data_pb2 as pb
from streamvis import data_pb2_grpc as pb_grpc
from .demo_async import demo_log_data_async
from .demo_sync import demo_log_data
from google.protobuf.empty_pb2 import Empty


def demo_sync_fn(
    grpc_uri: str, scope: str, delete_existing_names: bool=True, num_steps: str="2000"):
    num_steps = int(num_steps)
    demo_log_data(grpc_uri, scope, delete_existing_names, num_steps)

def demo_async_fn(
    grpc_uri: str, scope: str, delete_existing_names: bool=True, num_steps: str="2000"):
    num_steps = int(num_steps)
    asyncio.run(demo_log_data_async(grpc_uri, scope, delete_existing_names, num_steps))


def fetch(uri: str, scope: str=None, name: str=None):
    return fetch_with_patterns(uri=uri, scope_pattern=f"^{scope}$", name_pattern=f"^{name}$")

def fetch_with_patterns(uri: str, scope_pattern: str, name_pattern: str):
    channel = grpc.insecure_channel(uri)
    stub = pb_grpc.ServiceStub(channel)
    req = pb.RecordRequest(
        scope_pattern=scope_pattern,
        name_pattern=name_pattern,
        file_offset=0
    )
    datas = []
    i = 0
    for msg in stub.QueryRecords(req):
        match msg.WhichOneof("value"):
            case "index":
                index = util.Index.from_record_result(msg.index)
            case "data":
                datas.append(msg.data)
            case other:
                raise ValueError(
                    f"QueryRecords should only return index or data.  Returned {other}")

    return util.data_to_cds_flat(index, datas)


def scopes(uri: str) -> list[str]:
    channel = grpc.insecure_channel(uri)
    stub = pb_grpc.ServiceStub(channel)
    scopes = []
    for msg in stub.Scopes(Empty()):
        match msg.WhichOneof("value"):
            case "tag":
                scopes.append(msg.tag.scope)
            case other:
                raise ValueError(
                    f"Scopes() should only return Tag object.  Returned {other}")

    return scopes


def names(uri: str, scope: str) -> list[str]:
    channel = grpc.insecure_channel(uri)
    stub = pb_grpc.ServiceStub(channel)
    req = pb.ScopeRequest(scope=scope)
    names = []
    for msg in stub.Names(req):
        match msg.WhichOneof("value"):
            case "tag":
                names.append(msg.tag.name)
            case other:
                raise ValueError(
                    f"Names() should only return Tag object.  Returned {other}")
    return names 

def config(uri: str, scope: str) -> dict[str, Any]:
    channel = grpc.insecure_channel(uri)
    stub = pb_grpc.ServiceStub(channel)
    req = pb.ScopeRequest(scope=scope)
    configs = []

    for msg in stub.Configs(req):
        match msg.WhichOneof("value"):
            case "index":
                index = util.Index.from_record_result(msg.index)
            case "config":
                configs.append(msg.config)
    return util.export_configs(index, configs)


def serve(web_uri: str, grpc_uri: str, schema_file: str, refresh_seconds: float=2.0):
    from streamvis import server
    return server.make_server(web_uri, grpc_uri, schema_file, refresh_seconds)


def grpc_serve(path: str, port: str):
    from streamvis import grpc_server
    asyncio.run(grpc_server.serve(path, int(port)))


def counts(grpc_uri: str, scope: str):
    res = fetch(grpc_uri, scope)
    for (s, n, i), cds in res.items():
        shape_str = " ".join(f"{k}: {v.shape}" for k, v in cds.items())
        print(f"{s}\t{n}\t{i}\t{shape_str}")

def main():
    def print_list(fn, *args):
        out = fn(*args)
        for item in out:
            print(item)

    def print_dict(fn, *args):
        out = fn(*args)
        from pprint import pprint
        pprint(out)

    tasks = { 
            "web-serve": serve,
            "grpc-serve": grpc_serve,
            "logging-demo": demo_sync_fn,
            "logging-demo-async": demo_async_fn,
            "scopes": partial(print_list, scopes),
            "names": partial(print_list, names),
            "counts": counts,
            "config": partial(print_dict, config),
            }
    fire.Fire(tasks)

if __name__ == '__main__':
    main()
