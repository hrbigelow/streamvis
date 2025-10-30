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
from streamvis.v1 import data_pb2 as pb
from streamvis.v1 import data_pb2_grpc as pb_grpc
from .demo import log_data, log_data_async
from google.protobuf.empty_pb2 import Empty


def demo_sync_fn(
    grpc_uri: str, 
    scope: str, 
    delete_existing_names: bool=True, 
    num_steps: int=2000,
    report_every: int=100
):
    num_steps = int(num_steps)
    log_data(grpc_uri, scope, delete_existing_names, num_steps)

def demo_async_fn(
    grpc_uri: str, 
    scope: str, 
    delete_existing_names: bool=True, 
    num_steps: int=2000,
    report_every: int=100
):
    num_steps = int(num_steps)
    asyncio.run(log_data_async(grpc_uri, scope, delete_existing_names, num_steps))


"""
fetch and fetch_with_patterns return a dict with:
    keys: (scope, name, index)
    values: cds_data

    cds_data is a dict with:
       keys: axis names
       values: numpy array with shape [index, point]
"""
def fetch(uri: str, scope: str, name: str) -> dict[tuple, 'cds_data']:
    return fetch_with_patterns(uri=uri, scope_pattern=f"^{scope}$", name_pattern=f"^{name}$")

def fetch_with_patterns(
    uri: str, 
    scope_pattern: str, 
    name_pattern: str,
    flat_format: bool=True
) -> dict[tuple, 'cds_data']:
    try:
        channel = grpc.insecure_channel(uri)
        stub = pb_grpc.ServiceStub(channel)
        req = pb.DataRequest(
            scope_pattern=scope_pattern,
            name_pattern=name_pattern,
            file_offset=0
        )
        datas = []
        i = 0
        for msg in stub.QueryData(req):
            match msg.WhichOneof("value"):
                case "record":
                    record = util.Index.from_record_result(msg.record)
                case "data":
                    datas.append(msg.data)
                case other:
                    raise ValueError(
                        f"QueryRecords should only return index or data.  Returned {other}")

        if flat_format:
            return util.data_to_cds_flat(record, datas)
        else:
            return util.data_to_cds(record, datas)
    except grpc.RpcError as e:
        raise ValueError(f"RPC failed: {e.code()}: {e.details()}")
    finally:
        channel.close()

def liftover(
    source_uri: str,
    target_uri: str,
    scope_pattern: str
):
    """Transfers the content of scopes matching `scope_pattern` from `source_uri` to
    `target_uri`
    """
    results = fetch_with_patterns(source_uri, scope_pattern, ".*", flat_format=False)
    for scope, names in results.items():
        import pdb
        pdb.set_trace()
        logger = DataLogger(scope, target_uri, tensor_type="numpy",
                            delete_existing_names=True)


def scopes(uri: str) -> list[str]:
    channel = grpc.insecure_channel(uri)
    stub = pb_grpc.ServiceStub(channel)
    scopes = []
    for res in stub.Scopes(Empty()):
        scopes.append(res.scope)

    return scopes


def names(uri: str, scope: str) -> list[str]:
    channel = grpc.insecure_channel(uri)
    stub = pb_grpc.ServiceStub(channel)
    req = pb.NamesRequest(scope=scope)
    names = []
    for tag in stub.Names(req):
        names.append(tag.name)
    return names 

def config(uri: str, scope: str) -> dict[str, Any]:
    channel = grpc.insecure_channel(uri)
    stub = pb_grpc.ServiceStub(channel)
    req = pb.ConfigRequest(scope=scope)
    configs = []

    for res in stub.Configs(req):
        match res.WhichOneof("value"):
            case "index":
                index = util.Index.from_record_result(res.index)
            case "config":
                configs.append(res.config)
    return util.export_configs(index, configs)


def serve(web_uri: str, grpc_uri: str, schema_file: str, refresh_seconds: float=2.0):
    from streamvis import server
    return server.make_server(web_uri, grpc_uri, schema_file, refresh_seconds)


def grpc_serve(path: str, port: str):
    from streamvis import grpc_server
    asyncio.run(grpc_server.serve(path, int(port)))


def counts(grpc_uri: str, scope: str):
    res = fetch_with_patterns(grpc_uri, f"^{scope}$", ".+")
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
             "liftover": liftover,
            }
    fire.Fire(tasks)

if __name__ == '__main__':
    main()
