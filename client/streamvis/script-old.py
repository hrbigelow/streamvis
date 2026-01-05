import os
import fire
from typing import Any
import asyncio
import grpc
from grpc import aio
from dataclasses import dataclass
from functools import partial, update_wrapper
import sys
import math
import time
import numpy as np
import re
from pprint import pprint
from streamvis import util
from streamvis.logger import DataLogger
from streamvis.v1 import data_pb2 as pb
from streamvis.v1 import data_pb2_grpc as pb_grpc
from .demo import log_data, log_data_async
from google.protobuf.empty_pb2 import Empty

GRPC_URI=None
WEB_URI=None

def init_grpc_uri():
    global GRPC_URI
    GRPC_URI = os.getenv('STREAMVIS_GRPC_URI')
    if GRPC_URI is None:
        raise RuntimeError("streamvis requires STREAMVIS_GRPC_URI variable set")

def init_web_uri():
    global WEB_URI
    WEB_URI = os.getenv('STREAMVIS_WEB_URI')
    if WEB_URI is None:
        raise RuntimeError("streamvis server requires STREAMVIS_WEB_URI variable set")

def demo_sync_fn(
    scope: str, 
    delete_existing_names: bool=True, 
    num_steps: int=2000,
    step_sleep_ms: int=0,
):
    global GRPC_URI
    num_steps = int(num_steps)
    log_data(GRPC_URI, scope, delete_existing_names, num_steps, step_sleep_ms)

def demo_async_fn(
    scope: str, 
    delete_existing_names: bool=True, 
    num_steps: int=2000,
    report_every: int=100
):
    global GRPC_URI
    num_steps = int(num_steps)
    asyncio.run(log_data_async(GRPC_URI, scope, delete_existing_names, num_steps))


"""
fetch and fetch_with_patterns return a dict with:
    keys: (scope, name, index)
    values: cds_data

    cds_data is a dict with:
       keys: axis names
       values: numpy array with shape [index, point]
"""
def fetch(scope: str, name: str) -> dict[tuple, 'cds_data']:
    return fetch_with_patterns(scope_pattern=f"^{scope}$", name_pattern=f"^{name}$")

def fetch_with_patterns(
    scope_pattern: str, 
    name_pattern: str,
    window_size: int=None,
    stride: int=None,
    flat_format: bool=True
) -> dict[tuple, 'cds_data']:
    global GRPC_URI
    try:
        channel = grpc.insecure_channel(GRPC_URI)
        stub = pb_grpc.ServiceStub(channel)
        sampling = None
        if window_size is not None and stride is not None:
            sampling = pb.Sampling(
                    window_size=window_size,
                    reduction=pb.Reduction.REDUCTION_MEAN,
                    stride=stride
                    )

        req = pb.DataRequest(
            scope_pattern=scope_pattern,
            name_pattern=name_pattern,
            file_offset=0,
            sampling=sampling
        )
        datas = []
        i = 0
        for msg in stub.QueryData(req):
            match msg.WhichOneof("value"):
                case "record":
                    record = msg.record
                case "data":
                    datas.append(msg.data)
                case other:
                    raise ValueError(
                        f"QueryData should only return index or data.  Returned {other}")

        if flat_format:
            return util.data_to_cds_flat(record.scopes, record.names, datas)
        else:
            return util.data_to_cds(record.scopes, record.names, datas)
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


def get_scopes(scope_regex) -> list[str]:
    global GRPC_URI
    channel = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(channel)
    req = pb.ScopeRequest(scope_regex=scope_regex)
    scopes = []
    for res in stub.Scopes(req):
        scopes.append(res.scope)
    return scopes

def scopes(scope_regex):
    res = get_scopes(scope_regex)
    for line in res:
        print(line)

def get_names(grpc_uri: str, scope_regex: str, name_regex: str = ".*") -> set[tuple[str, str, dict]]:
    channel = grpc.insecure_channel(grpc_uri)
    stub = pb_grpc.ServiceStub(channel)
    req = pb.NamesRequest(scope_regex=scope_regex, name_regex=name_regex)
    names = set() 
    for tag in stub.Names(req):
        fields = frozenset(((f.name, util.PROTO_TO_STRING[f.dtype]) for f in tag.fields))
        names.add((tag.scope, tag.name, fields))
    return names

def names(scope_regex: str, name_regex: str = ".*") -> None:
    global GRPC_URI
    res = get_names(GRPC_URI, scope_regex, name_regex)
    for scope, name, fields in res:
        fields = ",".join(f[0] for f in fields)
        print("\t".join((scope, name, fields)))

def config(scope: str) -> dict[str, Any]:
    global GRPC_URI
    channel = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(channel)
    req = pb.ConfigRequest(scope=scope)
    configs = []

    for res in stub.Configs(req):
        match res.WhichOneof("value"):
            case "index":
                index = res.index
            case "config":
                configs.append(res.config)
    pprint(util.export_configs(index.scopes, configs))


def serve(refresh_seconds: float=2.0):
    global GRPC_URI, WEB_URI
    init_web_uri()
    from streamvis import server
    return server.make_server(WEB_URI, GRPC_URI, refresh_seconds)


def counts(scope_regex: str, name_regex: str):
    res = fetch_with_patterns(scope_regex, name_regex)
    for (s, n, i), cds in res.items():
        shape_str = " ".join(f"{k}: {v.shape}" for k, v in cds.items())
        print(f"{s}\t{n}\t{i}\t{shape_str}")

def delete_name(scope: str, name: str):
    global GRPC_URI
    channel = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(channel)
    req = pb.DeleteTagRequest(scope=scope, names=(name,))
    resp = stub.DeleteScopeNames(req)


def main():
    init_grpc_uri()

    tasks = { 
             "web-serve": serve,
             "logging-demo": demo_sync_fn, 
             "logging-demo-async": demo_async_fn,
             "scopes": scopes,
             "names": names,
             "counts": counts,
             "delete": delete_name,
             "config": config,
             "liftover": liftover,
            }
    fire.Fire(tasks)

if __name__ == '__main__':
    main()
