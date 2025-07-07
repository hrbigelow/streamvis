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
from . import demo_sync, demo_async
from . import data_pb2 as pb
from . import data_pb2_grpc as pb_grpc
from .demo_async import demo_log_data_async
from .demo_sync import demo_log_data
from google.protobuf.empty_pb2 import Empty


def export(path, scope=None, name=None):
    """Return all data full-matching scope_pat.

    Returns:
       (scope, name, index) => Dict[axis, data]
    """
    index_fh = util.get_log_handle(util.index_file(path), "rb")
    index = util.Index.from_filters(scope=scope, name=name)
    index.update(index_fh)
    data_fh = util.get_log_handle(util.data_file(path), "rb")
    cds_map = util.fetch_cds_data(data_fh, index)
    return util.flatten_keys(cds_map)

def local_scopes(path: str) -> list[str]:
    index = util.load_index(path)
    return index.scope_list

def local_names(path: str, scope: str=None) -> list[str]:
    index = util.load_index(path, scope)
    return index.name_list
    

def demo_sync(grpc_uri: str, scope: str, num_steps: str="2000"):
    num_steps = int(num_steps)
    demo_log_data(grpc_uri, scope, num_steps)

def demo_async(grpc_uri: str, scope: str, num_steps: str="2000"):
    num_steps = int(num_steps)
    asyncio.run(demo_log_data_async(grpc_uri, scope, num_steps))


async def gfetch(uri: str, scope: str=None, name: str=None):
    raise NotImplementedError
# async with aio.insecure_channel(uri) as chan:
        # stub = pb_grpc.RecordServiceStub(chan)
        # query = pb.QueryRequest(scope=scope, name=name) 
        # async for record in stub.QueryRecords(query):
            # pass

def fetch(uri: str, scope: str=None, name: str=None):
    channel = grpc.insecure_channel(uri)
    stub = pb_grpc.RecordServiceStub(channel)
    index = util.Index.from_scope_name(scope, name)
    pb_index = index.to_message()
    datas = []
    i = 0
    for record in stub.QueryRecords(pb_index):
        match record.type:
            case pb.INDEX:
                index = util.Index.from_message(record.index)
            case pb.DATA:
                datas.append(record.data)
    cds_map = util.data_to_cds(index, datas)
    return util.flatten_keys(cds_map)


def scopes(uri: str) -> list[str]:
    channel = grpc.insecure_channel(uri)
    stub = pb_grpc.RecordServiceStub(channel)
    scopes = []
    for record in stub.Scopes(Empty()):
        match record.type:
            case pb.STRING:
                scopes.append(record.value)
    return scopes


def names(uri: str, scope: str) -> list[str]:
    channel = grpc.insecure_channel(uri)
    stub = pb_grpc.RecordServiceStub(channel)
    request = pb.ScopeRequest(scope=scope)
    names = []
    for record in stub.Names(request):
        match record.type:
            case pb.STRING:
                names.append(record.value)
    return names 


def config(uri: str, scope: str) -> dict[str, Any]:
    channel = grpc.insecure_channel(uri)
    stub = pb_grpc.RecordServiceStub(channel)
    request = pb.ScopeRequest(scope=scope)
    configs = []
    for record in stub.Configs(request):
        match record.type:
            case pb.INDEX:
                index = util.Index.from_message(record.index)
            case pb.CONFIG:
                configs.append(record.config)
    return util.export_configs(index, configs)


def serve(web_uri: str, grpc_uri: str, schema_file: str, refresh_seconds: float=2.0):
    from streamvis import server
    return server.make_server(web_uri, grpc_uri, schema_file, refresh_seconds)


def grpc_serve(path: str, port: str):
    from streamvis import grpc_server
    asyncio.run(grpc_server.serve(path, int(port)))


def help():
    print("Usage:")
    print("script.py <task> <args...>")
    print("Available tasks: web-serve, grpc-serve, demo, demo-async, groups, list, scopes, names, export, delete")

def main():
    if len(sys.argv) < 2:
        help()
        return

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
            "logging-demo": demo_sync,
            "logging-demo-async": demo_async,
            "scopes": partial(print_list, scopes),
            "names": partial(print_list, names),
            "config": partial(print_dict, config),
            }
    task = sys.argv.pop(1)
    task_fun = tasks.get(task)
    if task_fun is None:
        help()
    task_fun(*sys.argv[1:])


if __name__ == '__main__':
    main()
