import asyncio
import grpc
from grpc import aio
from dataclasses import dataclass
from functools import partial
import sys
import hydra
from hydra.core.config_store import ConfigStore
from hydra.utils import instantiate
from omegaconf import DictConfig
import math
import time
import numpy as np
import re
from streamvis import util
from streamvis.logger import DataLogger
from . import data_pb2 as pb
from . import data_pb2_grpc as pb_grpc
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

def scopes(path: str) -> list[str]:
    index = util.load_index(path)
    return index.scope_list

def names(path: str, scope: str=None) -> list[str]:
    index = util.load_index(path, scope)
    return index.name_list
    

def demo(path: str, scope: str):
    asyncio.run(_demo(path, scope))


async def _demo(path, scope):
    """
    A demo application to log data with `scope` to `path`
    """
    logger = DataLogger(scope)
    logger.init(path, flush_every=2.0)
    await logger.start()

    N = 50
    L = 20
    left_data = np.random.randn(N, 2)

    for step in range(0, 10000, 10):
        time.sleep(0.1)
        # top_data[group, point], where group is a logical grouping of points that
        # form a line, and point is one of those points
        top_data = np.array(
                [
                    [math.sin(1 + s / 10) for s in range(step, step+10)],
                    [0.5 * math.sin(1.5 + s / 20) for s in range(step, step+10)],
                    [1.5 * math.sin(2 + s / 15) for s in range(step, step+10)]
                    ]) 

        left_data = left_data + np.random.randn(N, 2) * 0.1
        layer_mult = np.linspace(0, 10, L)

        await logger.write('top_left', x=[list(range(step, step+10))], y=top_data)

        mid_data = top_data[:,0]

        # (I,), None form
        await logger.write('middle', x=step, y=mid_data)

        # Distribute the L dimension along grid cells
        # data_rank3 = np.random.randn(L,N,2) * layer_mult.reshape(L,1,1)
        # logger.scatter_grid(plot_name='top_right', data=data_rank3, append=False,
         #        grid_columns=5, grid_spacing=1.0)
        await logger.write('loss', x=step, y=mid_data[0])

        if step % 10 == 0:
            print(f'Logged {step=}')
        """
        # Colorize the L dimension
        logger.scatter(plot_name='bottom_left', data=data_rank3, spatial_dim=2,
                append=False, color=ColorSpec('Viridis256', 0))

        # data4 = np.random.randn(N,3)
        data4 = np.random.uniform(size=(N,3))

        # Assign color within the spatial_dim
        logger.scatter(plot_name='bottom_right', data=data4, spatial_dim=1,
                append=False, color=ColorSpec('Viridis256'))
        """
    await logger.shutdown()

async def gfetch(uri: str, scope: str=None, name: str=None):
    async with aio.insecure_channel(uri) as chan:
        stub = pb_grpc.RecordServiceStub(chan)
        query = pb.QueryRequest(scope=scope, name=name) 
        async for record in stub.QueryRecords(query):
            pass

def gfetch_sync(uri: str, scope: str=None, name: str=None):
    channel = grpc.insecure_channel(uri)
    stub = pb_grpc.RecordServiceStub(channel)
    index = util.Index.from_scope_name(scope, name)
    pb_index = index.export()
    datas = []
    for record in stub.QueryRecords(pb_index):
        match record.type:
            case pb.INDEX:
                index = util.Index.from_message(record.index)
            case pb.DATA:
                datas.append(record.data)
    cds_map = util.data_to_cds(index, datas)
    return util.flatten_keys(cds_map)


def gscopes(uri: str) -> list[str]:
    channel = grpc.insecure_channel(uri)
    stub = pb_grpc.RecordServiceStub(channel)
    scopes = []
    for record in stub.Scopes(Empty()):
        match record.type:
            case pb.STRING:
                scopes.append(record.value)
    return scopes


def gnames(uri: str, scope: str) -> list[str]:
    channel = grpc.insecure_channel(uri)
    stub = pb_grpc.RecordServiceStub(channel)
    request = pb.ScopeRequest(scope=scope)
    names = []
    for record in stub.Names(request):
        match record.type:
            case pb.STRING:
                names.append(record.value)
    return names 



def serve(port: str, schema_file: str, log_path: str, refresh_seconds: float=2.0):
    from streamvis import server
    return server.make_server(int(port), schema_file, log_path, refresh_seconds)

def help():
    print("Usage:")
    print("script.py <task> <args...>")
    print("Available tasks: serve, demo, groups, list, scopes, names, export, delete")

def main():
    if len(sys.argv) < 2:
        help()
        return

    def print_list(fn, *args):
        out = fn(*args)
        for item in out:
            print(item)

    tasks = { 
            'serve': serve,
            'demo': demo,
            'scopes': partial(print_list, scopes),
            'names': partial(print_list, names),
            'gscopes': partial(print_list, gscopes),
            'gnames': partial(print_list, gnames),
            }
    task = sys.argv.pop(1)
    task_fun = tasks.get(task)
    if task_fun is None:
        help()
    task_fun(*sys.argv[1:])


if __name__ == '__main__':
    main()
