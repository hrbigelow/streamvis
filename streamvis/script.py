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
from streamvis import server, util
from streamvis.logger import DataLogger
from . import data_pb2 as pb
from . import data_pb2_grpc as pb_grpc
from google.protobuf.empty_pb2 import Empty


@dataclass
class DemoOpts:
    scope: str
    path: str


@dataclass
class ScopeOpts:
    path: str

@dataclass
class NameOpts:
    path: str
    scope: str

@dataclass
class ServerOpts:
    port: int
    schema_file: str
    log_file: str
    refresh_seconds: float = 2.0


cs = ConfigStore.instance()
cs.store(name="demo", node=DemoOpts)
cs.store(name="scopes", node=ScopeOpts)
cs.store(name="names", node=NameOpts)
cs.store(name="server", node=ServerOpts)


def _inventory(path, scopes='.*'):
    """Compute the set of totals for each group"""
    fh = util.get_log_handle(path, 'rb')
    packed = fh.read()
    fh.close()
    totals = {}
    seen_groups = {} # group_id -> pb.Group 
    scope_pat = re.compile(scopes)
    for item in util.unpack(packed):
        if isinstance(item, pb.Group):
            assert item.id not in seen_groups, f"Group {item.id} logged more than once"
            seen_groups[item.id]  = item # to check validity
            if not scope_pat.fullmatch(item.scope):
                continue
            totals[item.id] = 0
        elif isinstance(item, pb.Points):
            assert item.group_id in seen_groups, f"Item with group {item.group_id} logged before group"
            if item.group_id not in totals:
                continue
            totals[item.group_id] += util.num_point_data(item)
        elif isinstance(item, pb.Control):
            if item.action == pb.Action.DELETE:
                for group_id in list(totals):
                    group = seen_groups[group_id]
                    if group.scope == item.scope and group.name == item.name:
                        del totals[group_id]
        else:
            raise RuntimeError(f"Unknown item type: {type(item)}")
    return seen_groups, totals

def by_group(path, scopes='.*'):
    seen_groups, totals = _inventory(path, scopes)
    for group_id, total in totals.items():
        g = seen_groups[group_id]
        signature = ','.join(f'{f.name}:{f.type}' for f in g.fields)
        print(f'{g.id}\t{g.scope}\t{g.name}\t{signature}\t{g.index}\t{total}') 

def by_content(path, scopes='.*'):
    total_by_content = {} # (scope, name) => count
    seen_groups, totals = _inventory(path, scopes)
    for group_id, count in totals.items():
        group = seen_groups[group_id]
        key = group.scope, group.name
        total_by_content.setdefault(key, 0)
        total_by_content[key] += count 

    for (scope, name), count in total_by_content.items():
        print(f"{scope}\t{name}\t{count}")


def export(path, scope=None, name=None):
    """Return all data full-matching scope_pat.

    Returns:
       (scope, name, index) => Dict[axis, data]
    """
    metas_map, entries_map = util.load_index(path, scope, name)
    fh = util.get_log_handle(path, 'rb')
    entries = list(entries_map.values())
    cds_map = util.fetch_cds_data(fh, metas_map, entries)
    return util.flatten_keys(cds_map)

def scopes(path: str) -> list[str]:
    index_path = f"{path}.idx"
    index_fh = util.get_log_handle(index_path, "rb")
    packed = index_fh.read()
    index_fh.close()
    scope_num = 0
    seen_scopes = {} # 
    for item in util.unpack(packed):
        match item:
            case pb.Metadata(scope=scope):
                if scope not in seen_scopes:
                    seen_scopes[scope] = scope_num
                    scope_num += 1
            case pb.Entry():
                pass
            case pb.Control(scope=scope, action=action):
                if action == pb.Action.DELETE:
                    seen_scopes.pop(scope, None)
                else:
                    pass
    return [s for s, _ in sorted(seen_scopes.items(), key=lambda kv: kv[1])]

def names(path: str, scope: str=None) -> list[str]:
    index_path = f"{path}.idx"
    index_fh = util.get_log_handle(index_path, "rb")
    packed = index_fh.read()
    index_fh.close()
    names_num = 0
    seen_names = {}
    for item in util.unpack(packed):
        match item:
            case pb.Metadata(scope=iscope, name=name):
                if iscope == scope:
                    seen_names[name] = names_num
                    names_num += 1
            case pb.Control(scope=iscope, name=name, action=action):
                if iscope == scope and action == pb.Action.DELETE:
                    seen_names.pop(name, None)
    return [n for n, _ in sorted(seen_names.items(), key=lambda kv: kv[1])]


def delete(path, scope: str, name: str):
    """Delete the (scope, name) pair."""
    logger = DataLogger(scope)
    logger.init(path, 10)
    logger.delete_name(name)
    

@hydra.main(config_path=None, config_name="demo", version_base="1.2")
def demo(cfg: DictConfig):
    opts = instantiate(cfg)
    asyncio.run(_demo(opts.scope, opts.path))

async def _demo(scope, path):
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
    request = pb.QueryRequest(scope=scope, name=name)
    metas_map = {}
    datas = []
    for record in stub.QueryRecords(request):
        match record.type:
            case pb.METADATA:
                meta = record.metadata
                metas_map[meta.meta_id] = meta
            case pb.DATA:
                datas.append(record.data)
    cds_map = util.data_to_cds(metas_map, datas)
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



@hydra.main(config_path=None, config_name="server", version_base="1.2")
def serve(cfg: DictConfig):
    opts = instantiate(cfg)
    return server.make_server(
        opts.port, opts.schema_file, opts.log_file, opts.refresh_seconds)

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
            'groups': by_group,
            'list': by_content,
            'scopes': partial(print_list, scopes),
            'names': partial(print_list, names),
            }
    task = sys.argv.pop(1)
    task_fun = tasks.get(task)
    if task_fun is None:
        help()
    task_fun(*sys.argv[1:])


if __name__ == '__main__':
    main()
