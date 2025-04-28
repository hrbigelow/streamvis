import fire
import math
import time
import numpy as np
import re
from collections import defaultdict
from streamvis import server, util
from streamvis.logger import DataLogger
from . import data_pb2 as pb

def _load(path):
    fh = util.get_log_handle(path, 'rb')
    packed = fh.read()
    fh.close()

    return util.separate_messages(util.unpack(packed))

def inventory(path, scopes='.*'):
    """Print a summary inventory of data in `path` matching scopes."""
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
                    if group.scope == item.scope:
                        del totals[group_id]
        else:
            raise RuntimeError(f"Unknown item type: {type(item)}")

    for group_id, total in totals.items():
        g = seen_groups[group_id]
        signature = ','.join(f'{f.name}:{f.type}' for f in g.fields)
        print(f'{g.id}\t{g.scope}\t{g.name}\t{signature}\t{g.index}\t{total}') 

def inventory_bck(path, scopes='.*', names='.*'):
    """
    Print a summary inventory of data in `path` matching scopes
    """
    all_groups, all_points, _ = _load(path)
    # print(f'Inventory for {path}')
    def filter_fn(g):
        return re.match(scopes, g.scope) and re.match(names, g.name)
    groups = list(filter(filter_fn, all_groups))
    group_ids = set(g.id for g in groups)

    print('group.id\tscope\tname\tsignature\tindex\tnum_points')
    totals = defaultdict(int) # group_id -> num_points
    for p in all_points:
        if p.group_id not in group_ids:
            continue
        totals[p.group_id] += util.num_point_data(p)

    for g in groups:
        signature = ','.join(f'{f.name}:{f.type}' for f in g.fields)
        total_vals = totals[g.id]
        print(f'{g.id}\t{g.scope}\t{g.name}\t{signature}\t{g.index}\t{total_vals}') 

def scopes(path):
    """Print a list of all scopes, in order of first appearance"""
    fh = util.get_log_handle(path, 'rb')
    packed = fh.read()
    fh.close()
    seen_scopes_list = []
    seen_scopes_set = set()
    for item in util.unpack(packed):
        if isinstance(item, pb.Group):
            if item.scope not in seen_scopes_set:
                seen_scopes_set.add(item.scope)
                seen_scopes_list.append(item.scope)
        elif isinstance(item, pb.Control):
            if item.action == pb.Action.DELETE:
                seen_scopes_set.remove(item.scope)
                seen_scopes_list.remove(item.scope)
            
    for scope in seen_scopes_list:
        print(scope)

def export(path, scopes='.*'):
    """
    Export contents of data in `path` matching `scopes` in tsv format
    """
    groups, all_points = _load(path)
    filter_fn = lambda g: re.match(scopes, g.scope)
    for g in filter(filter_fn, groups):
        sig = tuple((f.name, f.type) for f in g.fields)
        points = [pt for pt in all_points if pt.group_id == g.id]
        for pt in points:
            valtups = util.values_tuples(0, pt, sig)
            for _, group_id, *vals in valtups:
                valstr = '\t'.join(f'{v:.3f}' for v in vals)
                print(f'{group_id}\t{pt.batch}\t{g.scope}\t{g.name}\t{g.index}\t{valstr}')
    

def demo_app(scope, path):
    """
    A demo application to log data with `scope` to `path`
    """
    logger = DataLogger(scope)
    buffer_max_elem = 100
    logger.init(path, buffer_max_elem)

    N = 50
    L = 20
    left_data = np.random.randn(N, 2)

    for step in range(0, 10000, 10):
        time.sleep(1.0)
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

        logger.write('top_left', x=[list(range(step, step+10))], y=top_data)

        mid_data = top_data[:,0]

        # (I,), None form
        logger.write('middle', x=step, y=mid_data)

        # Distribute the L dimension along grid cells
        # data_rank3 = np.random.randn(L,N,2) * layer_mult.reshape(L,1,1)
        # logger.scatter_grid(plot_name='top_right', data=data_rank3, append=False,
         #        grid_columns=5, grid_spacing=1.0)

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

def run():
    cmds = { 
            'serve': server.make_server,
            'demo': demo_app,
            'list': inventory,
            'scopes': scopes,
            'export': export
            }
    fire.Fire(cmds)

if __name__ == '__main__':
    run()


