import fire
from tensorflow.io.gfile import GFile
from streamvis import server, util

def inventory(path):
    """
    Print a summary inventory of data in `path`
    """
    fh = GFile(path, 'rb')
    packed = fh.read()
    fh.close()
    messages = util.unpack(packed)
    groups, all_points = util.separate_messages(messages)
    # print(f'Inventory for {path}')
    print('group.id\tscope\tname\tindex\tnum_points')
    for g in groups:
        points = list(filter(lambda p: p.group_id == g.id, all_points))
        total_vals = sum(util.num_point_data(p) for p in points)
        print(f'{g.id}\t{g.scope}\t{g.name}\t{g.index}\t{total_vals}') 

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
        sleep(1.0)
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

        logger.write('top_left', x=step, y=top_data)

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
            'list': inventory
            }
    fire.Fire(cmds)

if __name__ == '__main__':
    run()

