import numpy as np
import math
from time import sleep
from random import randint
from streamvis import Client, ColorSpec, GridSpec

def make_client(run_name, project, topic, write_log_path, append=False):
    if (project is None) != (topic is None):
        raise RuntimeError(
            f'`project` and `topic` must be provided together or both absent')
    if project is None and write_log_path is None:
        raise RuntimeError(
            f'At least `project` or `write_log_path` must be provided')

    client = Client(run_name)
    if project is not None:
        client.init_pubsub(project, topic)
    if write_log_path is not None:
        client.init_write_log(write_log_path)

    client.clear()

    # specifies rectangular packing layout of plots
    grid_map = dict(
            top_left=(0,0,1,1), # (top,left,height,width)
            top_right=(0,1,1,1),
            bottom_left=(1,0,1,1),
            bottom_right=(1,1,1,1)
            )

    # set the physical layout in the page for your plots
    client.set_layout(grid_map)

    N = 50
    L = 20
    left_data = np.random.randn(N, 2)

    for step in range(10000):
        sleep(1.0)
        top_data = [
                math.sin(1 + step / 10),
                0.5 * math.sin(1.5 + step / 20),
                1.5 * math.sin(2 + step / 15) 
                ]


        left_data = left_data + np.random.randn(N, 2) * 0.1
        layer_mult = np.linspace(0, 10, L)
        data_rank3 = np.random.randn(L,N,2) * layer_mult.reshape(L,1,1)

        client.tandem_lines('top_left', step, top_data, palette='Viridis256') 

        # Distribute the L dimension along grid cells
        client.scatter(plot_name='top_right', data=data_rank3, spatial_dim=2,
                append=False, grid=GridSpec(0, 5, 1.2))

        # Colorize the L dimension
        client.scatter(plot_name='bottom_left', data=data_rank3, spatial_dim=2,
                append=False, color=ColorSpec('Viridis256', 0))

        # data4 = np.random.randn(N,3)
        data4 = np.random.uniform(size=(N,3))

        # Assign color within the spatial_dim
        client.scatter(plot_name='bottom_right', data=data4, spatial_dim=1,
                append=False, color=ColorSpec('Viridis256'))

def run():
    import fire
    def publish(run_name: str, project: str, topic: str, log_file: str = None,
            append: bool = False):
        """
        Example test client using Pub/Sub publishing and optional write log

        :param run_name: unused
        :param project: GCP project_id of existing project with Pub/Sub API enabled
        :param topic: GCP Pub/Sub topic id.  Topic must already exist (will be
                      created by the server)
        :param log_file: path to local file to log all data produced.  File will be
                         created if not exists.
        :param append: if True, append to any existing log file
        """
        return make_client(run_name, project, topic, log_file, append)

    def file(run_name: str, log_file: str, append: bool = False):
        """
        Example test client writing to a log file only

        :param run_name: unused
        :param log_file: path to local file to log all data produced.  File will be
                         created if not exists.
        :param append: if True, append to any existing log file
        """
        return make_client(run_name, None, None, log_file, append)

    cmds = dict(publish=publish, file=file)
    fire.Fire(cmds)

