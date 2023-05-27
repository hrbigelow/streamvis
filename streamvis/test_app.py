import numpy as np
import math
from time import sleep
from random import randint
from streamvis import DataLogger, GridSpec

def make_logger(run_name, project, topic, write_log_path):
    if (project is None) != (topic is None):
        raise RuntimeError(
            f'`project` and `topic` must be provided together or both absent')
    if project is None and write_log_path is None:
        raise RuntimeError(
            f'At least `project` or `write_log_path` must be provided')

    logger = DataLogger(run_name)
    if project is not None:
        logger.init_pubsub(project, topic)
    if write_log_path is not None:
        logger.init_write_log(write_log_path)

    N = 50
    L = 20
    left_data = np.random.randn(N, 2)

    for step in range(10000):
        sleep(1.0)
        # [num_lines, num_new_points, xy] 
        top_data = np.array([
                step, math.sin(1 + step / 10),
                step, 0.5 * math.sin(1.5 + step / 20),
                step, 1.5 * math.sin(2 + step / 15)
                ]).reshape(3,1,2)

        left_data = left_data + np.random.randn(N, 2) * 0.1
        layer_mult = np.linspace(0, 10, L)

        logger.tandem_lines('top_left', top_data, palette='Viridis256') 

        # Distribute the L dimension along grid cells
        data_rank3 = np.random.randn(L,N,2) * layer_mult.reshape(L,1,1)
        logger.scatter_grid(plot_name='top_right', data=data_rank3, append=False,
                grid_columns=5, grid_spacing=1.0)

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
    import fire
    def pubsub(run_name: str, project: str, topic: str, log_file: str = None):
        """
        Example test logger using Pub/Sub publishing and optional write log

        :param run_name: unused
        :param project: GCP project_id of existing project with Pub/Sub API enabled
        :param topic: GCP Pub/Sub topic id.  Topic must already exist (will be
                      created by the server)
        :param log_file: path to local file to log all data produced.  File will be
                         created if not exists.
        """
        return make_logger(run_name, project, topic, log_file)

    def file(run_name: str, log_file: str):
        """
        Example test logger writing to a log file only

        :param run_name: unused
        :param log_file: path to local file to log all data produced.  File will be
                         created if not exists.
        """
        return make_logger(run_name, None, None, log_file)

    cmds = dict(pubsub=pubsub, file=file)
    fire.Fire(cmds)

if __name__ == '__main__':
    run()

