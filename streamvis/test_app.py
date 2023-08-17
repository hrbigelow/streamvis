import numpy as np
import math
import fire
from time import sleep
from streamvis.logger import DataLogger

def make_logger(scope, bucket_name, blob_name):
    logger = DataLogger(scope)
    buffer_max_size = 10
    logger.init(path, buffer_max_size)

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

        logger.write('top_left_1', x=step, y=math.sin(1 + step / 10))
        logger.write('top_left_2', x=step, y=0.5 * math.sin(1.5 + step / 20))
        logger.write('top_left_3', x=step, y=1.5 * math.sin(2 + step / 15))

        # Distribute the L dimension along grid cells
        data_rank3 = np.random.randn(L,N,2) * layer_mult.reshape(L,1,1)
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

if __name__ == '__main__':
    fire.Fire(make_logger)

