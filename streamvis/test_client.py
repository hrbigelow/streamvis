import numpy as np
import math
from time import sleep
from random import randint
from streamvis import Client, ColorSpec, GridSpec

def main(rest_uri, run_name):
    # point your client to the running sv_rest_server service
    client = Client(rest_uri, run_name)

    # clear the data on the REST server associated with run_name
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
        sleep(0.2)
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
    fire.Fire(main)

