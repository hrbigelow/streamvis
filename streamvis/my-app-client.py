import fire
import numpy as np
import math
from time import sleep
from random import randint
from streamvis import Client

def main(rest_uri, run_name):
    # point your client to the running sv_rest_server service
    client = Client(rest_uri, run_name)

    # clear the data on the REST server associated with run_name
    client.clear()

    # specifies rectangular packing layout of plots
    grid_map = dict(
            top_plot=(0,0,1,2), # (top,left,height,width)
            low_left_plot=(1,0,2,1),
            low_right_plot=(1,1,2,2)
            )

    # set the physical layout in the page for your plots
    client.set_layout(grid_map)

    N = 1000
    left_data = np.random.randn(N, 2)

    for step in range(10000):
        sleep(0.2)
        top_data = [
                step,
                math.sin(1 + step / 10),
                0.5 * math.sin(1.5 + step / 20),
                1.5 * math.sin(2 + step / 15) 
                ]

        left_data = left_data + np.random.randn(N, 2) * 0.1

        client.tandem_lines('top_plot', top_data) 
        client.scatter('low_left_plot', left_data.tolist(), spatial_dim=1, append=False)
        # client.scatter('low_right_plot', new_data)

if __name__ == '__main__':
    fire.Fire(main)

