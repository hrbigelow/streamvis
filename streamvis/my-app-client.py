import fire
import math
from time import sleep
from random import randint
from streamvis import Client

def main(rest_host, rest_port, run_name):
    client = Client(rest_host, rest_port, run_name)

    # clear the data on the REST server associated with run_name
    client.clear()

    # creates an empty map under REST path /run_name/main_plot
    # the Server init_page function will wait until data arrives here.
    grid_map = {
            'top_plot': (0, 0, 1, 2),
            'low_left_plot': (1, 0, 2, 1),
            'low_right_plot': (1, 1, 2, 2)
            }
    client.set_layout(grid_map)

    for step in range(10000):
        sleep(0.2)
        new_data = [
                step,
                math.sin(1 + step / 10),
                0.5 * math.sin(1.5 + step / 20),
                1.5 * math.sin(2 + step / 15) 
                ]

        client.scatter('top_plot', new_data, append=True) 
        client.scatter('low_left_plot', new_data)
        client.scatter('low_right_plot', new_data)

if __name__ == '__main__':
    fire.Fire(main)

