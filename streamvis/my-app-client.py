import fire
import math
from time import sleep
from random import randint
from streamvis import Client

def main(rest_host, rest_port, run_name):
    client = Client(rest_host, rest_port, run_name)

    # clear the data on the REST server
    client.clear()

    # send initialization data to the REST server, to be used by
    # bokeh-server init_page method
    ycolumns = [ 'y1', 'y2', 'y3' ]
    client.init(ycolumns)

    for step in range(10000):
        sleep(0.2)
        new_data = {
                'x': step,
                'y1': math.sin(1 + step / 10),
                'y2': 0.5 * math.sin(1.5 + step / 20),
                'y3': 1.5 * math.sin(2 + step / 15) 
                }
        # server's 'update_page' function will receive { 'main_plot': new_data }
        client.updatel(step, 'main_plot', new_data)

if __name__ == '__main__':
    fire.Fire(main)

