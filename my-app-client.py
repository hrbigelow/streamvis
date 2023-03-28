import fire
from time import sleep
from random import randint
from streamvis import Client

def main(rest_host, rest_port, app_name):
    init_url = f'http://{rest_host}:{rest_port}/init'
    update_url = f'http://{rest_host}:{rest_port}/step'
    client = Client(init_url, update_url)

    # clear the data on the REST server
    client.clear()

    # send initialization data to the REST server, to be used by
    # bokeh-server init_page method
    ycolumns = [ 'y1', 'y2', 'y3' ]
    client.init(app_name, ycolumns)

    for step in range(1000):
        sleep(1)
        new_data = {
                'x': step,
                'y1': randint(1, 10),
                'y2': randint(1, 10),
                'y3': randint(1, 10)
                }
        # server's 'update_page' function will receive { 'main_plot': new_data }
        client.sendl(step, 'main_plot', new_data)

if __name__ == '__main__':
    fire.Fire(main)

