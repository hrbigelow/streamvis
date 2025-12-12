import asyncio
import threading
import numpy as np
import signal
import sys
import os
import yaml
import re
import copy
import grpc

from bokeh.application import Application
from bokeh.application.handlers import Handler
from bokeh.server.server import Server
from bokeh.core.validation import silence
from bokeh.core.validation.warnings import EMPTY_LAYOUT, MISSING_RENDERERS

from streamvis import util
from streamvis.page import PageLayout

def make_server(web_uri, grpc_uri, refresh_seconds=3):  
    """Launch a Bokeh web server

    web_uri:  IP:PORT format to be used for accessing pages from the browser
    grpc_uri: IP:PORT format, used to access the grpc server 
    schema_file: yaml file specifying plot formats
    refresh_seconds: how frequently the server pulls newly logged data
    """
    page_handler = PageLayout(grpc_uri, refresh_seconds)
    page_app = Application(page_handler)
    apps = {"/": page_app}
    port = int(web_uri.split(":")[1])
    server = Server(apps, port=port, allow_websocket_origin=[web_uri])
    server.io_loop.run_sync(page_handler.initialize_grpc)

    def shutdown_handler(signum, frame):
        print(f'Server received {signal.Signals(signum).name}.')

    signal.signal(signal.SIGQUIT, shutdown_handler)
    signal.signal(signal.SIGHUP, shutdown_handler) 

    print(f'Web server is running on http://localhost:{port}')
    server.run_until_shutdown()


