import asyncio
import threading
import numpy as np
import signal
import sys
import os
import yaml
import re
import copy
from bokeh.application import Application
from bokeh.application.handlers import Handler
from bokeh.server.server import Server as BokehServer
from bokeh.core.validation import silence
from bokeh.core.validation.warnings import EMPTY_LAYOUT, MISSING_RENDERERS

from streamvis import util
from streamvis.page import PageLayout
from streamvis.index_page import IndexPage


class Server:
    def __init__(self, grpc_uri: str, refresh_seconds=2.0):
        """
        """
        silence(EMPTY_LAYOUT, True)
        silence(MISSING_RENDERERS, True)

        self.schema = {} # plot_name => schema
        self.grpc_uri = grpc_uri
        self.refresh_seconds = refresh_seconds

    @staticmethod
    def validate_patterns(**kwargs):
        for arg_name, arg_val in kwargs.items():
            try:
                re.compile(arg_val)
            except re.error as ex:
                raise RuntimeError(
                    f'Received invalid regex for {arg_name}: `{arg_val}`: {ex}')

    def load_schema(self, schema_file):
        """Parses and sets the schema, determines how plots are created from data."""
        try:
            with open(schema_file, 'r') as fh: 
                schema = yaml.safe_load(fh)
        except Exception as ex:
            raise RuntimeError(
                    f'Server could not open or parse schema file {schema_file}. '
                    f'Exception was: {ex}') from ex
        # validate schema: TODO
        self.schema = schema

def make_server(port, grpc_uri, schema_file, refresh_seconds=10):  
    """
    Launch a server on `port` using `schema_file` to configure plots of data in `path`
    """
    sv_server = Server(grpc_uri, refresh_seconds)
    sv_server.load_schema(schema_file)
    page_handler = PageLayout(sv_server)
    index_handler = IndexPage(sv_server) 
    page_app = Application(page_handler)
    index_app = Application(index_handler)
    apps = {"/": page_app, "/index": index_app}
    bokeh_server = BokehServer(apps, port=port)

    def shutdown_handler(signum, frame):
        print(f'Server received {signal.Signals(signum).name}.')

    signal.signal(signal.SIGQUIT, shutdown_handler)
    signal.signal(signal.SIGHUP, shutdown_handler) 

    print(f'Web server is running on http://localhost:{port}')
    bokeh_server.run_until_shutdown()


