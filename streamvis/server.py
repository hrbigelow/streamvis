import asyncio
import tornado
import threading
import numpy as np
import sqlite3
import signal
import sys
import os
import yaml
import re
import functools
from collections import defaultdict
from contextlib import contextmanager
from google.cloud import storage
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.application.handlers import Handler
from tornado.ioloop import IOLoop
from bokeh.server.server import Server as BokehServer
from bokeh.core.validation import silence
from bokeh.core.validation.warnings import EMPTY_LAYOUT, MISSING_RENDERERS
import pdb

from streamvis import util
from streamvis.page import PageLayout
from streamvis.index_page import IndexPage

class CleanupHandler(Handler):
    def __init__(self, sv_server):
        super().__init__()
        self.sv_server = sv_server

    def modify_document(self, doc):
        pass

    def on_server_unloaded(self, server_context):
        self.sv_server.shutdown()
    

class Server:
    def __init__(self, log_file: str, fetch_bytes=100000, refresh_seconds=2.0):
        """
        scopes: regex to match on Group.scope (see data.proto)
        name_pattern: regex to match on Group.name 
        """
        silence(EMPTY_LAYOUT, True)
        silence(MISSING_RENDERERS, True)

        self.schema = {} # plot_name => schema
        self.pages = {} # session_id => PageLayout

        self.log_file = log_file
        self.fetch_bytes = fetch_bytes
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
        # validate schema
        for plot_name, plot_schema in schema.items():
            try:
                self.validate_patterns(name_pattern=plot_schema['name_pattern'])
            except Exception as ex:
                raise RuntimeError(
                    f'Plot {plot_name} in schema file {schema_file} '
                    f'contained error:\n{ex}') from ex
        self.schema = schema

    def shutdown(self):
        """Cleanup actions.  What is needed for GCS?."""
        pass

    def add_page(self, doc):
        """Runs concurrently with refresh_server.

        Add a page object to the state machine
        REST API arguments:
        plots: comma-separate names of plots
        mode: 'row' or 'column'; what a box represents in the layout
        box_elems: comma-separated list of the number of plots each box contains
        box_part: comma-separated list of column width (or row height) proportions
        plot_part: comma-separate list of plot width (row mode) or height (column mode)
        """
        req = doc.session_context.request
        session_id = doc.session_context.id
        # print(f'got {session_id=}')

        if len(req.arguments) == 0:
            page = IndexPage(self, doc)
        else:
            page = PageLayout(self, doc)
            page.set_pagesize(1800, 900)
            page.process_request(req)

        # This seems only necessary if
        self.pages[session_id] = page
        page.start()


def make_server(port, schema_file, log_file, refresh_seconds=10):  
    """
    Launch a server on `port` using `schema_file` to configure plots of data in `path`
    """
    fetch_bytes = 10**10 # hack
    sv_server = Server(log_file, fetch_bytes, refresh_seconds)
    sv_server.load_schema(schema_file)
    handler = FunctionHandler(sv_server.add_page)
    cleanup = CleanupHandler(sv_server)
    bokeh_app = Application(handler, cleanup)
    bokeh_server = BokehServer({'/': bokeh_app}, port=port)
    # bokeh_server.io_loop.asyncio_loop.create_task(sv_server.refresh_server())

    def shutdown_handler(signum, frame):
        print(f'Server received {signal.Signals(signum).name}')
        sv_server.shutdown()

    signal.signal(signal.SIGQUIT, shutdown_handler)
    signal.signal(signal.SIGHUP, shutdown_handler) 

    print(f'Web server is running on http://localhost:{port}')
    bokeh_server.run_until_shutdown()


