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
from collections import defaultdict
from contextlib import contextmanager
from tensorflow.io.gfile import GFile
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
from streamvis.page import IndexPage, PageLayout

class LockManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.do_block = False

    def block(self):
        self.do_block = True
        return self

    def __enter__(self):
        self.lock.acquire(blocking=self.do_block)
        return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.release()
        self.do_block = False

class CleanupHandler(Handler):
    def __init__(self, sv_server):
        super().__init__()
        self.sv_server = sv_server

    def modify_document(self, doc):
        pass

    def on_server_unloaded(self, server_context):
        self.sv_server.shutdown()

    
class Server:
    def __init__(self, refresh_seconds=10.0):
        silence(EMPTY_LAYOUT, True)
        silence(MISSING_RENDERERS, True)
        self.data_lock = LockManager()
        self.schema = {} # plot_name => schema
        self.global_ordinal = 0 # globally unique ID across all tables
        self.groups = {} # group_id => Group
        self.sig_to_table = {} # signature => table name
        self.plot_to_sig = {} # plot_name => signature

        # should this (and the blob?) be protected? 
        self.blob_offset = 0
        self.page_lock = LockManager()
        self.pages = {} # map of session_id -> {page.PageLayout or page.IndexPage}
        # pretected by page_lock.  this is set when the server receives new data
        # and needs to update all pages
        self.data_update_pending = False 
        self.refresh_seconds = refresh_seconds

    def load_schema(self, schema_file):
        """
        Parses and sets the schema, determines how plots are created from data
        """
        try:
            with open(schema_file, 'r') as fh: 
                schema = yaml.safe_load(fh)
        except Exception as ex:
            raise RuntimeError(
                    f'Server could not open or parse schema file {schema_file}. '
                    f'Exception was: {ex}')
        self.schema = schema
        self.plot_groups = { name: [] for name in self.schema.keys() } 

    def init_data(self, path):
        """
        Initialize the data source
        """
        self.path = path
        self.connection = sqlite3.connect(':memory:', check_same_thread=False)

    def add_group(self, group):
        """
        Add entry to self.groups, maybe self.points_tables and self.plot_sig
        Call this under self.data_lock
        """
        sig = tuple((f.name, f.type) for f in group.fields)

        self.groups[group.id] = group
        cursor = self.connection.cursor()

        if sig not in self.sig_to_table:
            table_name = f't{len(self.sig_to_table)}'
            self.sig_to_table[sig] = table_name 
            fields = ',\n'.join(f'{f.name} {util.get_sql_type(f.type)}' for f in
                    group.fields)
            create_table_stmt = f"""
            CREATE TABLE {table_name} (
               ord INT,
               group_id INT,
               {fields}
            )
            """
            create_index_stmt = f"""
            CREATE UNIQUE INDEX {table_name}_gid_ord
            ON {table_name} (group_id, ord)
            """
            cursor.execute(create_table_stmt)
            cursor.execute(create_index_stmt)

        for plot_name, plot_schema in self.schema.items():
            existing_sig = self.plot_to_sig.setdefault(plot_name, sig)
            if sig != existing_sig:
                raise RuntimeError(
                    f'Group {group} matching schema for plot {plot_name} '
                    f'had signature {sig} which did not match existing signature'
                    f' {existing_sig}')
            if (re.match(plot_schema['scope_pattern'], group.scope) and
                    re.match(plot_schema['name_pattern'], group.name)):
                self.plot_groups[plot_name].append(group)

    def add_points(self, points_list):
        """
        Add the points contents to the database
        """
        accu = defaultdict(list)
        for point in points_list:
            group = self.groups[point.group_id]
            accu[group.id].extend(util.values_tuples(point, group))
            
        for group_id, vals in accu.items():
            group = self.groups[group_id]
            sig = tuple((f.name, f.type) for f in group.fields)
            table_name = self.sig_to_table[sig]
            with self.data_lock.block():
                go = self.global_ordinal
                self.global_ordinal += len(vals)
            ords = range(go, go + len(vals))
            vals = [(o, group.id, *p) for o, p in zip(ords, vals)]
            placeholder = ', '.join('?' for _ in range(len(sig) + 2))
            insert_stmt = f'INSERT INTO {table_name} VALUES ({placeholder})'
            with self.data_lock.block():
                cursor = self.connection.cursor()
                cursor.executemany(insert_stmt, vals)
                self.connection.commit()

    def new_cds_data(self, group_id, min_ordinal):
        """
        Return new CDS data for the glyph >= min_ordinal 
        May only be called under the data_lock.
        """
        # print(f'in new_cds_data for {group_id} at {min_ordinal}')
        group = self.groups[group_id]
        sig = tuple((f.name, f.type) for f in group.fields)
        column_names = [s[0] for s in sig]
        sql_select = ', '.join(column_names)
        table_name = self.sig_to_table[sig]
        new_points_stmt = f"""
        SELECT {sql_select} 
        FROM {table_name} 
        WHERE group_id = {group_id}
        AND ord >= {min_ordinal}
        ORDER BY ord
        """
        cursor = self.connection.cursor()
        cursor.execute(new_points_stmt)
        results = np.array(cursor.fetchall()).transpose()
        if results.size == 0:
            return None
        # return np.array(results)
        return dict(zip(column_names, results))

    def shutdown(self):
        """
        Cleanup actions.  What is needed for GCS?
        """
        pass

    def fetch_new_data(self):
        """
        Read any new data from the blob, updating the current position
        """
        # print('in fetch_new_data')
        try:
            fh = GFile(self.path, 'rb')
            fh.seek(self.blob_offset)
            packed = fh.read()
            # print(f'read {len(packed)} bytes')
        except BaseException as ex:
            return
            # print(f'Got exception {ex}')
        finally:
            fh.close()

        if len(packed) == 0:
            return

        # no need to protect this with lock.  It is only called here
        # and gcs_callback calls fetch_new_data in a loop
        self.blob_offset += len(packed)
        try:
            messages = util.unpack(packed)
            new_groups, new_points = util.separate_messages(messages)
        except Exception as ex:
            raise RuntimeError(
                    f'Could not unpack messages from GCS log file {self.path}. '
                    f'Got exception: {ex}')

        for g in new_groups:
            self.add_group(g)
        self.add_points(new_points)

    def update_pages(self):
        for page in self.pages.values():
            with self.page_lock:
                page.update()

    async def refresh_server(self):
        while True:
            # print('in refresh_server')
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.fetch_new_data)
            await loop.run_in_executor(None, self.update_pages)
            await asyncio.sleep(self.refresh_seconds)

    def add_page(self, doc):
        """
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

        doc.add_next_tick_callback(page.build_callback)
        page.update()

        with self.page_lock:
            self.pages[session_id] = page
            print(f'server has {len(self.pages)} pages')

    def delete_page(self, session_id):
        with self.page_lock:
            del self.pages[session_id]
            print(f'deleted page {session_id}.  server now has {len(self.pages)} pages.')

def make_server(port, schema_file, log_file, refresh_seconds=10):  
    """
    Launch a server on `port` using `schema_file` to configure plots of data in `path`
    """
    sv_server = Server(refresh_seconds)
    sv_server.load_schema(schema_file)
    sv_server.init_data(log_file)
    # print(f'loading {log_file} ... ', end='', flush=True)
    # sv_server.fetch_new_data()
    # print(f'done.  Loaded {sv_server.global_ordinal} records')
    handler = FunctionHandler(sv_server.add_page)
    cleanup = CleanupHandler(sv_server)
    bokeh_app = Application(handler, cleanup)
    bokeh_server = BokehServer({'/': bokeh_app}, port=port)

    # loop = asyncio.get_running_loop()
    # loop.run_forever()

    bokeh_server.io_loop.asyncio_loop.create_task(sv_server.refresh_server())

    def shutdown_handler(signum, frame):
        print(f'Server received {signal.Signals(signum).name}')
        sv_server.shutdown()

    signal.signal(signal.SIGQUIT, shutdown_handler)
    signal.signal(signal.SIGHUP, shutdown_handler) 

    # loop_wrap = tornado.ioloop.IOLoop.current()

    print(f'Web server is running on http://localhost:{port}')
    bokeh_server.run_until_shutdown()
    # bokeh_server.start()


