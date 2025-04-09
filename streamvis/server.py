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
from streamvis.page import IndexPage, PageLayout

class LockManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.do_block = False

    def block(self):
        self.do_block = True
        return self

    def __enter__(self):
        return self.lock.acquire(blocking=self.do_block)

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
    def __init__(self, fetch_bytes=100000, refresh_seconds=2.0, scopes='.*', names='.*'):
        """
        scopes: regex to match on Group.scope (see data.proto)
        name_pattern: regex to match on Group.name 
        """
        silence(EMPTY_LAYOUT, True)
        silence(MISSING_RENDERERS, True)

        self.schema = {} # plot_name => schema

        self.data_lock = LockManager()
        self.tables = {} # table => sig (fetch_new_data)
        self.groups = {} # group_id => Group (refresh_server)
        self.global_ordinal = 0 # globally unique ID (refresh_server) (add_page)
        self.plot_to_sig = {} # plot_name => signature (fetch_new_data)
        self.blob_offset = 0 # (fetch_new_data)
        self.fetch_bytes = fetch_bytes
        self.refresh_seconds = refresh_seconds
        self.scope_pattern = scopes
        self.name_pattern = names

        self.page_lock = LockManager()
        self.pages = {} # map of session_id -> {page.PageLayout or page.IndexPage}
        self.validate_patterns(scope_pattern=scopes, name_pattern=names)
    @staticmethod
    def validate_patterns(**kwargs):
        for arg_name, arg_val in kwargs.items():
            try:
                re.compile(arg_val)
            except re.error as ex:
                raise RuntimeError(
                    f'Received invalid regex for {arg_name}: `{arg_val}`: {ex}')

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
        # validate schema
        for plot_name, plot_schema in schema.items():
            try:
                self.validate_patterns(name_pattern=plot_schema['name_pattern'])
            except Exception as ex:
                raise RuntimeError(
                    f'Plot {plot_name} in schema file {schema_file} '
                    f'contained error:\n{ex}')
        self.schema = schema
        self.plot_groups = { name: [] for name in self.schema.keys() } 

    def init_data(self, path):
        """
        Initialize the data source
        """
        self.path = path
        self.connection = sqlite3.connect(':memory:', check_same_thread=False)

    @staticmethod
    def table_name(sig):
        return 't' + str(abs(hash(sig)))

    def add_group(self, group):
        """
        Add entry to self.groups, maybe self.points_tables and self.plot_sig
        """
        sig = tuple((f.name, f.type) for f in group.fields)
        table = self.table_name(sig)

        self.groups[group.id] = group
        cursor = self.connection.cursor()

        if table not in self.tables:
            self.tables[table] = sig
            fields = ',\n'.join(f'{f.name} {util.get_sql_type(f.type)}' for f in
                    group.fields)
            create_table_stmt = f"""
            CREATE TABLE {table} (
               ord INT,
               group_id INT,
               {fields}
            )
            """
            create_index_stmt = f"""
            CREATE UNIQUE INDEX {table}_gid_ord
            ON {table} (group_id, ord)
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
            if (re.match(self.scope_pattern, group.scope) and
                re.match(self.name_pattern, group.name) and
                re.match(plot_schema['name_pattern'], group.name)):
                self.plot_groups[plot_name].append(group)
                # print(f'{self.name_pattern} {self.scope_pattern} '
                      # f'Adding {group.scope} {group.name}')

    def scope_name_index(self, plot_name, query_group):
        """
        Computes the index of the (scope, name) pair associated with `plot_name` for
        the current server scope.  Indexes are assigned to each distinct (scope,
        name) pair in the order they are encountered in the log file.
        """
        index = 0
        temp = {}
        query_scope_name = query_group.scope, query_group.name
        with self.data_lock.block():
            filter_fn = lambda g: re.match(self.scope_pattern, g.scope)
            for group in filter(filter_fn, self.plot_groups[plot_name]):
                scope_name = group.scope, group.name
                if scope_name not in temp:
                    temp[scope_name] = index
                    index += 1
                if scope_name == query_scope_name:
                    break
        return temp[query_scope_name]

    def load_rows(self, table, rows):
        if len(rows) == 0:
            return
        num_fields = len(rows[0])
        placeholder = ', '.join('?' for _ in range(num_fields))
        insert_stmt = f'INSERT INTO {table} VALUES ({placeholder})'
        cursor = self.connection.cursor()
        cursor.executemany(insert_stmt, rows)

    def add_points(self, points_list):
        @functools.lru_cache
        def get_table(group_id):
            g = self.groups[group_id]
            sig = tuple((f.name, f.type) for f in g.fields)
            return self.table_name(sig)

        for table, sig in self.tables.items():
            rows = []
            for pt in filter(lambda p: get_table(p.group_id) == table, points_list):
                go = self.global_ordinal
                vals = util.values_tuples(go, pt, sig)
                self.global_ordinal += len(vals)
                rows.extend(vals)
            self.load_rows(table, rows)

    def new_cds_data(self, group_id, min_ordinal):
        """
        Return new CDS data for the glyph >= min_ordinal 
        """
        # print(f'in new_cds_data for {group_id} at {min_ordinal}')
        with self.data_lock as lock_acquired:
            if not lock_acquired:
                return None
            group = self.groups[group_id]
            sig = tuple((f.name, f.type) for f in group.fields)
            table = self.table_name(sig)
            column_names = [s[0] for s in sig]
            sql_select = ', '.join(column_names)
            new_points_stmt = f"""
            SELECT {sql_select} 
            FROM {table} 
            WHERE group_id = {group_id}
            AND ord >= {min_ordinal}
            ORDER BY ord
            """
            cursor = self.connection.cursor()
            cursor.execute(new_points_stmt)
            results = np.array(cursor.fetchall()).transpose()
            if results.size == 0:
                return None
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
        try:
            fh = util.get_log_handle(self.path, 'rb')
            fh.seek(self.blob_offset)
            packed = fh.read(self.fetch_bytes)
        except BaseException as ex:
            raise RuntimeError(f'Could not read from {self.path}: {ex}')
        finally:
            fh.close()

        if len(packed) == 0:
            return True

        try:
            messages, remain_bytes = util.unpack(packed)
            processed_bytes = len(packed) - remain_bytes
            self.blob_offset += processed_bytes 
            end_reached = (len(packed) < self.fetch_bytes)
            new_groups, new_points = util.separate_messages(messages)
        except Exception as ex:
            raise RuntimeError(
                    f'Could not unpack messages from GCS log file {self.path}. '
                    f'Got exception: {ex}')

        with self.data_lock.block():
            for g in new_groups:
                self.add_group(g)
            self.add_points(new_points)
        return end_reached

    def update_pages(self):
        for page in self.pages.values():
            page.update()

    async def refresh_server(self):
        loop = asyncio.get_running_loop()
        while True:
            end_reached = await loop.run_in_executor(None, self.fetch_new_data)
            await loop.run_in_executor(None, self.update_pages)
            cycle_delay = self.refresh_seconds if end_reached else 0.2
            await asyncio.sleep(cycle_delay)

    def add_page(self, doc):
        """
        Runs concurrently with refresh_server

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

    def delete_page(self, session_id):
        with self.page_lock.block():
            del self.pages[session_id]
            print(f'deleted page {session_id}.  server now has {len(self.pages)} pages.')

def make_server(port, schema_file, log_file, refresh_seconds=10, scopes='.*', names='.*'):  
    """
    Launch a server on `port` using `schema_file` to configure plots of data in `path`
    """
    fetch_bytes = 10**10 # hack
    sv_server = Server(fetch_bytes, refresh_seconds, scopes, names)
    sv_server.load_schema(schema_file)
    sv_server.init_data(log_file)
    handler = FunctionHandler(sv_server.add_page)
    cleanup = CleanupHandler(sv_server)
    bokeh_app = Application(handler, cleanup)
    bokeh_server = BokehServer({'/': bokeh_app}, port=port)
    bokeh_server.io_loop.asyncio_loop.create_task(sv_server.refresh_server())

    def shutdown_handler(signum, frame):
        print(f'Server received {signal.Signals(signum).name}')
        sv_server.shutdown()

    signal.signal(signal.SIGQUIT, shutdown_handler)
    signal.signal(signal.SIGHUP, shutdown_handler) 

    print(f'Web server is running on http://localhost:{port}')
    bokeh_server.run_until_shutdown()


