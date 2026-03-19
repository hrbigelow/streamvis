import asyncio
import grpc
import uuid
import numpy as np
from typing import Literal
import threading
import time
from . import dbutil
from .dbutil import SeriesValues

from .v1 import data_pb2 as pb
from .v1 import data_pb2_grpc as pb_grpc


class BaseLogger:
    def __init__(
        self,
        grpc_uri: str,
        max_chunk_size: int,
        flush_every: float,
    ):
        self.queues = {} # series_name => Queue
        self.array_types = {} # series_name => tuple(field_type, ...)
        self.chan = grpc.insecure_channel(grpc_uri)
        self.stub = pb_grpc.ServiceStub(self.chan)
        self.max_chunk_size = max_chunk_size
        self.flush_every = flush_every
        self.run_handle = None

    def _get_series(self):
        self.all_series = {}
        req = pb.ListSeriesRequest()
        for msg in self.stub.ListSeries(req):
            self.all_series[msg.name] = msg

    def _get_fields(self):
        self.all_fields = {}
        req = pb.ListFieldsRequest()
        for msg in self.stub.ListFields(req):
            self.all_fields[msg.name] = msg

    def _create_or_replace_run(self):
        if self.run_handle is not None:
            req = pb.ReplaceRunRequest(run_handle=self.run_handle)
            _ = self.stub.ReplaceRun(req)
        else:
            req = pb.CreateRunRequest()
            resp = self.stub.CreateRun(req) # always succeeds
            self.run_handle = resp.run_handle

    def set_run_handle(self, handle: str):
        try:
            uuid.UUID(handle)
        except ValueError as ve:
            raise RuntimeError(f"handle `{handle}` is not a valid UUID string: %s", ve)
        self.run_handle = handle

    def set_run_attributes(self, /, **attrs):
        """Write a set of attributes to associate with this run.

        This is useful for recording hyperparameters, settings, configuration etc.
        for the program.  Can only be called once for the life of the logger.
        """
        if self.run_handle is None:
            raise RuntimeError(f"Cannot call set_run_attributes until run started")

        req = pb.SetRunAttributesRequest(run_handle=self.run_handle)

        for key, val in attrs.items():
            field = self.all_fields.get(key)
            if field is None:
                raise RuntimeError(
                    f"There is no Field named `{key}`.  "
                    "To create a new field, run one of:\n"
                    "streamvis create-field ...\n"
                    "grpcurl -plaintext $STREAMVIS_GRPC_URI streamvis.v1.Service/CreateField\n")
            attr = dbutil.make_field_value(field, val)
            req.attrs.append(attr)

        _ = self.stub.SetRunAttributes(req)

    def write(self, series_name: str, /, **field_values):
        """
        Append data to a series.
        series_name: the name of the Series to be appended.  This must
          exist in the database.  See streamvis list-series / create-series
        fields: a dict containing values matching the field data type of Field in the Series 
        shapes must be broadcastable to a common shape.
        """
        series = self.all_series.get(series_name)
        if series is None:
            raise RuntimeError(
                f"Unknown series: {series_name}\n"
                f"To see available series, run\n"
                "streamvis list-series")

        if series_name not in self.array_types:
            self.array_types[series_name] = names = {}
            for arg, val in field_values.items():
                names[arg] = dbutil.get_array_type(val)

        # check consistent types
        _types = self.array_types[series_name]
        for arg, val in field_values.items():
            target_type = _types.get(arg, None)
            if target_type != dbutil.get_array_type(val):
                raise RuntimeError(
                    f"Argument {arg} previously had type {target_type}"
                    f"but now has type {dbutil.get_array_type(val)}. "
                    f"write must use consistent types across calls")

        # append data
        # each entry in arrays is one of (np.ndarray, jax.Array, or torch.Tensor)
        arrays = [] # field values in Series.Field order.
        array_types = [] # numpy, torch, or jax
        shapes = []
        for coord in series.coords:
            val = field_values.pop(coord.name, None) 
            if val is None:
                raise RuntimeError(
                    f"Series {series_name} coord {coord.name} value missing from input.  "
                    f"Values given are: {', '.join(field_values.keys())}")
            try:
                ary = dbutil.convert_to_array(val)
                field_type = dbutil.get_element_type(ary)
                if field_type != coord.data_type:
                    raise ValueError(
                        f"Series field type is {coord.data_type} but given {field_type}") 

            except ValueError as ve:
                raise ValueError(f"Error processing value for field {coord.name}: {ve}")
            shapes.append(dbutil.array_shape(ary))
            arrays.append(ary) 

        if len(field_values) != 0:
            raise RuntimeError(
                f"Value provided for field names {', '.join(field_values.keys())} "
                f"which are not fields of series {series_name}")

        try:
            bcast_shape = np.broadcast_shapes(*shapes)
        except ValueError as ve:
            z = zip(series.coords, shapes)
            raise RuntimeError(
                f"Field data were not broadcastable: "
                ", ".join(f"{c.name}: {sh}" for c, sh in z))

        if series_name not in self.queues:
            self.queues[series_name] = asyncio.Queue() 
        
        points = SeriesValues(tuple(arrays), bcast_shape)
        self.queues[series_name].put_nowait(points)

    def _flush_all_series(self) -> bool:
        all_done = True 
        for series_name in self.queues.keys():
            queue = self.queues[series_name]
            series = self.all_series[series_name]
            done = self._flush_series(series, queue)
            all_done = all_done and done
        return all_done 

    def _flush_series(self, series: pb.Series, queue: asyncio.Queue) -> bool:
        current_chunk = []
        current_shape = None
        current_size = 0
        finished = False

        while not queue.empty():
            item = queue.get_nowait()
            if item is None:
                finished = True

            should_process = (
                    item is None
                    or (current_shape is not None and item.shape() != current_shape)
                    or (current_size + item.num_points() > self.max_chunk_size))

            """
            print(f"in _flush_series with {series.name} and "
                  f"current_shape: {current_shape}, "
                  f"item.shape(): {item.shape()}, "
                  f"current_size: {current_size}, "
                  f"item.num_points(): {item.num_points()}, "
                  f"should_process: {should_process}")
            """

            if should_process and current_chunk:
                self._process_chunk(current_chunk, series)
                current_chunk = []
                current_shape = None
                current_size = 0

            if finished:
                break

            current_chunk.append(item)
            current_shape = item.shape() 
            current_size += item.num_points() 

        # now, process if anything is there to process
        # print(f"{series.name}: {len(current_chunk)}")
        if len(current_chunk) > 0:
            self._process_chunk(current_chunk, series)

        return finished 

    def _process_chunk(
        self, 
        chunk: list[SeriesValues], 
        series: pb.Series,
    ):
        req = pb.AppendToSeriesRequest(series_handle=series.handle, run_handle=self.run_handle)
        chunk = dbutil.stack_series_values(chunk)
        field_datas = chunk.to_exported()

        for coord, ary in zip(series.coords, field_datas):
            msg = dbutil.encode_array(coord.field_handle, ary)
            """
            if ary.size == 3168:
                np.set_printoptions(linewidth=180, threshold=100000)
                print(ary)
                print(
                        f"series '{series.name}': {len(msg.base)=}, "
                        f"{ary.dtype=}, {ary.size=}, {ary.shape=}")
            """
            req.field_vals.append(msg)

        try:
            resp = self.stub.AppendToSeries(req)
        except Exception as ex:
            print(f"Got exception {ex} calling AppendToSeries. Ignoring!")


class AsyncDataLogger(BaseLogger):

    def __init__(
        self, 
        grpc_uri: str,
        max_chunk_size: int=100000,
        flush_every: float=2.0
    ):
        super().__init__(grpc_uri, max_chunk_size, flush_every)
        self.exiting = False

    async def flush_buffer(self):
        while True:
            if not self._flush_all_series():
                if self.exiting:
                    break
            try:
                await asyncio.sleep(self.flush_every)
            except asyncio.CancelledError:
                print(f"flush_buffer cancelled")
                break

    async def __aenter__(self):
        self._task_group = asyncio.TaskGroup()
        await self._task_group.__aenter__()
        self._create_or_replace_run()
        self._get_series()
        self._get_fields()
        self._task_group.create_task(self.flush_buffer())
        return self

    async def __aexit__(self, *args):
        for queue in self.queues.values():
            queue.put_nowait(None)
        self.exiting = True
        await self._task_group.__aexit__(*args)
        self.chan.close()
        self._task_group = None

    async def write(self, series_name: str, /, **data):
        """The default write function for the async logger.

        If using this instead of write_sync, there is no need to call yield_to_flush"""
        super().write(series_name, **data)
        await asyncio.sleep(0) # explicit yield

    def write_sync(self, series_name: str, /, **data):
        """A convenience function to avoid making every function async.

        If using write_sync, you must periodically call yield_to_flush() to allow
        the flush task to wake up.
        """
        super().write(series_name, **data)

    async def yield_to_flush(self):
        """An explicit yield function to allow buffer flush.

        If you only use write_sync, call this periodically.
        """
        await asyncio.sleep(0)

class DataLogger(BaseLogger):
    """The synchronous data logger."""

    def __init__(
        self, 
        grpc_uri: str,
        max_chunk_size: int=100000,
        flush_every: float=2.0
    ):
        super().__init__(grpc_uri, max_chunk_size, flush_every)
        self.exiting = False
        self._flush_thread = threading.Thread(target=self.flush_buffer, daemon=True)

    def start(self):
        self._create_or_replace_run()
        self._get_series()
        self._get_fields()
        self._flush_thread.start()

    def flush_buffer(self):
        while True:
            if not super()._flush_all_series():
                if self.exiting:
                    break
            time.sleep(self.flush_every)

    def stop(self):
        self.exiting = True
        for queue in self.queues.values():
            queue.put_nowait(None)
        self._flush_thread.join()

    def write(self, series_name: str, /, **data):
        super().write(series_name, **data)

