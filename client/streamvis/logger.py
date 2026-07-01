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
from . import rpc_client


class BaseLogger:
    def __init__(self, max_chunk_size: int, flush_every: float, dry_run: bool):
        self.dry_run = dry_run
        self.queues = {} # frozenset[field_name, ...] => Queue
        self.array_types = {} # frozenset[field_name, ...] => dict[field_name, array_type] 
        self.field_handles = {} # frozenset[field_name, ...] => tuple([field_name, handle), ...]
        self.chan = rpc_client.get_channel()
        self.stub = pb_grpc.ServiceStub(self.chan)
        self.max_chunk_size = max_chunk_size
        self.flush_every = flush_every
        self.run_handle = None

    def _get_fields(self):
        self.all_fields = { f.name: f for f in rpc_client.list_fields(self.stub) }

    def _create_or_replace_run(self):
        if self.run_handle is not None:
            req = pb.ReplaceRunRequest(run_handle=self.run_handle)
            _ = self.stub.ReplaceRun(req)
        else:
            req = pb.CreateRunRequest()
            resp = self.stub.CreateRun(req) # always succeeds
            self.run_handle = resp.run_handle

    def set_run_handle(self, handle: str):
        if self.dry_run:
            return
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
        if self.dry_run:
            return

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

    def add_run_tags(self, *tags: list[str]):
        """Add tags to this run  
        """
        if self.dry_run:
            return

        if self.run_handle is None:
            raise RuntimeError(f"Cannot call set_run_attributes until run started")

        req = pb.AddRunTagsRequest(run_handle=self.run_handle, tags=tags)
        _ = self.stub.AddRunTags(req)

    def write(self, **field_values):
        """
        Append data to a run.
        field_values: each key must be the name of an existing field in the database,
        and each value must match the data type.  See:
        streamvis list-fields | jq

        Values can be Python, numpy, pytorch, jax and can be scalars, lists, or
        multi-dimensional.  All values must be broadcastable together to one shape.
        """
        if self.dry_run:
            return

        # check field names exist
        for fname in field_values.keys():
            if fname not in self.all_fields:
                raise RuntimeError(
                    f"attempted to write field name `{fname}`, which is not a "
                    f"registered field.  Use streamvis list-fields | jq "
                    f"and streamvis create-field ...")

        fieldset = frozenset(field_values.keys())
        _types = self.array_types.get(fieldset, None)
        if _types is None:
            self.array_types[fieldset] = _types = { 
                k: dbutil.get_array_type(v) for k, v in field_values.items() 
            }
            self.field_handles[fieldset] = tuple((f, self.all_fields[f].handle) for f in fieldset)

        # check consistent types
        for arg, val in field_values.items():
            target_type = _types.get(arg, None)
            if target_type != dbutil.get_array_type(val):
                raise RuntimeError(
                    f"Argument {arg} had array type {target_type} in previous call"
                    f"but now has type {dbutil.get_array_type(val)}. "
                    f"write must use consistent types across calls")

        # append data
        # each entry in arrays is one of (np.ndarray, jax.Array, or torch.Tensor)
        arrays = [] 
        shapes = []
        fnames = []

        for fname, _ in self.field_handles[fieldset]:
            val = field_values.get(fname, None)
            if val is None:
                raise RuntimeError(
                    f"value for field {fname} missing from input.  "
                    f"Values given are: {', '.join(field_values.keys())}")
            try:
                ary = dbutil.convert_to_array(val)
                elem_type = dbutil.get_element_type(ary)
                target_elem_type = self.all_fields.get(fname).data_type
                if elem_type != target_elem_type:
                    raise ValueError(
                        f"field `{fname}` is registered with element type "
                        f"{target_elem_type} but was provided `{elem_type}`") 

            except ValueError as ve:
                raise ValueError(f"Error processing value for field {fname}: {ve}")
            fnames.append(fname)
            shapes.append(dbutil.array_shape(ary))
            arrays.append(ary) 

        try:
            bcast_shape = np.broadcast_shapes(*shapes)
        except ValueError as ve:
            raise RuntimeError(
                f"Field data were not broadcastable: "
                ", ".join(f"{fname}: {sh}" for fname, sh in zip(fnames, shapes)))

        queue = self.queues.get(fieldset, None)
        if queue is None:
            self.queues[fieldset] = queue = asyncio.Queue() 

        points = SeriesValues(tuple(arrays), bcast_shape)
        queue.put_nowait(points)

    def _flush_all(self) -> bool:
        all_done = True 
        for fieldset, queue in self.queues.items():
            handles = tuple(h for _, h in self.field_handles[fieldset])
            done = self._flush(handles, queue)
            all_done = all_done and done
        return all_done 

    def _flush(self, handles: tuple[str], queue: asyncio.Queue) -> bool:
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
            print(f"in _flush with "
                  f"current_shape: {current_shape}, "
                  f"item.shape(): {item.shape()}, "
                  f"current_size: {current_size}, "
                  f"item.num_points(): {item.num_points()}, "
                  f"should_process: {should_process}")
            """

            if should_process and current_chunk:
                self._process_chunk(handles, current_chunk)
                current_chunk = []
                current_shape = None
                current_size = 0

            if finished:
                break

            current_chunk.append(item)
            current_shape = item.shape() 
            current_size += item.num_points() 

        # now, process if anything is there to process
        # print(f"{len(current_chunk)}")
        if len(current_chunk) > 0:
            self._process_chunk(handles, current_chunk)

        return finished 

    def _process_chunk(self, handles: tuple[str], chunk: list[SeriesValues]):
        req = pb.AppendToRunRequest(run_handle=self.run_handle)
        # print("before stack_series_values:")
        # for s in chunk:
            # print(s)
        chunk = dbutil.stack_series_values(chunk)
        # print("after stack_series_values:")
        # print(chunk)
        field_datas = chunk.to_exported()

        for handle, ary in zip(handles, field_datas):
            msg = dbutil.encode_array(handle, ary)
            req.field_vals.append(msg)

        try:
            resp = self.stub.AppendToRun(req)
        except Exception as ex:
            print(f"Got exception {ex} calling AppendToRun. Ignoring!")


class AsyncDataLogger(BaseLogger):

    def __init__(
        self, 
        *,
        max_chunk_size: int=100000,
        flush_every: float=2.0,
        dry_run: bool=False,
    ):
        super().__init__(max_chunk_size, flush_every, dry_run)
        self.exiting = False

    async def flush_buffer(self):
        if self.dry_run:
            return
        while True:
            if not self._flush_all():
                if self.exiting:
                    break
            try:
                await asyncio.sleep(self.flush_every)
            except asyncio.CancelledError:
                print(f"flush_buffer cancelled")
                break

    async def __aenter__(self):
        if self.dry_run:
            return
        self._task_group = asyncio.TaskGroup()
        await self._task_group.__aenter__()
        self._create_or_replace_run()
        self._get_fields()
        self._task_group.create_task(self.flush_buffer())
        return self

    async def __aexit__(self, *args):
        if self.dry_run:
            return
        for queue in self.queues.values():
            queue.put_nowait(None)
        self.exiting = True
        await self._task_group.__aexit__(*args)
        self.chan.close()
        self._task_group = None

    async def write(self, **data):
        """The default write function for the async logger.

        If using this instead of write_sync, there is no need to call yield_to_flush"""
        super().write(**data)
        await asyncio.sleep(0) # explicit yield

    def write_sync(self, **data):
        """A convenience function to avoid making every function async.

        If using write_sync, you must periodically call yield_to_flush() to allow
        the flush task to wake up.
        """
        super().write(**data)

    async def yield_to_flush(self):
        """An explicit yield function to allow buffer flush.

        If you only use write_sync, call this periodically.
        """
        await asyncio.sleep(0)

class DataLogger(BaseLogger):
    """The synchronous data logger."""

    def __init__(
        self, 
        *,
        max_chunk_size: int=100000,
        flush_every: float=2.0,
        dry_run: bool=False,
    ):
        super().__init__(max_chunk_size, flush_every, dry_run)
        self.exiting = False
        self._flush_thread = threading.Thread(target=self.flush_buffer, daemon=True)

    def start(self):
        if self.dry_run:
            return
        self._create_or_replace_run()
        self._get_fields()
        self._flush_thread.start()

    def flush_buffer(self):
        if self.dry_run:
            return
        while True:
            if not super()._flush_all():
                if self.exiting:
                    break
            time.sleep(self.flush_every)

    def stop(self):
        if self.dry_run:
            return
        self.exiting = True
        for queue in self.queues.values():
            queue.put_nowait(None)
        self._flush_thread.join()

    def write(self, **data):
        super().write(**data)

