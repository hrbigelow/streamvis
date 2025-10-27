from dataclasses import dataclass
from typing import Literal, Iterable, Generator
import threading
import queue
import copy
import os
import enum
import numpy as np
import asyncio
import grpc
from .v1 import data_pb2 as pb
from .v1 import data_pb2_grpc as pb_grpc
import random
import time
import signal
from . import util

class Action(enum.Enum):
    DELETE_SCOPE = 0
    DELETE_NAME = 1

MAX_ELEMS_PER_REQUEST = 800_000 # 3.2M or 80% of maximum grpc request 

@dataclass
class DataItem:
    name: str
    start_index: int
    data: dict[str, 'tensor']

    def split(self) -> Generator['DataItem', None, None]:
        shape_set = set(ten.shape for ten in self.data.values())
        assert len(shape_set) == 1, "Can't call split yet"
        dim1 = shape_set.pop()[1]
        num_elems = sum(np.prod(ten.shape) for ten in self.data.values())
        num_splits = int(np.ceil(num_elems / MAX_ELEMS_PER_REQUEST))
        steps = np.linspace(0, dim1, num_splits+1, dtype=int)
        for beg, end in zip(steps[:-1], steps[1:]):
            data = {k: v[:,beg:end] for k, v in self.data.items()}
            item = DataItem(self.name, self.start_index, data)
            yield item


class BaseLogger:
    """
    Create one instance of this in the producer script, to send data to
    a bokeh server.
    """
    def __init__(
        self, 
        scope: str, 
        grpc_uri: str,
        tensor_type: Literal["jax", "torch", "numpy"]="numpy",
        delete_existing_names: bool=True,
        flush_every: float=2.0,
    ):
        """scope: a string which defines the scope in the logical point grouping."""
        self.scope = scope
        self.logged_names = {} # name => pb.Name 
        self.buffer = asyncio.Queue()
        random.seed(time.time())
        self.config_written = False
        self.delete_existing_names = delete_existing_names
        self.deleted_names = set() # name: str
        self.uri = grpc_uri
        self.chan = grpc.insecure_channel(grpc_uri) 
        self.stub = pb_grpc.ServiceStub(self.chan)
        self.flush_every = flush_every
        match tensor_type:
            case "jax":
                import jax.numpy as jnp
                self.to_array = jnp.array
                self.concat = jnp.concatenate 
                self.to_numpy = lambda ary: np.array(ary)
                self.broadcast_arrays = jnp.broadcast_arrays
                self.tensor_size = jnp.size
            case "numpy":
                def downcast(ary):
                    if np.issubdtype(ary.dtype, np.integer):
                        return ary.astype(np.int32)
                    elif np.issubdtype(ary.dtype, np.floating):
                        return ary.astype(np.float32)
                    else:
                        return ary

                self.to_array = lambda x: downcast(np.array(x))
                self.concat = np.concat
                self.to_numpy = lambda x: x
                self.broadcast_arrays = np.broadcast_arrays
                self.tensor_size = np.size
            case "torch":
                import torch
                self.to_array = torch.tensor
                self.concat = lambda arrays, axis: torch.cat(arrays, dim=axis)
                self.to_numpy = lambda ary: ary.detach().numpy()
                self.broadcast_arrays = torch.broadcast_tensors 
                self.tensor_size = torch.numel
            case other:
                raise RuntimeError(f"unsupported tensor type: '{other}'")

    def _init_scope(self):
        """This must be called before any calls to write."""
        req = pb.WriteScopeRequest(scope=self.scope)
        resp = self.stub.WriteScope(req)
        self.scope_id = resp.scope_id

    def delete_scope(self):
        """Call this to delete all data under this scope."""
        request = pb.ScopeRequest(scope=scope)
        names = []
        for record in stub.Names(request):
            names.append(record.name)
        request = pb.ScopeNameRequest(scope=scope, names=names)
        self.stub.DeleteScopeNames(request)

    def upscale_inputs(self, data) -> dict[str, 'tensor']:
        """
        Reshape all data to have shape: (index,point) 
        Shape transformations will be:
        () -> (1, 1)
        (point,) -> (1, point)
        (index,point) -> (index,point)
        """
        def up2(k, v):
            if v.ndim == 0:
                return v[None,None]
            elif v.ndim == 1:
                return v[None,:]
                # return v[:,None]
            elif v.ndim == 2:
                return v
            else:
                raise RuntimeError(
                    f'Datum {k} had shape {v.shape}.  Only rank 0, 1, or 2 data are '
                    f'allowed')

        keys, vals = list(zip(*data.items()))

        try:
            vals = [up2(k, v) for k, v in data.items()]
            vals = self.broadcast_arrays(*vals)
        except BaseException:
            raise RuntimeError(
                f'Data shapes aren\'t broadcastable: ' +
                ', '.join(f'{k}: {v.shape}' for k, v in data.items()))

        return dict(zip(keys, vals))

    def write_config(self, attrs: dict):
        """Write a set of attributes to associate with this scope.

        This is useful for recording hyperparameters, settings, configuration etc.
        for the program.  Can only be called once for the life of the logger.
        """
        if self.scope_id is None:
            raise RuntimeError(f"write_config can only be called in `async with logger` context")
        if self.config_written:
            raise RuntimeError(f"Can only call write_config once during run")
        config_req = pb.WriteConfigRequest(scope_id=self.scope_id, attributes=attrs)
        self.stub.WriteConfig(config_req)
        self.config_written = True

    def write(self, name: str, /, start_index: int=0, **data):
        """Writes new data, possibly creating one or more Group items.

        Inputs:
        name:  
          the `name` field of the (scope, name, index) tuple that will be associated
          with these points.
        start_index:
          the starting index value to assign to the data
        data: 
          map of field_name => item, with the following logic.

        1. all data items (whether rank 0, 1, or 2) are implicitly broadcasted 
           with shape (1,1).  The final shape denotes (index, point)

        2. points are then written to (name, index)

        The common idioms for writing series of points:

        x[point], y[index, point]
        """
        if self.delete_existing_names and name not in self.deleted_names:
            req = pb.DeleteTagRequest(scope=self.scope, names=(name,))
            self.stub.DeleteScopeNames(req)
            self.deleted_names.add(name)

        try:
            data = {k: self.to_array(v) for k, v in data.items()}
        except BaseException as ex:
            raise RuntimeError(f"Couldn't convert data to tensors")
        try:
            data = self.upscale_inputs(data)
        except BaseException as ex:
            raise RuntimeError(f'{name=}, got exception {ex}')

        item = DataItem(name, start_index, data)
        self.buffer.put_nowait(item)

    def total_elems(self, item: DataItem):
        return sum(self.tensor_size(ten) for l in item.data.values() for ten in l)

    def _coalesce_items(self, data_items: list[DataItem]) -> list[DataItem]:
        """Condense data items by name and start_index"""

        items = sorted(data_items, key=lambda d: (d.name, d.start_index))
        out_items = []
        out_item = None
        item_iter = iter(items)
        prev_name, prev_index = None, None
        while True:
            item = next(item_iter, None)
            if item is None:
                if out_item is not None:
                    out_items.append(out_item)
                break
            if prev_name != item.name or prev_index != item.start_index:
                if out_item is not None:
                    out_items.append(out_item)
                out_item = DataItem(
                    item.name,
                    item.start_index,
                    {k: [] for k in item.data.keys()}
                )
            for k, v in item.data.items():
                out_item.data[k].append(v)

            prev_name = item.name
            prev_index = item.start_index

        for out_item in out_items:
            cds = {k: self.concat(vs, axis=1) for k, vs in out_item.data.items()}
            out_item.data = cds

        split_items = [sp for item in out_items for sp in item.split()]
        return split_items


    def write_content(self, data_items: list[DataItem]):
        all_messages = []
        pending_names = {} # str -> pb.Name
        # find any names not yet logged
        for data_item in data_items:
            pb_name = self.logged_names.get(data_item.name, None)
            if pb_name is None and data_item.name not in pending_names:
                field_sig = tuple((k, v.dtype) for k, v in data_item.data.items())
                pb_name = util.make_name_message(self.scope_id, data_item.name, field_sig)
                pending_names[data_item.name] = pb_name

        req = pb.WriteNameRequest(names=pending_names.values())
        resp = self.stub.WriteNames(req)
        for pb_name in resp.names:
            self.logged_names[pb_name.name] = pb_name 

        requests = []
        sizes = []
        for data_item in data_items:
            data_elems = self.total_elems(data_item)
            if data_elems > MAX_ELEMS_PER_REQUEST:
                raise RuntimeError(
                    f"Single item with {data_elems} elements exceeds max of "
                    f"${MAX_ELEMS_PER_REQUEST}")

            if len(sizes) == 0 or sizes[-1] + data_elems > MAX_ELEMS_PER_REQUEST:
                request = pb.WriteDataRequest()
                requests.append(request)
                sizes.append(0)

            data = {k: self.to_numpy(v) for k, v in data_item.data.items()}
            pb_name = self.logged_names[data_item.name]
            messages = util.make_data_messages(
                pb_name.name_id, data, data_item.start_index, pb_name.fields)
            requests[-1].datas.extend(messages)
            sizes[-1] += data_elems

        # print(f"Sending requests of element counts: {sizes}")
        assert all(size <= MAX_ELEMS_PER_REQUEST for size in sizes), "Exceeded allowed size"

        try:
            for req in requests:
                self.stub.WriteData(req)
        except grpc.RpcError as ex:
            raise RuntimeError(f"Could not write request to grpc: {ex}") from ex


    def _flush_buffer(self) -> bool:
        data_items = []
        more_work = True
        while not self.buffer.empty():
            data_item = self.buffer.get_nowait()
            if data_item is None:
                more_work = False
                break
            data_items.append(data_item) 

        data_items = self._coalesce_items(data_items)
        self.write_content(data_items)
        return more_work 


class AsyncDataLogger(BaseLogger):

    def __init__(
        self, 
        scope: str, 
        grpc_uri: str,
        tensor_type: Literal["jax", "torch", "numpy"]="numpy",
        delete_existing_names: bool=True,
        flush_every: float=2.0
    ):
        super().__init__(scope, grpc_uri, tensor_type, delete_existing_names, flush_every)
        self.buffer = asyncio.Queue()

    async def flush_buffer(self):
        while True:
            if not self._flush_buffer():
                break
            try:
                await asyncio.sleep(self.flush_every)
            except asyncio.CancelledError:
                print(f"flush_buffer cancelled")
                break

    async def __aenter__(self):
        self._task_group = asyncio.TaskGroup()
        await self._task_group.__aenter__()
        self._task_group.create_task(self.flush_buffer())
        self._init_scope()
        return self

    async def __aexit__(self, *args):
        self.buffer.put_nowait(None) # sentinel, allow flush task to finish normally
        await self._task_group.__aexit__(*args)
        self.chan.close()
        self._task_group = None

    async def write(self, name: str, /, start_index: int=0, **data):
        """The default write function for the async logger.

        If using this instead of write_sync, there is no need to call yield_to_flush"""
        super().write(name, start_index, **data)
        await asyncio.sleep(0) # explicit yield

    def write_sync(self, name: str, /, start_index: int=0, **data):
        """A convenience function to avoid making every function async.

        If using write_sync, you must periodically call yield_to_flush() to allow
        the flush task to wake up.
        """
        super().write(name, start_index, **data)

    async def yield_to_flush(self):
        """An explicit yield function to allow buffer flush.

        If you only use write_sync, call this periodically.
        """
        await asyncio.sleep(0)

class DataLogger(BaseLogger):
    """The synchronous data logger."""

    def __init__(
        self, 
        scope: str, 
        grpc_uri: str,
        tensor_type: Literal["jax", "torch", "numpy"]="numpy",
        delete_existing_names: bool=True,
        flush_every: float=2.0
    ):
        super().__init__(scope, grpc_uri, tensor_type, delete_existing_names, flush_every)
        self.buffer = queue.Queue()
        self._flush_thread = threading.Thread(target=self.flush_buffer, daemon=True)

    def start(self):
        self._flush_thread.start()
        super()._init_scope()

    def flush_buffer(self):
        while True:
            if not self._flush_buffer():
                break
            time.sleep(self.flush_every)

    def stop(self):
        self.buffer.put_nowait(None)
        self._flush_thread.join()

    def write(self, name: str, /, start_index: int=0, **data):
        super().write(name, start_index, **data)

