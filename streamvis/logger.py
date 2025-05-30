from dataclasses import dataclass
from typing import Literal
import copy
import os
import enum
import numpy as np
import asyncio
import grpc
from grpc import aio
from . import data_pb2 as pb
from . import data_pb2_grpc as pb_grpc
import random
import time
import signal
from . import util

class Action(enum.Enum):
    DELETE_SCOPE = 0
    DELETE_NAME = 1

@dataclass
class DataItem:
    name_id: int
    data: dict[str, 'tensor']
    start_index: int

@dataclass
class NameItem:
    name_id: int
    name: str
    field_sig: tuple[tuple[str, np.dtype], ...]


class DataLogger:
    """
    Create one instance of this in the producer script, to send data to
    a bokeh server.
    """
    def __init__(self, scope: str, delete_existing: bool=True):
        """scope: a string which defines the scope in the logical point grouping."""
        self.scope = scope
        self.seen_names = {} # name_id => pb.Name 
        self.buffer = asyncio.Queue()
        self.uint32_max = (1 << 32) - 1 
        random.seed(time.time())
        self.config_written = False
        self.delete_existing = delete_existing

    def init(
        self, 
        grpc_uri: str,
        flush_every: float = 2.0,
        tensor_type: Literal["jax", "torch", "numpy"]="numpy",
    ):
        """
        Initialize logger to log data to the given path.
        path:  filesystem path or gs:// resource
        flush_every:  (seconds) period for flushing to disk
        """
        self.uri = grpc_uri
        self.chan = aio.insecure_channel(grpc_uri) 
        self.stub = pb_grpc.RecordServiceStub(self.chan)
        self.flush_every = flush_every
        match tensor_type:
            case "jax":
                import jax.numpy as jnp
                self.to_array = jnp.array
                self.concat = jnp.concatenate 
                self.to_numpy = lambda ary: np.array(ary)
                self.broadcast_arrays = jnp.broadcast_arrays
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
            case "torch":
                import torch
                self.to_array = torch.tensor
                self.concat = lambda arrays, axis: torch.cat(arrays, dim=axis)
                self.to_numpy = lambda ary: ary.detach().numpy()
                self.broadcast_arrays = torch.broadcast_tensors 
            case other:
                raise RuntimeError(f"unsupported tensor type: '{other}'")

    async def __aenter__(self):
        self._task_group = asyncio.TaskGroup()
        await self._task_group.__aenter__()
        self._task_group.create_task(self.flush_buffer())
        request = pb.WriteScopeRequest(scope=self.scope, do_delete=self.delete_existing)
        response = await self.stub.WriteScope(request)
        self.scope_id = response.value
        return self

    async def __aexit__(self, *args):
        self.buffer.put_nowait(None) # sentinel, allow flush task to finish normally
        await self._task_group.__aexit__(*args)
        await self.chan.close()
        self._task_group = None

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

    async def write_config(self, attrs: dict):
        """Write a set of attributes to associate with this scope.

        This is useful for recording hyperparameters, settings, configuration etc.
        for the program.  Can only be called once for the life of the logger.
        """
        if self.scope_id is None:
            raise RuntimeError(f"write_config can only be called in `async with logger` context")
        if self.config_written:
            raise RuntimeError(f"Can only call write_config once during run")
        config_req = pb.WriteConfigRequest(scope_id=self.scope_id, attributes=attrs)
        await self.stub.WriteConfig(config_req)
        self.config_written = True

    def write_sync(self, name: str, /, start_index: int=0, **data):
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
        try:
            data = {k: self.to_array(v) for k, v in data.items()}
        except BaseException as ex:
            raise RuntimeError(f"Couldn't convert data to tensors")
        try:
            data = self.upscale_inputs(data)
        except BaseException as ex:
            raise RuntimeError(f'{name=}, got exception {ex}')

        name_id = util.get_name_id(self.scope_id, name) 
        if name_id not in self.seen_names:
            field_sig = tuple((k, v.dtype) for k, v in data.items())
            name_item = NameItem(name_id, name, field_sig)
            self.seen_names[name_id] = name_item
            self.buffer.put_nowait(name_item)
        self.buffer.put_nowait(DataItem(name_id, data, start_index))

    async def write(self, name: str, /, start_index: int=0, **data):
        self.write_sync(name, **data)
        await asyncio.sleep(0) # explicit yield

    async def yield_to_flush(self):
        """An explicit yield function to allow buffer flush.

        If you only use write_sync, call this periodically.
        """
        await asyncio.sleep(0)

    def _convert_to_messages(self, data_items: list[DataItem]) -> list[pb.Data]:
        all_messages = []
        for data_item in data_items:
            name_item = self.seen_names.get(data_item.name_id)
            if name_item is None:
                raise RuntimeError("Unknown error")
            entry_id = random.randint(0, self.uint32_max)
            data = {k: self.to_numpy(v) for k, v in data_item.data.items()}
            messages = util.make_data_messages(
                entry_id, name_item.name_id, data, data_item.start_index, name_item.field_sig)
            all_messages.extend(messages)
        return all_messages

    async def write_content(self, data_items: list[DataItem], name_items: list[NameItem]):
        datas = self._convert_to_messages(data_items)
        names = [util.make_name_message(n.name_id, self.scope_id, n.name, n.field_sig) for
                n in name_items]
        request = pb.WriteDataRequest(names=names, datas=datas)
        await self.stub.WriteData(request)

    async def flush_buffer(self):
        more_work = True 
        while True:
            content_as_list = {} # data_id => Dict[str, list['tensor']]
            name_items = []
            while not self.buffer.empty():
                work = await self.buffer.get()
                match work:
                    case None:
                        more_work = False
                        break
                    case DataItem(name_id=name_id, data=data, start_index=start_index):
                        data_id = (name_id, start_index)
                        if data_id not in content_as_list:
                            content_as_list[data_id] = {k: [] for k in data.keys()}
                        cdslist = content_as_list[data_id]
                        for k, v in data.items():
                            cdslist[k].append(v)
                    case NameItem():
                        name_items.append(work)
                    case _:
                        raise RuntimeError(f"flush_buffer: Unknown work: {type(work)}")

            # Collate datas on-device
            data_items = []
            for (name_id, start_index), cdslist in content_as_list.items():
                cds = {k: self.concat(vs, axis=1) for k, vs in cdslist.items()}
                data_item = DataItem(name_id, cds, start_index)
                data_items.append(data_item)

            await self.write_content(data_items, name_items)
            if not more_work:
                # print(f"flush_buffer finished all work")
                break
            try:
                await asyncio.sleep(self.flush_every)
            except asyncio.CancelledError:
                print(f"flush_buffer cancelled")
                break


