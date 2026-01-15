import asyncio
import grpc
import numpy as np
from typing import Literal
import threading
import time
from . import dbutil

from .v1 import data_pb2 as pb
from .v1 import data_pb2_grpc as pb_grpc

# Store in little endian
SIG_TO_DTYPE = {
    'float': np.dtype('<f4'),
    'int': np.dtype('<i4'),
}


class Series:
    def __init__(self, handle: str, structure: dict[str, str]):
        self.handle = handle
        self.queue = asyncio.Queue()
        self.structure = dict(structure)
        self.field_names = tuple(self.structure.keys())
        self.field_types = tuple(self.structure[fn] for fn in self.field_names)

class BaseLogger:
    def __init__(
        self,
        grpc_uri: str,
        tensor_type: Literal["jax", "torch", "numpy"],
        max_chunk_size: int,
        flush_every: float,
    ):
        self.logged_series = {} # series_name => Series
        self.chan = grpc.insecure_channel(grpc_uri)
        self.stub = pb_grpc.ServiceStub(self.chan)
        self.max_chunk_size = max_chunk_size
        self.flush_every = flush_every
        self.run_handle = None

        match tensor_type:
            case "jax":
                import jax.numpy as jnp
                self.to_array = jnp.array
                self.concat = jnp.concatenate 
                self.stack_arrays = jnp.stack
                self.to_numpy = lambda ary: np.array(ary)
                self.broadcast_arrays = jnp.broadcast_arrays
                self.tensor_size = jnp.size
                self.tensor_shape = jnp.shape
                self.is_float = lambda ary: jnp.issubdtype(ary.dtype, jnp.floating)
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
                self.stack_arrays = np.stack
                self.to_numpy = lambda x: x
                self.broadcast_arrays = np.broadcast_arrays
                self.tensor_size = np.size
                self.tensor_shape = np.shape
                self.is_float = lambda ary: np.issubdtype(ary.dtype, np.floating)
            case "torch":
                import torch
                self.to_array = torch.tensor
                self.concat = lambda arrays, axis: torch.cat(arrays, dim=axis)
                self.stack_arrays = torch.stack
                self.to_numpy = lambda ary: ary.detach().numpy()
                self.broadcast_arrays = torch.broadcast_tensors 
                self.tensor_size = torch.numel
                self.tensor_shape = lambda ary: tuple(ary.shape)
                self.is_float = lambda ary: ary.is_floating_point() 
            case other:
                raise RuntimeError(f"unsupported tensor type: '{other}'")

    def _get_series(self):
        self.all_series = {}
        req = pb.ListSeriesRequest()
        for msg in self.stub.ListSeries(req):
            self.all_series[msg.series_name] = msg

    def _create_run(self):
        req = pb.CreateRunRequest()
        resp = self.stub.CreateRun(req) # always succeeds
        self.run_handle = resp.run_handle
        self._get_series()

    def set_run_attributes(self, attrs: dict):
        """Write a set of attributes to associate with this run.

        This is useful for recording hyperparameters, settings, configuration etc.
        for the program.  Can only be called once for the life of the logger.
        """
        if self.run_handle is None:
            raise RuntimeError(f"Cannot call set_run_attrs until run started")

        req = pb.SetRunAttributesRequest(run_handle=self.run_handle)
        for key, val in attrs.items():
            if not isinstance(key, str):
                raise RuntimeError(f"All attribute keys must be strings")
            match val:
                case int(): req.attrs[key].int_val = val
                case float(): req.attrs[key].float_val = val
                case str(): req.attrs[key].text_val = val
                case bool(): req.attrs[key].bool_val = val
                case _: raise RuntimeError(
                    f"All Attribute values must be one of (int, float, bool, str)")
        _ = self.stub.SetRunAttributes(req)

    def write(self, series_name: str, /, **fields):
        """
        Append data to a series.
        If this is the first call to write to this `series_name`,
        creates it if needed.  If the series already exists, the structure must
        match, otherwise it is an error

        series_name: the name of the series to be appended
        fields: a dict containing tensor-like (or scalar) values.  The set of all
        shapes must be broadcastable to a common shape.
        """
        structure = self._get_structure(**fields)

        if series_name not in self.logged_series:
            msg = self.all_series.get(series_name)
            if msg is None:
                raise RuntimeError(
                    f"Series {series_name} doesn't exist.  Create it with:\n"
                    f"`streamvis create-series` or\n"
                    f"`grpcurl ... streamvis.v1.Service/CreateSeries`\n")
            self.logged_series[series_name] = Series(msg.series_handle, msg.structure)

        series = self.logged_series[series_name]
        if structure != series.structure:
            raise RuntimeError(
                    f"Field structure doesn't match:\n"
                    f"Existing structure: {series.structure}\n"
                    f"Current structure: {structure}\n")

        # append data
        try:
            arrays = tuple(self.to_array(fields[fn]) for fn in series.field_names)
        except BaseException as ex:
            raise RuntimeError(f"Couldn't convert data to tensors")
        try:
            arrays = self.broadcast_arrays(*arrays)
        except BaseException:
            z = zip(series.field_names, arrays)
            raise RuntimeError(
                f"Field data were not broadcastable: "
                ", ".join(f"{k}: {v.shape}" for k, v in z))

        series.queue.put_nowait(arrays)

    def _get_structure(self, **fields):
        structure = {}
        for name, ary in fields.items():
            is_float = self.is_float(self.to_array(ary))  
            structure[name] = 'float' if is_float else 'int'
        return structure

    def _flush_all_series(self) -> bool:
        finished = {}
        for series in self.logged_series.values():
            finished[series.handle] = self._flush_series(series)
        return all(finished.values())

    def _flush_series(self, series: Series) -> bool:
        current_chunk = []
        current_shape = None
        current_size = 0
        finished = False

        def _item_size(item):
            return self.tensor_size(item[0])

        def _item_shape(item):
            return self.tensor_shape(item[0])

        while not series.queue.empty():
            item = series.queue.get_nowait()
            if item is None:
                finished = True

            should_process = (
                    item is None
                    or (current_shape is not None and _item_shape(item) != current_shape)
                    or (current_size + _item_size(item) > self.max_chunk_size))

            if should_process and current_chunk:
                self._process_chunk(
                    current_chunk, 
                    series.handle, 
                    series.field_names,
                    series.field_types)
                current_chunk = []
                current_shape = None
                current_size = 0

            if finished:
                break

            current_chunk.append(item)
            current_shape = _item_shape(item)
            current_size += _item_size(item)

        return finished 

    def _process_chunk(
        self, 
        chunk: list[any], 
        series_handle: str, 
        field_names: list[str],
        field_types: list[str]
    ):
        """
        1) transpose the list of lists
        2) stack each inner list
        3) convert to numpy
        4) encode the stacked array
        5) instantiate the pb.EncTyp
        6) launch the Append request
        """
        req = pb.AppendToSeriesRequest(
            series_handle=series_handle,
            run_handle=self.run_handle,
            field_names=field_names
        )

        fields = tuple(zip(*tuple(chunk)))
        for fname, ftype, ary in zip(field_names, field_types, fields):
            stacked = self.stack_arrays(ary)
            npary = self.to_numpy(stacked).astype(SIG_TO_DTYPE[ftype])
            enc = dbutil.encode_array(npary)
            if ftype == 'float':
                optvals = tuple(pb.OptionalFloat(value=sp) for sp in enc.range_spans)
                msg = pb.EncTyp(
                    base=enc.base.tobytes(), 
                    shape=enc.shape,
                    fval=pb.FloatValues(values=optvals)
                )
            elif ftype == 'int':
                optvals = tuple(pb.OptionalInt(value=sp) for sp in enc.range_spans)
                msg = pb.EncTyp(
                    base=enc.base.tobytes(), 
                    shape=enc.shape,
                    ival=pb.IntValues(values=optvals)
                )
            else:
                raise RuntimeError(f"Invalid ftype: {ftype}")
            req.field_vals.append(msg)

        resp = self.stub.AppendToSeries(req)
        if not resp.success:
            raise RuntimeError(f"AppendToSeries request failed")


class AsyncDataLogger(BaseLogger):

    def __init__(
        self, 
        grpc_uri: str,
        tensor_type: Literal["jax", "torch", "numpy"]="numpy",
        max_chunk_size: int=100000,
        flush_every: float=2.0
    ):
        super().__init__(grpc_uri, tensor_type, max_chunk_size, flush_every)
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
        self._create_run()
        self._task_group.create_task(self.flush_buffer())
        return self

    async def __aexit__(self, *args):
        for series in self.logged_series.values():
            series.queue.put_nowait(None)
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
        tensor_type: Literal["jax", "torch", "numpy"]="numpy",
        max_chunk_size: int=100000,
        flush_every: float=2.0
    ):
        super().__init__(grpc_uri, tensor_type, max_chunk_size, flush_every)
        self.exiting = False
        self._flush_thread = threading.Thread(target=self.flush_buffer, daemon=True)

    def start(self):
        self._create_run()
        self._flush_thread.start()

    def flush_buffer(self):
        while True:
            if not super()._flush_all_series():
                if self.exiting:
                    break
            time.sleep(self.flush_every)

    def stop(self):
        self.exiting = True
        for series in self.logged_series.values():
            series.queue.put_nowait(None)
        self._flush_thread.join()

    def write(self, series_name: str, /, **data):
        super().write(series_name, **data)

