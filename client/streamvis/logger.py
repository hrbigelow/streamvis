import asyncio
import grpc
import numpy as np
from typing import Literal
import threading

from .v1 import data_pb2 as pb
from .v1 import data_pb2_grpc as pb_grpc

# Store in little endian
SIG_TO_DTYPE = {
    'f32': np.dtype('<f4'),
    'i32': np.dtype('<i4'),
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
        scope: str,
        grpc_uri: str,
        tensor_type: Literal["jax", "torch", "numpy"]="numpy",
        delete_existing_scope: bool=False,
        delete_existing_series: bool=True,
        flush_every: float=2.0,
    ):
        self.scope = scope
        self.delete_existing_scope = delete_existing_scope
        self.delete_existing_series = delete_existing_series
        self.logged_series = {} # series_name => Series
        self.chan = grpc.insecure_channel(grpc_uri)
        self.stub = pb_grpc.ServiceStub(self.chan)
        self.flush_every = flush_every
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

    def _init_scope(self):
        req = pb.GetScopeRequest(
                scope_name=self.scope, 
                delete_existing=self.delete_existing_scope)
        resp = self.stub.MakeOrGetScope(req)
        self.scope_handle = h = resp.scope_handle
        if h is None:
            raise RuntimeError(f"Couldn't create scope '{self.scope}'")

    def _get_structure(self, **fields):
        structure = {}
        for name, ary in fields.items():
            is_float = self.is_float(self.to_array(ary))  
            structure[name] = 'f32' if is_float else 'i32'
        return structure

    def write(self, series_name: str, /, **fields):
        """
        Append data to a series.
        If this is the first call to write to this `series_name`,
        creates it if needed.  If delete_existing_series is True, deletes any
        existing series by this name.

        series_name: the name of the series to be appended
        fields: a dict containing tensor-like (or scalar) values.  The set of all
        shapes must be broadcastable to a common shape.
        """
        structure = self._get_structure(**fields)

        if series_name not in self.logged_series:
            req = pb.GetSeriesRequest(
                scope_handle=self.scope_handle,
                series_name=series_name,
                structure=structure,
                delete_existing=self.delete_existing_series
            )
            resp = self.stub.MakeOrGetSeries(req)
            self.series_handle = h = resp.series_handle
            if h is None:
                raise RuntimeError(f"Couldn't create series '{series_name}'")
            self.logged_series[series_name] = Series(h, structure)

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

    def _flush_all_series(self) -> bool:
        more_work = False
        for series in self.logged_series.values():
            if self._flush_series(series):
                more_work = True
        return more_work

    def _flush_series(self, series: Series) -> bool:
        current_chunk = []
        current_shape = None
        current_size = 0
        more_work = True

        def _item_size(item):
            return self.tensor_size(item[0])

        def _item_shape(item):
            return self.tensor_shape(item[0])

        while not series.queue.empty():
            item = series.queue.get_nowait()
            if item is None:
                more_work = False

            should_process = (
                    item is None
                    or (current_shape is None or _item_shape(item) == current_shape)
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

            if not more_work:
                break

            current_chunk.append(item)
            current_shape = _item_shape(item)
            current_size += _item_size(item)

        return more_work

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
            field_names=field_names
        )

        fields = tuple(zip(*tuple(chunk)))
        for fname, ftype, ary in zip(field_names, field_types, fields):
            stacked = self.stack_arrays(ary)
            npary = self.to_numpy(stacked).astype(SIG_TO_DTYPE[ftype])
            enc = dbutil.encode_array(npary)
            if ftype == 'f32':
                msg = pb.EncTyp(
                    base=enc.base.tobytes(), 
                    shape=enc.shape,
                    fval=pb.FloatValues(enc.range_spans)
                )
            elif ftype == 'i32':
                msg = pb.EncTyp(
                    base=enc.base.tobytes(), 
                    shape=enc.shape,
                    ival=pb.IntValues(enc.range_spans)
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
        scope: str, 
        grpc_uri: str,
        tensor_type: Literal["jax", "torch", "numpy"]="numpy",
        delete_existing_scope: bool=True,
        delete_existing_series: bool=True,
        flush_every: float=2.0
    ):
        super().__init__(scope, grpc_uri, tensor_type, delete_existing_scope, 
                         delete_existing_series, flush_every)

    async def flush_buffer(self):
        while True:
            if not self._flush_all_series():
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
        for series in self.logged_series.values():
            series.queue.put_nowait(None)
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
        delete_existing_scope: bool=False,
        delete_existing_series: bool=True,
        flush_every: float=2.0
    ):
        super().__init__(scope, grpc_uri, tensor_type, delete_existing_scope, 
                         delete_existing_series, flush_every)
        self._flush_thread = threading.Thread(target=self.flush_buffer, daemon=True)

    def start(self):
        self._flush_thread.start()
        super()._init_scope()

    def flush_buffer(self):
        while True:
            if not self._flush_all_series():
                break
            time.sleep(self.flush_every)

    def stop(self):
        for series in self.logged_series.values():
            series.queue.put_nowait(None)
        self._flush_thread.join()

    def write(self, name: str, /, start_index: int=0, **data):
        super().write(name, start_index, **data)

