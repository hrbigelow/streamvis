from typing import Literal
import os
import enum
import numpy as np
import asyncio
import fcntl
import random
import time
import signal
from . import util


class Action(enum.Enum):
    DELETE = 0

class DataLogger:
    """
    Create one instance of this in the producer script, to send data to
    a bokeh server.
    """
    def __init__(self, scope: str):
        """scope: a string which defines the scope in the logical point grouping."""
        self.scope = scope
        self.metadata_seen = {} # meta_id => list[tuple[str, dtype]]
        self.buffer = asyncio.Queue()
        self.elem_count = 0
        self.uint32_max = (1 << 32) - 1 
        random.seed(time.time())

    def init(
        self, 
        path: str, 
        flush_every: float = 2.0,
        tensor_type: Literal["jax", "torch", "numpy"]="jax",
    ):
        """
        Initialize logger to log data to the given path.
        path:  filesystem path or gs:// resource
        flush_every:  (seconds) period for flushing to disk
        """
        self.data_fh = util.get_log_handle(path, "ab")
        self.index_fh = util.get_log_handle(f"{path}.idx", "ab")
        self.flush_every = flush_every
        match tensor_type:
            case "jax":
                import jax.numpy as jnp
                self.to_array = jnp.array
                self.concat = jnp.concatenate 
                self.to_numpy = lambda ary: np.array(ary)
                self.broadcast_arrays = jnp.broadcast_arrays
            case "numpy":
                self.to_array = np.array
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

    async def start(self):
        self._task_group = asyncio.TaskGroup()
        await self._task_group.__aenter__()
        self.flush_task = self._task_group.create_task(self.flush_buffer())

    async def shutdown(self):
        """
        Call shutdown in a SIGINT or SIGTERM signal handler in your main application
        for a clean exit.  Unfortunately, this is not always possible, for example
        when running in Google Colab.
        """
        await self._task_group.__aexit__(None, RuntimeError("bla"), None)
        self._task_group = None
        self.flush_task = None
        self.data_fh.close()
        self.index_fh.close()

    def metadata_id(self, name: str):
        return util.metadata_id(self.scope, name)

    @staticmethod
    def safe_write(fh, content: bytes) -> int:
        """Write to fh in concurrency-safe way.  Return current offset after write."""
        if not isinstance(content, bytes):
            raise RuntimeError(f"content must be bytes")

        try:
            # Only lock during the actual write operation
            fcntl.flock(fh, fcntl.LOCK_EX)
            _ = fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
            current_offset = fh.tell()
            return current_offset
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)

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

    def write_sync(self, name, /, **data):
        """Writes new data, possibly creating one or more Group items.

        Inputs:
        name:  
          the `name` field of the (scope, name, index) tuple that will be associated
          with these points.
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

        meta_id = self.metadata_id(name)
        if meta_id not in self.metadata_seen:
            self.metadata_seen[meta_id] = tuple((k, v.dtype) for k, v in data.items())
        self.buffer.put_nowait((name, data))

    async def write(self, name, /, **data):
        self.write_sync(name, **data)
        await asyncio.sleep(0) # explicit yield

    async def yield_to_flush(self):
        """An explicit yield function to allow buffer flush.

        If you only use write_sync, call this periodically.
        """
        await asyncio.sleep(0)

    def _write_content(
        self,
        content: dict[str, dict[str, 'tensor']], # name => cds
        metadata: dict[int, str],                # meta_id => name
        deleted_names: set[str]):                # set[name]

        entry_args = []
        data_bytes = []
        rel_offsets = [0]
        for meta_id, data in content.items():
            field_sig = self.metadata_seen.get(meta_id)
            if field_sig is None:
                raise RuntimeError("Unknown error")
            entry_id = random.randint(0, self.uint32_max)
            data = {k: self.to_numpy(v) for k, v in data.items()}
            packed = util.pack_data(entry_id, data, field_sig)
            data_bytes.append(packed)
            rel_offsets.append(rel_offsets[-1] + len(packed))
            entry_args.append((entry_id, meta_id))
        all_data_bytes = b''.join(data_bytes)
        global_end = self.safe_write(self.data_fh, all_data_bytes)
        global_off = global_end - len(all_data_bytes)

        abs_offsets = [off+global_off for off in rel_offsets]
        z = zip(entry_args, abs_offsets[:-1], abs_offsets[1:])
        entry_bytes = []
        for (entry_id, meta_id), beg_offset, end_offset in z: 
            packed = util.pack_entry(entry_id, meta_id, beg_offset, end_offset)
            entry_bytes.append(packed)
        all_entry_bytes = b''.join(entry_bytes)

        deletes_bytes = []
        for name in deleted_names: 
            packed = util.pack_control(self.scope, name)
            deletes_bytes.append(packed)
        all_deletes_bytes = b''.join(deletes_bytes)

        meta_bytes = []
        for meta_id, name in metadata.items():
            field_sig = self.metadata_seen.get(meta_id)
            if field_sig is None:
                raise RuntimeError("Unknown error")
            packed = util.pack_metadata(meta_id, self.scope, name, field_sig)
            meta_bytes.append(packed)
        all_meta_bytes = b''.join(meta_bytes)
        all_index_bytes = all_deletes_bytes + all_meta_bytes + all_entry_bytes
        _ = self.safe_write(self.index_fh, all_index_bytes)

    async def flush_buffer(self):
        while True:
            deleted_names = set()
            content_as_list = {} # meta_id => Dict[str, list['tensor']]
            metas = {} # meta_id => name
            num_items = self.buffer.qsize()
            print(f"flush_buffer processing {num_items} items...", end="", flush=True)
            for _ in range(num_items):
                name, item = await self.buffer.get()
                meta_id = self.metadata_id(name)

                if isinstance(item, Action):
                    if item == Action.DELETE:
                        deleted_names.add(name)
                        content_as_list.pop(meta_id, None)
                        metas.pop(meta_id, None)
                elif isinstance(item, dict):
                    if meta_id not in content_as_list:
                        content_as_list[meta_id] = {k: [] for k in item.keys()}
                        metas[meta_id] = name

                    cdslist = content_as_list[meta_id]
                    for k, v in item.items():
                        cdslist[k].append(v)
                else:
                    raise RuntimeError(f"flush_buffer: Unknown item type: {type(item)}")

            print(f"collated...", end="", flush=True)
            content = {}
            for meta_id, cdslist in content_as_list.items():
                cds = {k: self.concat(vs, axis=1) for k, vs in cdslist.items()}
                content[meta_id] = cds
            print(f"concatted...", end="", flush=True)

            self._write_content(content, metas, deleted_names)
            print("done.", flush=True)
            try:
                await asyncio.sleep(self.flush_every)
            except asyncio.CancelledError:
                break

    def delete_name(self, name: str):
        """Logs a Control with a DELETE action to the log file with this logger's scope.
        and the provided `name`.

        This will be processed by the server to delete all points and groups
        with scope and name.  To purge these from the log file, currently the server
        must be stopped and the implementation will be TODO"""
        self.buffer.put_nowait((name, Action.DELETE))


