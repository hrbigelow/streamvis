import socket
import sys
import random
import asyncio
import grpc
from grpc import aio
import re
from . import util
from . import data_pb2 as pb
from . import data_pb2_grpc as pb_grpc
from google.protobuf.empty_pb2 import Empty

class AsyncRecordService(pb_grpc.RecordServiceServicer):
    def __init__(self, path: str):
        self.data_path = util.data_file(path)
        self.index_path = util.index_file(path)
        self.append_index_fh = util.get_log_handle(self.index_path, "ab")
        self.append_data_fh = util.get_log_handle(self.data_path, "ab")
        self.read_index_fh = util.get_log_handle(self.index_path, "rb")
        self.read_data_fh = util.get_log_handle(self.data_path, "rb")
        self.last_issued_id = self._get_max_id()

    def _get_max_id(self):
        index = util.Index.from_filters()
        index.update(self.read_index_fh)
        return index.max_id


    def issue_id(self):
        assert self.last_issued_id is not None, "Must scan index first"
        self.last_issued_id += 1
        return self.last_issued_id

    async def QueryRecords(self, request: pb.Index, context):
        index = util.Index.from_message(request)
        index.update(self.read_index_fh)
        datas_map = util.load_data(self.read_data_fh, index.entry_list)
        pb_index = index.to_message()
        rec = pb.StreamedRecord(type=pb.INDEX, index=pb_index)
        await context.write(rec)

        for entry in index.entry_list:
            datas = datas_map[entry.entry_id]
            for data in datas:
                data.name_id = entry.name_id
                rec = pb.StreamedRecord(type=pb.DATA, data=data)
                await context.write(rec)

    async def Scopes(self, request: Empty, context):
        """Return a list of scope names that have content"""
        index = util.Index.from_filters()
        index.update(self.read_index_fh)
        for scope_name in index.scope_list:
            rec = pb.StreamedRecord(type=pb.STRING, value=scope_name)
            await context.write(rec)

    async def Names(self, request: pb.ScopeRequest, context):
        scope_pat = f"^{request.scope}$"
        index = util.Index.from_filters(scope_filter=scope_pat)
        index.update(self.read_index_fh)
        for name in index.name_list:
            rec = pb.StreamedRecord(type=pb.STRING, value=name)
            await context.write(rec)

    async def Configs(self, request: pb.ScopeRequest, context):
        scope_pat = f"^{request.scope}$"
        index = util.Index.from_filters(scope_filter=scope_pat)
        index.update(self.read_index_fh)

        pb_index = index.to_message()
        rec = pb.StreamedRecord(type=pb.INDEX, index=pb_index)
        await context.write(rec)

        cfgs_map = util.load_data(self.read_data_fh, index.config_entry_list)
        for cfg_entry in index.config_entry_list:
            cfgs = cfgs_map[cfg_entry.entry_id]
            for cfg in cfgs:
                cfg.scope_id = cfg_entry.scope_id
                rec = pb.StreamedRecord(type=pb.CONFIG, config=cfg)
                await context.write(rec)

    async def WriteScope(self, request: pb.WriteScopeRequest, context):
        scope_id = self.issue_id()
        pack = util.pack_scope(scope_id, request.scope)
        util.safe_write(self.append_index_fh, pack)
        return pb.IntegerResponse(value=scope_id)

    async def WriteConfig(self, request: pb.WriteConfigRequest, context):
        entry_id = self.issue_id() 
        pb_config = pb.Config(entry_id=entry_id, attributes=request.attributes)
        pack = util.pack_message(pb_config)
        end_offset = util.safe_write(self.append_data_fh, pack)
        beg_offset = end_offset - len(pack)
        config_entry = util.pack_config_entry(entry_id, request.scope_id, beg_offset, end_offset)
        util.safe_write(self.append_index_fh, config_entry)
        return Empty()

    async def WriteNames(self, request: pb.WriteNameRequest, context):
        name_packs = []
        for name in request.names:
            name.name_id = self.issue_id()
            name_packs.append(util.pack_message(name))
        pack = b''.join(name_packs)
        util.safe_write(self.append_index_fh, pack)

        for name in request.names:
            rec = pb.StreamedRecord(type=pb.NAME, name=name)
            await context.write(rec)

    async def DeleteScopeNames(self, request:pb.ScopeNameRequest, context):
        messages = []
        for name in request.names:
            messages.append(util.pack_delete_name(request.scope, name))
        pack = b''.join(messages)
        util.safe_write(self.append_index_fh, pack)
        return Empty()

    async def WriteData(self, request: pb.WriteDataRequest, context):
        data_packs = []
        for pb_data in request.datas:
            pb_data.entry_id = self.issue_id()
            data_packs.append(util.pack_message(pb_data))

        lengths = [len(p) for p in data_packs]
        pack = b''.join(data_packs)
        global_end = util.safe_write(self.append_data_fh, pack)
        global_beg = global_end - len(pack)
        rel_offsets = [global_beg]
        for l in lengths:
            rel_offsets.append(rel_offsets[-1] + l)

        entry_packs = []
        for pb_data, beg, end in zip(request.datas, rel_offsets[:-1], rel_offsets[1:]):
            entry = pb.DataEntry(
                    entry_id=pb_data.entry_id, name_id=pb_data.name_id,
                    beg_offset=beg, end_offset=end)
            entry_packs.append(util.pack_message(entry))

        entry_pack = b''.join(entry_packs)
        util.safe_write(self.append_index_fh, entry_pack)
        return Empty()


    async def RewriteLogs(self, request: Empty, context):
        """Rewrite the index and data log files."""
        pass


def port_in_use(port: int) -> bool:
    """Check if a port is already in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(('', port))
            return False
        except OSError:
            return True


async def serve(path: str, port: int):
    if port_in_use(port):
        print(f"Error: port {port} already in use")
        sys.exit(1)

    try:
        server = aio.server()
        rs = AsyncRecordService(path)
        pb_grpc.add_RecordServiceServicer_to_server(rs, server)
        server.add_insecure_port(f"[::]:{port}")
        await server.start()
        print(f"gRPC server started on port {port}")
        await server.wait_for_termination()
    except KeyboardInterrupt:
        await server.stop(5.0)
        print("Shutdown due to Ctrl-C")
    finally:
        await server.stop(5.0)
        print("Shutdown")

if __name__ == "__main__":
    path, port = sys.argv[1], int(sys.argv[2])
    asyncio.run(serve(path, port))

