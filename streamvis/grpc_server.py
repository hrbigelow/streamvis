import sys
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
        self.index_fh = util.get_log_handle(self.index_path, "rb")
        self.data_fh = util.get_log_handle(self.data_path, "rb")

    async def QueryRecords(self, request: pb.Index, context):
        index = util.Index.from_message(request)
        index.update(self.index_fh)
        datas_map = util.load_data(self.data_fh, index.entry_list)
        pb_index = index.export()
        rec = pb.StreamedRecord(type=pb.INDEX, index=pb_index)
        await context.write(rec)

        for entry in index.entry_list:
            datas = datas_map[entry.entry_id]
            for data in datas:
                data.name_id = entry.name_id
                rec = pb.StreamedRecord(type=pb.DATA, data=data)
                await context.write(rec)

    async def Scopes(self, request: Empty, context):
        index = util.Index.from_filters()
        index.update(self.index_fh)
        for scope in index.scope_list:
            rec = pb.StreamedRecord(type=pb.STRING, value=scope.scope)
            await context.write(rec)

    async def Names(self, request: pb.ScopeRequest, context):
        scope_pat = re.compile(f"^{request.scope}$")
        index = util.Index.from_filters(scope_filter=scope_pat)
        index.update(self.index_fh)
        for name in index.name_list:
            rec = pb.StreamedRecord(type=pb.STRING, value=name.name)
            await context.write(rec)

    async def Configs(self, request: pb.ScopeRequest, context):
        scope_pat = re.compile(f"^{request.scope}$")
        index = util.Index.from_filters(scope_filter=scope_pat)
        index.update(self.index_fh)

        pb_index = index.export()
        rec = pb.StreamedRecord(type=pb.INDEX, index=pb_index)
        await context.write(rec)

        cfgs_map = util.load_data(self.data_fh, index.config_entry_list)
        for cfg_entry in index.config_entry_list:
            cfgs = cfgs_map[cfg_entry.entry_id]
            for cfg in cfgs:
                cfg.scope_id = cfg_entry.scope_id
                rec = pb.StreamedRecord(type=pb.CONFIG, config=cfg)
                await context.write(rec)



async def serve(path: str, port: int):
    server = aio.server()
    rs = AsyncRecordService(path)
    pb_grpc.add_RecordServiceServicer_to_server(rs, server)
    server.add_insecure_port(f"[::]:{port}")
    await server.start()
    await server.wait_for_termination()

if __name__ == "__main__":
    asyncio.run(serve(*sys.argv[1:]))

