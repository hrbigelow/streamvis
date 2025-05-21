import sys
import asyncio
import grpc
from grpc import aio
from . import util
from . import data_pb2 as pb
from . import data_pb2_grpc as pb_grpc

def get_optional(msg, field_name):
    return getattr(msg, field_name) if msg.HasField(field_name) else None

class AsyncRecordService(pb_grpc.RecordServiceServicer):
    def __init__(self, path: str):
        self.path = path
        self.data_fh = util.get_log_handle(path, "rb")

    async def QueryRecords(self, request, context):
        scope = get_optional(request, "scope")
        name = get_optional(request, "name")
        metas_map, entries_map = util.load_index(self.path, scope, name)
        entries = list(entries_map.values())
        datas_map = util.load_data(self.data_fh, entries)
        # import pdb
        # pdb.set_trace()
        for meta in metas_map.values():
            rec = pb.StreamedRecord(type=pb.METADATA, metadata=meta)
            await context.write(rec)

        for entry_id, datas in datas_map.items():
            ent = entries_map[entry_id]
            for data in datas:
                data.meta_id = ent.meta_id
                rec = pb.StreamedRecord(type=pb.DATA, data=data)
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

