import asyncio
import grpc
from grpc import aio
from . import data_pb2 as pb
from . import data_pb2_grpc as pb_grpc

async def fetch(uri: str, scope: str=None, name: str=None):
    async with aio.insecure_channel(uri) as chan:
        stub = pb_grpc.RecordServiceStub(chan)
        query = pb.QueryRequest(scope=scope, name=name) 
        async for record in stub.QueryRecords(query):

