import os
import fire
import grpc
from streamvis.v1 import data_pb2 as pb
from streamvis.v1 import data_pb2_grpc as pb_grpc

from .demo import log_data, log_data_async

GRPC_URI=None
WEB_URI=None

def init_grpc_uri():
    global GRPC_URI
    GRPC_URI = os.getenv('STREAMVIS_GRPC_URI')
    if GRPC_URI is None:
        raise RuntimeError("streamvis requires STREAMVIS_GRPC_URI variable set")

def init_web_uri():
    global WEB_URI
    WEB_URI = os.getenv('STREAMVIS_WEB_URI')
    if WEB_URI is None:
        raise RuntimeError("streamvis server requires STREAMVIS_WEB_URI variable set")

def demo_sync_fn(
    num_steps: int=2000,
    step_sleep_ms: int=0,
):
    global GRPC_URI
    num_steps = int(num_steps)
    log_data(GRPC_URI, num_steps, step_sleep_ms)

def demo_async_fn(
    num_steps: int=2000,
    report_every: int=100
):
    global GRPC_URI
    num_steps = int(num_steps)
    asyncio.run(log_data_async(GRPC_URI, num_steps))

def create_attr(
    attr_name: str,
    attr_type: str,
    attr_desc: str
):
    global GRPC_URI
    chan = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(chan)
    req = pb.CreateAttributeRequest(
        attr_name=attr_name,
        attr_type=attr_type,
        attr_desc=attr_desc
    )
    resp = stub.CreateAttribute(req)

def create_series(
    series_name: str,
    series_structure: dict
):
    global GRPC_URI
    chan = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(chan)
    req = pb.CreateSeriesRequest(
        series_name=series_name,
        structure=series_structure
    )
    resp = stub.CreateSeries(req)

def list_series():
    chan = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(chan)
    req = pb.ListSeriesRequest()
    for msg in stub.ListSeries(req):
        print(msg)


def main():
    init_grpc_uri()

    tasks = { 
             "create-attr": create_attr,
             "create-series": create_series,
             "list-series": list_series,
             # "web-serve": serve,
             "logging-demo": demo_sync_fn, 
             "logging-demo-async": demo_async_fn,
             # "names": names,
             # "counts": counts,
             # "delete": delete_name,
             # "config": config,
             # "liftover": liftover,
            }
    fire.Fire(tasks)

if __name__ == '__main__':
    main()
