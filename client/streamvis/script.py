import os
import fire

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
    scope: str, 
    delete_existing_series: bool=True, 
    num_steps: int=2000,
    step_sleep_ms: int=0,
):
    global GRPC_URI
    num_steps = int(num_steps)
    log_data(GRPC_URI, scope, delete_existing_series, num_steps, step_sleep_ms)

def demo_async_fn(
    scope: str, 
    delete_existing_series: bool=True, 
    num_steps: int=2000,
    report_every: int=100
):
    global GRPC_URI
    num_steps = int(num_steps)
    asyncio.run(log_data_async(GRPC_URI, scope, delete_existing_series, num_steps))


def main():
    init_grpc_uri()

    tasks = { 
             # "web-serve": serve,
             "logging-demo": demo_sync_fn, 
             "logging-demo-async": demo_async_fn,
             # "scopes": scopes,
             # "names": names,
             # "counts": counts,
             # "delete": delete_name,
             # "config": config,
             # "liftover": liftover,
            }
    fire.Fire(tasks)

if __name__ == '__main__':
    main()
