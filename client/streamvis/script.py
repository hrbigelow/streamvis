import os
import fire
import grpc
import dateparser
from streamvis.v1 import data_pb2 as pb
from streamvis.v1 import data_pb2_grpc as pb_grpc
from . import dbutil, rpc_client
from google.protobuf import json_format 
from typing import Any
import json

from .demo import log_data, log_data_async

GRPC_URI=None
WEB_URI=None

def msg_to_json(msg, indent=None):
    d = json_format.MessageToDict(
            msg,
            preserving_proto_field_name=True,
        )
    return json.dumps(d, indent=indent)

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

def create_field(
        name: str,
        data_type: str,
        description: str
        ):
    global GRPC_URI
    chan = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(chan)
    req = pb.CreateFieldRequest(name=name, data_type=data_type, description=description)
    resp = stub.CreateField(req)

def create_series(
        series_name: str,
        *field_names: list[str]
        ):
    global GRPC_URI
    chan = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(chan)
    req = pb.CreateSeriesRequest(
            series_name=series_name,
            field_names=field_names
            )
    resp = stub.CreateSeries(req)

def create_run():
    global GRPC_URI
    chan = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(chan)
    req = pb.CreateRunRequest()
    resp = stub.CreateRun(req)
    print(msg_to_json(resp))


def set_run_attributes(run_handle, /, attrs: dict[str, Any]):
    global GRPC_URI
    chan = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(chan)
    req = pb.ListFieldsRequest()
    fields = stub.ListFields(req)
    fields_map = { f.name: f for f in fields }

    req = pb.SetRunAttributesRequest(run_handle=run_handle)
    for field_name, value in attrs.items():
        field = fields_map.get(field_name)
        if field is None:
            raise RuntimeError(f"No field named {field_name}")
        attr = dbutil.make_field_value(field, value)
        req.attrs.append(attr)
    stub.SetRunAttributes(req)

def delete_empty_series(series_name: str):
    global GRPC_URI
    chan = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(chan)
    req = pb.DeleteEmptySeriesRequest(series_name=series_name)
    _ = stub.DeleteEmptySeries(req)

def add_run_tag(
        tag: str,
        after: str|None = None,
        until: str|None = None,
        req_tags: list[str]|None = None,
        match_all_tags: bool = False,
        ):
    if req_tags is None:
        req_tags = []

    if until is not None:
        until = dateparser.parse(until, settings={"RETURN_AS_TIMEZONE_AWARE": True})

    if after is not None:
        after = dateparser.parse(after, settings={"RETURN_AS_TIMEZONE_AWARE": True})

    global GRPC_URI
    chan = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(chan)

    rf = rpc_client.get_run_filter(req_tags, match_all_tags, after, until)
    req = pb.AddRunTagRequest(run_filter=rf, tag=tag)
    resp = stub.AddRunTag(req)

def delete_run_tag(run_handle: str, tag: str):
    global GRPC_URI
    chan = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(chan)
    req = pb.DeleteRunTagRequest(run_handle=run_handle, tag=tag)
    _ = stub.DeleteRunTag(req)

def list_series():
    global GRPC_URI
    chan = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(chan)
    req = pb.ListSeriesRequest()
    for msg in stub.ListSeries(req):
        print(json_format.MessageToString(msg))

def list_runs(
        after: str|None = None,
        until: str|None = None,
    ):

    if until is not None:
        until = dateparser.parse(until, settings={"RETURN_AS_TIMEZONE_AWARE": True})

    if after is not None:
        after = dateparser.parse(after, settings={"RETURN_AS_TIMEZONE_AWARE": True})

    global GRPC_URI
    chan = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(chan)
    rf = rpc_client.get_run_filter([], False, after, until)
    req = pb.ListRunsRequest(run_filter=rf)
    for msg in stub.ListRuns(req):
        print(msg_to_json(msg))

def list_fields():
    global GRPC_URI
    chan = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(chan)
    req = pb.ListFieldsRequest()
    for msg in stub.ListFields(req):
        print(msg_to_json(msg))

def list_attribute_values():
    global GRPC_URI
    chan = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(chan)
    req = pb.ListAttributeValuesRequest()
    for msg in stub.ListAttributeValues(req):
        print(msg_to_json(msg))

def list_run_starts():
    global GRPC_URI
    chan = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(chan)
    req = pb.ListStartedAtRequest()
    for msg in stub.ListStartedAt(req):
        print(msg_to_json(msg))

def list_tags():
    global GRPC_URI
    chan = grpc.insecure_channel(GRPC_URI)
    stub = pb_grpc.ServiceStub(chan)
    req = pb.ListTagsRequest()
    for msg in stub.ListTags(req):
        print(msg_to_json(msg))


COMMANDS = { 
            "create-field": create_field,
            "create-series": create_series,
            "create-run": create_run,
            "set-run-attributes": set_run_attributes,
            "delete-empty-series": delete_empty_series,
            "add-run-tag": add_run_tag,
            "delete-run-tag": delete_run_tag,
            "list-series": list_series,
            "list-runs": list_runs,
            "list-fields": list_fields,
            "list-attribute-values": list_attribute_values,
            "list-run-starts": list_run_starts,
            "list-tags": list_tags,
            "logging-demo": demo_sync_fn, 
            "logging-demo-async": demo_async_fn,
            }

def main():
    init_grpc_uri()
    fire.Fire(COMMANDS)

if __name__ == '__main__':
    main()
