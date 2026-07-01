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

def create_field(
        name: str,
        data_type: str,
        description: str
        ):
    stub = rpc_client.get_service_stub()
    req = pb.CreateFieldRequest(name=name, data_type=data_type, description=description)
    resp = stub.CreateField(req)

def create_run():
    stub = rpc_client.get_service_stub()
    req = pb.CreateRunRequest()
    resp = stub.CreateRun(req)
    print(msg_to_json(resp))


def set_run_attributes(run_handle, /, attrs: dict[str, Any]):
    stub = rpc_client.get_service_stub()
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

def _resolve_run_filter_args(
    after: str|None,
    until: str|None,
    pos_tags: list[str]|None,
    pos_match_all_tags: bool,
    neg_tags: list[str]|None,
    neg_match_all_tags: bool,
) -> pb.RunFilter:

    if pos_tags is None:
        pos_tags = []

    if neg_tags is None:
        neg_tags = []

    if until is not None:
        until = dateparser.parse(until, settings={"RETURN_AS_TIMEZONE_AWARE": True})

    if after is not None:
        after = dateparser.parse(after, settings={"RETURN_AS_TIMEZONE_AWARE": True})

    return rpc_client.get_run_filter(
		pos_tags, pos_match_all_tags, neg_tags, neg_match_all_tags, after, until)


def add_run_tag(
        tag_to_add: str,
        after: str|None = None,
        until: str|None = None,
        pos_tags: list[str]|None = None,
        pos_match_all_tags: bool = False,
		neg_tags: list[str]|None = None,
		neg_match_all_tags: bool = False,
):
    stub = rpc_client.get_service_stub()
    rf = _resolve_run_filter_args(
        after, until, pos_tags, pos_match_all_tags, neg_tags, neg_match_all_tags)

    req = pb.ListRunsRequest(run_filter=rf)
    handles = tuple(r.handle for r in stub.ListRuns(req))
    for handle in handles:
        rtreq = pb.AddRunTagRequest(handle, tags=[tag])
        _ = stub.AddRunTags(rtreq)

def delete_run_tag(
        tag_to_del: str,
        after: str|None = None,
        until: str|None = None,
        pos_tags: list[str]|None = None,
        pos_match_all_tags: bool = False,
		neg_tags: list[str]|None = None,
		neg_match_all_tags: bool = False,
):
    stub = rpc_client.get_service_stub()
    rf = _resolve_run_filter_args(
        after, until, pos_tags, pos_match_all_tags, neg_tags, neg_match_all_tags)
    req = pb.ListRunsRequest(run_filter=rf)
    handles = tuple(r.handle for r in stub.ListRuns(req))

    for handle in handles:
        req = pb.DeleteRunTagRequest(run_handle=run_handle, tag=tag_to_del)
        _ = stub.DeleteRunTag(req)

def list_runs(
    after: str|None = None,
    until: str|None = None,
    pos_tags: list[str]|None = None,
    pos_match_all_tags: bool = False,
    neg_tags: list[str] = None,
    neg_match_all_tags: bool = False,
):
    stub = rpc_client.get_service_stub()
    rf = _resolve_run_filter_args(
        after, until, pos_tags, pos_match_all_tags, neg_tags, neg_match_all_tags)
    req = pb.ListRunsRequest(run_filter=rf)
    for run in stub.ListRuns(req):
        print(msg_to_json(run))

def list_fields():
    stub = rpc_client.get_service_stub()
    req = pb.ListFieldsRequest()
    for msg in stub.ListFields(req):
        print(msg_to_json(msg))

def list_attribute_values():
    stub = rpc_client.get_service_stub()
    req = pb.ListAttributeValuesRequest()
    for msg in stub.ListAttributeValues(req):
        print(msg_to_json(msg))

def list_run_starts():
    stub = rpc_client.get_service_stub()
    req = pb.ListStartedAtRequest()
    for msg in stub.ListStartedAt(req):
        print(msg_to_json(msg))

def list_tags():
    stub = rpc_client.get_service_stub()
    req = pb.ListTagsRequest()
    for msg in stub.ListTags(req):
        print(msg_to_json(msg))


COMMANDS = { 
            "create-field": create_field,
            "create-run": create_run,
            "set-run-attributes": set_run_attributes,
            "add-run-tag": add_run_tag,
            "delete-run-tag": delete_run_tag,
            "list-runs": list_runs,
            "list-fields": list_fields,
            "list-attribute-values": list_attribute_values,
            "list-run-starts": list_run_starts,
            "list-tags": list_tags,
            }

def main():
    init_grpc_uri()
    fire.Fire(COMMANDS)

if __name__ == '__main__':
    main()
