from dataclasses import asdict
from functools import wraps
import json
from protobuf_to_dict import protobuf_to_dict
from dacite import from_dict

from schemes.graph import GraphNode, GraphRelation
from configs.config import logger


def raise_customized_error(capture, target):
    def _raise_customized_error(func):
        @wraps(func)
        def wapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except capture:
                raise target
        return wapper
    return _raise_customized_error


def raise_grpc_error(capture, grpc_status_code):
    def _raise_grpc_error(func):
        @wraps(func)
        def wrapper(self, request, context):
            try:
                return func(self, request, context)
            except capture as e:
                context.set_code(grpc_status_code)
                if hasattr(e, "desc"):
                    context.set_details(e.desc)
                else:
                    context.set_details("Maybe RPC Error.")
        return wrapper
    return _raise_grpc_error


def deco_log_error(logger):
    def _deco_log_error(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if logger:
                    logger.exception(e)
                raise e
                # return {"errors": {"code": e.code, "desc": e.desc}}
        return wrapper
    return _deco_log_error


def convert_node_to_graphnode(node):
    label = str(node.labels)[1:]
    dct = dict(node)
    name = dct.pop("name")
    gn = GraphNode(label, name, dct)
    return gn


def convert_relation_to_graph_relation(relation):
    start = convert_node_to_graphnode(relation.start_node)
    end = convert_node_to_graphnode(relation.end_node)
    kind = list(relation.types())[0]
    props = dict(relation)
    gr = GraphRelation(start, end, kind, props)
    return gr


def convert_query_to_scheme():
    def _convert_query_to_scheme(func):
        @wraps(func)
        def wrapper(self, qin, **kwargs):
            query = func(self, qin, **kwargs)
            result = []
            for gobj in query:
                if gobj.relationships:
                    obj = convert_relation_to_graph_relation(gobj)
                else:
                    obj = convert_node_to_graphnode(gobj)
                result.append(obj)
            return result
        return wrapper
    return _convert_query_to_scheme


def convert_request_to(target):
    """
    convert different kinds of request to needed input.

    there are 4 needed inputs:
    - GraphNode
    - GraphRelation
    - RawString
    - ExtractorInput
    """
    def _convert_request_to(func):
        @wraps(func)
        def wrapper(self, request, context):
            dctreq = protobuf_to_dict(request)
            if "props" in dctreq:
                req_props = dctreq["props"]
                dctreq["props"] = json.loads(req_props)
            if "start" in dctreq:
                start_props = dctreq["start"]["props"]
                dctreq["start"]["props"] = json.loads(start_props)
            if "end" in dctreq:
                end_props = dctreq["end"]["props"]
                dctreq["end"]["props"] = json.loads(end_props)
            request = from_dict(target, dctreq)
            result = func(self, request, context)
            return result
        return wrapper
    return _convert_request_to


def convert_graphobj_to_dict(graphobj):
    """
    A graphobj is a GraphNode or GraphRelation
    """
    dct = asdict(graphobj)
    if "props" in dct:
        dct["props"] = json.dumps(dct["props"])
    if "start" in dct:
        start_props = dct["start"]["props"]
        dct["start"]["props"] = json.dumps(start_props)
    if "end" in dct:
        end_props = dct["end"]["props"]
        dct["end"]["props"] = json.dumps(end_props)
    return dct
