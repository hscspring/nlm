import argparse
from concurrent import futures
import grpc
from grpc import StatusCode

import nlm_pb2
import nlm_pb2_grpc

from py2neo.database import Graph


from nlm import NLMLayer

from utils.utils import raise_grpc_error, deco_log_error
from utils.utils import convert_request_to, convert_graphobj_to_dict

from schemes.extractor import ExtractorInput, RawString
from schemes.graph import GraphNode, GraphRelation

from configs.config import neo_sche, neo_host, neo_port, neo_user, neo_pass
from configs.config import logger


parser = argparse.ArgumentParser(
    description='Setup your NLM Server.')
parser.add_argument(
    '-fn', dest='fuzzy_node', type=bool, default=False,
    help='Whether to use fuzzy node to query. \
    If is, the props will never update.')
parser.add_argument(
    '-ai', dest='add_inexistence', type=bool, default=False,
    help='Whether to add an inexistent Node or Relation.')
parser.add_argument(
    '-up', dest='update_props', type=bool, default=False,
    help='Whether to update props of a Node or Relation.')
args = parser.parse_args()


graph = Graph(scheme=neo_sche, host=neo_host, port=neo_port,
              user=neo_user, password=neo_pass)
mem = NLMLayer(graph=graph,
               fuzzy_node=args.fuzzy_node,
               add_inexistence=args.add_inexistence,
               update_props=args.update_props)


class NLMService(nlm_pb2_grpc.NLMServicer):

    @raise_grpc_error(Exception, StatusCode.INTERNAL)
    @deco_log_error(logger)
    @convert_request_to(GraphNode)
    def NodeRecall(self, request, context):
        result = mem(request)
        gn = result[0] if result else request
        dctgn = convert_graphobj_to_dict(gn)
        return nlm_pb2.GraphNode(**dctgn)

    @raise_grpc_error(Exception, StatusCode.INTERNAL)
    @deco_log_error(logger)
    @convert_request_to(GraphRelation)
    def RelationRecall(self, request, context):
        result = mem(request)
        gr = result[0] if result else request
        dctgr = convert_graphobj_to_dict(gr)
        return nlm_pb2.GraphRelation(**dctgr)

    @raise_grpc_error(Exception, StatusCode.INTERNAL)
    @deco_log_error(logger)
    @convert_request_to(RawString)
    def StrRecall(self, request, context):
        result = mem(request)
        go = result[0] if result else None
        if isinstance(go, GraphNode):
            dctgn = convert_graphobj_to_dict(go)
            gop = nlm_pb2.GraphNode(**dctgn)
        elif isinstance(go, GraphRelation):
            dctgr = convert_graphobj_to_dict(go)
            gop = nlm_pb2.GraphRelation(**dctgr)
        else:
            gop = nlm_pb2.GraphNode(**{})
        return nlm_pb2.GraphOutput(gn=gop)

    @raise_grpc_error(Exception, StatusCode.INTERNAL)
    @deco_log_error(logger)
    @convert_request_to(ExtractorInput)
    def NLURecall(self, request, context):
        result = mem(request)
        go = result[0] if result else None
        if isinstance(result, GraphNode):
            dctgn = convert_graphobj_to_dict(result)
            gop = nlm_pb2.GraphNode(**dctgn)
        elif isinstance(go, GraphRelation):
            dctgr = convert_graphobj_to_dict(result)
            gop = nlm_pb2.GraphRelation(**dctgr)
        else:
            gop = nlm_pb2.GraphNode(**{})
        return nlm_pb2.GraphOutput(gn=gop)


def serve(host, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    nlm_pb2_grpc.add_NLMServicer_to_server(NLMService(), server)
    server.add_insecure_port('{}:{}'.format(host, port))
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve("localhost", 8080)
