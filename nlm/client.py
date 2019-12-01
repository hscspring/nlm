import json
from dataclasses import dataclass
import grpc

import nlm_pb2
import nlm_pb2_grpc


def deco_exception(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except grpc.RpcError as e:
            print(e.details())
    return wrapper


@dataclass
class NLMClient:

    host: str
    port: int

    def __post_init__(self):
        self.channel = grpc.insecure_channel(
            "{}:{}".format(self.host, self.port))
        self.stub = nlm_pb2_grpc.NLMStub(self.channel)

    @deco_exception
    def recall_node(self, label: str, name: str, props: dict = {}):
        request = nlm_pb2.GraphNode(
            label=label, name=name, props=json.dumps(props))
        response = self.stub.NodeRecall(request)
        return response

    @deco_exception
    def recall_relation(self, start: nlm_pb2.GraphNode, end: nlm_pb2.GraphNode,
                        kind: str, props: dict = {}):
        request = nlm_pb2.GraphRelation(
            start=start, end=end, kind=kind, props=json.dumps(props))
        response = self.stub.RelationRecall(request)
        return response

    @deco_exception
    def str_recall(self, text: str):
        request = nlm_pb2.RawString(text=text)
        response = self.stub.StrRecall(request)
        return response

    @deco_exception
    def nlu_recall(self, text: str, intent: str = "", entities: list = []):
        request = nlm_pb2.NLMInput(text=text, intent=intent, entities=entities)
        response = self.stub.NLURecall(request)
        return response


if __name__ == '__main__':
    nlmc = NLMClient(host="localhost", port=8080)

    label = "Person"
    name = "AliceFive"
    props = {"age": 24}
    node = nlmc.recall_node(label, name, props)
    print(node)
    print("="*50)

    start = nlm_pb2.GraphNode(label="Person", name="AliceThreeNotExist", props=json.dumps({}))
    end = nlm_pb2.GraphNode(label="Person", name="AliceOne", props=json.dumps({}))
    kind = "LIKES"
    props = {"roles": "husband"}
    relation = nlmc.recall_relation(start, end, kind, props)
    print(relation)
    print("="*50)

    rawstr = "test, test, test"
    res1 = nlmc.str_recall(rawstr)
    print(res1)
    print("="*50)

    rawstr = "test, test, test"
    entity1 = nlm_pb2.Entity(entity="Person", value="Alice")
    entity2 = nlm_pb2.Entity(entity="Person", value="Bob")
    intent = "Social"
    res2 = nlmc.nlu_recall(text=rawstr, intent=intent, entities=[entity1, entity2])
    print(res2)
