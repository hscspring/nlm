import os
import sys
import pytest

import json
import grpc

ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_PATH)

import nlm_pb2
import nlm_pb2_grpc

@pytest.fixture(scope='module')
def grpc_add_to_server():
    from nlm_pb2_grpc import add_NLMServicer_to_server

    return add_NLMServicer_to_server


@pytest.fixture(scope='module')
def grpc_servicer():
    from server import NLMService

    return NLMService()


@pytest.fixture(scope='module')
def grpc_stub_cls(grpc_channel):
    from nlm_pb2_grpc import NLMStub

    return NLMStub


def test_recall_node_exist(grpc_stub):
    label = "Person"
    name = "AliceFive"
    props = {"age": 24}
    request = nlm_pb2.GraphNode(
        label=label, name=name, props=json.dumps(props))
    response = grpc_stub.NodeRecall(request)
    assert isinstance(response, nlm_pb2.GraphNode)
    assert response.label == label
    assert response.name == name
    assert isinstance(response.props, str)
    assert json.loads(response.props) == {"age": 24,
                                          "occupation": "scientist",
                                          "sex": "female"}


def test_recall_node_not_exist(grpc_stub):
    label = "Person"
    name = "AliceFive1"
    props = {"age": 24}
    request = nlm_pb2.GraphNode(
        label=label, name=name, props=json.dumps(props))
    response = grpc_stub.NodeRecall(request)
    assert isinstance(response, nlm_pb2.GraphNode)
    assert response.label == label
    assert response.name == name
    assert isinstance(response.props, str)
    assert json.loads(response.props) == props


def test_recall_relation_exist(grpc_stub):
    start = nlm_pb2.GraphNode(
        label="Person", name="AliceThree", props=json.dumps({}))
    end = nlm_pb2.GraphNode(
        label="Person", name="AliceOne", props=json.dumps({}))
    kind = "LOVES"
    props = {"roles": "husband"}
    request = nlm_pb2.GraphRelation(
        start=start, end=end, kind=kind, props=json.dumps(props))
    response = grpc_stub.RelationRecall(request)
    assert isinstance(response, nlm_pb2.GraphRelation)
    assert response.start != start
    assert response.end != end
    assert response.start.name == start.name
    assert response.end.label == end.label
    assert response.kind == kind
    assert isinstance(response.props, str)
    assert json.loads(response.props) != props


def test_recall_relation_exist_fuzzy_kind(grpc_stub):
    start = nlm_pb2.GraphNode(
        label="Person", name="AliceThree", props=json.dumps({}))
    end = nlm_pb2.GraphNode(
        label="Person", name="AliceOne", props=json.dumps({}))
    kind = "LOVEING"
    props = {"roles": "husband"}
    request = nlm_pb2.GraphRelation(
        start=start, end=end, kind=kind, props=json.dumps(props))
    response = grpc_stub.RelationRecall(request)
    assert isinstance(response, nlm_pb2.GraphRelation)
    assert response.start != start
    assert response.end != end
    assert response.start.name == start.name
    assert response.end.label == end.label
    assert response.kind == "LOVES"
    assert isinstance(response.props, str)
    assert json.loads(response.props) != props


def test_recall_relation_exist_only_props1(grpc_stub):
    start = nlm_pb2.GraphNode(
        label="Person", name="AliceThree", props=json.dumps({}))
    end = nlm_pb2.GraphNode(
        label="Person", name="AliceOne", props=json.dumps({}))
    props = {"roles": "husband"}
    request = nlm_pb2.GraphRelation(
        start=start, end=end, kind=None, props=json.dumps(props))
    response = grpc_stub.RelationRecall(request)
    assert isinstance(response, nlm_pb2.GraphRelation)
    assert response.start != start
    assert response.end != end
    assert response.start.name == start.name
    assert response.end.label == end.label
    assert response.kind == "LOVES"
    assert isinstance(response.props, str)
    assert json.loads(response.props) != props


def test_recall_relation_exist_only_props2(grpc_stub):
    start = nlm_pb2.GraphNode(
        label="Person", name="AliceThree", props=json.dumps({}))
    end = nlm_pb2.GraphNode(
        label="Person", name="AliceOne", props=json.dumps({}))
    props = {"from": 2009}
    request = nlm_pb2.GraphRelation(
        start=start, end=end, kind=None, props=json.dumps(props))
    response = grpc_stub.RelationRecall(request)
    assert isinstance(response, nlm_pb2.GraphRelation)
    assert response.start != start
    assert response.end != end
    assert response.start.name == start.name
    assert response.end.label == end.label
    assert response.kind == "WORK_WITH"
    assert isinstance(response.props, str)
    assert json.loads(response.props) != props


def test_recall_relation_start_end_not_exist(grpc_stub):
    start = nlm_pb2.GraphNode(
        label="Person", name="AliceThreeNotExist", props=json.dumps({}))
    end = nlm_pb2.GraphNode(
        label="Person", name="AliceOneNotExist", props=json.dumps({}))
    kind = "LOVES"
    props = {}
    request = nlm_pb2.GraphRelation(
        start=start, end=end, kind=kind, props=json.dumps(props))
    response = grpc_stub.RelationRecall(request)
    assert isinstance(response, nlm_pb2.GraphRelation)
    assert response.start == start
    assert response.end == end
    assert response.kind == kind
    assert isinstance(response.props, str)
    assert json.loads(response.props) == props


def test_recall_relation_start_not_exist1(grpc_stub):
    start = nlm_pb2.GraphNode(
        label="Person", name="AliceThreeNotExist", props=json.dumps({}))
    end = nlm_pb2.GraphNode(
        label="Person", name="AliceOne", props=json.dumps({}))
    kind = "LOVES"
    props = {}
    request = nlm_pb2.GraphRelation(
        start=start, end=end, kind=kind, props=json.dumps(props))
    response = grpc_stub.RelationRecall(request)
    assert isinstance(response, nlm_pb2.GraphRelation)
    assert response.start.name == "AliceThree"
    assert response.end != end
    assert response.kind == kind
    assert isinstance(response.props, str)
    assert json.loads(response.props) != props


def test_recall_relation_start_not_exist2(grpc_stub):
    start = nlm_pb2.GraphNode(
        label="Person", name="AliceThreeNotExist", props=json.dumps({}))
    end = nlm_pb2.GraphNode(
        label="Person", name="AliceOne", props=json.dumps({}))
    kind = "LIKES"
    props = {}
    request = nlm_pb2.GraphRelation(
        start=start, end=end, kind=kind, props=json.dumps(props))
    response = grpc_stub.RelationRecall(request)
    assert isinstance(response, nlm_pb2.GraphRelation)
    assert response.start.name == "AliceSeven"
    assert response.end != end
    assert response.kind == kind
    assert isinstance(response.props, str)
    assert json.loads(response.props) != props


def test_recall_rawstring_exist(grpc_stub):
    text = "some text"
    request = nlm_pb2.RawString(text=text)
    response = grpc_stub.StrRecall(request)
    pass


def test_recall_rawstring_not_exist(grpc_stub):
    text = "not exist text"
    request = nlm_pb2.RawString(text=text)
    response = grpc_stub.StrRecall(request)
    assert isinstance(response, nlm_pb2.GraphOutput)
    assert isinstance(response.gn, nlm_pb2.GraphNode)
    assert response.gn.label == ""
    assert response.gn.name == ""
    assert response.gn.props == ""


def test_recall_nlu_exist(grpc_stub):
    text = "some text"
    entity1 = nlm_pb2.Entity(entity="Person", value="Alice")
    entity2 = nlm_pb2.Entity(entity="Person", value="Bob")
    intent = "Social"
    request = nlm_pb2.NLMInput(
        text=text, intent=intent, entities=[entity1, entity2])
    response = grpc_stub.NLURecall(request)
    pass


def test_recall_nlu_not_exist(grpc_stub):
    text = "some text"
    entity1 = nlm_pb2.Entity(entity="Person", value="AliceNotExist")
    entity2 = nlm_pb2.Entity(entity="Person", value="BobNotExist")
    intent = "Social"
    request = nlm_pb2.NLMInput(
        text=text, intent=intent, entities=[entity1, entity2])
    response = grpc_stub.NLURecall(request)
    assert isinstance(response, nlm_pb2.GraphOutput)
    assert isinstance(response.gn, nlm_pb2.GraphNode)
    assert response.gn.label == ""
    assert response.gn.name == ""
    assert response.gn.props == ""







