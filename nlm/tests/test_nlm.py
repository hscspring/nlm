import os
import sys
import pytest

ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_PATH)

from py2neo.database import Graph
from schemes.graph import GraphNode, GraphRelation
from schemes.extractor import Entity, ExtractorInput
from nlm import NLMLayer


graph = Graph(port=7688)
mem = NLMLayer(graph=graph)


def test_nlm_instance_call_graphnode():
    start = GraphNode("Person", "AliceThree")
    res = mem(start)
    query = res[0]
    assert isinstance(query, GraphNode) == True
    assert query.label == "Person"
    assert query.name == "AliceThree"
    assert query.props == {'age': 22, 'sex': 'male'}


def test_nlm_instance_call_graphrelation():
    start = GraphNode("Person", "AliceThree")
    end = GraphNode("Person", "AliceOne")
    relation = GraphRelation(start, end, "LOVES")
    res = mem(relation)
    query = res[0]
    assert isinstance(query, GraphRelation)
    assert query.kind == "LOVES"
    assert query.props == {'from': 2011, 'roles': 'husband'}
    assert query.start.label == "Person"
    assert query.start.name == "AliceThree"
    assert query.start.props == {'age': 22, 'sex': 'male'}


def test_nlm_instance_call_str():
    # mem()
    pass


def test_nlm_instance_call_extractorinput():
    # mem()
    text = "AliceThree 和 AliceOne 是什么关系？"
    intent = "Social"
    entity1 = Entity("Person", "AliceThree")
    entity2 = Entity("Person", "AliceOne")
    entities = [entity1, entity2]
    ext_in1 = ExtractorInput(text, intent, entities)

    ext_in2 = ExtractorInput(text)


def test_nlm_instance_call_otherinput():
    assert mem(1) == []
    assert mem(1.2) == []


def test_extract_relation_or_node():
    # mem.extract_relation_or_node()
    pass


def test_extract_relation_or_node_invalid_parameter_value_type():
    ext_in = ExtractorInput(text=1)
    try:
        res = mem.extract_relation_or_node(ext_in)
    except Exception as e:
        assert e.code == 20001


def test_query_add_update_without_addinexist_without_fuzzynode():
    start = GraphNode("Person", "AliceThree")
    end = GraphNode("Person", "AliceOne")
    relation = GraphRelation(start, end, "LOVES")
    res1 = mem.query_add_update(relation, add_inexistence=False, fuzzy_node=False)
    query1 = res1[0]
    assert len(res1) == 1
    assert query1.kind == "LOVES"
    assert query1.props == {'from': 2011, 'roles': 'husband'}
    assert query1.start.label == "Person"
    assert query1.start.name == "AliceThree"
    assert query1.start.props == {'age': 22, 'sex': 'male'}

    res2 = mem.query_add_update(start, add_inexistence=False, fuzzy_node=False)
    query2 = res2[0]
    assert len(res2) == 1
    assert query2.label == "Person"
    assert query2.name == "AliceThree"
    assert query2.props == {'age': 22, 'sex': 'male'}


def test_query_add_update_without_addinexist_with_fuzzynode():
    start = GraphNode("Person", "AliceTh")
    end = GraphNode("Person", "AliceO")
    relation = GraphRelation(start, end, "LOVES")
    res1 = mem(relation, add_inexistence=False, fuzzy_node=True)
    query1 = res1[0]
    assert len(res1) == 1
    assert query1.kind == "LOVES"
    assert query1.props == {'from': 2011, 'roles': 'husband'}
    assert query1.start.label == "Person"
    assert query1.start.name == "AliceThree"
    assert query1.start.props == {'age': 22, 'sex': 'male'}

    res2 = mem(start, add_inexistence=False, fuzzy_node=True)
    query2 = res2[0]
    assert len(res2) == 1
    assert query2.label == "Person"
    assert query2.name == "AliceThree"
    assert query2.props == {'age': 22, 'sex': 'male'}


def test_query_add_update_with_addinexist_without_fuzzynode():
    start = GraphNode("Person", "AliceSeven", {'age': 23, 'sex': 'male'}) # node does not exist
    end = GraphNode("Person", "AliceOne", {'age': 22}) # exist age = 20
    relation = GraphRelation(start, end, "LIKES", {'from': 2011, 'roles': 'friend'})

    res2 = mem(start, add_inexistence=True, fuzzy_node=False)
    assert res2 == []

    res0 = mem(end, update_props=True, fuzzy_node=False)
    query1 = res0[0]
    assert query1.props == {"age": 22, "sex": "female", "occupation": "teacher"}
    assert query1.label == "Person"
    assert query1.name == "AliceOne"

    res1 = mem(relation, add_inexistence=True, fuzzy_node=False)
    assert res1 == []

    res3 = mem(relation)
    query = res3[0]
    assert query.kind == "LIKES"
    assert query.props == {'from': 2011, 'roles': 'friend'}


def test_query_add_update_with_addinexist_with_fuzzynode():
    # actual do not update
    start = GraphNode("Person", "AliceTh")
    end = GraphNode("Person", "AliceO")
    relation = GraphRelation(start, end, "LOVES")
    res1 = mem(relation, add_inexistence=True, fuzzy_node=True)
    query1 = res1[0]
    assert len(res1) == 1
    assert query1.kind == "LOVES"
    assert query1.props == {'from': 2011, 'roles': 'husband'}
    assert query1.start.label == "Person"
    assert query1.start.name == "AliceThree"
    assert query1.start.props == {'age': 22, 'sex': 'male'}

    res2 = mem(start, add_inexistence=True, fuzzy_node=True)
    query2 = res2[0]
    assert len(res2) == 1
    assert query2.label == "Person"
    assert query2.name == "AliceThree"
    assert query2.props == {'age': 22, 'sex': 'male'}


def test_nodes_num():
    assert mem.nodes_num == 8

def test_relationships_num():
    assert mem.relationships_num == 8






