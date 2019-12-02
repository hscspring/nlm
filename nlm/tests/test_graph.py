import os
import sys
import pytest

from py2neo.database import Graph
from py2neo.data import Node, Relationship

ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_PATH)


from schemes.graph import GraphNode, GraphRelation
from graph.graph import NLMGraph


nlmg = NLMGraph(graph=Graph(port=7688))
nlmg.graph.delete_all()


def test_graph_connected():
    assert nlmg.graph.database.name != None


@pytest.fixture
def make_node():
    props = {"age": 20, "sex": "female"}
    node = GraphNode("Person", "Alice", props)
    return node


@pytest.fixture
def make_node_no_props():
    return GraphNode("Person", "Alice")


@pytest.fixture
def make_updated_node():
    props = {"age": 20, "sex": "female", "occupation": "teacher"}
    node = GraphNode("Person", "Alice", props)
    return node

@pytest.fixture
def make_props():
    return  {"age": 21, "sex": "male"}


def test_add_node(make_props):
    node = nlmg.add_node("Person", "Bob", make_props)
    assert node.identity >= 0
    assert dict(node) == {"name": "Bob", "age": 21, "sex": "male"}
    nlmg.graph.delete(node)

def test_check_update_node_not_exist(make_node):
    new = nlmg.check_update_node(make_node)
    assert new.identity >= 0
    assert dict(new) == {"name": "Alice", "age": 20, "sex": "female"}
    nlmg.graph.delete(new)


def test_check_update_node_is_exist(make_node):
    node = nlmg.check_update_node(make_node)
    new = nlmg.check_update_node(make_node)
    assert new.identity >= 0
    assert dict(new) == {"name": "Alice", "age": 20, "sex": "female"}
    nlmg.graph.delete_all()


def test_check_update_node_is_exist_not_update(make_node, make_updated_node):
    node = nlmg.check_update_node(make_node)
    new = nlmg.check_update_node(make_updated_node, update_props=False)
    assert new.identity >= 0
    assert dict(new) == {"name": "Alice", "age": 20, "sex": "female"}
    nlmg.graph.delete_all()


def test_check_update_node_is_exist_update(make_node, make_updated_node):
    node = nlmg.check_update_node(make_node)
    new = nlmg.check_update_node(make_updated_node, update_props=True)
    assert new.identity >= 0
    assert dict(new) == {"name": "Alice", "age": 20,
                         "sex": "female",  "occupation": "teacher"}
    nlmg.graph.delete_all()


def test_no_props_node(make_node_no_props):
    new = nlmg.check_update_node(make_node_no_props)
    assert new.identity >= 0
    assert dict(new) == {"name": "Alice"}
    nlmg.graph.delete(new)


def test_no_props_update_node(make_updated_node):
    node = nlmg.check_update_node(GraphNode("Person", "Alice"))
    new = nlmg.check_update_node(make_updated_node, update_props=True)
    assert new.identity == node.identity
    assert dict(new) == {"name": "Alice", "age": 20,
                         "sex": "female",  "occupation": "teacher"}
    nlmg.graph.delete_all()


@pytest.fixture
def make_relation():
    start = GraphNode("Person", "Bob", {"age": 22, "occupation": "engineer"})
    end = GraphNode("Person", "Alice", {"age": 20, "sex": "female"})
    props = {"roles": "boyfriend"}
    return GraphRelation(start, end, "LOVES", props)


@pytest.fixture
def make_relaition_no_props():
    start = GraphNode("Person", "Bob", {"age": 22, "occupation": "engineer"})
    end = GraphNode("Person", "Alice", {"age": 20, "sex": "female"})
    return GraphRelation(start, end, "LOVES")


@pytest.fixture
def make_updated_relation():
    start = GraphNode("Person", "Bob", {"age": 22, "occupation": "engineer"})
    end = GraphNode("Person", "Alice", {"age": 20, "sex": "female"})
    props = {"roles": "husband"}
    return GraphRelation(start, end, "LOVES", props)


def test_add_relation_not_exist():
    start = Node("Person", name="Bob")
    end = Node("Person", name="Alice")
    new = nlmg.add_relationship(start, end, "LOVES", {"roles": "boyfriend"})
    assert new.identity >= 0
    assert dict(new) == {"roles": "boyfriend"}
    nlmg.graph.delete(new)


def test_add_relation_is_exist_not_update(make_relation):
    relation = nlmg.check_update_relationship(make_relation)
    new = nlmg.check_update_relationship(make_relation)
    assert new.identity >= 0
    assert dict(new) == {"roles": "boyfriend"}
    nlmg.graph.delete_all()


def test_add_relation_is_exist_update(make_relation,
                                      make_updated_relation):
    relation = nlmg.check_update_relationship(make_relation)
    new = nlmg.check_update_relationship(make_updated_relation, update_props=True)
    assert new.identity == relation.identity
    assert dict(new) == {"roles": "husband"}
    nlmg.graph.delete_all()


def test_no_props_relation(make_relaition_no_props):
    new = nlmg.check_update_relationship(make_relaition_no_props)
    assert new.identity >= 0
    assert dict(new) == {}
    nlmg.graph.delete(new)


def test_no_props_update_relation(make_relaition_no_props,
                                  make_updated_relation):
    relation = nlmg.check_update_relationship(make_relaition_no_props)
    new = nlmg.check_update_relationship(make_updated_relation, update_props=True)
    assert new.identity == relation.identity
    assert dict(new) == {"roles": "husband"}
    nlmg.graph.delete_all()


def test_add_graphnode(make_node):
    node = nlmg.add(make_node)
    assert node.identity >= 0
    assert dict(node) == {"name": "Alice", "age": 20, "sex": "female"}
    nlmg.graph.delete(node)


def test_add_graphrelationship(make_relation):
    relation = nlmg.add(make_relation)
    assert relation.identity >= 0
    assert dict(relation) == {"roles": "boyfriend"}
    nlmg.graph.delete(relation)


def test_add_graphrelationship_without_kind(make_relation):
    make_relation.kind = None
    res = nlmg.add(make_relation)
    assert len(res) == 2
    assert dict(res[0]) == {"name": "Bob", "age": 22, "occupation": "engineer"}
    nlmg.graph.delete_all()


def test_add_with_invalid_input():
    try:
        nlmg.add("df")
    except Exception as e:
        assert e.code == 20000


def test_update_graphnode(make_node, make_updated_node):
    node = nlmg.add(make_node)
    new = nlmg.update(make_updated_node)
    assert new.identity >= 0
    assert dict(new) == {"name": "Alice", "age": 20, "sex": "female", "occupation": "teacher"}
    nlmg.graph.delete(node)


def test_update_graphrelationship(make_relation, make_updated_relation):
    relation = nlmg.add(make_relation)
    new = nlmg.update(make_updated_relation)
    assert new.identity >= 0
    assert dict(new) == {"roles": "husband"}
    nlmg.graph.delete(relation)


def test_update_graphrelationship_without_kind(make_relation):
    make_relation.kind = None
    relation = nlmg.add(make_relation)
    make_relation.start = GraphNode("Person", "Bob", {"age": 23, "occupation": "scientist"})
    new = nlmg.update(make_relation)
    assert len(new) == 2
    assert dict(new[0]) == {"name": "Bob", "age": 23, "occupation": "scientist"}
    nlmg.graph.delete_all()


def test_update_with_invalid_input():
    try:
        nlmg.add("df")
    except Exception as e:
        assert e.code == 20000


def test_query_with_invalid_input():
    try:
        qres = nlmg.query(123)
    except Exception as e:
        assert e.code == 20000


@pytest.fixture
def make_nodes_for_query():
    node1 = GraphNode("Person", "AliceOne", 
        {"age": 20, "sex": "female", "occupation": "teacher"})

    node2 = GraphNode("Person", "AliceTwo", 
        {"age": 21, "occupation": "teacher"})

    node3 = GraphNode("Person", "AliceThree", 
        {"age": 22, "sex": "male"})

    node4 = GraphNode("Person", "AliceFour", 
        {"age": 23, "sex": "male", "occupation": "doctor"})

    node5 = GraphNode("Person", "AliceFive", 
        {"age": 24, "sex": "female", "occupation": "scientist"})

    node6 = GraphNode("Person", "AliceSix", 
        {"age": 25, "sex": "female"})

    for node in [node1, node2, node3, node4, node5, node6]:
        nlmg.check_update_node(node)
    return (node1, node2, node3, node4, node5, node6)


@pytest.fixture
def make_relations_for_query(make_nodes_for_query):
    (node1, node2, node3, node4, node5, node6) = make_nodes_for_query
    rela1 = GraphRelation(node3, node1, "LOVES", 
        {"roles": "husband", "from": 2011})
    rela2 = GraphRelation(node3, node1, "WORK_WITH", 
        {"roles": "boss", "from": 2009})
    rela3 = GraphRelation(node2, node3, "LOVES", 
        {"roles": "student", "at": 2005})
    rela4 = GraphRelation(node4, node2, "LOVES")
    rela5 = GraphRelation(node5, node6, "WORK_WITH", 
        {"roles": "workmates"})
    rela6 = GraphRelation(node6, node4, "LOVES", 
        {"from": 2012})
    rela7 = GraphRelation(node1, node5, "KNOWS", 
        {"roles": "friend"})
    for rela in [rela1, rela2, rela3, rela4, rela5, rela6, rela7]:
        nlmg.check_update_relationship(rela)


def test_query_with_graph_relation_normal(make_relations_for_query):
    qin = GraphRelation(GraphNode("Person", "AliceOne"), GraphNode("Person", "AliceFive"), "KNOWS")
    res = nlmg.query(qin)
    assert dict(res[0]) == {"roles": "friend"}


def test_query_with_graph_node():
    qin1 = GraphNode("Person", "Alice")
    res1 = nlmg.query(qin1)
    assert res1 != None

    qin2 = GraphNode("Person", "AliceOne")
    res2 = nlmg.query(qin2)
    assert dict(res2[0]) == {"name": "AliceOne", "age": 20, "sex": "female", "occupation": "teacher"}

    qin3 = GraphNode("Person", "Alice", {"age": 21})
    res3 = nlmg.query(qin3)
    assert res3 == []

def test_query_with_graph_node_without_fuzzy():
    qin = GraphNode("Person", "Alice", {"age": 21})
    res = nlmg.query(qin, fuzzy=False)
    assert res == []


def test_query_with_graph_relation_exist_nodes_without_relation():
    qin = GraphRelation(GraphNode("Person", "AliceOne"), GraphNode("Person", "AliceFive"), "WRONG")
    res = nlmg.query(qin)
    assert dict(res[0]) == {"roles": "friend"}

def test_query_with_graph_relation_not_exist_nodes_with_relation():
    qin2 = GraphRelation(GraphNode("Person", "AliceOne1"), GraphNode("Person", "AliceFive1"), "LOVES")
    res2 = nlmg.query(qin2, topn=5)
    assert res2 == []

    # start, end are None
    qin6 = GraphRelation(GraphNode("Fruit", "Apple"), GraphNode("Animal", "Monkey"), "KNOWS")
    res6 = nlmg.query(qin6)
    assert res6 == []

    
def test_query_with_graph_relation_not_exist_start_with_relation():
    # start is None
    qin4 = GraphRelation(GraphNode("Animal", "Monkey"), GraphNode("Person", "AliceTwo"), "LOVES")
    res4 = nlmg.query(qin4)
    assert dict(res4[0]) == {}
    assert len(res4) == 1

def test_query_with_graph_relation_not_exist_end_with_relation():
    # end is None
    qin5 = GraphRelation(GraphNode("Person", "AliceTwo"), GraphNode("Animal", "Monkey"), "LOVES")
    res5 = nlmg.query(qin5)
    assert dict(res5[0]) == {"roles": "student", "at": 2005}


def test_query_with_graph_relation_with_fuzzy_node():
    qin1 = GraphRelation(GraphNode("Person", "AliceTw"), GraphNode("Person", "AliceTh"), "LOVES")
    res1 = nlmg.query(qin1, topn=5, fuzzy=True)
    assert len(res1) == 1

    qin2 = GraphRelation(GraphNode("Person", "Alice"), GraphNode("Animal", "Monkey"), "WRONG")
    res2 = nlmg.query(qin2, fuzzy=True)
    assert len(res2) == 0

    qin3 = GraphRelation(GraphNode("Person", "Alice"), GraphNode("Animal", "Monkey"), "WRONG")
    res3 = nlmg.query(qin3, fuzzy=True)
    assert len(res3) == 0



def test_query_with_graph_relation_without_fuzzy_node():
    qin1 = GraphRelation(GraphNode("Person", "Alice"), GraphNode("Animal", "Monkey"), "LOVES")
    res1 = nlmg.query(qin1, topn=5, fuzzy=False)
    assert res1 == []

    qin2 = GraphRelation(GraphNode("Person", "Alice"), GraphNode("Animal", "Monkey"), "WRONG")
    res2 = nlmg.query(qin2, fuzzy=False)
    assert res2 == []

def test_query_with_graph_relation_without_kind_without_fuzzy_node():
    qin1 = GraphRelation(GraphNode("Person", "Alice"), GraphNode("Animal", "Monkey"))
    res1 = nlmg.query(qin1, topn=5, fuzzy=False)
    assert res1 == []

    qin1 = GraphRelation(GraphNode("Person", "AliceTwo"), GraphNode("Person", "AliceThree"))
    res1 = nlmg.query(qin1, topn=5, fuzzy=False)
    assert len(res1) == 1


def test_query_with_graph_relation_without_kind_with_fuzzy_node():
    qin1 = GraphRelation(GraphNode("Person", "Alice"), GraphNode("Animal", "Monkey"))
    res1 = nlmg.query(qin1, topn=5, fuzzy=True)
    assert len(res1) >= 1

    qin1 = GraphRelation(GraphNode("Person", "AliceTw"), GraphNode("Person", "AliceTh"))
    res1 = nlmg.query(qin1, topn=5, fuzzy=True)
    assert len(res1) == 1


def test_query_with_cypher_overstep():
    q = "CREATE (n:Person { name: 'Andy', title: 'Developer' })"
    try:
        nlmg.query(q)
    except Exception as e:
        assert e.code == 40001

def test_query_with_cypher():
    q = "MATCH (a:Person) RETURN a.age, a.name LIMIT 4"
    res = nlmg.query(q)
    assert len(res) == 4
    assert "a.name" in res[0]
    assert "a.age" in res[0]


def test_labels():
    labels = nlmg.labels
    assert len(labels) == 1
    assert isinstance(labels, frozenset)
    assert "Person" in labels

def test_relationship_types():
    rt = nlmg.relationship_types
    assert len(rt) == 3
    assert isinstance(rt, frozenset)
    assert "LOVES" in rt

def test_nodes_num():
    assert nlmg.nodes_num == 6

def test_relationships_num():
    assert nlmg.relationships_num == 7

def test_all_nodes():
    res = []
    for item in nlmg.nodes:
        res.append(item)
    assert len(res) == 6
    assert isinstance(res[0], Node)

def test_all_relationships():
    res = []
    for item in nlmg.relationships:
        res.append(item)
    assert len(res) == 7
    assert isinstance(res[0], Relationship)

def test_excute_pass():
    res = nlmg.excute("CREATE (n:Person { name: 'Andy', title: 'Developer' })")
    assert res["contained_updates"] == True
    assert res["nodes_created"] == 1
    assert res["properties_set"] == 2
    assert res["labels_added"] == 1


if __name__ == '__main__':
    print(ROOT_PATH)
    print(nlmg)
    print(nlmg.graph)
