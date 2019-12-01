"""
Graph
====================================
The core module of Graph
"""

from dataclasses import dataclass
from typing import List
import types

from py2neo.data import Node, Relationship, Subgraph
from py2neo.database import Graph
from py2neo.matching import NodeMatcher, RelationshipMatcher

import re

from schemes.graph import GraphNode, GraphRelation
from schemes.error import InputError, QueryError, DatabaseError, OverstepError

from utils.utils import raise_customized_error


@dataclass
class NLMGraph:

    """
    The Memory Graph.

    Parameters
    -----------
    graph: Graph
        The Neo4j Graph instance.
    """

    graph: Graph

    def __post_init__(self):
        self.nmatcher = NodeMatcher(self.graph)
        self.rmatcher = RelationshipMatcher(self.graph)

    @raise_customized_error(Exception, DatabaseError)
    def push_graph(self, subgraph: Subgraph) -> bool:
        """
        Push a subgraph (node, relationship, subgraph) to the Neo database.
        """
        tx = self.graph.begin()
        tx.create(subgraph)
        tx.commit()
        return tx.finished()

    def add_node(self, label: str, name: str, props: dict) -> Node:
        """
        Add a Node to database.

        Parameters
        ------------
        label: Node label
        name: Node name
        props: Node property

        Returns
        --------
        out: a Node.
        """
        node = Node(label, name=name, **props)
        self.push_graph(node)
        return node

    def check_update_node(self, nlmgn: GraphNode,
                          update_props: bool = False) -> Node:
        """
        Check whether the given node is already in the graph.

        Parameters
        ------------
        nlmgn: GraphNode
            The defined Node data type.
            Includes name, labels and properties.
        
        Returns
        --------
        out: Node
            Whether it is already in the graph.
            If is, update with the new properties, if necessary and return the updated node.
            If not, return the created Node (and need to commit to the graph).
        """
        label, name, props = nlmgn.label, nlmgn.name, nlmgn.props
        neogn = self.nmatcher.match(label, name=name).first()
        if neogn:
            if update_props:
                node = self.update_property(neogn, props)
            else:
                node = neogn
        else:
            node = self.add_node(label, name, props)
        return node

    @raise_customized_error(Exception, DatabaseError)
    def update_property(self, neog_oj, props: dict):
        """
        Update a neo graph node or relationship.

        Parameters
        ------------
        neog_oj: neo graph object, Node or Relationship

        Returns
        --------
        out: updated  Node or Relationship
        """
        neog_oj_props = dict(neog_oj)
        if props and props != neog_oj_props:
            # make sure new props is behind the exisited props.
            neog_oj.update({**neog_oj_props, **props})
            # only can be pushed when neog_oj is already in the graph
            # so we do not need push_graph function here
            self.graph.push(neog_oj)
        return neog_oj

    def add_relationship(self, start: Node, end: Node,
                         kind: str, props: dict) -> Relationship:
        """
        Add a Relationship to database.

        Parameters
        ------------
        start: start Node
        end: end Node
        kind: Relationship kind
        props: Relationship property

        Returns
        --------
        out: a Relationship.
        """
        relation = Relationship(start, kind, end, **props)
        self.push_graph(relation)
        return relation

    def check_update_relationship(self, nlmgr: GraphRelation,
                                  update_props: bool = False) -> Relationship:
        """
        Parameters
        ------------
        nlmgr: GraphRelation
            The defined Relationship data type.
            Includes kind, start, end and properties.

        Returns
        --------
        out: Relationship
        """
        kind, props = nlmgr.kind, nlmgr.props
        start = self.check_update_node(nlmgr.start, update_props)
        end = self.check_update_node(nlmgr.end, update_props)
        neogr = self.rmatcher.match((start, end), r_type=kind).first()
        if neogr:
            if update_props:
                relation = self.update_property(neogr, props)
            else:
                relation = neogr
        else:
            relation = self.add_relationship(start, end, kind, props)
        return relation

    def add(self, gin: GraphRelation or GraphNode
            ) -> Node or List[Node] or Relationship:
        """
        Add a Node or Relationship to the database.

        Parameters
        ------------
        gin: A GraphNode or GraphRelation (kind could be None)

        Returns
        --------
        out: A Node or Relationship.
        """
        if isinstance(gin, GraphNode):
            return self.add_node(gin.label, gin.name, gin.props)
        elif isinstance(gin, GraphRelation) and gin.kind:
            start = self.check_update_node(gin.start)
            end = self.check_update_node(gin.end)
            return self.add_relationship(start, end, gin.kind, gin.props)
        elif isinstance(gin, GraphRelation) and gin.kind == None:
            start = self.check_update_node(gin.start)
            end = self.check_update_node(gin.end)
            return (start, end)
        else:
            raise InputError

    def update(self, gin: GraphRelation or GraphNode
               ) -> Node or List[Node] or Relationship:
        """
        Update the property of a Node or Relationship to the database.

        Parameters
        ------------
        gin: A GraphNode or GraphRelation (kind could be None)

        Returns
        --------
        out: A Node or Relationship.
        """
        if isinstance(gin, GraphNode):
            return self.check_update_node(gin, update_props=True)
        elif isinstance(gin, GraphRelation) and gin.kind:
            return self.check_update_relationship(gin, update_props=True)
        elif isinstance(gin, GraphRelation) and gin.kind == None:
            start = self.check_update_node(gin.start, update_props=True)
            end = self.check_update_node(gin.end, update_props=True)
            return (start, end)
        else:
            raise InputError

    def query(self, qin, topn=1, limit=10, fuzzy=False) -> list:
        """
        Query by user given.
        
        Parameters
        -----------
        qin: could be GraphNode, GraphRelation, or just Cypher.

        Returns
        ---------
        out: queried Nodes or Relationships.

        """
        if isinstance(qin, GraphNode):
            ret = self._query_by_node(qin, topn, limit, fuzzy)
        elif isinstance(qin, GraphRelation):
            ret = self._query_by_relation(qin, topn, limit, fuzzy)
        elif isinstance(qin, str):
            ret = self._query_by_cypher(qin)
        else:
            raise InputError
        return ret

    def _sort_matched(self, matched_nodes: list, props: dict) -> list:
        """
        Sort matched nodes by comparing their properties with the given props.
        """
        ret = []
        for node in matched_nodes:
            nprops = dict(node)
            num = 0
            for k, v in props.items():
                if k in nprops and nprops[k] == v:
                    num += 1
            ret.append((node, num))
        sorted_ret = sorted(ret, key=lambda x: x[1], reverse=True)
        return [n for (n, _) in sorted_ret]

    @raise_customized_error(Exception, QueryError)
    def _query_by_node(self, gn: GraphNode,
                       topn: int,
                       limit: int,
                       fuzzy: bool) -> List[Node]:
        """
        Query node by given label and name.
        If None, then by those nodes whose nodes contains the given name
        """
        label, name, props = gn.label, gn.name, gn.props
        nmatch = self.nmatcher.match(label)
        nodes = nmatch.where(name=name).limit(limit)
        if fuzzy and nodes.first() == None:
            nodes = nmatch.where(name__contains=name).limit(limit)
        nmlst = list(nodes)
        return self.__from_match_to_return(nmlst, props, topn)

    @raise_customized_error(Exception, QueryError)
    def _query_by_relation(self, gr: GraphRelation,
                           topn: int,
                           limit: int,
                           fuzzy: bool) -> List[Relationship]:
        """
        Query relations by given start, end and kind.
        If start and end are None, return [].
        If start or end is None, then by kind and start or end.

        If result is None, then by start or end, or by both.
        """
        starts = self._query_by_node(gr.start, topn=1, limit=5, fuzzy=fuzzy)
        ends = self._query_by_node(gr.end, topn=1, limit=5, fuzzy=fuzzy)
        start = starts[0] if starts else None
        end = ends[0] if ends else None
        kind, props = gr.kind, gr.props
        # print("start: ", start)
        # print("end:", end)
        if not start and not end:
            return []
        # start, end could be None
        # r_type could be None
        relations = self.rmatcher.match((start, end), r_type=kind).limit(limit)
        if relations.first() == None and start and end:
            relations = self.rmatcher.match((start, end)).limit(limit)
        rmlst = list(relations)
        return self.__from_match_to_return(rmlst, props, topn)

    def _query_by_cypher(self, cypher: str) -> types.GeneratorType:
        """
        Return a generator, the content depends on your query input.
        """
        pattern_match = re.compile(r'^ ?MATCH')
        pattern_limit = re.compile(r'LIMIT \d')
        if not pattern_match.search(cypher):
            raise OverstepError
        searched_limit = pattern_limit.search(cypher)
        topn = int(searched_limit.group().split()[-1]) if searched_limit else 5
        try:
            cursor = self.graph.run(cypher)
            res = []
            n = 0
            for item in cursor:
                res.append(item.data())
                n += 1
                if n == topn:
                    return res
        except Exception as e:
            raise QueryError

    def __from_match_to_return(self, matched_list: list,
                               props: dict, topn: int) -> list:
        if not matched_list:
            return []
        if len(matched_list) > 1 and props:
            ret = self._sort_matched(matched_list, props)[:topn]
        else:
            ret = matched_list[:topn]
        return ret

    @property
    def labels(self) -> frozenset:
        """all labels""" 
        return self.graph.schema.node_labels

    @property
    def relationship_types(self) -> frozenset:
        """all relation types"""
        return self.graph.schema.relationship_types

    @property
    def nodes_num(self) -> int:
        """all nodes amounts"""
        return len(self.graph.nodes)

    @property
    def relationships_num(self) -> int:
        """all relations amounts"""
        return len(self.graph.relationships)

    @property
    def nodes(self) -> types.GeneratorType:
        """all nodes (a generator)"""
        return iter(self.graph.nodes.match())

    @property
    def relationships(self) -> types.GeneratorType:
        """all relations (a generator)"""
        return iter(self.graph.relationships.match())

    def excute(self, cypher) -> dict:
        """
        Be careful to use this function.
        Especially when you're updating the database.
        This function will not check the duplicated nodes or relationships.
        """
        try:
            run = self.graph.run(cypher)
            return dict(run.stats())
        except Exception as e:
            raise InputError


if __name__ == '__main__':
    import os
    import sys
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.append(root)
    nlmg = NLMGraph(graph=Graph(port=7688))

    start = GraphNode("Person", "AliceThree")
    end = GraphNode("Person", "AliceOne")
    qin = GraphRelation(start, end)
    res = nlmg.query(qin, topn=2, fuzzy=True)
    print(res)
