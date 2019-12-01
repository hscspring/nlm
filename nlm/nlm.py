"""
Main
====================================
The core module of NLM project
"""

from dataclasses import dataclass
from dacite import from_dict

from typing import Any, List

from models.extractor import NLMExtractor
from graph.graph import NLMGraph

from schemes.extractor import ExtractorInput, RawString
from schemes.graph import GraphNode, GraphRelation
from schemes.error import ParameterError

from utils.utils import convert_query_to_scheme




@dataclass
class NLMLayer(NLMGraph):

    """
    Parameters
    ----------
    fuzzy_node: whether to use fuzzy search when querying.
    add_inexistence: whether to add the inexistent nodes or relations to the database when querying.
    update_props: whether to update the props you have given in the query if match.
    """

    fuzzy_node: bool = False
    add_inexistence: bool = False
    update_props: bool = False

    @convert_query_to_scheme()
    def query_add_update(self, qin: GraphNode or GraphRelation, **kwargs
                         ) -> List[GraphNode or GraphRelation]:
        """
        Query a node, two nodes(actually a relation), a relation.

        If add_inexistence:

        - a node: directly add_node
        - two nodes: check_update_node(update_props=False) of (start,end)
        - a relation: check_update_node(update_props=False) of (start,end), add_relation

        If update_props:

        - a node: check_update_node(update_props=True)
        - two nodes: check_update_node(update_props=True) of two(start, end)
        - a relation: check_update_node(update_props=True) of two(start, end), check_update_relation(update_props=True)
        """

        if not (isinstance(qin, GraphNode) or
                isinstance(qin, GraphRelation)):
            raise ParameterError

        fuzzy_node = kwargs.get("fuzzy_node", self.fuzzy_node)
        add_inexistence = kwargs.get("add_inexistence", self.add_inexistence)
        update_props = kwargs.get("update_props", self.update_props)
        topn = kwargs.get("topn", 1)

        query = self.query(qin, topn=topn, fuzzy=fuzzy_node)

        # ATTENTION: this will automatically update the query props.
        # So the props of your query result will be changed.
        if update_props and query and not fuzzy_node:
            self.update(qin)

        # However, this will not update the query props.
        if add_inexistence and not query:
            self.add(qin)

        # print("QUERY: ", query)
        return query

    def extract_relation_or_node(self, ext_in: ExtractorInput):
        try:
            from_dict(data_class=ExtractorInput,
                      data={"text": ext_in.text,
                            "intent": ext_in.intent,
                            "entities": ext_in.entities})
        except Exception as e:
            raise ParameterError
        ext_out = NLMExtractor.extract(ext_in)
        return GraphNode("Demo", "demo_node")

    def __call__(self, inputs: Any, **kwargs) -> list:
        """
        Query (add or update) with NLMLayer inputs.

        Parameters
        -----------
        inputs: A GraphNode or GraphRelation or RawString or ExtractorInput.

        Returns
        --------
        out: A list of GraphNode or GraphRelations.
        """
        # print("INPUTS: ", inputs)
        if isinstance(inputs, GraphRelation) or isinstance(inputs, GraphNode):
            ext_out = inputs
        elif isinstance(inputs, RawString):
            ext_out = self.extract_relation_or_node(
                ExtractorInput(text=inputs.text))
        elif isinstance(inputs, ExtractorInput):
            ext_out = self.extract_relation_or_node(inputs)
        else:
            return []
        return self.query_add_update(ext_out, **kwargs)


if __name__ == '__main__':
    from configs.config import neo_sche, neo_host, neo_port, neo_user, neo_pass
    from py2neo.database import Graph

    graph = Graph(port=7688)
    nlm = NLMLayer(graph=graph)

    start = GraphNode("Person", "AliceThreeNotExist")
    end = GraphNode("Animal", "Monkey")
    end = GraphNode("Person", "AliceOne")
    relation = GraphRelation(start, end, "LIKES")

    nlu_out = GraphRelation(start, end, "LOVES")

    etin = ExtractorInput(text="hhh")

    # nlu_out = "1"
    res = nlm(relation, add_inexistence=True)
    # res = nlm.query_add_update("MATCH (a:Person) RETURN a.age, a.name LIMIT 4")

    nlm2 = NLMLayer(graph=graph)
    assert (nlm2 == nlm)

    print("RES: ", res)
