from dataclasses import dataclass, field
from typing import List

@dataclass
class GraphNode:
    """
    It's recommended to use a unique name
    """
    label: str
    name: str
    props: dict = field(default_factory=dict)

@dataclass
class GraphRelation:
    start: GraphNode
    end: GraphNode
    kind: str = None
    props: dict = field(default_factory=dict)

# @dataclass
# class GraphOutput:
#     gn: GraphNode = None
#     gr: GraphRelation = None