from dataclasses import dataclass
from dacite import from_dict

from schemes.extractor import ExtractorInput
from schemes.graph import GraphNode, GraphRelation


from configs.config import extract_model


@dataclass
class NLMExtractor:
    """
    Returns
    --------
    out: most related GraphRelation, if None, most related GraphNode
    """
    model = extract_model

    @classmethod
    def extract(cls, ext_in: ExtractorInput) -> GraphRelation or GraphNode:
        return ext_in