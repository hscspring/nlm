from dataclasses import dataclass, field
from typing import List


@dataclass
class Entity:
    entity: str
    value: str

@dataclass
class ExtractorInput:
    """Actually it's just the NLU output."""
    text: str
    intent: str = ""
    entities: List[Entity] = field(default_factory=list)

@dataclass
class RawString:
    text: str