from enum import Enum


class DqPriority(Enum):
    MANDATORY = "mandatory"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class DqSource(Enum):
    SOURCE = "source"
    TRANSFORM = "transform"
    LOOKUP = "lookup"


class DqType(Enum):
    MISSING = "missing"
    INCORRECT = "incorrect"
    DUPLICATE = "duplicate"
