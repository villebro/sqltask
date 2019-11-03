from enum import Enum


class Priority(Enum):
    MANDATORY = "mandatory"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class Source(Enum):
    SOURCE = "source"
    TRANSFORM = "transform"
    LOOKUP = "lookup"


class Category(Enum):
    MISSING = "missing"
    INCORRECT = "incorrect"
    DUPLICATE = "duplicate"
