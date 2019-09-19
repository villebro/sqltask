from enum import Enum
from typing import NamedTuple, Optional

from sqlalchemy.engine import Engine
from sqlalchemy.schema import MetaData


class DqSource(Enum):
    SOURCE = "source"
    TRANSFORM = "transform"
    LOOKUP = "lookup"


class DqSeverity(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class EngineSpec(NamedTuple):
    engine: Engine
    metadata: MetaData
    chunksize: Optional[int]


class TableIdentifier(NamedTuple):
    table: Optional[str]
    engine: Optional[str]
    schema: Optional[str]
