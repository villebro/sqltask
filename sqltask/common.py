from enum import Enum
from typing import Any, Dict, NamedTuple, Optional

from sqlalchemy.engine import Engine
from sqlalchemy.schema import MetaData, Table


class DqSource(Enum):
    SOURCE = "source"
    TRANSFORM = "transform"
    LOOKUP = "lookup"


class DqSeverity(Enum):
    MANDATORY = "mandatory"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class EngineSpec(NamedTuple):
    name: str
    engine: Engine
    metadata: MetaData


class TableSpec(NamedTuple):
    engine_name: str
    table: Table


class TableIdentifier(NamedTuple):
    table: Optional[str]
    engine: Optional[str]
    schema: Optional[str]


class QuerySpec(NamedTuple):
    sql: str
    params: Dict[str, Any]
    engine_name: str
