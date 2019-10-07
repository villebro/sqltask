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


class EngineContext(NamedTuple):
    engine: Engine
    metadata: MetaData
    schema: Optional[str]


class TableContext(NamedTuple):
    table: Table
    engine_context: EngineContext
    batch_params: Optional[Dict[str, Any]]
    rowid_column_name: Optional[str]
    timestamp_column_name: Optional[str]
    schema: Optional[str]


class QueryContext(NamedTuple):
    sql: str
    params: Dict[str, Any]
    engine_context: EngineContext
