from collections import UserDict
from enum import Enum
from typing import Any, Dict, NamedTuple, Optional

from sqlalchemy.engine import Engine
from sqlalchemy.schema import MetaData, Table


class DqSource(Enum):
    SOURCE = "source"
    TRANSFORM = "transform"
    LOOKUP = "lookup"


class DqPriority(Enum):
    MANDATORY = "mandatory"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class EngineContext(NamedTuple):
    name: str
    engine: Engine
    metadata: MetaData
    schema: Optional[str]


class TableContext(NamedTuple):
    name: str
    table: Table
    engine_context: EngineContext
    batch_params: Optional[Dict[str, Any]]
    rowid_column_name: Optional[str]
    timestamp_column_name: Optional[str]
    schema: Optional[str]


class QueryContext(NamedTuple):
    sql: str
    params: Dict[str, Any]
    table_context: Optional[TableContext]
    engine_context: EngineContext


class OutputRow(UserDict):
    def __init__(self, table_context: TableContext):
        super().__init__(table_context.batch_params)
        self.table_context = table_context
