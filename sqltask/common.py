from collections import UserDict
from enum import Enum
from typing import Any, Dict, List, NamedTuple, Optional, Type, TYPE_CHECKING

from sqlalchemy.engine import Engine
from sqlalchemy.schema import MetaData, Table
if TYPE_CHECKING:
    from sqltask.engine_specs.base import BaseEngineSpec

class DqSource(Enum):
    SOURCE = "source"
    TRANSFORM = "transform"
    LOOKUP = "lookup"


class DqPriority(Enum):
    MANDATORY = "mandatory"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class DqType(Enum):
    MISSING = "missing"
    INCORRECT = "incorrect"
    DUPLICATE = "duplicate"


class EngineContext(NamedTuple):
    name: str
    engine: Engine
    engine_spec: Type["BaseEngineSpec"]
    metadata: MetaData
    schema: Optional[str]


class TableContext(NamedTuple):
    name: str
    table: Table
    engine_context: EngineContext
    batch_params: Optional[Dict[str, Any]]
    info_column_names: Optional[List[str]]
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
