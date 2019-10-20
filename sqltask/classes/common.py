from collections import UserDict
from typing import (
    Any,
    Dict,
    List,
    NamedTuple,
    Optional,
    Tuple,
    TYPE_CHECKING,
    Union
)
from sqlalchemy.schema import Column, Table
from sqltask.classes.dq import DqType

if TYPE_CHECKING:
    from sqltask.classes.context import EngineContext


class TableContext(NamedTuple):
    name: str
    table: Table
    engine_context: "EngineContext"
    batch_params: Optional[Dict[str, Any]]
    info_column_names: Optional[List[str]]
    timestamp_column_name: Optional[str]
    schema: Optional[str]


class QueryContext(NamedTuple):
    sql: str
    params: Dict[str, Any]
    table_context: Optional[TableContext]
    engine_context: "EngineContext"


class OutputRow(UserDict):
    def __init__(self, table_context: TableContext):
        super().__init__(table_context.batch_params)
        self.table_context = table_context


class TransformationResult(NamedTuple):
    value: Any
    dq_type: Optional[DqType]


class BaseDataSource:
    def __init__(self, name: str):
        self.name = name

    def __iter__(self) -> "Lookup":
        raise NotImplementedError("`__iter__` not implemented")


class Lookup(UserDict):
    def __init__(self, data_source: BaseDataSource,  kv: Union[Dict, Tuple]):
        super().__init__(kv)
        self.data_source = data_source
