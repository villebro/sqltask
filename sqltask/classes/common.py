from collections import UserDict
from typing import TYPE_CHECKING, Any, Dict, Mapping, NamedTuple, Optional

if TYPE_CHECKING:
    from sqltask.classes.engine import EngineContext
    from sqltask.classes.table import TableContext  # noqa: F401


class QueryContext(NamedTuple):
    sql: str
    params: Dict[str, Any]
    table_context: Optional["TableContext"]
    engine_context: "EngineContext"


class BaseDataSource:
    def __init__(self, name: str):
        self.name = name

    def __iter__(self):
        raise NotImplementedError("`__iter__` not implemented")


class Lookup(UserDict):
    def __init__(self, data_source: BaseDataSource, kv: Mapping[Any, Any]):
        super().__init__(kv)
        self.data_source = data_source


class UrlParams(NamedTuple):
    database: Optional[str]
    schema: Optional[str]
