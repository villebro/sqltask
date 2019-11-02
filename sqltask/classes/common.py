from typing import (
    Any,
    Dict,
    Iterator,
    Mapping,
    NamedTuple,
    Optional,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from sqltask.classes.engine import EngineContext
    from sqltask.classes.table import TableContext  # noqa: F401


class QueryContext(NamedTuple):
    sql: str
    params: Dict[str, Any]
    table_context: Optional["TableContext"]
    engine_context: "EngineContext"


class BaseDataSource:
    def __init__(self, name: Optional[str]):
        self.name = name

    def __iter__(self) -> Iterator[Mapping[str, Any]]:
        raise NotImplementedError("`__iter__` not implemented")


class UrlParams(NamedTuple):
    database: Optional[str]
    schema: Optional[str]
