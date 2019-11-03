from typing import (
    Any,
    Dict,
    NamedTuple,
    Optional,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from sqltask.base.engine import EngineContext
    from sqltask.base.table import TableContext  # noqa: F401


class QueryContext(NamedTuple):
    sql: str
    params: Dict[str, Any]
    table_context: Optional["TableContext"]
    engine_context: "EngineContext"


class UrlParams(NamedTuple):
    database: Optional[str]
    schema: Optional[str]
