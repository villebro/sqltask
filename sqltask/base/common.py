from typing import TYPE_CHECKING, Any, Dict, NamedTuple, Optional

if TYPE_CHECKING:
    from sqltask.base.engine import EngineContext
    from sqltask.base.table import BaseTableContext  # noqa: F401


class QueryContext(NamedTuple):
    sql: str
    params: Dict[str, Any]
    table_context: Optional["BaseTableContext"]
    engine_context: "EngineContext"


class UrlParams(NamedTuple):
    database: Optional[str]
    schema: Optional[str]
