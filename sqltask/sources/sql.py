import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional, Sequence

from sqlalchemy.engine import RowProxy
from sqlalchemy.sql import text

from sqltask.base.lookup_source import BaseLookupSource
from sqltask.base.row_source import BaseRowSource

if TYPE_CHECKING:
    from sqltask.base.engine import EngineContext


logger = logging.getLogger(__name__)


class SqlRowSource(BaseRowSource):
    def __init__(
            self,
            sql: str,
            params: Dict[str, Any],
            engine_context: "EngineContext",
            name: Optional[str] = None,
            database: Optional[str] = None,
            schema: Optional[str] = None,
    ):
        """
        :param sql: sql query with parameter values prefixed with a colon, e.g.
        `WHERE dt <= :batch_date`
        :param params: mapping between parameter keys and values, e.g.
        `{"batch_date": date(2010, 1, 1)}`
        :param name: name of data source
        :param database: database to use when executing query. Uses database defined
        in sql_engine if left undefined.
        :param schema: schema to use when executing query. Uses schema defined
        in sql_engine if left undefined.
        :param engine_context: engine used to execute the query.
        """
        params = params or {}
        database = database or engine_context.database
        schema = schema or engine_context.schema
        super().__init__(name)
        self.sql = sql
        self.params = params or {}
        self.database = database or engine_context.database
        self.schema = schema or engine_context.schema
        self.engine_context = engine_context.create_new(
            database=self.database, schema=self.schema
        )

    def __repr__(self):
        return self.name or "<undefined>"

    def __iter__(self) -> Iterator[RowProxy]:
        logger.debug(f"Executing query for SQL row source: {self}")
        rows = self.engine_context.engine.execute(text(self.sql), self.params)
        row_number = 0
        for row_number, row in enumerate(rows):
            yield row
        logger.debug(
            f"Finished reading {row_number + 1} rows from SQL row source: {self}"
        )


class SqlLookupSource(BaseLookupSource):
    """
    A convenience wrapper that creates a BaseLookupSource with a SqlRowSource as the
    data source.
    """
    def __init__(self,
                 name: str,
                 sql: str,
                 params: Dict[str, Any],
                 engine_context: "EngineContext",
                 keys: Sequence[str],
                 database: Optional[str] = None,
                 schema: Optional[str] = None,
                 ):
        row_source = SqlRowSource(
            name=name,
            sql=sql,
            params=params,
            engine_context=engine_context,
            database=database,
            schema=schema)
        super().__init__(name=name, row_source=row_source, keys=keys)
