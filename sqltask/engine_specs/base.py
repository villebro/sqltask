import logging
from typing import Any, Dict, List, Optional

from sqlalchemy.engine.url import URL
from sqlalchemy.sql import text
from sqltask.common import TableContext

log = logging.getLogger('sqltask')


class BaseEngineSpec:
    """
    Generic spec defining default behaviour for SqlAlchemy engines.
    """
    engine: Optional[str] = None
    supports_column_comments = True
    supports_table_comments = True
    supports_schemas = True

    @classmethod
    def insert_rows(cls, output_rows: List[Dict[str, Any]],
                    table_context: TableContext) -> None:
        """
        Default function for

        :param output_rows:
        :param table_context:
        :return:
        """
        with table_context.engine_context.engine.begin() as conn:
            conn.execute(table_context.table.insert(), *output_rows)

    @classmethod
    def truncate_rows(cls, table_context: TableContext,
                      batch_params: Dict[str, Any]) -> None:
        """
        Delete old rows from target table that match the execution parameters.

        :param table: Output table
        :param execution_columns: execution
        :param params:
        :return:
        """
        table = table_context.table
        engine = table_context.engine_context.engine
        where_clause = " AND ".join(
            [f"{col} = :{col}" for col in batch_params.keys()])
        stmt = f"DELETE FROM {table.name} WHERE {where_clause}"
        engine.execute(text(stmt), batch_params)

    @classmethod
    def get_schema_name(cls, url: URL) -> Optional[str]:
        """
        Extract schema name from URL instance. Assumes that the schema name is what
        cmes after a slash in the database name, e.g. `database/schema`.

        :param url: SqlAlchemy URL instance
        :return: schema name
        """
        schema = None
        if cls.supports_schemas and "/" in url.database:
            schema = url.database.split("/")[1]
        return schema
