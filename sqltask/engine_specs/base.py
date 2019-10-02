from typing import Any, Dict, List, Optional

from sqlalchemy import inspect
from sqlalchemy.schema import Column, Table
from sqlalchemy.sql import text


class BaseEngineSpec:
    """
    Generic spec defining default behaviour for SqlAlchemy engines.
    """
    engine: Optional[str] = None
    supports_column_comments = True
    supports_table_comments = True

    @classmethod
    def insert_rows(cls, output_rows: List[Dict[str, Any]], output_spec: Table) -> None:
        """
        Default function for

        :param output_rows:
        :param output_spec:
        :return:
        """
        with output_spec.bind.begin() as conn:
            conn.execute(output_spec.insert(), *output_rows)

    @classmethod
    def truncate_rows(cls, table: Table, execution_columns: List[Column],
                      params: Dict[str, Any]) -> None:
        """
        Delete old rows from target table that match the execution parameters.

        :param table: Output table
        :param execution_columns: execution
        :param params:
        :return:
        """
        where_clause = " AND ".join(
            [f"{col.name} = :{col.name}" for col in execution_columns])
        stmt = f"DELETE FROM {table.name} WHERE {where_clause}"
        table.bind.execute(text(stmt), params)

    @classmethod
    def migrate_schema(cls, table: Table) -> None:
        """
        Migrate all table schemas to target engines. Create new tables if missing,
        add missing columns if table exists but not all columns present.
        """
        if table.bind.has_table(table.name):
            inspector = inspect(table.bind)
            cols_existing = [col['name'] for col in inspector.get_columns(table.name)]
            for column in table.columns:
                if column.name not in cols_existing:
                    print(f'{column.name} is missing')
                    stmt = f'ALTER TABLE {table.name} ADD COLUMN {column.name} {str(column.type)}'
                    table.bind.execute(stmt)
        else:
            table.metadata.create_all(tables=[table])
