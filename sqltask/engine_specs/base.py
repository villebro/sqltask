import logging
from enum import Enum
from typing import Optional, Sequence

from sqlalchemy.engine.url import URL
from sqlalchemy.schema import Column
from sqlalchemy.sql import text

from sqltask.classes.table import TableContext

log = logging


class UploadType(Enum):
    SQL_INSERT = 1
    SQL_INSERT_MULTIROW = 2
    CSV = 3


class BaseEngineSpec:
    """
    Generic spec defining default behaviour for SqlAlchemy engines.
    """
    engine: Optional[str] = None
    default_upload_type = UploadType.SQL_INSERT
    supported_uploads: Sequence[UploadType] = (
        UploadType.SQL_INSERT,
        UploadType.SQL_INSERT_MULTIROW,
    )
    supports_column_comments = True
    supports_table_comments = True
    supports_schemas = True

    @classmethod
    def insert_rows(cls,
                    table_context: "TableContext",
                    upload_type: Optional[UploadType] = None) -> None:
        """
        Default method for inserting data into database. This

        :param output_rows: Rows to upload.
        :param table_context: Table context on which the upload should be based.
        :param upload_type: If undefined, defaults to whichever ´UploadType` is defined
        in `default_upload_type`.
        """
        upload_type = upload_type or cls.default_upload_type
        if upload_type == UploadType.SQL_INSERT:
            cls._insert_rows_sql_insert(table_context)
        elif upload_type == UploadType.SQL_INSERT_MULTIROW:
            cls._insert_rows_sql_insert_multirow(table_context)
        elif upload_type == UploadType.CSV:
            cls._insert_rows_csv(table_context)
        else:
            raise NotImplementedError(f"Unsupported upload type: {upload_type}")

    @classmethod
    def _insert_rows_sql_insert(cls,
                                table_context: "TableContext"
                                ) -> None:
        """
        Insert rows using standard insert statements. Not very performant, but mostly
        universally supported.
        """
        if UploadType.SQL_INSERT not in cls.supported_uploads:
            raise Exception(f"SQL INSERT not supported by `{cls.__name__}`")
        with table_context.engine_context.engine.begin() as conn:
            conn.execute(table_context.table.insert(), *table_context.output_rows)

    @classmethod
    def _insert_rows_sql_insert_multirow(cls,
                                         table_context: "TableContext",
                                         chunksize: int = 5000
                                         ) -> None:
        """
        Insert rows using standard insert statements. Not very performant, but mostly
        universally supported.
        """
        if UploadType.SQL_INSERT not in cls.supported_uploads:
            raise Exception(f"SQL INSERT not supported by `{cls.__name__}`")
        with table_context.engine_context.engine.begin() as conn:
            conn.execute(table_context.table.insert().values(table_context.output_rows))

    @classmethod
    def _insert_rows_csv(cls, table_context: "TableContext") -> None:
        raise NotImplementedError(f"`{cls.__name__}` does not support CSV upload")

    @classmethod
    def truncate_rows(cls, table_context: "TableContext") -> None:
        """
        Delete old rows from target table that match the execution parameters.

        :param table_context: Output table
        """
        table = table_context.table
        engine = table_context.engine_context.engine
        batch_params = table_context.batch_params
        if batch_params:
            where_clause = " WHERE " + " AND ".join(
                [f"{col} = :{col}" for col in batch_params.keys()])
        else:
            where_clause = ""
        stmt = f"DELETE FROM {table.name}{where_clause}"
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
        database = url.database
        if cls.supports_schemas and database is not None and "/" in database:
            schema = database.split("/")[1]
        return schema

    @classmethod
    def add_column(cls,
                   table_context: TableContext,
                   column: Column,
                   ) -> None:
        """
        Add a column to target table

        :param table_context: table which to alter
        :param column: column to be added
        :return:
        """
        table_name = table_context.table.name
        logging.debug(f"Add column `{column.name}` to table `{table_name}`")
        stmt = f'ALTER TABLE {table_name} ADD COLUMN ' \
               f'{column.name} {str(column.type)}'
        table_context.engine_context.engine.execute(stmt)

    @classmethod
    def drop_column(cls,
                    table_context: TableContext,
                    column_name: Column,
                    ) -> None:
        """
        Add a column to target table

        :param table_context: table which to alter
        :param column_name: column to drop
        :return:
        """
        table_name = table_context.table.name
        logging.debug(f"Drop column `{column_name}` from table `{table_name}`")
        stmt = f'ALTER TABLE {table_name} DROP COLUMN {column_name}'
        table_context.engine_context.engine.execute(stmt)
