import logging
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Type

from sqlalchemy import types
from sqlalchemy.engine.url import URL
from sqlalchemy.schema import Column
from sqlalchemy.sql import text
from sqlalchemy.sql.type_api import TypeEngine

from sqltask.base.common import UrlParams
from sqltask.base.table import BaseTableContext
from sqltask.utils.engine_specs import get_escaped_string_value

logger = logging.getLogger(__name__)


class UploadType(Enum):
    SQL_INSERT = 1
    CSV = 2


VALID_COLUMN_TYPES: Dict[Type[TypeEngine], Tuple[Any, ...]] = {
    types.Date: (date,),
    types.DATE: (date,),
    types.DateTime: (date, datetime),
    types.DATETIME: (date, datetime),
    types.INT: (int,),
    types.INTEGER: (int,),
    types.Integer: (int,),
    types.Float: (int, float, Decimal),
    types.String: (str,),
    types.NVARCHAR: (str,),
    types.VARCHAR: (str,),
    types.SmallInteger: (int,),
    types.SMALLINT: (int,),
    types.BIGINT: (int,),
    types.BigInteger: (int,),
    types.Numeric: (int, float, Decimal),
    types.NUMERIC: (int, float, Decimal),
}


class BaseEngineSpec:
    """
    Generic spec defining default behaviour for SqlAlchemy engines.
    """
    engine: Optional[str] = None
    default_upload_type = UploadType.SQL_INSERT
    supported_uploads: Set[UploadType] = {UploadType.SQL_INSERT}
    supports_primary_keys = True
    supports_column_comments = True
    supports_table_comments = True
    supports_schemas = True
    insert_chunksize = 10000
    empty_where_clause = ""

    @classmethod
    def insert_rows(cls,
                    table_context: "BaseTableContext",
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
        elif upload_type == UploadType.CSV:
            cls._insert_rows_csv(table_context)
        else:
            raise NotImplementedError(f"Unsupported upload type: {upload_type}")

    @classmethod
    def _insert_rows_sql_insert(cls, table_context: "BaseTableContext") -> None:
        """
        Insert rows using standard insert statements. Not very performant, but mostly
        universally supported.
        """
        if UploadType.SQL_INSERT not in cls.supported_uploads:
            raise Exception(f"SQL INSERT not supported by `{cls.__name__}`")
        with table_context.engine_context.engine.begin() as conn:
            while table_context.output_rows:
                insert_chunk: List[Dict[str, Any]] = []
                while table_context.output_rows and \
                        len(insert_chunk) < cls.insert_chunksize:
                    insert_chunk.append(table_context.output_rows.pop())
                conn.execute(table_context.table.insert(), insert_chunk)

    @classmethod
    def _insert_rows_csv(cls, table_context: "BaseTableContext") -> None:
        raise NotImplementedError(f"`{cls.__name__}` does not support CSV upload")

    @classmethod
    def truncate_rows(cls, table_context: "BaseTableContext") -> None:
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
            where_clause = cls.empty_where_clause
        stmt = f"DELETE FROM {table.name}{where_clause}"
        engine.execute(text(stmt), batch_params)

    @classmethod
    def modify_url(cls, url: URL, database: Optional[str], schema: Optional[str]) -> None:
        """
        Modify the url to point to a new schema.

        :param url: SqlAlchemy URL instance
        :param database: database to point the new URL to
        :param schema: schema to point the new URL to
        """
        database_current = url.database
        schema_current = None
        if not cls.supports_schemas or database is None:
            return
        if "/" in database_current:
            database_current, schema_current = database_current.split("/")

        if database is None:
            database = database_current
        if schema is None:
            schema = schema_current

        if schema is None:
            url.database = database
        else:
            url.database = database + "/" + schema

    @classmethod
    def get_url_params(cls, url: URL) -> UrlParams:
        """
        Extract schema name from URL instance. Assumes that the schema name is what
        cmes after a slash in the database name, e.g. `database/schema`.

        :param url: SqlAlchemy URL instance
        :return: schema name
        """
        schema = None
        database = url.database
        if cls.supports_schemas and database is not None and "/" in database:
            database, schema = database.split("/")
        return UrlParams(database=database, schema=schema)

    @classmethod
    def add_column(cls,
                   table_context: BaseTableContext,
                   column: Column,
                   ) -> None:
        """
        Add a column to target table

        :param table_context: table which to alter
        :param column: column to be added
        :return:
        """
        table_name = table_context.table.name
        dialect = table_context.engine_context.engine.dialect
        logging.debug(f"Add column `{column.name}` to table `{table_name}`")
        stmt = f"ALTER TABLE {table_name} ADD COLUMN " \
               f"{column.name} {column.type.compile(dialect=dialect)}"
        if column.default is not None:
            if isinstance(column.default, str):
                default_value = f"'{get_escaped_string_value(column.default)}'"
            else:
                default_value = column.default
            stmt += f" DEFAULT {default_value}"
        if column.autoincrement is True:
            stmt += " AUTOINCREMENT"
        if column.nullable is True:
            stmt += " NULL"
        else:
            stmt += " NOT NULL"
        if cls.supports_primary_keys and column.primary_key is True:
            stmt += " PRIMARY KEY"
        if cls.supports_column_comments and column.comment:
            comment = get_escaped_string_value(column.comment)
            stmt += f" COMMENT '{comment}'"
        table_context.engine_context.engine.execute(stmt)

    @classmethod
    def drop_column(cls,
                    table_context: BaseTableContext,
                    column_name: Column,
                    ) -> None:
        """
        Add a column to target table

        :param table_context: table which to alter
        :param column_name: column to drop
        :return:
        """
        table_name = table_context.table.name
        logging.info(f"Drop column `{column_name}` from table `{table_name}`")
        stmt = f'ALTER TABLE {table_name} DROP COLUMN {column_name}'
        table_context.engine_context.engine.execute(stmt)

    @classmethod
    def update_table_comment(cls,
                             table_context: BaseTableContext,
                             comment: str):
        """
        Update the comment of a table.

        :param table_context: table which to alter
        :param comment: new coment
        :return:
        """
        table_name = table_context.table.name
        logging.info(f"Change comment on table `{table_name}`")
        comment = get_escaped_string_value(comment)
        stmt = f"COMMENT ON TABLE {table_name} IS '{comment}'"
        table_context.engine_context.engine.execute(stmt)

    @classmethod
    def update_column_comment(cls,
                              table_context: BaseTableContext,
                              column_name: str,
                              comment: str):
        """
        Update the comment of a column.

        :param table_context: table which to alter
        :param column_name: column whose comment is to be updated
        :param comment: new coment
        :return:
        """
        table_name = table_context.table.name
        logging.info(f"Change comment on table `{table_name}`")
        comment = get_escaped_string_value(comment)
        stmt = f"COMMENT ON COLUMN {table_name}.{column_name} IS '{comment}'"
        table_context.engine_context.engine.execute(stmt)

    @classmethod
    def validate_column_value(cls, value: Any, column: Column) -> None:
        """
        Ensure that a value is compatible with the target column. The method doesn't
        return a value, only raises an Exception if the value and target column type
        are incompatible.

        :param value: value to insert into a column of a database table
        :param column: The target column
        """
        global VALID_COLUMN_TYPES
        name = column.name
        valid_types = VALID_COLUMN_TYPES.get(type(column.type))
        if column.nullable and value is None:
            pass
        elif not column.nullable and value is None:
            raise ValueError(f"Column {name} cannot be null")
        elif valid_types is None:
            # type checking not defined for this type
            pass
        else:
            type_ = column.type
            length = type_.length if hasattr(type_, "length") else None  # type: ignore
            if type(value) not in valid_types:
                raise ValueError(f"Column {name} type {column.type} is not compatible "
                                 f"with type {type(value).__name__}: {value}")
            if isinstance(value, str) and length is not None \
                    and len(value) > length:
                raise ValueError(f"Column {name} only supports "
                                 f"{length} "
                                 f"character strings, given value is {len(value)} "
                                 f"characters.")
