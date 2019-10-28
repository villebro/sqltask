import logging
from collections import UserDict
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import sqlalchemy as sa
from sqlalchemy.schema import Column, Table
from sqlalchemy.types import String

from sqltask.classes import dq

if TYPE_CHECKING:
    from sqltask.classes.engine import EngineContext


class TableContext:
    def __init__(
            self,
            name: str,
            engine_context: "EngineContext",
            columns: List[Column],
            comment: Optional[str] = None,
            schema: Optional[str] = None,
            batch_params: Optional[Dict[str, Any]] = None,
            timestamp_column_name: Optional[str] = None,
            table_params: Dict[str, Any] = None,
    ):
        """
        Create a new table context.

        :param name: Name of target table in database.
        :param engine_context: engine to bind table to.
        :param columns: All columns in table.
        :param comment: Table comment.
        :param schema: Schema to use. If left unspecified, falls back to the schema
        provided by the engine context
        :param batch_params: Mapping between column names and values that are used to
        delete old rows in the table.
        :param timestamp_column_name: Name of column used for populating etl timestamp.
        """
        # comment is apparently not Optional, so needs to be passed via kwargs
        table_params = table_params or {}
        if comment:
            table_params["comment"] = comment
        table = Table(name,
                      engine_context.metadata,
                      *columns,
                      **table_params)

        # Finalize main table context after dq table context is created
        self.name = name
        self.table = table
        self.engine_context = engine_context
        self.batch_params = batch_params or {}
        self.timestamp_column_name = timestamp_column_name
        self.schema = schema or engine_context.schema
        self.output_rows: List[Dict[str, Any]] = []

    def get_new_row(self) -> "OutputRow":
        """
        Get a new row intended to be added to the table.
        """
        output_row = OutputRow(self)
        if self.timestamp_column_name:
            output_row[self.timestamp_column_name] = datetime.utcnow()
        return output_row

    def delete_rows(self) -> None:
        """
        Delete old rows from target table that match batch parameters.
        """
        self.engine_context.engine_spec.truncate_rows(self)

    def insert_rows(self) -> None:
        """
        Insert rows into target tables.
        """
        self.engine_context.engine_spec.insert_rows(self)

    def migrate_schema(self) -> None:
        """
        Migrate table schema to correspond to table definition.
        """
        table = self.table
        engine = self.engine_context.engine
        metadata = self.engine_context.metadata
        if engine.has_table(table.name, schema=self.schema):
            # table exists, add column
            inspector = sa.inspect(engine)
            cols_existing = [col['name'] for col in inspector.get_columns(table.name)]
            for column in table.columns:
                if column.name not in cols_existing:
                    self.engine_context.engine_spec.add_column(self, column)

            # remove redundant columns
            cols_new = {col.name: col for col in table.columns}
            for column_name in cols_existing:
                if column_name not in cols_new:
                    self.engine_context.engine_spec.drop_column(self, column_name)
        else:
            # table doesn't exist, create new
            logging.debug(f"Create new table `{table.name}`")
            metadata.create_all(tables=[table])


class DqTableContext(TableContext):
    """
    A table context with the ability to log data quality issues
    """
    def __init__(
            self,
            name: str,
            engine_context: "EngineContext",
            columns: List[Column],
            comment: Optional[str] = None,
            schema: Optional[str] = None,
            batch_params: Optional[Dict[str, Any]] = None,
            timestamp_column_name: Optional[str] = None,
            table_params: Dict[str, Any] = None,
            dq_table_name: Optional[str] = None,
            dq_engine_context: Optional["EngineContext"] = None,
            dq_schema: Optional[str] = None,
            dq_info_column_names: Optional[List[str]] = None,
            dq_table_params: Dict[str, Any] = None,
    ):
        """
        :param name: Name of target table in database.
        :param engine_context: engine to bind table to.
        :param columns: All columns in table.
        :param comment: Table comment.
        :param schema: Schema to use. If left unspecified, falls back to the schema
        provided by the engine context
        :param batch_params: Mapping between column names and values that are used to
        delete old rows in the table.
        :param timestamp_column_name: Name of column used for populating etl timestamp.
        :param table_params: Additional parameters to be passed as kwargs to the
        Table constructor.
        :param dq_table_name: Name of data quality table. Defaults to original table
        name + `_dq`
        :param dq_engine_context: Engine context used for creating data quality table.
        Defaults to `engine_context` if omitted
        :param dq_schema: Schema used for creating data quality table. Defaults to
        `schema` if left blank
        :param dq_info_column_names: Name of columns to be appended to the data
        quality table for informational purposes that aren't primary keys, e.g.
        customer name.
        :param table_params: Additional parameters to be passed as kwargs to the
        data quality Table constructor.
        """
        super().__init__(
            name=name,
            engine_context=engine_context,
            columns=columns,
            comment=comment,
            schema=schema,
            batch_params=batch_params,
            timestamp_column_name=timestamp_column_name,
            table_params=table_params,
        )

        dq_table_name = dq_table_name or self.name + "_dq"
        dq_engine_context = dq_engine_context or self.engine_context
        dq_timestamp_column_name = self.timestamp_column_name or "etl_timestamp"
        dq_schema = dq_schema or self.schema
        dq_info_column_names = dq_info_column_names or []
        dq_table_params = dq_table_params or {}

        # primary key columns
        batch_columns = []
        primary_key_columns = []
        info_columns = []

        for column in self.table.columns:
            # make copy of column and remove primary keys as one row might
            # have several data quality issues. Also make nullable, as data quality
            # issues don't necessarily relate to a specific column in the target table
            column_copy = column.copy()
            column_copy.primary_key = False
            column_copy.nullable = True
            if column.name in self.batch_params:
                batch_columns.append(column_copy)
            elif column.primary_key:
                primary_key_columns.append(column_copy)
            elif column.name in dq_info_column_names:
                info_columns.append(column_copy)

        default_dq_columns = [
            Column("source", String, comment="Source of issue"),
            Column("priority", String, comment="Priority of issue"),
            Column("category", String, comment="Category of issue"),
            Column("column_name", String, comment="Affected column in target table"),
            Column("message", String, comment="Verbose description of the issue"),
        ]

        dq_columns = batch_columns + primary_key_columns + info_columns + \
            default_dq_columns

        dq_schema = dq_schema or self.schema
        self.dq_table_context = TableContext(
            name=dq_table_name,
            engine_context=dq_engine_context,
            columns=dq_columns,
            comment=comment,
            schema=dq_schema,
            batch_params=batch_params,
            timestamp_column_name=dq_timestamp_column_name,
            table_params=dq_table_params,
        )

    def delete_rows(self) -> None:
        """
        Delete old rows from target table that match batch parameters.
        """
        super().delete_rows()
        self.dq_table_context.delete_rows()

    def insert_rows(self) -> None:
        """
        Insert rows into target tables.
        """
        super().insert_rows()
        self.dq_table_context.insert_rows()

    def migrate_schema(self) -> None:
        """
        Migrate table schema to correspond to table definition.
        """
        super().migrate_schema()
        self.dq_table_context.migrate_schema()


class OutputRow(UserDict):
    def __init__(self, table_context: TableContext):
        super().__init__(table_context.batch_params)
        self.table_context = table_context
        if table_context.timestamp_column_name:
            self[table_context.timestamp_column_name] = datetime.utcnow()

    def log_dq(self, column_name: Optional[str], category: dq.Category,
               priority: dq.Priority, source: dq.Source,
               message: Optional[str] = None):
        """
        Log data quality issue to be recorded in data quality table.

        :param column_name: Name of affected column in target table.
        :param category: The type of data quality issue.
        :param source: To what phase the data quality issue relates.
        :param priority: What the priority of the data quality issue is.
        Should be None for aggregate data quality issues.
        :param message: Verbose description of observed issue.
        """
        if column_name not in self.table_context.table.columns:
            raise Exception(f"Column `{column_name}` not in table "
                            f"`{self.table_context.table.name}`")

        if not isinstance(self.table_context, DqTableContext):
            raise Exception("table is not instance of DqTableContext")

        dq_table_context = self.table_context.dq_table_context
        if dq_table_context is None:
            raise Exception(f"No dq table context defined for {self.table_context.name}")
        dq_output_row = dq_table_context.get_new_row()

        dq_output_row.update({
            "source": str(source.value),
            "priority": str(priority.value),
            "category": str(category.value),
            "column_name": column_name,
            "message": message
        })

        # make sure all keys from dq table are included in final row object
        dq_row = {}
        for column in dq_table_context.table.columns:
            # Add additional info columns from main row
            if column.name not in dq_output_row and column.name in self.keys():
                dq_output_row[column.name] = self[column.name]
            elif column.name not in dq_output_row:
                raise Exception(f"No column `{column.name}` in output row for table "
                                f"`{dq_output_row.table_context.table.name}`")
            dq_row[column.name] = dq_output_row[column.name]
        dq_table_context.output_rows.append(dq_row)

    def append(self) -> None:
        """
        Append the row to the table.
        """

        output_row = {}
        for column in self.table_context.table.columns:
            if column.name not in self:
                raise Exception(f"No column `{column.name}` in output row for table "
                                f"`{self.table_context.name}`")
            output_row[column.name] = self[column.name]
        self.table_context.output_rows.append(output_row)
