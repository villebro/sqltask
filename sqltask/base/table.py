import logging
from collections import UserDict
from datetime import datetime
from typing import (TYPE_CHECKING, Any, Callable, Dict, List, Mapping,
                    Optional, Sequence)

import sqlalchemy as sa
from sqlalchemy.schema import Column, Table
from sqlalchemy.types import String

from sqltask.base import dq
from sqltask.base.row_source import BaseRowSource

if TYPE_CHECKING:
    from sqltask.base.engine import EngineContext

logger = logging.getLogger(__name__)


class BaseTableContext:
    """
    The BaseTableContext class contains everything necessary for creating/modifying a
    target table/schema and inserting/removing rows.
    """
    def __init__(
            self,
            name: str,
            engine_context: "EngineContext",
            columns: List[Column],
            comment: Optional[str] = None,
            database: Optional[str] = None,
            schema: Optional[str] = None,
            batch_params: Optional[Dict[str, Any]] = None,
            timestamp_column_name: Optional[str] = None,
            table_params: Dict[str, Any] = None,
    ):
        """
        :param name: Name of target table in database.
        :param engine_context: engine to bind table to.
        :param columns: All columns in table.
        :param comment: Table comment.
        :param database: Database to use. If left unspecified, falls back to the database
        provided by the engine context
        :param schema: Schema to use. If left unspecified, falls back to the schema
        provided by the engine context
        :param batch_params: Mapping between column names and values that are used to
        delete old rows in the table.
        :param timestamp_column_name: Name of column used for populating etl timestamp.
        """
        # comment is apparently not Optional, so needs to be passed via kwargs
        table_params = table_params or {}
        self.columns = {column.name: column for column in columns}
        if comment:
            table_params["comment"] = comment

        table = Table(name,
                      engine_context.metadata,
                      *columns,
                      **table_params)

        # Finalize main table context after dq table context is created
        self.name = name
        self.table = table
        self.database = database or engine_context.database
        self.schema = schema or engine_context.schema
        self.engine_context = engine_context.create_new(
            database=self.database, schema=self.schema)
        self.batch_params = batch_params or {}
        self.timestamp_column_name = timestamp_column_name
        self.output_rows: List[Dict[str, Any]] = []

    def get_new_row(self) -> "BaseOutputRow":
        """
        Get a new row intended to be added to the table.
        """
        output_row = BaseOutputRow(self)
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
        engine_spec = self.engine_context.engine_spec
        metadata = self.engine_context.metadata

        if engine.has_table(table.name, schema=self.schema):
            inspector = sa.inspect(engine)

            # update table comment if different from current comment
            if engine_spec.supports_table_comments:
                table_comment = inspector.get_table_comment(table.name)
                if table.comment != table_comment:
                    engine_spec.update_table_comment(self, table.comment)

            # check if existing columns are up to date
            cols_existing = {col['name']: col
                             for col in inspector.get_columns(table.name)}
            for column in table.columns:
                col_existing = cols_existing.get(column.name)

                # add columns if not in table
                if not col_existing:
                    logger.info(f"Add column `{column.name}` to table `{table.name}`")
                    self.engine_context.engine_spec.add_column(self, column)
                else:
                    if engine_spec.supports_column_comments and \
                            column.comment is not None and \
                            col_existing["comment"] != column.comment:
                        # update column comment if different from current comment
                        logger.info(
                            f"Update comment on column `{column.name}` in "
                            f"table `{table.name}`")
                        engine_spec.update_column_comment(
                            self, column.name, column.comment)

            # remove redundant columns
            cols_new = {col.name: col for col in table.columns}
            for column_name in cols_existing:
                if column_name not in cols_new:
                    logger.info(
                        f"Remove redundant column `{column_name}` from "
                        f"table `{table.name}`")
                    self.engine_context.engine_spec.drop_column(self, column_name)
        else:
            # table doesn't exist, create new
            logger.debug(f"Create new table `{table.name}`")
            metadata.create_all(tables=[table])

    def map_all(self,
                row_source: BaseRowSource,
                mappings: Optional[Dict[str, str]] = None,
                funcs: Optional[Dict[str, Callable[[Any], Any]]] = None,
                ) -> None:
        """
        Convenience method for mapping all rows and columns from the input row source
        to the output table in a one-to-one fashion. The optional arguments `mappings`
        and `funcs` can be used to specify alternative column name mappings and
        conversion functions.

        :param row_source: Input row source to map to the outout table.
        :param mappings: mapping from target column name to source column name. If the
               source and target names differ for one or several columns, these can be
               specified here. Example: {"customer_name": "cust_n"} would map the values
               in the source column "cust_n" to the target column "customer_name".
        :param funcs: mapping from target column name to callable function. If the source
               and target types differ for one or several columns, a callable can be
               specified here. Typically this is needed when ingesting data from a CSV
               file where the source data types are always strings, but might
               need to be cast to int, float or Decimal. Example: {"customer_age": int}
               would call `int()` on the source value.
        """
        for input_row in row_source:
            output_row = self.get_new_row()
            output_row.map_all(
                input_row=input_row,
                mappings=mappings,
                funcs=funcs,
                auto_append=True,
            )


class DqTableContext(BaseTableContext):
    """
    A :class:`~sqltask.base.table.TableContext` child class with support for logging
    data quality issues to a separate data quality table.
    A  with the ability to log data quality issues
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
        self.dq_table_context = BaseTableContext(
            name=dq_table_name,
            engine_context=dq_engine_context,
            columns=dq_columns,
            comment=comment,
            schema=dq_schema,
            batch_params=batch_params,
            table_params=dq_table_params,
        )

    def get_new_row(self) -> "DqOutputRow":
        """
        Get a new row intended to be added to the table.
        """
        output_row = DqOutputRow(self)
        if self.timestamp_column_name:
            output_row[self.timestamp_column_name] = datetime.utcnow()
        return output_row

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


class BaseOutputRow(UserDict):
    """
    A class for storing cell values for a single row in a
    :class:`~sqltask.base.table.TableContext` table. When the object is created,
    all batch parameters are prepopulated.
    """
    def __init__(self, table_context: BaseTableContext):
        self.table_context = table_context
        super().__init__(table_context.batch_params)
        if table_context.timestamp_column_name:
            self[table_context.timestamp_column_name] = datetime.utcnow()

    def __setitem__(self, key, value):
        # validate column value if table schema defined
        if self.table_context.columns is not None:
            target_column = self.table_context.columns.get(key)
            if target_column is None:
                raise KeyError(f"Column not found in target schema: {key}")
            engine_spec = self.table_context.engine_context.engine_spec
            engine_spec.validate_column_value(value, target_column)
        super().__setitem__(key, value)

    def map_all(self,
                input_row: Mapping[str, Any],
                mappings: Optional[Dict[str, str]] = None,
                funcs: Optional[Dict[str, Callable[[Any], Any]]] = None,
                columns: Optional[Sequence[str]] = None,
                auto_append: bool = False) -> None:
        """
        Convenience method for mapping column values one-to-one from an input row
        to the output row. Will only map any unmapped columns, i.e. if the target
        row has columns "customer_id" and "customer_name", and "customer_name" has
        already been populated, only "customer_id" will be mapped.

        :param input_row: the input row to map values from.
        :param mappings: mapping from target column name to source column name. If the
               source and target names differ for one or several columns, these can be
               specified here. Example: {"customer_name": "cust_n"} would map the values
               in the source column "cust_n" to the target column "customer_name".
        :param funcs: mapping from target column name to callable function. If the source
               and target types differ for one or several columns, a callable can be
               specified here. Typically this is needed when ingesting data from a CSV
               file where the source data types are always strings, but might
               need to be cast to int, float or Decimal. Example: {"customer_age": int}
               would call `int()` on the source value.
        :param columns: A list of column names to map. If undefined, tries to map all
               unmapped columns in target row.
        :param auto_append: Call append after mapping rows if the mapping operation
               is successful.
        """
        mappings = mappings or {}
        funcs = funcs or {}
        if columns is None:
            target_columns = set([column.name for column in
                                  self.table_context.table.columns])
            for column in self.keys():
                target_columns.remove(column)
        else:
            target_columns = set(columns)
        for target_column in target_columns:
            source_column = mappings.get(target_column, target_column)
            if source_column not in input_row:
                raise Exception(f"Column not in input row: `{source_column}`")
            func = funcs.get(target_column)
            if func is not None:
                self[target_column] = func(input_row[source_column])
            else:
                self[target_column] = input_row[source_column]
        if auto_append:
            self.append()

    def append(self) -> None:
        """
        Append the row to the target table.  :func:`~sqltask.base.table.OutputRow.append`
        should only be called once all cell values for the row have been fully populated,
        as any changes.
        """

        output_row = {}
        for column in self.table_context.columns.values():
            if column.name not in self:
                raise Exception(f"No column `{column.name}` in output row for table "
                                f"`{self.table_context.name}`")
            output_row[column.name] = self[column.name]
        self.table_context.output_rows.append(output_row)


class DqOutputRow(BaseOutputRow):
    def __init__(self, table_context: DqTableContext):
        super().__init__(table_context)
        self.logging_enabled = True

    def set_logging_enabled(self, enabled: bool) -> None:
        """
        If logging is set to false, data quality issues will not be passed to the
        log table. This is useful for rows with lower priority data, e.g. inactive
        users, whose data quality may be of poorer quality due to being stale.

        :param enabled: set to True to log issues; False to ignore calls to `log_dq``
        """
        self.logging_enabled = enabled

    def log_dq(self, column_name: Optional[str], category: dq.Category,
               priority: dq.Priority, source: dq.Source,
               message: Optional[str] = None) -> None:
        """
        Log data quality issue to be recorded in data quality table. If logging
        has been disabled by calling `set_logging_enabled(False)`, data quality
        issues will be ignored.

        :param column_name: Name of affected column in target table.
        :param category: The type of data quality issue.
        :param source: To what phase the data quality issue relates.
        :param priority: What the priority of the data quality issue is.
               Should be None for aggregate data quality issues.
        :param message: Verbose description of observed issue.
        """
        if self.logging_enabled is False:
            return

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
