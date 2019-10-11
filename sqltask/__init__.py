from datetime import datetime
import logging
import os
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Set
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.engine import create_engine, ResultProxy, RowProxy
from sqlalchemy.schema import Column, MetaData, Table
from sqlalchemy.sql import text
from sqlalchemy.types import String
from sqltask.common import DqPriority, DqSource, DqType, EngineContext, OutputRow, TableContext, QueryContext
from sqltask.engine_specs import get_engine_spec
from sqltask.exceptions import ExecutionArgumentException, MandatoryValueMissingException

# initialize logging
log = logging.getLogger('sqltask')
log_level = os.getenv("SQLTASK_LOG_LEVEL")
if log_level:
    log.setLevel(log_level)

ch = logging.StreamHandler()
ch.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
log.addHandler(ch)


class SqlTask:
    def __init__(self, **batch_params):
        """
        Main class in library.

        :param batch_params: Mapping from batch column name to value
        """
        self._tables: Dict[str, TableContext] = {}
        self._dq_tables: Dict[str, TableContext] = {}
        self._engines: Dict[str, EngineContext] = {}
        self._lookup_queries: Dict[str, QueryContext] = {}
        self._source_queries: Dict[str, QueryContext] = {}
        self._lookup_cache: Dict[str, Union[Set[Union[Any, Tuple[Any, ...]]],
                                            Dict[Union[Any, Tuple[Any, ...]],
                                                 Union[Any, Dict[str, Any]]]]] = {}
        self._output_rows: Dict[str, List[Dict[str, Any]]] = {}
        self._dq_output_rows: Dict[str, List[Dict[str, Any]]] = {}
        self.batch_params: Dict[str, Any] = batch_params or {}

    def add_engine(self,
                   name: str,
                   url: str,
                   schema: Optional[str] = None,
                   **kwargs) -> EngineContext:
        """
        Add a new engine to be used by sources, sinks and lookups.

        :param url: SqlAlchemy URL of engine.
        :param name: alias by which the engine is referenced during during operations.
        :param schema: Schema to use. If left unspecified, falls back to the schema
        defined in the engine URL if supported
        :param kwargs: additional parameters to be passed to metadata object.
        :return: created engine context
        """
        engine = create_engine(url)
        metadata = MetaData(bind=engine, **kwargs)
        engine_spec = get_engine_spec(engine.name)
        schema = schema or engine_spec.get_schema_name(engine.url)
        engine_context = EngineContext(name, engine, engine_spec, metadata, schema)
        self._engines[name] = engine_context
        log.debug(f"Added new engine `{name}` using `{engine_spec.__name__}` on schema `{schema}`")
        return engine_context

    def add_table(self,
                  name: str,
                  engine_context: EngineContext,
                  columns: List[Column],
                  schema: Optional[str] = None,
                  batch_params: Optional[Dict[str, Any]] = None,
                  info_column_names: Optional[List[str]] = None,
                  timestamp_column_name: Optional[str] = None,
                  dq_table_name: Optional[str] = None,
                  dq_engine_context: Optional[EngineContext] = None,
                  dq_schema: Optional[str] = None,
                  **kwargs) -> TableContext:
        """
        Add a table schema.

        :param name: Name of target table in database.
        :param engine_context: Name of engine to bind table to.
        :param columns: Additional columns beyond default and batch columns.
        :param schema: Schema to use. If left unspecified, falls back to the schema
        provided by the engine context
        :param batch_params: Mapping between column names and values that are used to
        delete old rows in the table.
        :param info_column_names: Name of columns to be appended to the data quality
        table for informational purposes that aren't primary keys, e.g. customer name.
        :param timestamp_column_name: Name of column used for populating etl timestamp.
        :param dq_table_name: Name of data quality table. Defaults to original table
        name + `_dq`
        :param dq_engine_context: Engine context used for creating data quality table.
        Defaults to `engine_context` if omitted
        :param dq_schema: Schema used for creating data quality table. Defaults to
        `schema` if left blank
        :param kwargs: Additional parameters to pass to Table constructor
        :return: created table context
        """
        table = Table(name, engine_context.metadata, *columns, **kwargs)
        schema = schema or engine_context.schema
        table_context = TableContext(name, table, engine_context, batch_params,
                                     info_column_names, timestamp_column_name, schema)
        self._tables[name] = table_context
        self._output_rows[name] = []

        # Initialize data quality table
        dq_table_name = dq_table_name or name + "_dq"
        dq_engine_context = dq_engine_context or engine_context
        dq_timestamp_column_name = timestamp_column_name or "etl_timestamp"
        info_column_names = info_column_names or []
        batch_params = batch_params or {}

        # primary key columns
        batch_columns = []
        primary_key_columns = []
        info_columns = []

        for column in columns:
            # make copy of column and remove primary keys as one row might
            # have several data quality issues. Also make nullable, as data quality
            # issues don't necessarily relate to a specific column in the target table
            column_copy = column.copy()
            column_copy.primary_key = False
            column_copy.nullable = True
            if column.name in batch_params:
                batch_columns.append(column_copy)
            elif column.primary_key:
                primary_key_columns.append(column_copy)
            elif column.name in info_column_names:
                info_columns.append(column_copy)

        dq_columns = batch_columns + primary_key_columns + info_columns + [
            Column("dq_rowid", String, comment="Unique row id"),
            Column("source", String, comment="Source of issue"),
            Column("priority", String, comment="Priority of issue"),
            Column("dq_type", String, comment="Type of issue"),
            Column("column_name", String, comment="Affected column in target table"),
        ]
        dq_table = Table(dq_table_name, dq_engine_context.metadata, *dq_columns, **kwargs)
        dq_schema = dq_schema or schema
        dq_table_context = TableContext(name, dq_table, dq_engine_context,
                                        batch_params, info_column_names,
                                        dq_timestamp_column_name, dq_schema)
        self._dq_tables[name] = dq_table_context
        self._dq_output_rows[name] = []
        return table_context

    def get_table_context(self, name: str) -> TableContext:
        """
        Retrieve table context

        :param name: name of table context
        :return: predefined table context
        """
        table_context = self._tables.get(name)
        if table_context is None:
            raise Exception(f"Undefined table context: {name}")
        return table_context

    def get_dq_table_context(self, name: str) -> TableContext:
        """
        Retrieve table context for data quality issues

        :param name: name of table context
        :return: predefined table context
        """
        dq_table_context = self._dq_tables.get(name)
        if dq_table_context is None:
            raise Exception(f"Undefined data quality table context: {name}")
        return dq_table_context

    def transform(self) -> None:
        """
        Main transformation method where target rows should be generated.
        """
        raise NotImplementedError("`transform` method must be implemented")

    def validate(self) -> None:
        """
        Abstract validation method that is executed after transformation is completed.
        Should be implemented to validate aggregate measures that can't be validated
        during transformation.
        """
        pass

    def log_dq(self,
               source: DqSource,
               priority: DqPriority,
               dq_type: DqType,
               column_name: Optional[str] = None,
               output_row: Optional[OutputRow] = None,
               table_context: Optional[TableContext] = None,
               ) -> None:
        """
        Log data quality issue to be recorded in data quality table.

        :param source: To what phase the data quality issue relates.
        :param priority: What the priority of the data quality issue is.
        :param column_name: Name of affected column in target table.
        :param dq_type: The type of data quality issue.
        :param output_row: Input row to which the data quality issue corresponds.
        Should be None for aggregate data quality issues.
        :param table_context: name of table context. Only necessary for non-row specific
        data quality issues
        """
        if output_row is not None:
            table_context = output_row.table_context
        elif table_context is None:
            raise Exception("Both `output_row` and `table_name` undefined")

        dq_table_context = self.get_dq_table_context(table_context.name)

        dq_output_row = self.get_new_row(dq_table_context.name, _dq_table=True)

        dq_output_row.update({
            "dq_rowid": str(uuid4()),
            "source": str(source.value),
            "priority": str(priority.value),
            "dq_type": str(dq_type.value),
            "column_name": column_name,
        })
        dq_row = {}
        for column in dq_table_context.table.columns:
            if output_row is None and column.name not in dq_output_row:
                # E.g. lookups don't have an output_row, and hence no specifc column
                dq_output_row[column.name] = None
            elif output_row is not None and column.name not in dq_output_row and \
                    column.name in output_row:
                dq_output_row[column.name] = output_row[column.name]
            elif column.name not in dq_output_row:
                raise Exception(f"No column `{column.name}` in output row for table `{dq_output_row.table_context.table.name}`")
            dq_row[column.name] = dq_output_row[column.name]
        self._dq_output_rows[dq_table_context.name].append(dq_row)

    def get_new_row(self, table_name: str, _dq_table: bool = False) -> OutputRow:
        """
        Returns an empty row based on the schema of the table.

        :param table_name: Name of output table. If left empty, uses default output table
        :param _dq_table: Is the row for a data quality table
        :return: An output row prepopulated with batch and etl columns.
        """
        if _dq_table:
            table_context = self.get_dq_table_context(table_name)
        else:
            table_context = self.get_table_context(table_name)
        output_row = OutputRow(table_context)
        if table_context.timestamp_column_name:
            output_row[table_context.timestamp_column_name] = datetime.utcnow()
        return output_row

    def add_source_query(self, name: str, sql: str,
                         params: Optional[Dict[str, Any]],
                         engine_context: EngineContext) -> QueryContext:
        """
        Add a query that can be iterated over with the `self.get_source_rows` method.

        :param name: reference to query when calling `get_rows()`
        :param sql: sql query with parameter values prefixed with a colon, e.g.
        `WHERE dt <= :batch_date`
        :param params: mapping between parameter keys and values, e.g.
        `{"batch_date": date(2010, 1, 1)}`
        :param engine_context: engine context used to execute the query
        :return: The generated query context instance
        """
        params = params or {}
        query_context = QueryContext(sql, params, None, engine_context)
        self._source_queries[name] = query_context
        return query_context

    def add_lookup_query(self, name: str, sql: str,
                         params: Optional[Dict[str, Any]],
                         table_context: TableContext,
                         engine_context: EngineContext) -> QueryContext:
        """
        Add a query that can `self.get_lookup(name)` method. The results are not
        eagerly populated, but rather only once they are requested the first time
        with `get_lookup`.

        :param name: reference to query when calling `get_lookup()`
        :param sql: sql query with parameter values prefixed with a colon, e.g.
        `WHERE dt <= :batch_date`
        :param params: mapping between parameter keys and values, e.g.
        `{"batch_date": date(2010, 1, 1)}`
        :param table_context: table context used for logging data quality issues
        :param engine_context: engine context used to execute the query
        :return: The generated query context instance
        """
        params = params or {}
        query_context = QueryContext(sql, params, table_context, engine_context)
        self._lookup_queries[name] = query_context
        return query_context

    def add_row(self, row: OutputRow) -> None:
        """
        Add a row to an output table.

        :param name: Name of output table.
        :param row: Row object to add to the output table.
        """
        output_row = {}
        for column in row.table_context.table.columns:
            if column.name not in row:
                raise Exception(f"No column `{column.name}` in output row for table `{row.table_context.name}`")
            output_row[column.name] = row[column.name]
        self._output_rows[row.table_context.name].append(output_row)

    def _get_source_query(self, name: str) -> QueryContext:
        """
        Retrieve a source query context. Raises exception if query context is not found
        """
        query_context = self._source_queries.get(name)
        if query_context is None:
            raise Exception(f"No source query defined: `{name}`")
        return query_context

    def _get_lookup_query(self, name: str) -> QueryContext:
        """
        Retrieve a lookup query context. Raises exception if query context is not found
        """
        query_context = self._lookup_queries.get(name)
        if query_context is None:
            raise Exception(f"No lookup query defined: `{name}`")
        return query_context

    def get_source_rows(self, name: str) -> ResultProxy:
        """
        Get results for a predefined query.

        :param name: name of query that has been added with the `self.add_source_query`
        method.

        :return: The result from `engine.execute()`
        """
        log.debug(f"Retrieving source query `{name}`")
        query_context = self._get_source_query(name)
        engine = query_context.engine_context.engine
        return engine.execute(text(query_context.sql), query_context.params)

    def get_lookup(self,
                   name: str,
                   keys: int = 1) -> Union[Set[Union[Any, Tuple[Any, ...]]],
                                           Dict[Union[Any, Tuple[Any, ...]],
                                                Union[Any, Dict[str, Any]]]]:
        """
        Get results for a predefined lookup query. The results for are cached when the
        method is called for the first time.

        :param name: name of query that has been added with the `self.add_lookup_query`
        method.
        :param keys: number of values in the lookup key. If the number of columns
        returned by the query equals `keys`, the lookup will be a set. However, if the
        number of columns returned by the query is greater than `keys`, the lookup will be
        a dict. If the number of columns is one greater than `keys`, the value of the dict
        will be `Any`. If the number of columns is two or greater than `keys`, the value of
        the dict will be a dict where the keys are the column names of the value columns,
        and the values are their respective values.

        :return: A lookup, which can be a single or
        """

        lookup = self._lookup_cache.get(name)
        if lookup is None:
            log.debug(f"Caching lookup `{name}`")
            row_count, duplicate_count = 0, 0
            query_context = self._get_lookup_query(name)
            engine = query_context.engine_context.engine
            rows = engine.execute(text(query_context.sql), query_context.params)
            column_names = [column[0] for column in rows.cursor.description]
            cursor = rows.cursor
            if keys < 1:
                raise Exception(f"A minimum of 1 key is needed for a lookup")
            elif len(column_names) < keys:
                raise Exception(f"Too few columns in lookup `name`: {len(cursor.description)} found, expected at least {keys}")
            elif len(column_names) == keys:
                log.debug(f"Creating set-based lookup `{name}`")
                lookup = set()
            else:
                log.debug(f"Creating dict-based lookup `{name}`")
                lookup = {}

            for row in rows:
                row_count += 1
                if isinstance(lookup, set):
                    # set-based lookup (only key)
                    if len(row) == 1:
                        # single key where key is `Any`
                        lookup.add(row[0])
                    else:
                        # multi-key where key is `Tuple[..., Any]`
                        lookup.add(tuple([value for value in row]))
                else:
                    # regular key-value lookup
                    if keys == 1:
                        key = row[0]
                    else:
                        key = tuple([row[i] for i in range(keys)])

                    if len(column_names) == keys + 1:
                        value = row[keys]
                    else:
                        value = {column_names[i]: row[i]
                                 for i in range(keys, len(column_names))}
                    if key in lookup:
                        duplicate_count += 1
                    else:
                        lookup[key] = value

            if duplicate_count > 0:
                log.warning(f"Query result for lookup `{name}` has {duplicate_count} duplicate keys, ignoring duplicate rows")
            log.info(f"Finished populating lookup `{name}` with {len(lookup)} rows")
            self._lookup_cache[name] = lookup
        return lookup

    def migrate_schemas(self) -> None:
        """
        Migrate all table schemas to target engines. Create new tables if missing,
        add missing columns if table exists but not all columns present.
        """
        tables_existing: List[TableContext] = []
        tables_missing: List[TableContext] = []
        all_tables = list(self._tables.values()) + list(self._dq_tables.values())

        for table_context in all_tables:
            engine = table_context.engine_context.engine
            table = table_context.table
            if engine.has_table(table.name, schema=table_context.schema):
                tables_existing.append(table_context)
            else:
                tables_missing.append(table_context)

        # create new tables
        for table_context in tables_missing:
            table = table_context.table
            log.debug(f"Create new table `{table.name}`")
            table_context.engine_context.metadata.create_all(tables=[table])

        # alter existing tables
        for table_context in tables_existing:
            table = table_context.table
            engine = table_context.engine_context.engine
            inspector = sa.inspect(engine)
            cols_existing = [col['name'] for col in inspector.get_columns(table.name)]
            for column in table.columns:
                if column.name not in cols_existing:
                    log.debug(f"Add column `{column.name}` to table `{table.name}`")
                    stmt = f'ALTER TABLE {table.name} ADD COLUMN {column.name} {str(column.type)}'
                    engine.execute(stmt)

    def truncate_rows(self) -> None:
        """
        Delete old rows from target table that match batch parameters.
        """
        table_contexts = list(self._tables.values()) + list(self._dq_tables.values())
        for table_context in table_contexts:
            table_context.engine_context.engine_spec.truncate_rows(
                table_context, self.batch_params)

    def insert_rows(self) -> None:
        """
        Insert rows into target tables.
        """
        for table_context in self._tables.values():
            output_rows = self._output_rows[table_context.name]
            engine_spec = table_context.engine_context.engine_spec
            engine_spec.insert_rows(output_rows, table_context)

        for name, table_context in self._dq_tables.items():
            output_rows = self._dq_output_rows[table_context.name]
            engine_spec = table_context.engine_context.engine_spec
            engine_spec.insert_rows(output_rows, table_context)

    def map_row(self,
                column_source: str,
                column_target: str,
                priority: DqPriority,
                row_source: RowProxy,
                row_target: OutputRow,
                default_value: Any = None,
                dq_function: Optional[Callable[[Any], Optional[DqType]]] = None) -> Any:
        """
        Perform a simple mapping from source to target. Returns the mapped value

        :param column_source: column name in source row.
        :param column_target: column name in target row.
        :param priority: Priority of data. if the value is None and is classified as
        `DqPriority.MANDATORY`, the method will raise an Exception.
        :param row_source: The source row from which to map the source column value
        :param row_target: The target row to map the column value.
        :param default_value: The default value to assign to the column if the source
        row has a `None` value.
        :param dq_function: A function that receives the column value and returns None
        if no data quality issue detected, otherwise a `DqType` enum describing the type
        of data quality issue.
        :return: The value in the source, i.e. `row_source[column_source]`
        """
        value = row_source[column_source]
        if value is None and priority == DqPriority.MANDATORY:
            raise MandatoryValueMissingException(f"Mandatory mapping from column `{column_source}` to `{column_target}` undefined")
        elif dq_function is not None:
            dq_type = dq_function(value)
            if dq_type:
                self.log_dq(source=DqSource.SOURCE,
                            priority=priority,
                            dq_type=dq_type,
                            column_name=column_target,
                            output_row=row_target)
        elif value is None:
            self.log_dq(source=DqSource.SOURCE,
                        priority=priority,
                        dq_type=DqType.MISSING,
                        column_name=column_target,
                        output_row=row_target)
        value = value if value is not None else default_value
        row_target[column_target] = value
        return value

    def execute_migration(self):
        log.debug("Start schema migrate")
        self.migrate_schemas()

    def execute_etl(self):
        log.debug(f"Start transform")
        self.transform()
        log.debug(f"Start validate")
        self.validate()
        log.debug(f"Start truncate")
        self.truncate_rows()
        log.debug(f"Start insert")
        self.insert_rows()
        log.debug(f"Finish etl")

    def execute(self):
        self.execute_migration()
        self.execute_etl()
