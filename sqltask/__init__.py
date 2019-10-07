from datetime import datetime
import logging
from typing import Any, Callable, Dict, Iterator, List, Optional
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.engine import create_engine, ResultProxy, RowProxy
from sqlalchemy.schema import Column, MetaData, Table
from sqlalchemy.sql import text
from sqlalchemy.types import DateTime, String
from sqltask.common import DqSeverity, DqSource, EngineContext, TableContext, QueryContext
from sqltask.engine_specs import get_engine_spec
from sqltask.exceptions import ExecutionArgumentException, MandatoryValueMissingException

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
        self._lookup_cache: Dict[str, Dict[str, Any]] = {}
        self._output_rows: Dict[str, List[Dict[str, Any]]] = {}
        self._dq_output_rows: Dict[str, List[Dict[str, Any]]] = {}
        self.batch_params: Dict[str, Any] = batch_params or {}

    def init_engines(self):
        raise NotImplementedError("`init_engines` method must be implemented")

    def init_schema(self):
        raise NotImplementedError("`init_schema` method must be implemented")

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
        engine_context = EngineContext(engine, metadata, schema)
        self._engines[name] = engine_context
        return engine_context

    def add_table(self,
                  name: str,
                  engine_context: EngineContext,
                  columns: List[Column],
                  schema: Optional[str] = None,
                  batch_params: Optional[Dict[str, Any]] = None,
                  rowid_column_name: Optional[str] = None,
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
        :param rowid_column_name: Name of column used for populating row id.
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
        table_context = TableContext(table, engine_context, batch_params,
                                     rowid_column_name, timestamp_column_name, schema)
        self._tables[name] = table_context

        dq_table_name = dq_table_name or name + "_dq"
        dq_engine_context = dq_engine_context or engine_context
        dq_rowid_column_name = rowid_column_name or "_rowid"
        dq_timestamp_column_name = timestamp_column_name or "_rowid"

        column_dict = {column.name: column for column in columns}
        batch_columns = [column for column in column_dict.values()]

        dq_columns = batch_columns + [
            Column(dq_rowid_column_name, String, comment="Built-in row id", nullable=False),
            Column(dq_timestamp_column_name, DateTime, comment="Timestamp when row was created", nullable=False),
            Column('source', String, comment="Source of issue", nullable=False),
            Column('severity', String, comment="Severity of issue", nullable=False),
            Column('message', String, comment="Verbose description of issue", nullable=False),
        ]
        dq_table = Table(dq_table_name, dq_engine_context, *dq_columns, **kwargs)
        dq_schema = dq_schema or schema
        dq_table_context = TableContext(dq_table, dq_engine_context, batch_params,
                                        dq_rowid_column_name, dq_timestamp_column_name,
                                        dq_schema)
        self._dq_tables[name] = dq_table_context
        return table_context

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
               severity: DqSeverity,
               message: Optional[str] = None,
               row: Optional[Dict[str, Any]] = None,
               ) -> None:
        """
        Log data quality issue to be recorded in data quality table.

        :param source: To what phase the data quality issue relates.
        :param severity: What the severity of the data quality issue is.
        :param message: Verbose message describing the data quality issue.
        :param row: Input row to which the data quality issue corresponds. Should be
        None for aggregate data quality issues.
        """
        dq_row = self.get_new_row()

        if row:
            dq_row["_rowid"] = row["_rowid"]
        dq_row.update({"source": str(source.value),
                       "severity": str(severity.value),
                       "message": message})
        self._get_output_rows(self._get_dq_table_name()).append(dq_row)

    def get_new_row(self) -> Dict[str, Any]:
        """
        Returns an empty row based on the schema of the table.

        :param name: Name of output table. If left empty, uses default output table
        :return: A `DataFrame` with one row with all None values.
        """
        row = {
            '_rowid': uuid4(),
            '_tstamp': datetime.utcnow()
        }
        row.update(self.params)
        return row

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
        query_context = QueryContext(sql, params, engine_context)
        self._source_queries[name] = query_context
        return query_context

    def add_lookup_query(self, name: str, sql: str,
                         params: Optional[Dict[str, Any]],
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
        :param engine_context: engine context used to execute the query
        """
        query_context = QueryContext(sql, params, engine_context)
        self._lookup_queries[name] = query_context
        return query_context

    def add_row(self, row: Dict[str, Any], name: Optional[str] = None,
                engine: Optional[str] = None) -> None:
        """
        Add a row to an output table.

        :param row: Row object to add to the output table
        :param name: Name of output table. None defaults to default output table
        :param engine:
        :return:
        """
        # if name is None, default to main table
        if name is None:
            name = self._main_table_name
        table_spec = self._table_specs[name]
        output_rows = self._get_output_rows(name)

        output_row = {}
        for key, value in row.items():
            if key not in table_spec.table.columns:
                raise Exception(f"No column `{key}` in table `{name or '<default>'}` for engine `{engine or '<default>'}`")
            output_row[key] = value
        output_rows.append(output_row)

    def get_source_rows(self, name: str) -> ResultProxy:
        """
        Get results for a predefined query.

        :param name: name of query that has been added with the `self.add_source_query`
        method.

        :return: The result from `engine.execute()`
        """
        query_context = self._source_queries.get(name)
        engine = query_context.engine_context.engine
        return engine.engine.execute(text(query_context.sql), query_context.params)

    def get_lookup(self, name: str) -> Dict[Any, Any]:
        """
        Get results for a predefined lookup query. The results for are cached when the
        method is called for the first time.

        :param name: name of query that has been added with the `self.add_lookup_query`
        method.

        :return: Result
        """
        lookup = self._lookup_cache.get(name)
        if lookup is None:
            query_context = self._lookup_queries.get(name)
            engine = query_context.engine_context.engine
            rows = engine.execute(text(query_context.sql), query_context.params)
            lookup = {}
            for row in rows:
                if len(row) < 2:
                    raise Exception(f"Lookup `{name}` must contain at least 2 columns")
                elif len(row) == 2:
                    key, value = row[0], row[1]
                else:
                    key = row[0]
                    value = []
                    for col in row[1:]:
                        value.append(col)
                    value = tuple(value)
                if key in lookup:
                    self.log_dq(DqSource.LOOKUP, DqSeverity.MEDIUM,
                                f"Duplicate key `{key}` in lookup `{name}`")
                lookup[key] = value
            self._lookup_cache[name] = lookup
        return lookup

    def migrate_schemas(self) -> None:
        """
        Migrate all table schemas to target engines. Create new tables if missing,
        add missing columns if table exists but not all columns present.
        """
        tables_existing: List[Table] = []
        tables_missing: List[Table] = []

        for table_spec in self._table_specs.values():
            table = table_spec.table
            if table.bind.has_table(table.name):
                tables_existing.append(table)
            else:
                tables_missing.append(table)

        # create new tables
        for table in tables_missing:
            table.metadata.create_all(tables=[table])

        # alter existing tables
        for table in tables_existing:
            inspector = sa.inspect(table.bind)
            cols_existing = [col['name'] for col in inspector.get_columns(table.name)]
            for column in table.columns:
                if column.name not in cols_existing:
                    print(f'{column.name} is missing')
                    stmt = f'ALTER TABLE {table.name} ADD COLUMN {column.name} {str(column.type)}'
                    table.bind.execute(stmt)

    def truncate_rows(self) -> None:
        """
        Delete old rows from target table that match batch parameters.
        """
        for table_spec in self._table_specs.values():
            table = table_spec.table
            where_clause = " AND ".join([f"{col.name} = :{col.name}" for col in self.batch_columns])
            stmt = f"DELETE FROM {table.name} WHERE {where_clause}"
            table.bind.execute(text(stmt), self.params)

    def insert_rows(self) -> None:
        """
        Insert rows into target tables.
        """
        for name, table_spec in self._table_specs.items():
            table = table_spec.table
            engine_spec = engines[table.bind.name if table.bind.name in engines else None]
            engine_spec.insert_rows(self._get_output_rows(name), table)

    def get_sql_rows(self, sql: str, params: Dict[str, Any],
                     engine_name: Optional[str] = None) -> Iterator[RowProxy]:
        """
        Get results for a sql query.

        :param sql: Query that returns a result set.
        :param params: Execution parameters to be inserted into the query.
        :param engine_name: Name of engine to execute the query.
        :return: A single row from the result set (yield).
        """
        params = params or {}
        engine_schema = self._get_engine_spec(engine_name)
        rows = engine_schema.engine.execute(text(sql), params)
        for row in rows:
            yield row

    def map_row(self, column_source: str, column_target: str,
                data_severity: DqSeverity, row_source: RowProxy,
                row_target: Dict[str, Any],
                dq_function: Optional[Callable[[Any], bool]] = None) -> Any:
        """
        Perform a simple mapping from source to target. Returns the mapped value

        :param column_source: column name in source row.
        :param column_target: column name in target row.
        :param data_severity:
        :param row_source:
        :param row_target:
        :param dq_function:
        :return: The value in the source, i.e. `row_source[column_source]`
        """
        value = row_source[column_source]
        if value is None and data_severity == DqSeverity.MANDATORY:
            raise MandatoryValueMissingException(f"Mandatory mapping from column `{column_source}` to `{column_target}` undefined")
        elif value is None:
            self.log_dq(DqSource.SOURCE, data_severity, f"Mapping from column `{column_source}` to `{column_target}` undefined", row_target)
        row_target[column_target] = value
        return value

    def execute_migration(self):
        print("migration start: " + str(datetime.now()))
        self.init_engines()
        self.init_schema()
        self.init_dq_schema()
        self.migrate_schemas()
        print("migration start: " + str(datetime.now()))

    def execute_etl(self):
        print("transform start: " + str(datetime.now()))
        self.transform()
        self.validate()
        self.truncate_rows()
        self.insert_rows()
        print("transform end" + str(datetime.now()))

    def execute(self):
        self.execute_migration()
        self.execute_etl()
