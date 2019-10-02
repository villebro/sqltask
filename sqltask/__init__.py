from datetime import datetime
import logging
from typing import Any, Callable, Dict, Iterator, List, Optional
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.engine import create_engine, ResultProxy, RowProxy
from sqlalchemy.schema import Column, MetaData, Table
from sqlalchemy.sql import text
from sqlalchemy.types import DateTime, String
from sqltask.common import DqSeverity, DqSource, EngineSpec, TableSpec, QuerySpec
from sqltask.engine_specs import engines
from sqltask.exceptions import ExecutionArgumentException, MandatoryValueMissingException

class SqlTask:
    _main_table_name: Optional[str] = None
    _engine_specs: Dict[Optional[str], EngineSpec] = {}
    _table_specs: Dict[str, TableSpec] = {}
    execution_columns: Optional[List[Column]] = None

    def __init__(self, **params):
        self._queries: Dict[str, QuerySpec] = {}
        self._lookups: Dict[str, Dict[str, Any]] = {}
        self._lookup_queries: Dict[str, QuerySpec] = {}
        self.output_rows: Dict[str, List[Dict[str, Any]]] = {}
        execution_column_names = [column.name for column in self.execution_columns]
        for col in execution_column_names:
            if col not in params:
                raise ExecutionArgumentException(f"Execution argument undefined: `{col}`")
        for arg in params:
            if arg not in execution_column_names:
                raise ExecutionArgumentException(f"Execution argument unsupported: `{arg}`")
        self.params: Dict[str, Any] = params

    @classmethod
    def init_engines(cls):
        raise NotImplementedError("`init_engines` method must be implemented")

    @classmethod
    def init_schema(cls):
        raise NotImplementedError("`init_schema` method must be implemented")

    @classmethod
    def _get_dq_table_name(cls) -> str:
        """
        Get data quality table name. Defaults to main table with `_dq` suffix.
        """
        return cls._table_specs[cls._main_table_name].table.name + "_dq"

    def _get_output_rows(self, name: Optional[str]) -> List[Dict[str, Any]]:
        """
        Get the output list for a specific table.

        :param name: name of table to output to. Defaults to main table if None.
        :return: List of data quality rows
        """
        name = name if name else self._main_table_name
        output_rows = self.output_rows.get(name)
        if output_rows is None:
            output_rows = []
            self.output_rows[name] = output_rows
        return output_rows

    @classmethod
    def init_dq_schema(cls) -> None:
        """
        Initialize data quality table schema.
        """
        columns_default = [
            Column('_rowid', String, comment="Built-in row id", nullable=False),
            Column('_tstamp', DateTime, comment="Timestamp when row was created", nullable=False),
            Column('source', String, comment="Source of issue", nullable=False),
            Column('severity', String, comment="Severity of issue", nullable=False),
            Column('message', String, comment="Verbose description of issue", nullable=False),
        ]
        columns_execution = [column.copy() for column in cls.execution_columns]
        columns = columns_default + columns_execution

        table_name = cls._get_dq_table_name()
        engine_spec = cls._get_engine_spec()
        table = Table(table_name, engine_spec.metadata, *columns,
                      comment="The data quality table",
                      snowflake_clusterby=[col.name for col in columns_execution])
        table_spec = TableSpec(engine_name=engine_spec.name, table=table)
        cls._table_specs[table_name] = table_spec

    @classmethod
    def transform(cls) -> None:
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

    @classmethod
    def _get_engine_spec(cls, engine_name: Optional[str] = None) -> EngineSpec:
        """

        :param engine_name:
        :return:
        """
        if engine_name is None:
            table_spec = cls._table_specs[cls._main_table_name]
            engine_spec = cls._get_engine_spec(table_spec.engine_name)
            engine_name = engine_spec.name
        engine_spec = cls._engine_specs.get(engine_name)
        if engine_spec is None:
            raise Exception(f"Engine not created: `{engine_name or '<default>'}`")
        return engine_spec

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
                         engine_name: str) -> None:
        """
        Add a query that can be iterated over with the `self.get_source_rows` method.

        :param name: reference to query when calling `get_rows()`
        :param sql: sql query with parameter values prefixed with a colon, e.g.
        `WHERE dt <= :batch_date`
        :param params: mapping between parameter keys and values, e.g.
        `{"batch_date": date(2010, 1, 1)}`
        :param engine_name: name of engine used to execute the query
        """
        self._queries[name] = QuerySpec(sql, params, engine_name)

    def add_lookup_query(self, name: str, sql: str,
                         params: Optional[Dict[str, Any]],
                         engine_name: str) -> None:
        """
        Add a query that can `self.get_lookup(name)` method. The results are not
        eagerly populated, but rather only once they are requested the first time
        with `get_lookup`.

        :param name: reference to query when calling `get_lookup()`
        :param sql: sql query with parameter values prefixed with a colon, e.g.
        `WHERE dt <= :batch_date`
        :param params: mapping between parameter keys and values, e.g.
        `{"batch_date": date(2010, 1, 1)}`
        :param engine_name: name of engine used to execute the query
        """
        self._lookup_queries[name] = QuerySpec(sql, params, engine_name)

    def get_source_rows(self, name: str) -> ResultProxy:
        """
        Get results for a predefined query.

        :param name: name of query that has been added with the `self.add_source_query`
        method.

        :return: The result from `engine.execute()`
        """
        query_spec = self._queries.get(name)
        engine = self._get_engine_spec(query_spec.engine_name)
        return engine.engine.execute(text(query_spec.sql), query_spec.params)

    def get_lookup(self, name: str) -> Dict[Any, Any]:
        """
        Get results for a predefined lookup query.

        :param name: name of query that has been added with the `self.add_lookup_query`
        method.

        :return: Result
        """
        lookup = self._lookups.get(name)
        if lookup is None:
            query_spec = self._lookup_queries.get(name)
            engine_spec = self._get_engine_spec(query_spec.engine_name)
            rows = engine_spec.engine.execute(text(query_spec.sql), query_spec.params)
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
            self._lookups[name] = lookup
        return lookup

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

    @classmethod
    def add_engine(cls, name: str, url: str, **kwargs) -> None:
        """
        Add a new engine to be used by sources, sinks and lookups.

        :param url: SqlAlchemy URL of engine.
        :param name: alias by which the engine is referenced during during operations.
        :param kwargs: additional parameters to be passed to metadata object.
        """
        engine = create_engine(url)
        metadata = MetaData(bind=engine, **kwargs)
        cls._engine_specs[name] = EngineSpec(name, engine, metadata)

    @classmethod
    def add_table(cls, name: str, columns: List[Column], engine_name: str,
                  **kwargs) -> None:
        """
        Add a table schema.

        :param name: Name of target table in database.
        :param columns: Additional columns beyond default and execution columns.
        :param engine_name: Name of engine to bind table to.
        :param kwargs: Additional parameters to pass to Table constructor
        """
        engine_spec = cls._get_engine_spec(engine_name)
        columns_default = [
            Column("_rowid", String, comment="Built-in row id", nullable=False),
            Column("_tstamp", DateTime, comment="Timestamp when row was created", nullable=False),
        ]
        columns_execution = [column.copy() for column in cls.execution_columns]
        all_columns = columns_default + columns_execution + columns
        table = Table(name, engine_spec.metadata, *all_columns, **kwargs)
        cls._main_table_name = name
        table_spec = TableSpec(engine_name=engine_name, table=table)
        cls._table_specs[name] = table_spec

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
        Delete old rows from target table that match execution parameters.
        """
        for table_spec in self._table_specs.values():
            table = table_spec.table
            where_clause = " AND ".join([f"{col.name} = :{col.name}" for col in self.execution_columns])
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

    def execute(self):
        print(1)
        self.init_engines()
        print(2)
        self.init_schema()
        print(3)
        self.init_dq_schema()
        print(4)
        self.migrate_schemas()
        print(5)
        self.transform()
        print(6)
        self.validate()
        print(7)
        self.truncate_rows()
        print("8" + str(datetime.now()))
        self.insert_rows()
        print("9" + str(datetime.now()))
