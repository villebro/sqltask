import logging
import os
from typing import Any, Dict

from sqltask.classes.common import BaseDataSource, Lookup
from sqltask.classes.sql import LookupSource
from sqltask.classes.table import OutputRow, TableContext

__version__ = '0.3.2'

# initialize logging
log = logging.getLogger('sqltask')
log_level = os.getenv("SQLTASK_LOG_LEVEL")
if log_level:
    log.setLevel(log_level)


class SqlTask:
    def __init__(self, **batch_params):
        """
        Main class in library.

        :param batch_params: Mapping from batch column name to value
        """
        self._tables: Dict[str, TableContext] = {}
        self._data_sources: Dict[str, BaseDataSource] = {}
        self._lookup_sources: Dict[str, LookupSource] = {}
        self.batch_params: Dict[str, Any] = batch_params or {}

    def add_table(self, table_context: TableContext) -> None:
        """
        Add a table schema.

        :param table_context: a table context to be added to the task
        """
        self._tables[table_context.name] = table_context

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

    def get_new_row(self, name: str) -> OutputRow:
        """
        Returns an empty row based on the schema of the table.

        :param name: Name of output table.
        :return: An output row prepopulated with batch and etl columns.
        """
        table_context = self._tables.get(name)
        if table_context is None:
            raise Exception(f"Undefined table context: `{name}`")
        return OutputRow(table_context)

    def add_data_source(self, data_source: BaseDataSource) -> None:
        """
        Add a data source that can be iterated over.

        :param data_source: an instance of base class BaseDataSource
        """
        self._data_sources[data_source.name] = data_source

    def add_lookup_source(self, lookup_source: LookupSource) -> None:
        """
        Add a data source that can be iterated over.

        :param lookup_source: an instance of base class LookupSource
        """
        self._lookup_sources[lookup_source.name] = lookup_source

    def get_data_source(self, name: str) -> BaseDataSource:
        """
        Get results for a predefined query.

        :param name: name of query that has been added with the `self.add_source_query`
        method.

        :return: The DataSource instance that can be iterated over
        """
        log.debug(f"Retrieving source query `{name}`")
        data_source = self._data_sources.get(name)
        if data_source is None:
            raise Exception(f"Data source `{data_source}` not found")
        return data_source

    def get_lookup(self, name: str) -> Lookup:
        """
        Get results for a predefined lookup query. The results for are cached when the
        method is called for the first time.

        :param name: name of query that has been added with the `self.add_lookup_query`
        method.

        :return: A lookup, which can be a single or
        """
        lookup = self._lookup_sources.get(name)
        if lookup is None:
            raise Exception(f"Lookup `{name}` not found")
        return lookup.get_lookup()

    def insert_rows(self) -> None:
        """
        Insert rows in target tables.
        """
        for table_context in self._tables.values():
            table_context.insert_rows()

    def delete_rows(self) -> None:
        """
        Delete rows in target tables.
        """
        for table_context in self._tables.values():
            table_context.delete_rows()

    def migrate_schemas(self) -> None:
        """
        Migrate all table schemas to target engines. Create new tables if missing,
        add missing columns if table exists but not all columns present.
        """
        for table_context in self._tables.values():
            table_context.migrate_schema()

    def execute_migration(self):
        log.debug("Start schema migrate")
        self.migrate_schemas()

    def execute_etl(self):
        log.debug(f"Start transform")
        self.transform()
        log.debug(f"Start validate")
        self.validate()
        log.debug(f"Start delete")
        self.delete_rows()
        log.debug(f"Start insert")
        self.insert_rows()
        log.debug(f"Finish etl")

    def execute(self):
        self.execute_migration()
        self.execute_etl()
