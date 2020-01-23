import logging
from typing import Any, Dict

from sqltask.base.lookup_source import BaseLookupSource
from sqltask.base.row_source import BaseRowSource
from sqltask.base.table import BaseOutputRow, BaseTableContext

# initialize logging
logger = logging.getLogger(__name__)


class SqlTask:
    def __init__(self, **batch_params):
        """
        Main class in library.

        :param batch_params: Mapping from batch column name to value
        """
        self._tables: Dict[str, BaseTableContext] = {}
        self._row_sources: Dict[str, BaseRowSource] = {}
        self._lookups: Dict[str, BaseLookupSource] = {}
        self.batch_params: Dict[str, Any] = batch_params or {}

    def add_table(self, table_context: BaseTableContext) -> None:
        """
        Add a table schema.

        :param table_context: a table context to be added to the task
        """
        if table_context.name is None:
            raise Exception("Cannot add table with undefined name.")
        self._tables[table_context.name] = table_context

    def get_table_context(self, name: str) -> BaseTableContext:
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
        pass

    def validate(self) -> None:
        """
        Abstract validation method that is executed after transformation is completed.
        Should be implemented to validate aggregate measures that can't be validated
        during transformation.
        """
        pass

    def get_new_row(self, name: str) -> BaseOutputRow:
        """
        Returns an empty row based on the schema of the table.

        :param name: Name of output table.
        :return: An output row prepopulated with batch and etl columns.
        """
        table_context = self._tables.get(name)
        if table_context is None:
            raise Exception(f"Undefined table context: `{name}`")
        return table_context.get_new_row()

    def add_row_source(self, row_source: BaseRowSource) -> None:
        """
        Add a data source that can be iterated over.

        :param row_source: an instance of base class BaseDataSource
        """
        if row_source.name is None:
            raise Exception("Cannot add data source with undefined name")
        self._row_sources[row_source.name] = row_source

    def add_lookup_source(self, lookup_source: BaseLookupSource) -> None:
        """
        Add a data source that can be iterated over.

        :param lookup_source: an instance of base class LookupSource
        """
        if lookup_source.name is None:
            raise Exception("Cannot add data source with undefined name")
        self._lookups[lookup_source.name] = lookup_source

    def get_row_source(self, name: str) -> BaseRowSource:
        """
        Get results for a predefined query.

        :param name: name of query that has been added with the `self.add_source_query`
        method.

        :return: The DataSource instance that can be iterated over
        """
        logger.debug(f"Retrieving source query `{name}`")
        row_source = self._row_sources.get(name)
        if row_source is None:
            raise Exception(f"Data source `{row_source}` not found")
        return row_source

    def get_lookup_source(self, name: str) -> BaseLookupSource:
        """
        Get results for a predefined lookup query. The results for are cached when the
        method is called for the first time.

        :param name: name of query that has been added with the `self.add_lookup_query`
        method.

        :return: A lookup, which can be a single or
        """
        lookup = self._lookups.get(name)
        if lookup is None:
            raise Exception(f"Lookup `{name}` not found")
        return lookup

    def insert_rows(self) -> None:
        """
        Insert rows in target tables.
        """
        for table_context in self._tables.values():
            table_context.insert_rows()

    def post_insert(self) -> None:
        """
        Optional step to execute after insertion is completed. Usually used to execute
        sql statements that don't require row-by-row transformation
        """
        pass

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
        logger.debug("Start schema migrate")
        self.migrate_schemas()

    def execute_etl(self):
        logger.debug(f"Start transform")
        self.transform()
        logger.debug(f"Start validate")
        self.validate()
        logger.debug(f"Start delete")
        self.delete_rows()
        logger.debug(f"Start insert")
        self.insert_rows()
        logger.debug(f"Start post insert")
        self.post_insert()
        logger.debug(f"Finish etl")

    def execute(self):
        self.execute_migration()
        self.execute_etl()
