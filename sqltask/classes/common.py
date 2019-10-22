from uuid import uuid4

from collections import UserDict
from datetime import datetime
from typing import (
    Any,
    Dict,
    List,
    NamedTuple,
    Optional,
    Tuple,
    TYPE_CHECKING,
    Union
)

from sqlalchemy.schema import Table

from sqltask.classes import dq
if TYPE_CHECKING:
    from sqltask.classes.context import EngineContext


class TableContext:
    def __init__(
            self,
            name: str,
            table: Table,
            engine_context: "EngineContext",
            batch_params: Optional[Dict[str, Any]],
            info_column_names: Optional[List[str]],
            timestamp_column_name: Optional[str],
            schema: Optional[str],
            output_rows: List[Dict[str, Any]],
            dq_table_context: Optional["TableContext"] = None,
    ):
        self.name = name
        self.table = table
        self.engine_context = engine_context
        self.batch_params = batch_params
        self.info_column_names = info_column_names
        self.timestamp_column_name = timestamp_column_name
        self.schema = schema
        self.output_rows = output_rows
        self.dq_table_context = dq_table_context

    def get_new_row(self) -> "OutputRow":
        output_row = OutputRow(self)
        if self.timestamp_column_name:
            output_row[self.timestamp_column_name] = datetime.utcnow()
        return output_row


class QueryContext(NamedTuple):
    sql: str
    params: Dict[str, Any]
    table_context: Optional[TableContext]
    engine_context: "EngineContext"


class OutputRow(UserDict):
    def __init__(self, table_context: TableContext):
        super().__init__(table_context.batch_params)
        self.table_context = table_context

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
            raise Exception(f"Column `{column_name}` not in table `{self.table_context.table.name}`")

        dq_table_context = self.table_context.dq_table_context
        dq_output_row = dq_table_context.get_new_row()

        dq_output_row.update({
            "rowid": str(uuid4()),
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
                raise Exception(f"No column `{column.name}` in output row for table `{dq_output_row.table_context.table.name}`")
            dq_row[column.name] = dq_output_row[column.name]
        dq_table_context.output_rows.append(dq_row)


class BaseDataSource:
    def __init__(self, name: str):
        self.name = name

    def __iter__(self) -> "Lookup":
        raise NotImplementedError("`__iter__` not implemented")


class Lookup(UserDict):
    def __init__(self, data_source: BaseDataSource,  kv: Union[Dict, Tuple]):
        super().__init__(kv)
        self.data_source = data_source
