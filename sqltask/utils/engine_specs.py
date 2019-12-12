import csv
import logging
import tempfile
from datetime import datetime

from sqltask.base.table import BaseTableContext

logger = logging.getLogger(__name__)


def create_tmp_csv(table_context: BaseTableContext, delimiter: str = "\t") -> str:
    """
    Creates a temporary csv file based on `output_rows`.

    :param table_context: Table context based on which the csv columns will be based.
    :param delimiter: Delmiter to use when exporting csv.
    :return: the path of the created temporary csv file.
    """
    csv_rows = []
    metadata = table_context.engine_context.metadata
    # the reason we want to reflect the table schema is because the columns might
    # be in a different order than in the table context definition. And since
    # some engines have the fully qualified table name as key (schema.table) in the tables
    # dict, while others only have the table name. Therefore, we need to loop through
    # the tables until we find the correct table. Slightly hackish.
    metadata.reflect(only=[table_context.name], schema=table_context.schema)
    target_table = None
    for table in metadata.tables.values():
        if table.name == table_context.name:
            target_table = table

    if target_table is None:
        raise Exception(f"Table {target_table} not found in schema despite reflection.")

    for row in table_context.output_rows:
        csv_row = []
        for column in target_table.columns:
            csv_row.append(row[column.name])
        csv_rows.append(csv_row)

    epoch = str(datetime.utcnow().timestamp())
    file_path = f"{tempfile.gettempdir()}/{target_table.name}_{epoch}.csv"
    logger.info(f"Creating temporary file `{file_path}`")

    with open(file_path, 'w', encoding="utf-8", newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=delimiter)
        writer.writerows(csv_rows)
    csv_file.close()
    return file_path


def get_escaped_string_value(value: str) -> str:
    """
    Escapes a string to be used in a sql expression

    :param value: string value to be escaped
    :return: escaped string value
    """
    return value.replace("'", "\\'")
