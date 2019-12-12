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
    if table_context.name not in metadata.tables:
        metadata.reflect(only=[table_context.name])
    columns = metadata.tables[table_context.name].columns
    for row in table_context.output_rows:
        csv_row = []
        for column in columns:
            csv_row.append(row[column.name])
        csv_rows.append(csv_row)

    table = table_context.table

    epoch = str(datetime.utcnow().timestamp())
    file_path = f"{tempfile.gettempdir()}/{table.name}_{epoch}.csv"
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
