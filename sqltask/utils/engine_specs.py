import csv
from datetime import datetime
import logging
import tempfile
from typing import Any, Dict,  List

from sqltask.common import TableContext


def create_tmp_csv(table_context: TableContext,
                   output_rows: List[Dict[str, Any]]) -> str:
    """
    Creates a temporary csv file based on `output_rows`.

    :param table_context:
    :param output_rows:
    :return: the path of the created temporary csv file.
    """
    csv_rows = []
    table = table_context.table
    for row in output_rows:
        csv_row = []
        for column in table.columns:
            csv_row.append(row[column.name])
        csv_rows.append(csv_row)

    table = table_context.table
    epoch = str(datetime.utcnow().timestamp())
    file_path = f"{tempfile.gettempdir()}/{table.name}_{epoch}.csv"
    logging.info(f"Creating temporary file `{file_path}`")

    with open(file_path, 'w', encoding="utf-8", newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter="\t")
        writer.writerows(csv_rows)
    csv_file.close()
    return file_path