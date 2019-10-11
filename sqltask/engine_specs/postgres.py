import csv
from datetime import datetime
import os
import tempfile
from typing import Any, Dict, List

from sqltask.engine_specs.base import BaseEngineSpec
from sqltask.common import TableContext


class PostgresEngineSpec(BaseEngineSpec):
    engine = 'postgres'
    supports_column_comments = True
    supports_table_comments = True
    supports_schemas = True

    @classmethod
    def insert_rows(cls, output_rows: List[Dict[str, Any]],
                    table_context: TableContext) -> None:
        """
        Postgres bulk loading is done by exporting the data to CSV and using the
        cursor `copy_from` method.

        :param output_rows: rows to upload
        :param table_context: the target table to upload into
        """
        csv_rows = []
        table = table_context.table
        engine = table_context.engine_context.engine
        for row in output_rows:
            csv_row = []
            for column in table.columns:
                csv_row.append(row[column.name])
            csv_rows.append(csv_row)

        epoch = str(datetime.utcnow().timestamp())
        file_path = f"{tempfile.gettempdir()}/{table.name}_{epoch}.csv"

        with open(file_path, 'w', encoding="utf-8", newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter="\t")
            writer.writerows(csv_rows)

        columns = [column.name for column in table_context.table.columns]

        with engine.connect() as conn:
            cursor = conn.cursor()
            cursor.copy_from(csv_file, table_context.table, columns=columns)
            os.remove(f"{file_path}")
        csv_file.close()
