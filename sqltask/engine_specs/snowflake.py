import csv
from datetime import datetime
import os
import tempfile
from typing import Any, Dict, List

from sqltask.engine_specs.base import BaseEngineSpec
from sqltask.common import TableContext


class SnowflakeEngineSpec(BaseEngineSpec):
    engine = "snowflake"

    @classmethod
    def insert_rows(cls, output_rows: List[Dict[str, Any]],
                    table_context: TableContext) -> None:
        """
        Snowflake only supports insertin 16,384 rows at a time. This divides the output
        into max 16,384 row chunks.

        :param output_rows:
        :param table_context:
        :return:
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

        with engine.connect() as conn:
            conn.execute(f"create or replace temporary stage {table.name}")
            conn.execute(f"put file://{file_path} @{table.name}")
            conn.execute(f"copy into {table.name} from @{table.name} FILE_FORMAT = (type = 'csv' field_delimiter = '\t' skip_header = 0 empty_field_as_null = true compression = gzip) force = true")
            os.remove(f"{file_path}")
        csv_file.close()
