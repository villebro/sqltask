import csv
from datetime import datetime
import os
import tempfile
from typing import Any, Dict, List

from sqltask.engine_specs.base import BaseEngineSpec
from sqltask.common import TableContext
from sqltask.utils.engine_specs import create_tmp_csv


class SnowflakeEngineSpec(BaseEngineSpec):
    engine = "snowflake"

    @classmethod
    def insert_rows(cls, output_rows: List[Dict[str, Any]],
                    table_context: TableContext) -> None:
        """
        Snowflake bulk loading is done by exporting the data to CSV and using the
        PUT + COPY statement to upload the data.

        :param output_rows: rows to upload
        :param table_context: the target table to upload into
        """
        file_path = create_tmp_csv(table_context, output_rows)
        table = table_context.table
        engine = table_context.engine_context.engine

        try:
            with engine.connect() as conn:
                conn.execute(f"CREATE OR REPLACE TEMPORARY STAGE {table.name}")
                conn.execute(f"PUT FILE://{file_path} @{table.name}")
                conn.execute(f"COPY INTO {table.name} FROM @{table.name} FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '\t' SKIP_HEADER = 0 EMPTY_FIELD_AS_NULL = TRUE COMPRESSION = GZIP) FORCE = TRUE")
        finally:
            os.remove(f"{file_path}")
