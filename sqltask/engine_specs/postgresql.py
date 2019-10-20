import os
from typing import Any, Dict, List

from sqltask.engine_specs.base import BaseEngineSpec, UploadType
from sqltask.classes.common import TableContext
from sqltask.utils.engine_specs import create_tmp_csv


class PostgresEngineSpec(BaseEngineSpec):
    engine = 'postgresql'
    supported_uploads = (UploadType.SQL_INSERT,
                         UploadType.SQL_INSERT_MULTIROW,
                         UploadType.CSV,
                         )
    default_upload_type = UploadType.CSV
    supports_column_comments = True
    supports_table_comments = True
    supports_schemas = True

    @classmethod
    def _insert_rows_csv(cls, output_rows: List[Dict[str, Any]],
                         table_context: TableContext) -> None:
        """
        Postgres bulk loading is done by exporting the data to CSV and using the
        cursor `copy_from` method.

        :param output_rows: rows to upload
        :param table_context: the target table to upload into
        """
        file_path = create_tmp_csv(table_context, output_rows)
        engine = table_context.engine_context.engine

        columns = [column.name for column in table_context.table.columns]

        engine.raw_connection()
        try:
            with engine.connect() as conn:
                cursor = conn.connection.cursor()
                with open(file_path, 'r', encoding="utf-8", newline='') as csv_file:
                    cursor.copy_from(file=csv_file,
                                     table=table_context.table.name,
                                     columns=columns,
                                     null="")
                    csv_file.close()
                    conn.connection.commit()
        finally:
            os.remove(f"{file_path}")
