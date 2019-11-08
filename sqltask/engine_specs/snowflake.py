import os

from sqltask.base.table import BaseTableContext
from sqltask.engine_specs.base import BaseEngineSpec, UploadType
from sqltask.utils.engine_specs import create_tmp_csv


class SnowflakeEngineSpec(BaseEngineSpec):
    engine = "snowflake"
    supported_uploads = {
        UploadType.SQL_INSERT,
        UploadType.CSV,
    }
    default_upload_type = UploadType.CSV
    supports_table_comments = False

    @classmethod
    def _insert_rows_csv(cls, table_context: BaseTableContext) -> None:
        """
        Snowflake bulk loading is done by exporting the data to CSV and using the
        PUT + COPY statement to upload the data.

        :param table_context: the target table to upload into
        """
        file_path = create_tmp_csv(table_context)
        table = table_context.table
        engine = table_context.engine_context.engine

        try:
            with engine.connect() as conn:
                conn.execute(f"CREATE OR REPLACE TEMPORARY STAGE {table.name}")
                conn.execute(f"PUT file://{file_path} @{table.name}")
                conn.execute(f"COPY INTO {table.name} FROM @{table.name} "
                             f"FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '\t' "
                             f"SKIP_HEADER = 0 EMPTY_FIELD_AS_NULL = TRUE "
                             f"COMPRESSION = GZIP) FORCE = TRUE")
        finally:
            os.remove(f"{file_path}")
