import os

from sqltask.classes.table import TableContext
from sqltask.engine_specs.base import BaseEngineSpec, UploadType
from sqltask.utils.engine_specs import create_tmp_csv


class MssqlEngineSpec(BaseEngineSpec):
    engine = 'mssql'
    supported_uploads = (UploadType.SQL_INSERT,
                         UploadType.SQL_INSERT_MULTIROW,
                         )
    default_upload_type = UploadType.SQL_INSERT_MULTIROW
    supports_column_comments = True
    supports_table_comments = True
    supports_schemas = True

    @classmethod
    def _insert_rows_csv(cls, table_context: TableContext) -> None:
        """
        MSSQL bulk loading is done by exporting the data to CSV and using the
        cursor `copy_from` method.

        :param table_context: the target table to upload into
        """
        file_path = create_tmp_csv(table_context)
#        engine = table_context.engine_context.engine
#        columns = [column.name for column in table_context.table.columns]

        with table_context.engine_context.engine.begin() as conn:
            stmt = f"BULK INSERT {table_context.table.name} FROM '{file_path}' " \
                   f"WITH (FIRSTROW = 2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '\n')"
            conn.execute(stmt)
        os.remove(f"{file_path}")
