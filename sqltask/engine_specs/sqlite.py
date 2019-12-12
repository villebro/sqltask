import logging

from sqlalchemy.schema import Column

from sqltask.base.table import BaseTableContext
from sqltask.engine_specs.base import BaseEngineSpec, UploadType


class SqliteEngineSpec(BaseEngineSpec):
    engine = 'sqlite'
    supported_uploads = {UploadType.SQL_INSERT}
    default_upload_type = UploadType.SQL_INSERT
    supports_column_comments = False
    supports_primary_keys = False
    supports_table_comments = False
    supports_schemas = False

    @classmethod
    def drop_column(cls,
                    table_context: BaseTableContext,
                    column_name: Column,
                    ) -> None:
        """
        On sqlite, columns must be dropped by renaming the old table, creating a new
        table and finally selecting values from the old table to the new one.

        :param table_context: Table context whose table to alter
        :param column_name: column to drop from table scchema
        """
        # TODO: this drops all unnecessary columns at once; no need to loop all cols
        engine = table_context.engine_context.engine
        metadata = table_context.engine_context.metadata
        table_name = table_context.table.name

        logging.debug(f"Drop column `{column_name}` from table `{table_name}`")
        engine.execute(f'ALTER TABLE {table_name} RENAME TO _{table_name}_old')
        metadata.create_all(tables=[table_context.table])
        cols = [column.name for column in table_context.table.columns]
        cols_select = ", ".join(cols)
        table_context.engine_context.engine.execute(
            f"INSERT INTO {table_name} ({cols_select}) "
            f"SELECT {cols_select} "
            f"FROM _{table_name}_old"
        )
        table_context.engine_context.engine.execute(
            f"DROP TABLE _{table_name}_old"
        )
