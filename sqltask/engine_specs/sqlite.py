from sqlalchemy.schema import Column

from sqltask.classes.table import TableContext
from sqltask.engine_specs.base import BaseEngineSpec, UploadType


class SqliteEngineSpec(BaseEngineSpec):
    engine = 'sqlite'
    default_upload_type = UploadType.SQL_INSERT
    supports_column_comments = False
    supports_table_comments = False
    supports_schemas = False

    @classmethod
    def drop_column(cls,
                    table_context: TableContext,
                    column_name: Column,
                    ) -> None:
        # TODO: sqlite doesn't support dropping columns; implement the workaround from
        # https: // www.techonthenet.com / sqlite / tables / alter_table.php
        pass
