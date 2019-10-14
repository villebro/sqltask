from typing import List, Dict, Any

from sqltask import TableContext
from sqltask.engine_specs.base import BaseEngineSpec, UploadType


class SqliteEngineSpec(BaseEngineSpec):
    engine = 'sqlite'
    default_upload_type = UploadType.SQL_INSERT
    supports_column_comments = False
    supports_table_comments = False
    supports_schemas = False
