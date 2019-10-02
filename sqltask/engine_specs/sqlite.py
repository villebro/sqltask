from sqltask.engine_specs.base import BaseEngineSpec


class SqliteEngineSpec(BaseEngineSpec):
    engine = 'sqlite'
    supports_column_comments = False
    supports_table_comments = False
