import os

from sqltask import SqlTask
from sqltask.base.engine import EngineContext


class BaseExampleTask(SqlTask):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        source_url = os.getenv("SQLTASK_SOURCE", "sqlite:///source.db")
        target_url = os.getenv("SQLTASK_TARGET", "sqlite:///target.db")
        self.ENGINE_SOURCE = EngineContext("source", source_url)
        self.ENGINE_TARGET = EngineContext("target", target_url)
