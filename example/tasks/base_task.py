import os

from sqltask import SqlTask


class BaseExampleTask(SqlTask):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        source_url = os.getenv("SQLTASK_SOURCE", "sqlite:///source.db")
        target_url = os.getenv("SQLTASK_TARGET", "sqlite:///target.db")
        self.ENGINE_SOURCE = self.add_engine("source", source_url)
        self.ENGINE_TARGET = self.add_engine("target", target_url)
