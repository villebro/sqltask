import logging
from typing import Optional

from sqlalchemy.engine import create_engine
from sqlalchemy.schema import MetaData

from sqltask.engine_specs import get_engine_spec


class EngineContext:
    def __init__(self,
                 name: str,
                 url: str,
                 schema: Optional[str] = None,
                 **kwargs):
        self.name = name
        self.engine = create_engine(url)
        self.engine_spec = get_engine_spec(self.engine.name)
        self.schema = schema or self.engine_spec.get_schema_name(self.engine.url)
        self.metadata = MetaData(bind=self.engine, schema=self.schema, **kwargs)
        logging.info(f"Created engine `{name}` using "
                     f"`{self.engine_spec.__name__}` on schema `{schema}`")
