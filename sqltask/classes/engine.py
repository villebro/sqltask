import logging
from typing import Any, Dict, Optional

from sqlalchemy.engine import create_engine
from sqlalchemy.schema import MetaData

from sqltask.engine_specs import get_engine_spec


class EngineContext:
    def __init__(self,
                 name: str,
                 url: str,
                 metadata_kwargs: Optional[Dict[str, Any]] = None,
                 ):
        self.name = name
        self.engine = create_engine(url)
        self.engine_spec = get_engine_spec(self.engine.name)
        self.schema = self.engine_spec.get_schema_name(self.engine.url)
        self.metadata_kwargs = metadata_kwargs or {}
        self.metadata = MetaData(
            bind=self.engine,
            schema=self.schema,
            **self.metadata_kwargs,
        )
        logging.info(f"Created engine `{name}` using "
                     f"`{self.engine_spec.__name__}` on schema `{self.schema}`")

    def create_new(self, schema: Optional[str] = None) -> "EngineContext":
        """
        Create a new EngineContext based on the current instance, but with a
        different schema.

        :param schema: the new schema
        :return: a new instance of EngineContext with different url
        """
        engine = create_engine(str(self.engine.url))
        self.engine_spec.modify_url(engine.url, schema=schema)
        return EngineContext(self.name, str(engine.url), **self.metadata_kwargs)
