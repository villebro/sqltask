import logging
from typing import Any, Dict, Optional

from sqlalchemy.engine import create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.schema import MetaData

from sqltask.engine_specs import get_engine_spec


class EngineContext:
    def __init__(self,
                 name: str,
                 url: str,
                 engine_params: Optional[Dict[str, Any]] = None,
                 metadata_params: Optional[Dict[str, Any]] = None,
                 ):
        self.name = name
        self.engine_params = engine_params or {}
        self.metadata_params = metadata_params or {}
        self.engine = create_engine(url, **self.engine_params)
        self.engine_spec = get_engine_spec(self.engine.name)
        url_params = self.engine_spec.get_url_params(self.engine.url)
        self.database, self.schema = url_params
        self.metadata = MetaData(
            bind=self.engine,
            schema=url_params.schema,
            **self.metadata_params,
        )
        if url_params.database and url_params.schema:
            url_str = url_params.database + "/" + url_params.schema
        else:
            url_str = url_params.database or "<Undefined>"
        logging.info(f"Created engine `{name}` using "
                     f"`{self.engine_spec.__name__}` on `{url_str}`")

    def create_new(self,
                   database: Optional[str],
                   schema: Optional[str],
                   ) -> "EngineContext":
        """
        Create a new EngineContext based on the current instance, but with a
        different schema.

        :param database: Database to use. If left unspecified, falls back to the database
               provided by the original engine context
        :param schema: Schema to use. If left unspecified, falls back to the schema
               provided by the original engine context
        :return: a new instance of EngineContext with different url
        """
        url = make_url(str(self.engine.url))
        self.engine_spec.modify_url(url, database=database, schema=schema)
        return EngineContext(self.name, str(url), **self.metadata_params)
