from sqlalchemy.engine import create_engine as sa_create_engine
from sqlalchemy.schema import MetaData
from sqltask import EngineContext


def create_engine(url: str, **kwargs) -> EngineContext:
    """
    Add a new engine to be used by sources, sinks and lookups.

    :param url: SqlAlchemy URL of engine.
    :param name: alias by which the engine is referenced during during operations.
    :param kwargs: additional parameters to be passed to metadata object.
    """
    engine = sa_create_engine(url)
    metadata = MetaData(bind=engine, **kwargs)
    return EngineContext('NONE', engine, metadata)
