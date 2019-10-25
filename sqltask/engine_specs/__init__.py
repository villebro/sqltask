import inspect
import pkgutil
from importlib import import_module
from pathlib import Path
from typing import Dict, Type

from sqltask.engine_specs.base import BaseEngineSpec

_engines: Dict[str, Type[BaseEngineSpec]] = {}

for (_, name, _) in pkgutil.iter_modules([Path(__file__).parent]):  # type: ignore
    imported_module = import_module("." + name, package=__name__)

    for i in dir(imported_module):
        attribute = getattr(imported_module, i)

        if (
            inspect.isclass(attribute)
            and issubclass(attribute, BaseEngineSpec)
            and attribute.engine != ""
        ):
            _engines[attribute.engine] = attribute


def get_engine_spec(engine_name: str) -> Type[BaseEngineSpec]:
    """
    Get an engine spec based on an engine name, e.g. snowflake.

    :param engine_name: Name of engine, i.e. engine.name
    :return: Engine spec for a given engine name. Returns `BaseEngineSpec`
    if engine does not have a dedicated spec available.
    """
    return _engines.get(engine_name, BaseEngineSpec)
