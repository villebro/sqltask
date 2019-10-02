from importlib import import_module
import inspect
from pathlib import Path
import pkgutil
from typing import Dict, Type

from sqltask.engine_specs.base import BaseEngineSpec

engines: Dict[str, Type[BaseEngineSpec]] = {}

for (_, name, _) in pkgutil.iter_modules([Path(__file__).parent]):  # type: ignore
    imported_module = import_module("." + name, package=__name__)

    for i in dir(imported_module):
        attribute = getattr(imported_module, i)

        if (
            inspect.isclass(attribute)
            and issubclass(attribute, BaseEngineSpec)
            and attribute.engine != ""
        ):
            engines[attribute.engine] = attribute
