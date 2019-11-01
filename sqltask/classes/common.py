from collections import namedtuple
import logging
from typing import Any, Dict, Iterable, NamedTuple, Optional, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from sqltask.classes.engine import EngineContext
    from sqltask.classes.table import TableContext  # noqa: F401


class QueryContext(NamedTuple):
    sql: str
    params: Dict[str, Any]
    table_context: Optional["TableContext"]
    engine_context: "EngineContext"


class BaseDataSource:
    def __init__(self, name: str):
        self.name = name

    def __iter__(self) -> Iterable[Dict[str, Any]]:
        raise NotImplementedError("`__iter__` not implemented")


class Lookup:
    def __init__(self,
                 name: Optional[str],
                 data_source: BaseDataSource,
                 columns: Sequence[str],
                 keys: Sequence[str]):
        logger = logging.getLogger(__name__)
        self.name = name or data_source.name
        self.data_source = data_source
        self.columns = columns
        self.keys = keys
        KeyTuple = namedtuple(self.name, keys)
        self.KeyTuple = KeyTuple
        self.store: Dict[KeyTuple, Any] = {}

        for row in self.data_source:
            if not all(key in row.keys() for key in self.keys):
                raise Exception(f"All keys ({', '.join(self.keys)}) should be present in "
                                f"input row keys ({', '.join(row.keys())})")
            key = self.KeyTuple(**{key: row[key] for key in self.keys})
            self.store[key] = row
        logger.info(f"Created lookup {self.name} with {len(self.store)} rows")

    def get(self, *unnamed_keys, **named_keys) -> Optional[Dict[str, Any]]:
        """
        Get a value from the lookup. Assuming the key for a Lookup is
        key1, key2, key3, the following are valid calls:

        >>> # only unnamed keys
        >>> lookup.get("val1", "val2", "val3")
        >>> # only named keys in non-original order
        >>> lookup.get(key3="val3", key1="val1", key2="val2")
        >>> # both named and unnamed keys
        >>> lookup.get("val1", key3="val3", key2="val2")

        :param args: unnamed key values to be used as keys
        :param kwargs: named key values to be used as keys
        :return: None if key undefined in internal dict.
        """
        key = self.KeyTuple(*unnamed_keys, **named_keys)
        return self.store.get(key)


class UrlParams(NamedTuple):
    database: Optional[str]
    schema: Optional[str]
