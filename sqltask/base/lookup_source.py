import logging
from typing import Any, Dict, Optional, Sequence, Tuple, cast

from sqltask.base.row_source import BaseRowSource

logger = logging.getLogger(__name__)


class BaseLookupSource:
    def __init__(self,
                 name: str,
                 row_source: BaseRowSource,
                 keys: Sequence[str]):
        self.name = name or row_source.name
        self.row_source = row_source
        self.keys = tuple(keys)
        self._store: Optional[Dict[Tuple, Any]] = None

    def _init_store(self):
        self._store = {}
        duplicate_count = 0
        for row in self.row_source:
            if not all(key in row.keys() for key in self.keys):
                raise Exception(f"All keys ({', '.join(self.keys)}) should be present in "
                                f"input row keys ({', '.join(row.keys())})")
            key = tuple([row[key] for key in self.keys])
            value = {key: value for key, value in row.items()}
            if key in self._store:
                duplicate_count += 1
            else:
                self._store[key] = value

        if duplicate_count > 0:
            logger.warning(
                f"Query result for lookup `{self.name}` has {duplicate_count} "
                f"duplicate keys, ignoring duplicate rows")
        logger.info(f"Created lookup {self.name} with {len(self._store)} rows")

    def get(self, *unnamed_keys, **named_keys) -> Dict[str, Any]:
        """
        Get a value from the lookup. Assuming the key for a Lookup is
        key1, key2, key3, the following are valid calls:

        >>> # only unnamed keys
        >>> lookup.get("val1", "val2", "val3")
        >>> # only named keys in non-original order
        >>> lookup.get(key3="val3", key1="val1", key2="val2")
        >>> # both named and unnamed keys
        >>> lookup.get("val1", key3="val3", key2="val2")

        If a row is not found in the lookup table, the method returns an empty dict.

        :param unnamed_keys: unnamed key values to be used as keys
        :param named_keys: named key values to be used as keys
        :return: A dict with keys as the column name and values as the cell values.
                 If key undefined in internal dict return an empty dict.
        """
        if self._store is None:
            self._init_store()

        store = cast(Dict[Tuple, Any], self._store)
        if len(unnamed_keys) + len(named_keys) != len(self.keys):
            raise Exception(f"Incorrect key count: expected {len(self.keys)} keys, "
                            f"got {len(unnamed_keys) + len(named_keys)}")
        keys = [key for key in unnamed_keys]
        for key in self.keys[len(unnamed_keys):]:
            if key not in named_keys:
                raise ValueError(f"Key not in lookup: {key}")
            keys.append(named_keys[key])
        return store.get(tuple(keys), {})
