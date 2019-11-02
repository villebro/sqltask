import logging
from typing import (
    Any,
    Dict,
    Optional,
    Sequence,
    Tuple,
)

from sqltask.classes.common import BaseDataSource

class Lookup:
    def __init__(self,
                 name: Optional[str],
                 data_source: BaseDataSource,
                 keys: Sequence[str]):
        logger = logging.getLogger(__name__)
        self.name = name or data_source.name
        self.data_source = data_source
        self.keys = tuple(keys)
        self.store: Dict[Tuple, Any] = {}

        duplicate_count = 0
        for row in self.data_source:
            if not all(key in row.keys() for key in self.keys):
                raise Exception(f"All keys ({', '.join(self.keys)}) should be present in "
                                f"input row keys ({', '.join(row.keys())})")
            key = tuple([row[key] for key in self.keys])
            value = {key: value for key, value in row.items()}
            if key in self.store:
                duplicate_count += 1
            else:
                self.store[key] = value

        logger.warning(
            f"Query result for lookup `{self.name}` has {duplicate_count} "
            f"duplicate keys, ignoring duplicate rows")
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

        :param unnamed_keys: unnamed key values to be used as keys
        :param named_keys: named key values to be used as keys
        :return: None if key undefined in internal dict.
        """
        if len(unnamed_keys) + len(named_keys) != len(self.keys):
            raise Exception(f"Incorrect key count: expected {len(self.keys)} keys, "
                            f"got {len(unnamed_keys) + len(named_keys)}")
        keys = [key for key in unnamed_keys]
        for key in self.keys[len(unnamed_keys):]:
            if key not in named_keys:
                raise Exception(f"Key not in lookup: {key}")
            keys.append(named_keys[key])
        return self.store.get(tuple(keys)) or {}
