from typing import Any, Iterator, Mapping, Optional


class BaseRowSource:
    """
    Base class for data sources that return iterable rows. A row from a BaseRowSource
    can be any Mapping from a key (=column name) to a value (=cell value) that can
    be referenced as follows:
    >>> for row in rows:
    >>>     column_value = row["column_name"]
    """
    def __init__(self, name: Optional[str] = None):
        self.name = name

    def __iter__(self) -> Iterator[Mapping[str, Any]]:
        raise NotImplementedError("`__iter__` not implemented")
