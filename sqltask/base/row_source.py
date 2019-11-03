from typing import Any, Iterator, Mapping, Optional


class BaseRowSource:
    def __init__(self, name: Optional[str] = None):
        self.name = name

    def __iter__(self) -> Iterator[Mapping[str, Any]]:
        raise NotImplementedError("`__iter__` not implemented")
