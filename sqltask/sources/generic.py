from typing import Any, Dict, Iterator, Optional, Sequence

from sqltask.base.lookup_source import BaseLookupSource
from sqltask.base.row_source import BaseRowSource


class DictRowSource(BaseRowSource):
    def __init__(self,
                 rows: Sequence[Dict[str, Any]],
                 name: Optional[str] = None,
                 ):
        super().__init__(name)
        self.rows = rows

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        for row in self.rows:
            yield row


class DictLookupSource(BaseLookupSource):
    def __init__(self,
                 name: str,
                 rows: Sequence[Dict[str, Any]],
                 keys: Sequence[str],
                 ):
        row_source = DictRowSource(rows=rows)
        super().__init__(name=name, row_source=row_source, keys=keys)


class ListRowSource(BaseRowSource):
    def __init__(self,
                 column_names: Sequence[str],
                 rows: Sequence[Sequence[Any]],
                 name: Optional[str] = None,
                 ):
        super().__init__(name)
        self.column_names = column_names
        self.rows = rows

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        for row in self.rows:
            yield {self.column_names[i]: row[i] for i in range(len(self.column_names))}


class ListLookupSource(BaseLookupSource):
    def __init__(self,
                 name: str,
                 column_names: Sequence[str],
                 rows: Sequence[Sequence[Any]],
                 keys: Sequence[str],
                 ):
        row_source = ListRowSource(column_names=column_names, rows=rows, name=name)
        super().__init__(name=name, row_source=row_source, keys=keys)
