import csv
from typing import List, Optional, Sequence

from sqltask.base.lookup_source import BaseLookupSource
from sqltask.base.row_source import BaseRowSource


class CsvRowSource(BaseRowSource):
    """
    Row source that reads from a CSV file.
    """

    def __init__(self,
                 file_path: str,
                 name: Optional[str] = None,
                 delimiter: str = ","):
        """
        :param name: name of data source.
        :param file_path: path to the csv file.
        :param delimiter: csv file delimiter.
        """

        super().__init__(name)
        self.file_path = file_path
        self.delimiter = delimiter

        # populate column names
        self.columns: List[str] = []
        with open(self.file_path, newline="") as csvfile:
            csvreader = csv.reader(csvfile, delimiter=self.delimiter)
            row = next(csvreader)
            for column in row:
                self.columns.append(column)

    def __iter__(self):
        with open(self.file_path, newline="") as csvfile:
            csvreader = csv.reader(csvfile, delimiter=self.delimiter)
            # skip header row
            next(csvreader)
            for in_row in csvreader:
                row_dict = {self.columns[i]: col for i, col in enumerate(in_row)}
                yield row_dict


class CsvLookupSource(BaseLookupSource):
    def __init__(self,
                 name: str,
                 file_path: str,
                 keys: Sequence[str],
                 delimiter: str = ",",
                 ):
        row_source = CsvRowSource(file_path=file_path, delimiter=delimiter)
        super().__init__(name=name, row_source=row_source, keys=keys)
