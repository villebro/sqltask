import csv
from typing import Optional, Sequence

from sqltask.base.lookup_source import BaseLookupSource
from sqltask.base.row_source import BaseRowSource


class CsvRowSource(BaseRowSource):
    """
    Row source that reads from a CSV file. Expects the first row to contain column
    names, with subsequent rows containing column values in the same order.
    """

    def __init__(self,
                 file_path: str,
                 name: Optional[str] = None,
                 delimiter: str = ",",
                 encoding: Optional[str] = None):
        """
        :param name: name of data source.
        :param file_path: path to the csv file.
        :param delimiter: csv file delimiter.
        :param encoding: Character encoding of csv file.
        """

        super().__init__(name)
        self.file_path = file_path
        self.delimiter = delimiter
        self.encoding = encoding

    def __iter__(self):
        """
        Iterate over

        :return:
        """
        columns = []
        row_number = 1
        with open(self.file_path, newline="") as csvfile:
            csvreader = csv.reader(csvfile, delimiter=self.delimiter)
            # skip header row
            row = next(csvreader)
            for column in row:
                columns.append(column)

            for in_row in csvreader:
                row_number += 1
                if len(in_row) != len(columns):
                    raise Exception(
                        f"Error reading CSV file {self.name} on row {row_number}: "
                        f"Expected {len(columns)} columns, found {len(in_row)}")
                row_dict = {columns[i]: col for i, col in enumerate(in_row)}
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
