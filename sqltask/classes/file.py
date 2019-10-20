import csv
import os
from typing import Tuple

from sqltask.classes.common import BaseDataSource, Lookup


class CsvDataSource(BaseDataSource):
    @classmethod
    def create(cls, name: str, file_path: Tuple[str, ...], delimiter: str = "\t"):
        """
        Factory method for creating an instance of the class. The preferred way of
        creating a csv datasource. The source csv file is required to provide the
        column names in the header row.

        :param name: name of data source.
        :param file_path: relative path to the csv file.
        :param delimiter: csv file delimiter.
        """
        return cls(name, os.path.join(*file_path), delimiter)

    def __init__(self, name: str, file_path, delimiter: str):
        super().__init__(name)
        self.file_path = file_path
        self.delimiter = delimiter

        # populate column names
        self.columns = []
        with open(self.file_path, newline='') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=self.delimiter)
            row = next(csvreader)
            for column in row:
                self.columns.append(column)

    def __iter__(self):
        with open(self.file_path, newline='') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=self.delimiter)
            # skip header row
            next(csvreader)
            for in_row in csvreader:
                row_dict = {self.columns[i]: col for i, col in enumerate(in_row)}
                yield Lookup(self, row_dict)
