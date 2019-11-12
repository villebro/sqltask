import csv
import logging
from typing import Optional, Sequence

from sqltask.base.lookup_source import BaseLookupSource
from sqltask.base.row_source import BaseRowSource
from sqltask.utils.file import detect_encode

logger = logging.getLogger(__name__)


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

        if encoding is None:
            logger.debug(
                f"Autodetecting encoding for CSV row source: "
                f"{name or file_path or '<undefined>'}"
            )
            result = detect_encode(file_path)
            encoding = result["encoding"]
            logger.debug(f"Detected file encoding: {encoding}")
        self.encoding = encoding

    def __repr__(self):
        return self.name or self.file_path or '<undefined>'

    def __iter__(self):
        """
        Iterate over

        :return:
        """
        columns = []
        row_number = 0
        logger.debug(
            f"Start reading CSV row source: {self}")
        with open(self.file_path, newline="", encoding=self.encoding) as csvfile:
            csvreader = csv.reader(csvfile, delimiter=self.delimiter)
            for in_row in csvreader:
                row_number += 1

                # read column names on first row
                if row_number == 1:
                    for column in in_row:
                        columns.append(column)
                    continue

                if len(in_row) != len(columns):
                    raise Exception(
                        f"Error reading row {row_number} of CSV file {self}: "
                        f"Expected {len(columns)} columns, found {len(in_row)}")
                row_dict = {columns[i]: col for i, col in enumerate(in_row)}
                yield row_dict
            logger.info(
                f"Finished reading {row_number - 1} rows for CSV row source: {self}"
            )


class CsvLookupSource(BaseLookupSource):
    def __init__(self,
                 name: str,
                 file_path: str,
                 keys: Sequence[str],
                 delimiter: str = ",",
                 ):
        row_source = CsvRowSource(file_path=file_path, delimiter=delimiter)
        super().__init__(name=name, row_source=row_source, keys=keys)
