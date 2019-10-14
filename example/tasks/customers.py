import csv
from datetime import datetime
import os

from sqlalchemy.schema import Column
from sqlalchemy.types import Date, String
from base_task import BaseExampleTask


class CustomersTask(BaseExampleTask):
    def __init__(self):
        super().__init__()

        self.add_table(
            name="customers",
            engine_context=self.ENGINE_SOURCE,
            columns=[
                Column("report_date", Date, comment="Monthly snapshot date", primary_key=True),
                Column("id", String(128), comment="Customer id", primary_key=True),
                Column("birthdate", String(10), comment="Birthdate of customer in unreliable yyyy-mm-dd string format", nullable=True),
            ],
            comment="The original customer table",
        )

        self.add_table(
            name="sector_codes",
            engine_context=self.ENGINE_SOURCE,
            columns=[
                Column("start_date", Date, comment="date when row becomes active (inclusive)", nullable=False),
                Column("end_date", Date, comment="date when row becomes inactive (non-inclusive)", nullable=False),
                Column("id", String(128), comment="Customer id (non-unique)", nullable=False),
                Column("sector_code", String(10), comment="Sector code of cutomer", nullable=True),
            ],
            comment="Sector codes for customers",
        )

    def transform(self) -> None:
        with open(os.path.join("..", "static_files", "customers.csv"), newline='') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',')
            next(csvreader)
            for in_row in csvreader:
                row = self.get_new_row("customers")
                row["report_date"] = datetime.strptime(in_row[0], "%Y-%m-%d").date()
                row["id"] = in_row[1]
                row["birthdate"] = in_row[2]
                self.add_row(row)

        with open(os.path.join("..", "static_files", "sector_codes.csv"), newline='') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',')
            next(csvreader)
            for in_row in csvreader:
                row = self.get_new_row("sector_codes")
                row["start_date"] = datetime.strptime(in_row[0], "%Y-%m-%d").date()
                row["end_date"] = datetime.strptime(in_row[1], "%Y-%m-%d").date()
                row["id"] = in_row[2]
                row["sector_code"] = in_row[3]
                self.add_row(row)


if __name__ == "__main__":
    task = CustomersTask()
    task.execute()
