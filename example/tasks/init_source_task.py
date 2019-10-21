from datetime import datetime
import os

from sqltask.classes.dq import DqPriority
from sqltask.classes.file import CsvDataSource
from sqlalchemy.schema import Column
from sqlalchemy.types import Date, String
from .base_task import BaseExampleTask


class InitSourceTask(BaseExampleTask):
    def __init__(self):
        super().__init__()

        # main customer table
        self.add_table(
            name="customers",
            engine_context=self.ENGINE_SOURCE,
            columns=[
                Column("report_date", Date, comment="Monthly snapshot date", primary_key=True),
                Column("id", String(128), comment="Customer id", primary_key=True),
                Column("birthday", String(10), comment="Birthdate of customer in unreliable yyyy-mm-dd string format", nullable=True),
            ],
            dq_disable=True,
            comment="The original customer table",
        )
        # csv file containing data
        self.add_data_source(CsvDataSource.create(
            name="customers.csv",
            file_path=(os.path.dirname(__file__), "..", "static_files", "customers.csv"),
            delimiter=",",
        ))

        # additional table with sector codes per customer
        self.add_table(
            name="sector_codes",
            engine_context=self.ENGINE_SOURCE,
            columns=[
                Column("start_date", Date, comment="date when row becomes active (inclusive)", nullable=False),
                Column("end_date", Date, comment="date when row becomes inactive (non-inclusive)", nullable=False),
                Column("id", String(128), comment="Customer id (non-unique)", nullable=False),
                Column("sector_code", String(10), comment="Sector code of cutomer", nullable=True),
            ],
            dq_disable=True,
            comment="Sector codes for customers",
        )
        # csv file containing data
        self.add_data_source(CsvDataSource.create(
            name="sector_codes.csv",
            file_path=(os.path.dirname(__file__), "..", "static_files", "sector_codes.csv"),
            delimiter=",",
        ))

    def transform(self) -> None:
        for in_row in self.get_data_source("customers.csv"):
            row = self.get_new_row("customers")
            row["report_date"] = datetime.strptime(in_row["report_date"], "%Y-%m-%d").date()
            row["id"] = in_row["id"]
            row["birthday"] = in_row["birthday"]
            self.add_row(row)

        for in_row in self.get_data_source("sector_codes.csv"):
            row = self.get_new_row("sector_codes")
            row["start_date"] = datetime.strptime(in_row["start_date"], "%Y-%m-%d").date()
            row["end_date"] = datetime.strptime(in_row["end_date"], "%Y-%m-%d").date()
            row["id"] = in_row["id"]
            row["sector_code"] = in_row["sector_code"]
            self.add_row(row)
