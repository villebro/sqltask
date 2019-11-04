# flake8: noqa: E501
import os
from datetime import datetime

from sqlalchemy.schema import Column
from sqlalchemy.types import Date, String

from sqltask.base.table import BaseTableContext
from sqltask.sources.csv import CsvRowSource

from .base_task import BaseExampleTask


class InitSourceTask(BaseExampleTask):
    def __init__(self):
        super().__init__()
        current_dir = os.path.dirname(__file__)

        # main customer table
        self.add_table(BaseTableContext(
            name="customers",
            engine_context=self.ENGINE_SOURCE,
            columns=[
                Column("report_date", Date, comment="Monthly snapshot date", primary_key=True),  # noqa
                Column("name", String(128), comment="Customer name", primary_key=True),
                Column("birthday", String(10), comment="Birthdate of customer in unreliable yyyy-mm-dd string format", nullable=True),  # noqa
            ],
            comment="The original customer table",
        ))
        # csv file containing data
        self.add_row_source(CsvRowSource(
            name="customers.csv",
            file_path=os.path.join(current_dir, "..", "static_files", "customers.csv"),
        ))

        # table containing customer's blood groups
        self.add_table(BaseTableContext(
            name="customer_blood_groups",
            engine_context=self.ENGINE_SOURCE,
            columns=[
                Column("start_date", Date, comment="Date when row becomes active (inclusive)", nullable=False),
                Column("end_date", Date, comment="Date when row becomes inactive (non-inclusive)", nullable=False),
                Column("name", String(128), comment="Customer name (non-unique)", nullable=False),
                Column("blood_group", String(20), comment="Blood group of cutomer", nullable=True),
            ],
            comment="Unreliable blood group data for customers",
        ))
        # csv file containing data
        self.add_row_source(CsvRowSource(
            name="customer_blood_groups.csv",
            file_path=os.path.join(current_dir, "..", "static_files", "customer_blood_groups.csv"),
        ))

    def transform(self) -> None:
        # populate customers table
        for in_row in self.get_row_source("customers.csv"):
            row = self.get_new_row("customers")
            row["report_date"] = datetime.strptime(
                in_row["report_date"], "%Y-%m-%d").date()
            # map remaining columns one-to-one and auto append
            row.map_all(in_row, auto_append=True)

        # populate customer blood group table
        for in_row in self.get_row_source("customer_blood_groups.csv"):
            row = self.get_new_row("customer_blood_groups")
            row["start_date"] = datetime.strptime(in_row["start_date"], "%Y-%m-%d").date()
            row["end_date"] = datetime.strptime(in_row["end_date"], "%Y-%m-%d").date()
            # map remaining columns one-to-one and auto append
            row.map_all(in_row, auto_append=True)
