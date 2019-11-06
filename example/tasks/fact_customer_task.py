# flake8: noqa: E501
import os
from datetime import date, datetime
from typing import cast

from sqlalchemy.schema import Column
from sqlalchemy.types import Date, DateTime, Integer, String

from sqltask.base import dq
from sqltask.base.exceptions import TooFewRowsException
from sqltask.base.table import DqOutputRow, DqTableContext
from sqltask.sources.csv import CsvLookupSource
from sqltask.sources.sql import SqlLookupSource, SqlRowSource

from .base_task import BaseExampleTask


class FactCustomerTask(BaseExampleTask):
    def __init__(self, report_date: date):
        super().__init__(report_date=report_date)

        # Define the metadata for the main fact table
        self.add_table(DqTableContext(
            name="fact_customer",
            engine_context=self.ENGINE_TARGET,
            columns=[
                Column("report_date", Date, comment="Date of snapshot", primary_key=True),
                Column("etl_timestamp", DateTime, comment="Timestamp when row was created", nullable=False),
                Column("customer_name", String(10), comment="Unique customer identifier (name)", primary_key=True),
                Column("birthdate", Date, comment="Birthdate of customer if defined and in the past", nullable=True),
                Column("age", Integer, comment="Age of customer in years if birthdate defined", nullable=True),
                Column("blood_group", String(3), comment="Blood group of the customer", nullable=True),
            ],
            comment="The customer table",
            timestamp_column_name="etl_timestamp",
            batch_params={"report_date": report_date},
        ))

        # Define the main query used to populate the target table
        self.add_row_source(SqlRowSource(
            name="main",
            sql="""
SELECT name,
       birthday
FROM customers
WHERE report_date = :report_date
            """,
            params={"report_date": report_date},
            engine_context=self.ENGINE_SOURCE,
        ))

        # Define a lookup source used for enriching the main source query
        self.add_lookup_source(SqlLookupSource(
            name="customer_blood_groups",
            keys=["name"],
            sql="""
SELECT name,
       blood_group
FROM customer_blood_groups
WHERE start_date <= :report_date
  AND end_date > :report_date
            """,
            params={"report_date": report_date},
            engine_context=self.ENGINE_SOURCE,
        ))

        # Define a lookup source with all valid blood groups (read directly from CSV)
        current_dir = os.path.dirname(__file__)
        self.add_lookup_source(CsvLookupSource(
            name="valid_blood_groups",
            keys=["blood_group"],
            file_path=os.path.join(
                current_dir, "..", "static_files", "valid_blood_groups.csv"
            ),
        ))

    def transform(self) -> None:
        report_date = self.batch_params["report_date"]
        customer_blood_group_lookup = self.get_lookup_source("customer_blood_groups")
        valid_blood_group_lookup = self.get_lookup_source("valid_blood_groups")
        for in_row in self.get_row_source("main"):
            row = cast(DqOutputRow, self.get_new_row("fact_customer"))

            # customer_name
            customer_name = in_row["name"]
            row["customer_name"] = customer_name

            # birthdate
            birthday = in_row["birthday"]
            age = None
            try:
                birthdate = datetime.strptime(birthday, "%Y-%m-%d").date() if birthday else None
                age = None
                if birthdate is None:
                    row.log_dq(
                        column_name="birthdate",
                        source=dq.Source.SOURCE,
                        priority=dq.Priority.MEDIUM,
                        category=dq.Category.MISSING,
                        message="Missing birthdate",
                    )
                elif birthdate > report_date:
                    row.log_dq(
                        column_name="birthdate",
                        source=dq.Source.SOURCE,
                        priority=dq.Priority.HIGH,
                        category=dq.Category.INCORRECT,
                        message=f"Birthdate in future: {birthday}",
                    )
                    birthdate = None
                else:
                    age = int((report_date - birthdate).days / 365.25)
            except ValueError:
                # parse error
                row.log_dq(
                    column_name="birthdate",
                    source=dq.Source.SOURCE,
                    priority=dq.Priority.HIGH,
                    category=dq.Category.INCORRECT,
                    message=f"Cannot parse birthdate: {birthday}"
                )
                birthdate = None
            row["birthdate"] = birthdate

            # age
            if age is None:
                row.log_dq(
                    column_name="age",
                    source=dq.Source.TRANSFORM,
                    priority=dq.Priority.MEDIUM,
                    category=dq.Category.MISSING,
                    message="Age is undefined due to undefined birthdate",
                )
            row["age"] = age

            # blood group

            # retrieve customer's blood group and make sure it's valid
            customer_blood_group = customer_blood_group_lookup.get(name=customer_name).get("blood_group")
            valid_blood_group = valid_blood_group_lookup.get(customer_blood_group).get("blood_group")

            if not customer_blood_group:
                row.log_dq(
                    column_name="blood_group",
                    source=dq.Source.SOURCE,
                    priority=dq.Priority.MEDIUM,
                    category=dq.Category.MISSING,
                    message="Blood group undefined in customer blood group table"
                )
            elif not valid_blood_group:
                row.log_dq(
                    column_name="blood_group",
                    source=dq.Source.SOURCE,
                    priority=dq.Priority.HIGH,
                    category=dq.Category.INCORRECT,
                    message=f"Invalid blood group: {customer_blood_group}"
                )

            row["blood_group"] = valid_blood_group

            # Finally add row to table output
            row.append()

    def validate(self):
        if len(self.get_table_context("fact_customer").output_rows) < 2:
            raise TooFewRowsException("There should never be less than 2 rows")
