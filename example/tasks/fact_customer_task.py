# flake8: noqa: E501
from datetime import date, datetime

from sqlalchemy.schema import Column
from sqlalchemy.types import Date, DateTime, Integer, String

from sqltask.classes import dq
from sqltask.classes.exceptions import TooFewRowsException
from sqltask.classes.sql import LookupSource, SqlDataSource
from sqltask.classes.table import DqTableContext

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
                Column("sector_code", String(10), comment="Sector code of customer", nullable=True),
            ],
            comment="The customer table",
            timestamp_column_name="etl_timestamp",
            batch_params={"report_date": report_date},
            dq_info_column_names=["etl_timestamp"],
        ))

        # Define the main query used to populate the target table
        self.add_data_source(SqlDataSource(
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
        self.add_lookup_source(LookupSource(
            name="sector_code",
            sql="""
SELECT name,
       sector_code
FROM sector_codes
WHERE start_date <= :report_date
  AND end_date > :report_date
            """,
            params={"report_date": report_date},
            engine_context=self.ENGINE_SOURCE,
        ))

    def transform(self) -> None:
        report_date = self.batch_params["report_date"]
        sector_code_lookup = self.get_lookup("sector_code")
        for in_row in self.get_data_source("main"):
            row = self.get_new_row("fact_customer")

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

            # sector_code
            sector_code = sector_code_lookup.get(customer_name)
            if sector_code is None:
                row.log_dq(
                    column_name="sector_code",
                    source=dq.Source.SOURCE,
                    priority=dq.Priority.LOW,
                    category=dq.Category.MISSING,
                    message="Sector code undefined in lookup table"
                )
            row["sector_code"] = sector_code

            # Finally add row to table output
            row.append()

    def validate(self):
        if len(self.get_table_context("fact_customer").output_rows) < 2:
            raise TooFewRowsException("There should never be less than 2 rows")
