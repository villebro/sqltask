from datetime import date, datetime

from sqlalchemy.schema import Column
from sqlalchemy.types import Date, DateTime, Integer, String
from .base_task import BaseExampleTask
from sqltask.classes.dq import DqSource, DqPriority, DqType
from sqltask.classes.exceptions import TooFewRowsException
from sqltask.classes.sql import LookupSource, SqlDataSource


class FactCustomerTask(BaseExampleTask):
    def __init__(self, report_date: date):
        super().__init__(report_date=report_date)

        table = self.add_table(
            name="fact_customer",
            engine_context=self.ENGINE_TARGET,
            columns=[
                Column("report_date", Date, comment="Date of snapshot", primary_key=True),
                Column("etl_timestamp", DateTime, comment="Timestamp when row was created", nullable=False),
                Column("customer_id", String(10), comment="Unique customer identifier", primary_key=True),
                Column("birthdate", Date, comment="Birthdate of customer if defined and in the past", nullable=True),
                Column("age", Integer, comment="Age of customer in years if birthdate defined", nullable=True),
                Column("sector_code", String(10), comment="Sector code of customer", nullable=True),
            ],
            comment="The customer table",
            timestamp_column_name="etl_timestamp",
            batch_params={"report_date": report_date},
        )
        self.add_data_source(SqlDataSource.create(
            name="main",
            sql="""
            SELECT id,
                   birthday
            FROM customers
            WHERE report_date = :report_date
            """,
            params={"report_date": report_date},
            engine_context=self.ENGINE_SOURCE,
        ))

        self.add_lookup_source(LookupSource(
            name="sector_code",
            sql="""
            SELECT id,
                   sector_code
            FROM sector_codes
            WHERE start_date <= :report_date and end_date > :report_date
            """,
            params={"report_date": report_date},
            engine_context=self.ENGINE_SOURCE,
        ))

    def transform(self) -> None:
        report_date = self.batch_params['report_date']
        sector_code_lookup = self.get_lookup("sector_code")
        for in_row in self.get_data_source("main"):
            row = self.get_new_row("fact_customer")

            # customer_id
            customer_id = in_row['id']
            row['customer_id'] = customer_id

            # birthdate
            birthday = in_row['birthday']
            age = None
            try:
                birthdate = datetime.strptime(birthday, "%Y-%m-%d").date() if birthday else None
                age = None
                if birthdate is None:
                    self.log_dq(source=DqSource.SOURCE,
                                priority=DqPriority.HIGH,
                                dq_type=DqType.MISSING,
                                column_name="birthdate",
                                output_row=row)
                elif birthdate > report_date:
                    self.log_dq(source=DqSource.SOURCE,
                                priority=DqPriority.HIGH,
                                dq_type=DqType.INCORRECT,
                                column_name="birthdate",
                                output_row=row)
                    birthdate = None
                else:
                    age = int((report_date - birthdate).days / 365.25)
            except ValueError:
                # parse error
                self.log_dq(source=DqSource.SOURCE,
                            priority=DqPriority.HIGH,
                            dq_type=DqType.INCORRECT,
                            column_name="birthdate",
                            output_row=row)
                birthdate = None
            row["birthdate"] = birthdate

            # age
            if age is None:
                self.log_dq(source=DqSource.TRANSFORM,
                            priority=DqPriority.MEDIUM,
                            dq_type=DqType.MISSING,
                            column_name="age",
                            output_row=row)
            row["age"] = age

            # sector_code
            sector_code = sector_code_lookup.get(customer_id)
            if sector_code is None:
                self.log_dq(source=DqSource.SOURCE,
                            priority=DqPriority.MEDIUM,
                            dq_type=DqType.MISSING,
                            column_name="sector_code",
                            output_row=row)
            row["sector_code"] = sector_code

            self.add_row(row)

        for i in range(10000):
            row = self.get_new_row("fact_customer")
            row["customer_id"] = 'a' + str(i)
            row["birthdate"] = None
            row["age"] = None
            row["sector_code"] = None
            self.add_row(row)

    def validate(self):
        if len(self._output_rows['fact_customer']) < 2:
            raise TooFewRowsException("Less than 2 rows")
