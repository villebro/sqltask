from datetime import date, datetime
import os

from sqlalchemy.schema import Column
from sqlalchemy.types import Date, DateTime, Integer, String
from sqltask import SqlTask, DqSource, DqPriority, DqType
from sqltask.exceptions import TooFewRowsException


class CustomerTask(SqlTask):
    def __init__(self, report_date: date):
        super().__init__(report_date=report_date)
        source_url = os.getenv("SQLTASK_SOURCE", "sqlite:///source.db")
        target_url = os.getenv("SQLTASK_TARGET", "sqlite:///target.db")
        source_engine = self.add_engine("source", source_url)
        target_engine = self.add_engine("target", target_url)

        table = self.add_table(
            name="customer",
            engine_context=target_engine,
            columns=[
                Column("report_date", Date, comment="Built-in row id", primary_key=True),
                Column("etl_timestamp", DateTime, comment="Timestamp when row was created", nullable=False),
                Column("customer_id", String(128), comment="Unique customer identifier", primary_key=True),
                Column("birthdate", Date, comment="Birthdate of customer if defined and in the past"),
                Column("age", Integer, comment="Age of customer in years if birthdate defined"),
                Column("sector_code", String(10), comment="Sector code of customer"),
            ],
            comment="The customer table",
            timestamp_column_name="etl_timestamp",
            batch_params={"report_date": report_date},
        )

        self.add_source_query(
            name="main",
            sql="""
            SELECT id,
                   birthday,
                   1 as num
            FROM (SELECT DATE('2019-06-30') AS report_date, '1234567' AS id, '1980-01-01' AS birthday UNION ALL 
                  SELECT DATE('2019-06-30') AS report_date, '2345678' AS id, '2080-01-01' AS birthday UNION ALL 
                  SELECT DATE('2019-06-30') AS report_date, '2245678' AS id, '1980-13-01' AS birthday UNION ALL 
                  SELECT DATE('2019-06-30') AS report_date, '3456789' AS id, NULL AS birthday)
            WHERE report_date = :report_date
            """,
            params={"report_date": report_date},
            engine_context=source_engine,
        )

        self.add_lookup_query(
            name="sector_code",
            sql="""
            SELECT customer_id,
                   sector_code
            FROM (SELECT DATE('2019-06-30') AS execution_date, '1234567' AS customer_id, '111211' AS sector_code UNION ALL 
                  SELECT DATE('2019-06-30') AS execution_date, '2345678' AS customer_id, '143' AS sector_code UNION ALL 
                  SELECT DATE('2019-06-30') AS execution_date, '2345678' AS customer_id, '143' AS sector_code UNION ALL 
                  SELECT DATE('2019-06-30') AS execution_date, '3456789' AS customer_id, NULL AS sector_code 
            )
            WHERE execution_date = :report_date
            """,
            params={"report_date": report_date},
            table_context=table,
            engine_context=source_engine,
        )

    def transform(self) -> None:
        report_date = self.batch_params['report_date']
        for in_row in self.get_source_rows("main"):
            row = self.get_new_row("customer")

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
            sector_code = self.get_lookup("sector_code").get(customer_id)
            if sector_code is None:
                self.log_dq(source=DqSource.SOURCE,
                            priority=DqPriority.MEDIUM,
                            dq_type=DqType.MISSING,
                            column_name="sector_code",
                            output_row=row)
            row["sector_code"] = sector_code

            self.add_row(row)

        for i in range(10000):
            row = self.get_new_row("customer")
            row["customer_id"] = 'a' + str(i)
            row["birthdate"] = None
            row["age"] = None
            row["sector_code"] = None
            self.add_row(row)

    def validate(self):
        if len(self._output_rows['customer']) < 2:
            raise TooFewRowsException("Less than 2 rows")


if __name__ == "__main__":
    task = CustomerTask(report_date=date(2019, 6, 30))
    task.execute()
