from datetime import date, datetime
import os

from sqlalchemy.schema import Column
from sqlalchemy.types import Date, DateTime, Float, String
from sqltask import SqlTask, DqSource, DqPriority, DqType
from sqltask.exceptions import TooFewRowsException


class CustomerTask(SqlTask):
    def __init__(self, report_date: date):
        super().__init__(report_date=report_date)
        source_engine = self.add_engine("source", os.getenv("SQLTASK_SOURCE"))
        target_engine = self.add_engine("target", os.getenv("SQLTASK_TARGET"))
        columns = [
            Column("report_date", String, comment="Built-in row id", primary_key=True),
            Column("etl_rowid", String, comment="Built-in row id", nullable=False),
            Column("etl_timestamp", DateTime, comment="Timestamp when row was created", nullable=False),
            Column("customer_id", String, comment="Unique customer identifier", primary_key=True),
            Column("birthdate", Date, comment="Birthdate of customer if defined and in the past"),
            Column("age", Float, comment="Age of customer in years if birthdate defined"),
            Column("sector_code", String, comment="Sector code of customer"),
        ]
        table = self.add_table("customer",
                               target_engine,
                               columns,
                               rowid_column_name="etl_rowid",
                               timestamp_column_name="etl_timestamp",
                               batch_params={"report_date": report_date}
                               )

        self.add_source_query("main", """
            SELECT id,
                   birthday,
                   1 as num
            FROM (SELECT DATE('2019-06-30') AS report_date, '1234567' AS id, DATE('1980-01-01') AS birthday UNION ALL 
                  SELECT DATE('2019-06-30') AS report_date, '2345678' AS id, DATE('2080-01-01') AS birthday UNION ALL 
                  SELECT DATE('2019-06-30') AS report_date, '3456789' AS id, NULL AS birthday)
            WHERE report_date = :report_date
            """, {"report_date": report_date}, source_engine)

        self.add_lookup_query("sector_code", """
            SELECT customer_id,
                   sector_code
            FROM (SELECT DATE('2019-06-30') AS execution_date, '1234567' AS customer_id, '111211' AS sector_code UNION ALL 
                  SELECT DATE('2019-06-30') AS execution_date, '2345678' AS customer_id, '143' AS sector_code UNION ALL 
                  SELECT DATE('2019-06-30') AS execution_date, '2345678' AS customer_id, '143' AS sector_code UNION ALL 
                  SELECT DATE('2019-06-30') AS execution_date, '3456789' AS customer_id, NULL AS sector_code 
            )
            WHERE execution_date = :report_date
            """, {"report_date": report_date}, table, source_engine)

    def transform(self) -> None:
        report_date = self.batch_params['report_date']
        for in_row in self.get_source_rows("main"):
            row = self.get_new_row("customer")

            # report_date
            row["report_date"] = report_date

            # customer_id
            customer_id = in_row['id']
            row['customer_id'] = customer_id

            # birthdate
            birthday = in_row['birthday']
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
                age = (report_date - birthdate).days / 365.25
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

        for i in range(0):
            row = self.get_new_row("customer")
            row["customer_id"] = 'abcd'
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
