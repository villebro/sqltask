from datetime import date
import os

from sqlalchemy.schema import Column
from sqlalchemy.types import Date, Float, String
from sqltask import SqlTask, DqSource, DqSeverity
from sqltask.exceptions import TooFewRowsException


class CustomerTask(SqlTask):
    @classmethod
    def init_schema(cls):

        # TODO: add example schema
        pass

    batch_columns = [
        Column('report_date', Date, comment="Snapshot date", primary_key=True),
    ]

    }

    def __init__(self, report_date: date):
        super().__init__(report_date=report_date)
        self.add_engine("source", os.getenv("SQLTASK_SOURCE"))
        self.add_engine("target", os.getenv("SQLTASK_TARGET"))
        columns = [
            Column("customer_id",
                   String,
                   comment="Unique customer identifier",
                   primary_key=True),
            Column("birthdate",
                   Date,
                   comment="Birthdate of customer if defined and in the past"),
            Column("age", Float,
                   comment="Age of customer in years if birthdate defined"),
            Column("sector_code",
                   String,
                   comment="Sector code of customer"),
        ]
        self.add_table("customer", columns, "target")

        self.add_source_query("main", """
            SELECT id,
                   birthday,
                   1 as num
            FROM (SELECT TO_DATE('2019-06-30') AS report_date, '1234567' AS id, TO_DATE('1980-01-01') AS birthday UNION ALL 
                  SELECT TO_DATE('2019-06-30') AS report_date, '2345678' AS id, TO_DATE('2080-01-01') AS birthday UNION ALL 
                  SELECT TO_DATE('2019-06-30') AS report_date, '3456789' AS id, NULL AS birthday)
            WHERE report_date = :report_date
            """, {"report_date": report_date}, "source")

        self.add_lookup_query("sector_code", """
            SELECT customer_id,
                   sector_code
            FROM (SELECT TO_DATE('2019-06-30') AS execution_date, '1234567' AS customer_id, '111211' AS sector_code UNION ALL 
                  SELECT TO_DATE('2019-06-30') AS execution_date, '2345678' AS customer_id, '143' AS sector_code UNION ALL 
                  SELECT TO_DATE('2019-06-30') AS execution_date, '2345678' AS customer_id, '143' AS sector_code UNION ALL 
                  SELECT TO_DATE('2019-06-30') AS execution_date, '3456789' AS customer_id, NULL AS sector_code 
            )
            WHERE execution_date = :report_date
            """, {"report_date": report_date}, "source")

    def transform(self) -> None:
        report_date = self.params['report_date']
        for in_row in self.get_source_rows("main"):
            row = self.get_new_row()

            # report_date
            row["report_date"] = report_date

            # customer_id
            customer_id = in_row['id']
            row['customer_id'] = customer_id

            # birthdate
            birthdate = in_row['birthday']
            age = None
            if birthdate is None:
                self.log_dq(DqSource.SOURCE, DqSeverity.HIGH, "Missing birthdate", row)
            elif birthdate > report_date:
                self.log_dq(DqSource.SOURCE, DqSeverity.HIGH,
                            f"birthdate in future: `{str(birthdate)}`", row)
                birthdate = None
            else:
                age = (report_date - birthdate).days / 365.25
            row["birthdate"] = birthdate
            row["age"] = age

            # sector_code
            sector_code = self.get_lookup("sector_code").get(customer_id)
            if sector_code is None:
                self.log_dq(DqSource.SOURCE, DqSeverity.MEDIUM,
                            "Missing sector code", row)
            row["sector_code"] = sector_code

            self.add_row(row)

        for i in range(0):
            row = self.get_new_row()
            row["customer_id"] = 'abcd'
            row["birthdate"] = None
            row["age"] = None
            row["sector_code"] = None
            self.add_row(row)

    def validate(self):
        if len(self.output_rows) < 2:
            raise TooFewRowsException("Less than 2 rows")


if __name__ == "__main__":
    task = CustomerTask(report_date=date(2019, 6, 30))
    task.execute()
