from datetime import date
import os

from sqltask import SqlTask, DqSource, DqSeverity
from sqltask.exceptions import TooFewRowsException
from sqltask.utils import create_engine


class CustomerTask(SqlTask):
    @classmethod
    def init_schema(cls):
        # TODO: add example schema
        pass

    batch_column_names = ["report_date"]
    engine_specs = {
        "source": create_engine(os.getenv("SQLTASK_SOURCE")),
        "target": create_engine(os.getenv("SQLTASK_TARGET")),
    }

    def __init__(self, report_date: date):
        super().__init__(report_date=report_date)

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

            # customer_id
            customer_id = in_row['id']
            row['customer_id'] = customer_id

            # birthday
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
