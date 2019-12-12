from datetime import date

from sqlalchemy.schema import Column
from sqlalchemy.types import Date, String

from sqltask.base.engine import EngineContext
from sqltask.base.table import BaseTableContext


def get_table_context() -> BaseTableContext:
    engine_context = EngineContext("source", "sqlite://")
    return BaseTableContext(
        name="table",
        engine_context=engine_context,
        columns=[
            Column("report_date", Date, primary_key=True),
            Column("customer_name", String(10), comment="Name", primary_key=True),
            Column("birthdate", Date, comment="Birthday", nullable=True),
        ],
        comment="The table",
        batch_params={"report_date": date(2019, 12, 31)},
    )


def populate_dummy_rows(table_context: BaseTableContext) -> None:
    rows = (
        (date(2019, 12, 31), "Jill", date(2009, 3, 31)),
        (date(2019, 12, 31), "Jack", date(1999, 2, 28))
    )
    for in_row in rows:
        row = table_context.get_new_row()
        row["report_date"] = in_row[0]
        row["customer_name"] = in_row[1]
        row["birthdate"] = in_row[2]
        row.append()
