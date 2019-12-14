from datetime import date
from typing import Dict, List, Optional

from sqlalchemy.schema import Column
from sqlalchemy.types import Date, String

from sqltask.base.engine import EngineContext
from sqltask.base.row_source import BaseRowSource
from sqltask.base.table import BaseTableContext
from sqltask.sources.generic import DictRowSource


def get_table_context(name: Optional[str] = None,
                      columns: Optional[List[Column]] = None,
                      ) -> BaseTableContext:
    engine_context = EngineContext("source", "sqlite://")
    return BaseTableContext(
        name=name or "tbl",
        engine_context=engine_context,
        columns=columns or [
            Column("report_date", Date, primary_key=True),
            Column("customer_name", String(20), comment="Name", primary_key=True),
            Column("birthdate", Date, comment="Birthday", nullable=True),
        ],
        comment="The table",
        batch_params={"report_date": date(2019, 12, 31)},
    )


def get_row_source(
        rename: Optional[Dict[str, str]] = None,
) -> BaseRowSource:
    rename = rename or {}
    customer_name = rename.get("customer_name", "customer_name")
    birthdate = rename.get("birthdate", "birthdate")

    return DictRowSource((
        {
            customer_name: "Jill",
            birthdate: date(2009, 3, 31),
        },
        {
            customer_name: "Jack",
            birthdate: date(1999, 2, 28),
        },
        {
            customer_name: "Mr Nobirthday",
            birthdate: None,
        },
    ))
