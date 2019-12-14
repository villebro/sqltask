import os
from datetime import date, datetime
from decimal import Decimal
from unittest import TestCase

from sqlalchemy.schema import Column
from sqlalchemy.types import Date, DateTime, Float, Integer, String

from sqltask.engine_specs.base import BaseEngineSpec
from sqltask.utils.engine_specs import create_tmp_csv
from tests.fixtures import get_row_source, get_table_context


class TestEngineSpecs(TestCase):
    def test_csv_export(self):
        table_context = get_table_context()
        table_context.migrate_schema()
        table_context.map_all(get_row_source())
        file_path = create_tmp_csv(table_context)
        os.remove(f"{file_path}")

    def test_validate_column_types(self):
        validate = BaseEngineSpec.validate_column_value
        str10_column = Column("str10_col", String(10), nullable=False)
        str_column = Column("str_col", String, nullable=False)
        int_column = Column("int_col", Integer())
        float_column = Column("float_col", Float(), nullable=False)
        date_column = Column("float_col", Date(), nullable=False)
        datetime_column = Column("float_col", DateTime(), nullable=False)
        self.assertIsNone(validate(date(2019, 12, 31), date_column))
        self.assertIsNone(validate(date(2019, 12, 31), datetime_column))
        self.assertIsNone(validate("abc", str10_column))
        self.assertIsNone(validate("1234567890", str10_column))
        self.assertIsNone(validate("123456789012345", str_column))
        self.assertIsNone(validate(Decimal("1234.567"), float_column))
        self.assertIsNone(validate(1.1, float_column))
        self.assertIsNone(validate(1, float_column))
        self.assertIsNone(validate(1, int_column))
        self.assertIsNone(validate(None, int_column))
        self.assertRaises(ValueError, validate, datetime.utcnow(), date_column)
        self.assertRaises(ValueError, validate, None, str_column)
        self.assertRaises(ValueError, validate, "12345678901", str10_column)
        self.assertRaises(ValueError, validate, 12345, str_column)
        self.assertRaises(ValueError, validate, 12345.5, int_column)
