from unittest import TestCase

from tests.fixtures import get_row_source, get_table_context


class TestBaseTableContext(TestCase):
    def test_migration_add_and_remove_columns(self):
        table_context = get_table_context()
        engine = table_context.engine_context.engine
        engine.execute("""
        CREATE TABLE tbl (
            customer_name VARCHAR(10) NOT NULL,
            report_date DATE NOT NULL,
            birthdate DATE NULL,
            redundant_field VARCHAR(128) NOT NULL
        )
        """)
        table_context.migrate_schema()

    def test_add_all(self):
        def func(value: str) -> str:
            return value + " 123"

        table_context = get_table_context()
        row_source = get_row_source(rename={"birthdate": "bdate"})
        table_context.map_all(
            row_source=row_source,
            mappings={"birthdate": "bdate"},
            funcs={"customer_name": func}
        )
        for idx, input_row in enumerate(row_source):
            output_row = table_context.output_rows[idx]
            self.assertIn("birthdate", output_row.keys())
            name = func(input_row["customer_name"])
            self.assertEqual(name, output_row["customer_name"])
