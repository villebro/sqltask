from unittest import TestCase

from tests.fixtures import get_table_context


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
