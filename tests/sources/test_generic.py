from unittest import TestCase

from sqltask.sources.generic import (DictLookupSource, DictRowSource,
                                     ListLookupSource, ListRowSource)


class DictSourceTestCase(TestCase):
    def test_row_source(self):
        input_rows = (
            {"a": 1, "b": "txt"},
            {"a": 2, "c": "xyz"},
        )
        row_source = DictRowSource(rows=input_rows)
        output_rows = []
        for row in row_source:
            output_rows.append(row)
        self.assertListEqual(output_rows, list(input_rows))

    def test_lookup_source(self):
        rows = (
            {"a": 1, "b": "txt", "d": 2},
            {"a": 2, "c": "xyz", "d": 100},
        )
        lookup_source = DictLookupSource(
            name="gerenic_lookup",
            rows=rows,
            keys=["a", "d"],
        )
        self.assertDictEqual(rows[0], lookup_source.get(1, 2))
        self.assertDictEqual(rows[0], lookup_source.get(d=2, a=1))
        self.assertDictEqual(rows[1], lookup_source.get(2, 100))
        self.assertDictEqual(rows[1], lookup_source.get(d=100, a=2))
        self.assertDictEqual({}, lookup_source.get(d=None, a=None))


class ListSourceTestCase(TestCase):
    def test_row_source(self):
        input_rows = (
            (1, "txt"),
            (2, "xyz"),
        )
        row_source = ListRowSource(column_names=("a", "b"), rows=input_rows)

        expected_rows = [
            {"a": 1, "b": "txt"},
            {"a": 2, "b": "xyz"},
        ]
        output_rows = []
        for row in row_source:
            output_rows.append(row)
        self.assertListEqual(expected_rows, output_rows)

    def test_lookup_source(self):
        input_rows = (
            (1, "txt", 2),
            (2, "xyz", 100),
        )
        lookup_source = ListLookupSource(
            name="gerenic_lookup",
            column_names=("a", "b", "c"),
            rows=input_rows,
            keys=["a", "c"],
        )

        expected_values = (
            {"a": 1, "b": "txt", "c": 2},
            {"a": 2, "b": "xyz", "c": 100},
        )

        self.assertDictEqual(expected_values[0], lookup_source.get(1, 2))
        self.assertDictEqual(expected_values[0], lookup_source.get(c=2, a=1))
        self.assertDictEqual(expected_values[1], lookup_source.get(2, 100))
        self.assertDictEqual(expected_values[1], lookup_source.get(c=100, a=2))
        self.assertRaises(ValueError, lookup_source.get, d=None, a=None)
