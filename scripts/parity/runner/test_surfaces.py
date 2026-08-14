from __future__ import annotations

import unittest
from types import SimpleNamespace

from .surfaces import GrpcSurface


class _Value:
    def __init__(self, kind: str, value: object) -> None:
        self._kind = kind
        setattr(self, kind, value)

    def WhichOneof(self, _name: str) -> str:
        return self._kind


def _row(*values: object) -> SimpleNamespace:
    return SimpleNamespace(
        values=[_Value("int_value", value) for value in values]
    )


class GrpcQueryResponseDecodingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.surface = GrpcSurface("unused", proto_path=SimpleNamespace())

    def test_decodes_current_sql_result_set_envelope(self) -> None:
        response = SimpleNamespace(
            columns=[SimpleNamespace(name="id"), SimpleNamespace(name="score")],
            rows=[_row(1, 20), _row(2, 30)],
        )

        columns, rows = self.surface._decode_query_responses([response])

        self.assertEqual(columns, ["id", "score"])
        self.assertEqual(rows, [[1, 20], [2, 30]])

    def test_decodes_legacy_row_stream_without_columns(self) -> None:
        columns, rows = self.surface._decode_query_responses([_row(1), _row(2)])

        self.assertIsNone(columns)
        self.assertEqual(rows, [[1], [2]])


if __name__ == "__main__":
    unittest.main()
