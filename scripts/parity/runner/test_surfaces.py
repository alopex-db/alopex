from __future__ import annotations

import os
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from .surfaces import GrpcSurface, product_env


class _Value:
    def __init__(self, kind: str, value: object) -> None:
        self._kind = kind
        setattr(self, kind, value)

    def WhichOneof(self, _name: str) -> str:
        return self._kind


def _row(*values: object) -> SimpleNamespace:
    return SimpleNamespace(values=[_Value("int_value", value) for value in values])


class ReleasedEnvironmentTests(unittest.TestCase):
    def test_removes_inherited_loader_path(self) -> None:
        with patch.dict(
            os.environ,
            {"ALOPEX_BINARY_SOURCE": "released", "LD_LIBRARY_PATH": "/repo/parser"},
            clear=False,
        ):
            env = product_env(Path("/repo"))

        self.assertIsNone(env.get("LD_LIBRARY_PATH"))


class GrpcQueryResponseDecodingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.surface = GrpcSurface("unused", proto_path=Path("/unused"))

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
