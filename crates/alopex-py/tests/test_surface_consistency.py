import json
from pathlib import Path

import pytest

from alopex import Database, Metric, TxnMode


def _load_expected() -> dict:
    fixture_path = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "cross_surface_expected.json"
    with fixture_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _diff_values(path: str, expected, actual, diffs: list[str]) -> None:
    if isinstance(expected, dict) and isinstance(actual, dict):
        keys = sorted(set(expected.keys()) | set(actual.keys()))
        for key in keys:
            next_path = f"{path}.{key}" if path else key
            if key not in expected:
                diffs.append(f"{next_path}: unexpected in actual")
            elif key not in actual:
                diffs.append(f"{next_path}: missing in actual")
            else:
                _diff_values(next_path, expected[key], actual[key], diffs)
        return
    if isinstance(expected, list) and isinstance(actual, list):
        if len(expected) != len(actual):
            diffs.append(f"{path}: length expected={len(expected)} actual={len(actual)}")
        for idx, (ev, av) in enumerate(zip(expected, actual)):
            _diff_values(f"{path}[{idx}]", ev, av, diffs)
        return
    if expected != actual:
        diffs.append(f"{path}: expected={expected!r} actual={actual!r}")


def _assert_with_diff(expected: dict, actual: dict) -> None:
    if expected == actual:
        return
    diffs: list[str] = []
    _diff_values("", expected, actual, diffs)
    joined = "\n  - ".join(diffs[:20])
    raise AssertionError(f"surface consistency mismatch\nexpected={expected}\nactual={actual}\ndiff:\n  - {joined}")


@pytest.mark.requires_numpy
def test_python_surface_consistency_uses_shared_expected_set(tmp_path):
    import numpy as np

    expected = _load_expected()
    db = Database.open(str(tmp_path / "surface-consistency-db"))
    txn = db.begin(TxnMode.READ_WRITE)
    txn.put(b"shared-key", b"shared-value")
    txn.upsert_vector(b"vec-a", None, np.array([1.0, 0.0], dtype=np.float32), Metric.COSINE)
    txn.upsert_vector(b"vec-b", None, np.array([0.0, 1.0], dtype=np.float32), Metric.COSINE)
    txn.commit()

    with db.begin(TxnMode.READ_ONLY) as txn:
        kv_value = txn.get(b"shared-key")
        results = txn.search_similar(np.array([1.0, 0.0], dtype=np.float32), Metric.COSINE, 1)

    actual = {
        "kv_value": kv_value.decode("utf-8") if kv_value else "",
        "python_vector_top_key": results[0].key.decode("utf-8") if results else "",
    }
    expected_subset = {
        "kv_value": expected["kv_value"],
        "python_vector_top_key": expected["python_vector_top_key"],
    }
    _assert_with_diff(expected_subset, actual)
