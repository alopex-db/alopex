import json
from pathlib import Path

import pytest

from alopex import Database, Metric, TxnMode


def _load_expected() -> dict:
    fixture_path = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "cross_surface_expected.json"
    with fixture_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _load_cluster_expected() -> dict:
    fixture_path = (
        Path(__file__).resolve().parents[3]
        / "tests"
        / "fixtures"
        / "cluster_status_cross_surface_expected.json"
    )
    with fixture_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _stable_cluster_status_fields(status: dict) -> dict:
    return {
        "schema_version": status["schema_version"],
        "mode": status["mode"],
        "identity": {
            "node_id": status["identity"]["node_id"],
            "cluster_id": status["identity"]["cluster_id"],
            "advertised_endpoint": status["identity"]["advertised_endpoint"],
            "role": status["identity"]["role"],
            "lifecycle_state": status["identity"]["lifecycle_state"],
            "metadata_schema_version": status["identity"]["metadata_schema_version"],
            "update_epoch": status["identity"]["update_epoch"],
        },
        "membership": {
            "schema_version": status["membership"]["schema_version"],
            "update_epoch": status["membership"]["update_epoch"],
            "source": status["membership"]["source"],
            "members": status["membership"]["members"],
        },
        "routing_capabilities": status["routing_capabilities"],
        "metrics_summary": status["metrics_summary"],
        "degraded": status["degraded"],
        "diagnostics": [
            {"code": item["code"], "degraded": item["degraded"]}
            for item in status["diagnostics"]
        ],
    }


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


def test_python_cluster_status_accessor_shape_and_embedded_compatibility():
    db = Database.open_in_memory()

    status = db.cluster_status()
    assert status["schema_version"] == 1
    assert status["mode"] == "single_node"
    assert status["identity"]["node_id"] == "local"
    assert status["identity"]["role"] == "gateway"
    assert status["identity"]["lifecycle_state"] == "unconfigured"
    assert status["membership"]["source"] == "local_default"
    assert status["membership"]["members"] == []
    assert status["routing_capabilities"] == {
        "local_only": True,
        "future_distributed_execution_required": True,
        "scatter_gather_simulated": True,
    }
    assert status["metrics_summary"]["source"] == "live_status_surface"
    assert status["metrics_summary"]["members"] == []
    assert status["degraded"] is False
    assert status["diagnostics"] == []

    txn = db.begin(TxnMode.READ_WRITE)
    txn.put(b"cluster-status-key", b"local-value")
    txn.commit()

    with db.begin(TxnMode.READ_ONLY) as txn:
        assert txn.get(b"cluster-status-key") == b"local-value"


def test_python_cluster_status_matches_cross_surface_fixture():
    expected = _load_cluster_expected()["server_cluster_status"]["single_node"]
    db = Database.open_in_memory()

    actual = _stable_cluster_status_fields(db.cluster_status())

    _assert_with_diff(expected, actual)


def test_python_routing_diagnostics_accessor_is_read_only_local_surface():
    db = Database.new()

    diagnostics = db.routing_diagnostics()
    assert diagnostics["schema_version"] == 1
    assert diagnostics["update_epoch"] == 0
    assert diagnostics["decision"] == "local_only"
    assert diagnostics["reason"] == "single_resolved_target"
    assert diagnostics["plan_id"] == "python_embedded_local"
    assert diagnostics["roles"] == ["gateway"]
    assert diagnostics["targets"] == []
    assert diagnostics["excluded_targets"] == []
    assert diagnostics["retry_summary"] is None


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
