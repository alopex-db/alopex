"""Offline SQL diagnostic coverage; no Docker or network calls."""
import sys
from pathlib import Path as _BootstrapPath
_OPS = _BootstrapPath(__file__).resolve().parents[1]
sys.path.insert(0, str(_OPS))
sys.path.insert(0, str(_OPS.parent / "test-env"))

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

HERE = Path(__file__).resolve().parents[1]
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "scripts/test-env"))

import diagnostic_container as lane
from common import digest
from orchestrate import ORACLE
from diagnostic import CLIENT_RECORDS_PER_CELL

SOURCE = lane.SOURCE


class FakeRun:
    def __init__(self, root, run_id):
        self.root = Path(root) / run_id
        self.root.mkdir(parents=True)
        self.path = self.root / "run.json"
        self.path.write_text("{}")
        self.record = {"phase": "preparing"}
        self.clean_error = None
        self.verified = 0
    def load(self):
        return self
    def clean(self, inspect, remove):
        if self.clean_error:
            raise self.clean_error
        self.record["phase"] = "deleted"
    def verify_collection(self):
        self.verified += 1


class Budget:
    def __init__(self):
        self.config = {"poll_seconds": 10}
        self.samples = iter([10_000_000_000, 10_100_000_000])
    def used(self):
        return next(self.samples)


def profile(root):
    assets = root / "assets.json"
    assets.write_text("{}")
    return {"run_id": "diagnostic-1", "private_output": str(root / "runs"),
            "assets": str(assets),
            "limits": {"case_seconds": 300, "run_seconds": 420, "stop_seconds": 10},
            "storage": {"poll_seconds": 10, "stop_bytes": 40_000_000_000,
                        "reserve_bytes": 1_000_000_000}}


def config():
    return {"source": SOURCE, "engine": "alopex-server", "case": "aggregate",
            "trial": 0, "mode": "off"}


class ContainerTests(unittest.TestCase):
    def test_success_uses_300_420_and_10_second_scan_then_deletes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            p = profile(root)
            run = FakeRun(p["private_output"], p["run_id"])
            budget = Budget()
            def collect(*args):
                run.record["phase"] = "collected"
            value = ({"case": "aggregate", "trial": 0, "mode": "off"}, [], [])
            with patch.object(lane, "Run", return_value=run), \
                 patch.object(lane, "MeasurementDocker", return_value=Mock()), \
                 patch.object(lane.envctl, "execute"), \
                 patch.object(lane.envctl, "collect", side_effect=collect):
                self.assertEqual(lane.execute_cell(p, budget, config(),
                                                   verify=lambda *a: value), value)
            self.assertEqual(run.record["phase"], "deleted")
            self.assertEqual(run.verified, 1)
            p["limits"]["run_seconds"] = 421
            with patch.object(lane.envctl, "execute") as execute:
                with self.assertRaises(Exception):
                    lane.execute_cell(p, Budget(), config())
                execute.assert_not_called()

    def test_collection_and_cleanup_errors_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            p = profile(root)
            run = FakeRun(p["private_output"], p["run_id"])
            with patch.object(lane, "Run", return_value=run), \
                 patch.object(lane, "MeasurementDocker", return_value=Mock()), \
                 patch.object(lane.envctl, "execute", side_effect=ValueError("CELL")), \
                 patch.object(lane.envctl, "collect", side_effect=ValueError("COLLECT")):
                with self.assertRaisesRegex(ValueError, "CELL") as caught:
                    lane.execute_cell(p, Budget(), config())
            self.assertTrue(getattr(caught.exception, "cleanup_errors", []))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            p = profile(root)
            run = FakeRun(p["private_output"], p["run_id"])
            run.clean_error = ValueError("CLEANUP")
            with patch.object(lane, "Run", return_value=run), \
                 patch.object(lane, "MeasurementDocker", return_value=Mock()), \
                 patch.object(lane.envctl, "execute"), \
                 patch.object(lane.envctl, "collect"):
                with self.assertRaisesRegex(ValueError, "CLEANUP"):
                    lane.execute_cell(p, Budget(), config(), verify=lambda *a: ({}, [], []))

    def test_collected_client_server_hashes_and_raw_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = FakeRun(root, "cell-1")
            cell = run.root / "collected/cell"
            cell.mkdir(parents=True)
            raw = [{"sample": i, "seconds": .1} for i in range(21)]
            def write(name, value, ndjson=False):
                path = cell / name
                path.write_text("".join(json.dumps(x) + "\n" for x in value)
                                if ndjson else json.dumps(value))
                return digest(path)
            raw_hash = write("raw.ndjson", raw, True)
            client_rows = [{} for _ in range(CLIENT_RECORDS_PER_CELL)]
            client_hash = write("client.ndjson", client_rows, True)
            server_hash = write("server-timing.json", [])
            write("resources.json", {})
            result = {"case": "aggregate", "trial": 0, "mode": "off",
                      "subject_identity": {"source": SOURCE},
                      "result_sha256": ORACLE["aggregate"], "raw_sha256": raw_hash}
            result_hash = write("measurement.json", result)
            status_hash = write("status.json", {"status": "passed", "worker_error": None,
                                               "stop_error": None,
                                               "measurement": {"result_sha256": result_hash,
                                                               "raw_sha256": raw_hash,
                                                               "client_sha256": client_hash}})
            report = {"schema": "alopex.sql-diagnostic-measurement/v1", "status": "passed",
                      "assets_digest": digest(root / "assets.json") if (root / "assets.json").exists() else "a" * 64,
                      "source": SOURCE, "case": "aggregate", "engine": "alopex-server",
                      "trial": 0, "mode": "off", "cell_status_sha256": status_hash,
                      "server_timing_sha256": server_hash}
            (run.root / "collected/result.json").write_text(json.dumps(report))
            with patch.object(lane, "verify_cell"):
                summary, clients, servers = lane.verify_collected(run, config(),
                                                                    report["assets_digest"])
                self.assertEqual(len(summary["raw_seconds"]), 21)
                self.assertEqual((len(clients), servers), (370, []))
                report["server_timing_sha256"] = "0" * 64
                (run.root / "collected/result.json").write_text(json.dumps(report))
                with self.assertRaises(Exception):
                    lane.verify_collected(run, config(), report["assets_digest"])


if __name__ == "__main__":
    unittest.main()
