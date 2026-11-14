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
from diagnostic import CLIENT_RECORDS_OFF_CELL, pair_order

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
            "trial": 0, "modes": ["on", "off"]}


class ContainerTests(unittest.TestCase):
    def test_success_uses_300_420_and_10_second_scan_then_deletes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            p = profile(root)
            run = FakeRun(p["private_output"], p["run_id"])
            budget = Budget()
            def collect(*args):
                run.record["phase"] = "collected"
            value = ([{"case": "aggregate", "trial": 0, "mode": mode}
                      for mode in ("on", "off")], [], [])
            with patch.object(lane, "Run", return_value=run), \
                 patch.object(lane, "MeasurementDocker", return_value=Mock()), \
                 patch.object(lane.envctl, "execute"), \
                 patch.object(lane.envctl, "collect", side_effect=collect):
                self.assertEqual(lane.execute_pair(p, budget, config(),
                                                   verify=lambda *a: value), value)
            self.assertEqual(run.record["phase"], "deleted")
            self.assertEqual(run.verified, 1)
            p["limits"]["run_seconds"] = 421
            with patch.object(lane.envctl, "execute") as execute:
                with self.assertRaises(Exception):
                    lane.execute_pair(p, Budget(), config())
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
                    lane.execute_pair(p, Budget(), config())
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
                    lane.execute_pair(p, Budget(), config(), verify=lambda *a: ([], [], []))

    def test_collected_client_server_hashes_and_raw_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = FakeRun(root, "cell-1")
            pair = run.root / "collected/pair"
            pair.mkdir(parents=True)
            server = {"pid": 10, "start_ticks": 20}
            def write(name, value, ndjson=False):
                path = pair / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("".join(json.dumps(x) + "\n" for x in value)
                                if ndjson else json.dumps(value))
                return digest(path)
            server_hash = write("server-timing.json", [])
            write("resources.json", {})
            cells = {}
            for mode in ("on", "off"):
                raw = [{"sample": i, "seconds": .1,
                        "pair_ordinal": 2 * i + pair_order(0, i).index(mode),
                        "resources": {"server_processes": [server]}} for i in range(21)]
                raw_hash = write(f"{mode}/raw.ndjson", raw, True)
                client_rows = ([{} for _ in range((370 if mode == "on" else CLIENT_RECORDS_OFF_CELL) - 21)] +
                               [{"phase": "measured", "sample": i, "outer_ns": 100_000_000,
                                 "pair_ordinal": 2 * i + pair_order(0, i).index(mode)}
                                for i in range(21)])
                client_hash = write(f"{mode}/client.ndjson", client_rows, True)
                result = {"case": "aggregate", "trial": 0, "mode": mode,
                          "subject_identity": {"source": SOURCE},
                          "result_sha256": ORACLE["aggregate"], "raw_sha256": raw_hash,
                          "pair_server": server, "fixture_sha256": ORACLE["aggregate"],
                          "pair_order": [["off", "on"] for _ in range(21)]}
                result_hash = write(f"{mode}/measurement.json", result)
                cells[mode] = {"result_sha256": result_hash, "raw_sha256": raw_hash,
                               "client_sha256": client_hash}
            status_hash = write("status.json", {"status": "passed", "worker_error": None,
                                               "stop_error": None,
                                               "measurement": {"cells": cells, "server": server,
                                                               "fixture_sha256": ORACLE["aggregate"]}})
            report = {"schema": "alopex.sql-diagnostic-measurement/v1", "status": "passed",
                      "assets_digest": digest(root / "assets.json") if (root / "assets.json").exists() else "a" * 64,
                      "source": SOURCE, "case": "aggregate", "engine": "alopex-server",
                      "trial": 0, "modes": ["on", "off"], "cell_status_sha256": status_hash,
                      "server_timing_sha256": server_hash}
            (run.root / "collected/result.json").write_text(json.dumps(report))
            with patch.object(lane, "verify_cell"):
                summaries, clients, servers = lane.verify_collected(run, config(),
                                                                    report["assets_digest"])
                self.assertEqual([len(summary["raw_seconds"]) for summary in summaries], [21, 21])
                self.assertEqual((len(clients), servers), (395, []))
                off_client = pair / "off/client.ndjson"
                original_client = off_client.read_bytes()
                changed = [json.loads(line) for line in original_client.splitlines()]
                changed[-1]["outer_ns"] += 1
                off_client.write_text("".join(json.dumps(row) + "\n" for row in changed))
                status_path = pair / "status.json"
                status = json.loads(status_path.read_text())
                status["measurement"]["cells"]["off"]["client_sha256"] = digest(off_client)
                status_path.write_text(json.dumps(status))
                report["cell_status_sha256"] = digest(status_path)
                (run.root / "collected/result.json").write_text(json.dumps(report))
                with self.assertRaisesRegex(Exception, "IDENTITY"):
                    lane.verify_collected(run, config(), report["assets_digest"])
                off_client.write_bytes(original_client)
                status["measurement"]["cells"]["off"]["client_sha256"] = digest(off_client)
                status_path.write_text(json.dumps(status))
                report["cell_status_sha256"] = digest(status_path)
                (run.root / "collected/result.json").write_text(json.dumps(report))
                report["server_timing_sha256"] = "0" * 64
                (run.root / "collected/result.json").write_text(json.dumps(report))
                with self.assertRaises(Exception):
                    lane.verify_collected(run, config(), report["assets_digest"])
                report["server_timing_sha256"] = server_hash
                off_path = pair / "off/measurement.json"
                off = json.loads(off_path.read_text())
                off["pair_server"] = {"pid": 11, "start_ticks": 20}
                off_path.write_text(json.dumps(off))
                status_path = pair / "status.json"
                status = json.loads(status_path.read_text())
                status["measurement"]["cells"]["off"]["result_sha256"] = digest(off_path)
                status_path.write_text(json.dumps(status))
                report["cell_status_sha256"] = digest(status_path)
                (run.root / "collected/result.json").write_text(json.dumps(report))
                with self.assertRaisesRegex(Exception, "IDENTITY"):
                    lane.verify_collected(run, config(), report["assets_digest"])


if __name__ == "__main__":
    unittest.main()
