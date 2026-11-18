"""Offline worker boundary tests using a fake qualified subject and runner."""
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

from . import adapter, oracle, worker


HEX = "1" * 63 + "2"
SUBJECT = {"source": {"commit": "a" * 40, "tree": "b" * 40, "profile": "release"},
           "server": {"pid": 201, "start_ticks": 301, "url": "http://127.0.0.1:1234"}}
COUNTERS = {"user_cpu_seconds": 0.1, "system_cpu_seconds": 0.2,
            "rss_before_bytes": 1024, "rss_after_bytes": 2048,
            "read_bytes": 0, "write_bytes": 0, "cancelled_write_bytes": 0}


class Observer:
    manifest_sha256 = HEX
    pid = 201
    start = 301

    def begin(self):
        return object()

    def end(self, before):
        return {"schema": "alopex.sql-operation-resources/v1", "status": "observed",
                "coverage": "stable_pid_namespace_allocation", "manifest_sha256": HEX,
                "client": {"pid": 101, "start_ticks": 401, **COUNTERS},
                "server_processes": [{"pid": 201, "start_ticks": 301, **COUNTERS}]}


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        os.chmod(root, 0o700)
        self.config = {"schema": "alopex.sql-four-case-pair/v1", "case": "insert-rollback",
                       "engine": "alopex-server", "modes": ["on", "off"], "trial": 1,
                       "source": SUBJECT["source"], "assets_digest": HEX,
                       "same_job_id": "123", "fixture_sha256": "3" * 63 + "4",
                       "outputs": {mode: {kind: str(root / f"{mode}.{kind}")
                                         for kind in ("raw", "client", "result")}
                                   for mode in ("on", "off")}}
        self.observer = Observer()
        self.connect = object()
        self.remote_type = type("Remote", (), {})

    def fake_measure(self, parity, case, trial, raw_paths, **kwargs):
        self.assertIs(kwargs["observer"], self.observer)
        self.assertEqual(kwargs["url"], SUBJECT["server"]["url"])
        self.assertIs(kwargs["connect"], self.connect)
        self.assertIs(kwargs["remote_type"], self.remote_type)
        summaries = {}
        clients = {}
        for mode in ("on", "off"):
            with worker.runner._open_private_raw(raw_paths[mode]) as stream:
                for sample in range(21):
                    resource = self.observer.end(None)
                    if hasattr(self, "mutate"):
                        self.mutate(mode, sample, resource)
                    row = {"sample": sample, "seconds": 0.001 * (sample + 1),
                           "pair_ordinal": 2 * sample + adapter.pair_order(trial, sample).index(mode),
                           "resources": resource}
                    stream.write(json.dumps(row) + "\n")
            summaries[mode] = {"case": case, "engine": "alopex-server", "server_client": True,
                               "fixture_rows": 1, "warmups": 3, "samples": 21,
                               "result_sha256": oracle.ORACLE_SHA256[case],
                               "rollback_sha256": oracle.ROLLBACK_SHA256,
                               "latency_p50_ms": 11}
            clients[mode] = [{"phase": "measured", "result_sha256": oracle.ORACLE_SHA256[case]}] * \
                adapter.expected_client_records(case, mode)
        return summaries, clients

    def run_worker(self):
        with patch.dict(os.environ, {"ALOPEX_PERF_SERVER_URL": SUBJECT["server"]["url"]}), \
                patch.object(worker.runner, "measure_pair", side_effect=self.fake_measure):
            return worker.run_pair(self.config, subject=SUBJECT, observer=self.observer,
                parity=object(), connect=self.connect, remote_type=self.remote_type,
                txn_mode=object(), resource_digest=HEX)

    def test_writes_private_audit_cells_with_rollback_identity(self):
        cells = self.run_worker()
        for mode in ("on", "off"):
            paths = self.config["outputs"][mode]
            self.assertEqual(json.loads(Path(paths["result"]).read_text()), cells[mode])
            self.assertEqual(cells[mode]["rollback_sha256"], oracle.ROLLBACK_SHA256)
            self.assertEqual(cells[mode]["result_sha256"], oracle.ORACLE_SHA256["insert-rollback"])
            self.assertEqual(len(Path(paths["raw"]).read_text().splitlines()), 21)
            self.assertEqual(len(Path(paths["client"]).read_text().splitlines()),
                             adapter.expected_client_records("insert-rollback", mode))
            self.assertEqual({stat.S_IMODE(Path(path).stat().st_mode) for path in paths.values()},
                             {0o600})

    def test_rejects_replaced_server_before_result_publication(self):
        def change(mode, sample, resource):
            if mode == "off" and sample == 7:
                resource["server_processes"][0]["start_ticks"] += 1
        self.mutate = change
        with self.assertRaisesRegex(ValueError, "SERVER_IDENTITY"):
            self.run_worker()
        self.assertFalse(any(Path(cell["result"]).exists() for cell in self.config["outputs"].values()))

    def test_accepts_stable_observed_server_child(self):
        def add_child(mode, sample, resource):
            resource["server_processes"].append({"pid": 202, "start_ticks": 302,
                                                  **COUNTERS})
        self.mutate = add_child
        self.assertEqual(set(self.run_worker()), {"on", "off"})

    def test_rejects_server_child_change_across_samples(self):
        def change_child(mode, sample, resource):
            resource["server_processes"].append({"pid": 202,
                "start_ticks": 303 if mode == "off" and sample == 7 else 302,
                **COUNTERS})
        self.mutate = change_child
        with self.assertRaisesRegex(ValueError, "SERVER_IDENTITY"):
            self.run_worker()

    def test_rejects_incomplete_resource_window(self):
        def change(mode, sample, resource):
            if mode == "on" and sample == 3:
                resource["status"] = "failed"
        self.mutate = change
        with self.assertRaisesRegex(ValueError, "RESOURCE_COVERAGE"):
            self.run_worker()

    def test_rejects_subject_and_observer_mismatch_before_runner(self):
        self.config["source"] = {**SUBJECT["source"], "tree": "c" * 40}
        with self.assertRaisesRegex(ValueError, "SUBJECT_BINDING"):
            self.run_worker()
        self.config["source"] = SUBJECT["source"]
        self.observer.pid += 1
        with self.assertRaisesRegex(ValueError, "RESOURCE_OBSERVER"):
            self.run_worker()

    def test_rejects_changed_manifest_and_unowned_output_parent(self):
        manifest = Path(self.temp.name) / "manifest.json"
        manifest.write_text("changed")
        with patch.dict(os.environ, {"ALOPEX_PERF_SERVER_URL": SUBJECT["server"]["url"]}), \
                patch.object(worker.runner, "measure_pair", side_effect=self.fake_measure):
            with self.assertRaisesRegex(ValueError, "RESOURCE_MANIFEST_CHANGED"):
                worker.run_pair(self.config, subject=SUBJECT, observer=self.observer,
                    parity=object(), connect=self.connect, remote_type=self.remote_type,
                    txn_mode=object(), resource_digest=HEX, manifest_path=manifest)
        self.assertFalse(any(Path(cell["result"]).exists() for cell in self.config["outputs"].values()))
        os.chmod(self.temp.name, 0o755)
        for cell in self.config["outputs"].values():
            for name in cell:
                cell[name] += ".new"
        with self.assertRaisesRegex(ValueError, "OUTPUT_PARENT"):
            self.run_worker()


if __name__ == "__main__":
    unittest.main()
