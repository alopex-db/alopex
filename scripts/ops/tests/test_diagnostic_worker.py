"""Exercise the diagnostic worker's frozen-source and original-timer boundary."""

import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock

from scripts.ops.wal_cost_next import diagnostic_worker


class DiagnosticWorkerTests(unittest.TestCase):
    def test_wrong_config_digest_stops_before_product_import(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "cell.json"
            config.write_text("{}")
            argv = ["diagnostic_worker.py", "--configuration", str(config),
                    "--config-sha256", "0" * 64, "--frozen-checkout", str(root)]
            with mock.patch.object(sys, "argv", argv), mock.patch.object(sys, "platform", "linux"), \
                 mock.patch.object(os, "getuid", return_value=1000), \
                 mock.patch.dict(os.environ, {"GITHUB_EVENT_NAME": "workflow_dispatch",
                                              "RUNNER_KIND": "github-hosted"}), \
                 mock.patch.object(diagnostic_worker.subprocess, "check_output",
                                   return_value=diagnostic_worker.FROZEN + "\n"):
                with self.assertRaisesRegex(RuntimeError, "config differs"):
                    diagnostic_worker.main()

    def test_fixed_fixture_writes_nonacceptance_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            frozen = root / "frozen"
            frozen.mkdir()
            source = frozen / "source.py"
            source.write_bytes(b"fixed source\n")
            native = root / "native.so"
            native.write_bytes(b"fixed native\n")
            output = root / "private"
            output.mkdir(mode=0o700)
            fixture = root / "fixture"
            config = root / "cell.json"
            config.write_text(json.dumps({
                "affinity": [3], "sources": {"source.py": hashlib.sha256(source.read_bytes()).hexdigest()},
                "phase": "write", "surface": "kv", "value_bytes": 16,
                "subject_manifest_sha256": "fixed-manifest", "options": {"fixed": True},
                "output": str(output), "fixture": str(fixture),
            }))
            events = []
            workload = types.ModuleType("public_api_cost")
            workload.OPTIONS = {"fixed": True}
            workload.SHAPES = (("kv", 16),)
            def original_measure(action):
                events.append("timer")
                return action(), 17
            workload.measure = original_measure
            def child(phase, path, surface, size, native_digest):
                self.assertEqual((phase, path, surface, size), ("write", fixture, "kv", 16))
                self.assertEqual(native_digest, hashlib.sha256(native.read_bytes()).hexdigest())
                class Pending:
                    def commit(self):
                        events.append("commit")
                        return "done"
                self.assertEqual(workload.measure(Pending().commit), ("done", 17))
            workload.child = child
            subject = types.ModuleType("subject_identity")
            subject.verify_subject = lambda: {"manifest_sha256": "fixed-manifest"}
            product = types.ModuleType("alopex")
            product._alopex = types.SimpleNamespace(__file__=str(native))
            rss = types.ModuleType("phase_rss")
            rss.parse_status = lambda raw: {"sample": len(raw)}
            baseline = types.ModuleType("wal_cost_commit_observer")
            baseline.observe = lambda action, timer: (timer(action), {"timer_nanoseconds": 17})
            original_read_text = Path.read_text
            def read_text(path, *args, **kwargs):
                if str(path) == "/proc/self/status":
                    return "status"
                return original_read_text(path, *args, **kwargs)
            argv = ["diagnostic_worker.py", "--configuration", str(config),
                    "--config-sha256", hashlib.sha256(config.read_bytes()).hexdigest(),
                    "--frozen-checkout", str(frozen)]
            modules = {"public_api_cost": workload, "subject_identity": subject,
                       "alopex": product, "phase_rss": rss,
                       "wal_cost_commit_observer": baseline}
            def scheduler(action, timer):
                events.append("sched-before")
                result = timer(action)
                events.append("sched-after")
                return result, {"original_timer_ns": 17, "acceptance_sample": False}
            with mock.patch.object(sys, "argv", argv), mock.patch.object(sys, "platform", "linux"), \
                 mock.patch.object(sys, "path", sys.path.copy()), \
                 mock.patch.object(os, "getuid", return_value=1000), \
                 mock.patch.object(os, "sched_getaffinity", return_value={3}, create=True), \
                 mock.patch.dict(os.environ, {"GITHUB_EVENT_NAME": "workflow_dispatch",
                                              "RUNNER_KIND": "github-hosted"}), \
                 mock.patch.object(diagnostic_worker.subprocess, "check_output",
                                   return_value=diagnostic_worker.FROZEN + "\n"), \
                 mock.patch.object(Path, "read_text", read_text), \
                 mock.patch.dict(sys.modules, modules), \
                 mock.patch.object(diagnostic_worker.time, "sleep",
                                   side_effect=lambda seconds: events.append(("hold", seconds))), \
                 mock.patch.object(diagnostic_worker, "scheduler_observe", scheduler):
                diagnostic_worker.main()
            self.assertEqual(events, ["sched-before", "timer", "commit", "sched-after",
                                      ("hold", 10)])
            observation = json.loads((output / "commit-observation.json").read_text())
            self.assertEqual(observation["timer_nanoseconds"], 17)
            self.assertEqual(observation["schedstat"]["original_timer_ns"], 17)
            self.assertFalse(observation["acceptance_sample"])


if __name__ == "__main__":
    unittest.main()
