"""Offline contract tests for the final SQL container boundary."""
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.ops.final_sql_runtime import container, worker


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class CollectedTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "run" / "collected"
        self.cell = self.root / "cell"
        self.cell.mkdir(parents=True)
        self.config = {"case": "select-all", "engine": "alopex-server", "trial": 0,
                       "phase": "preflight", "source": {"commit": "a" * 40}}
        self.common = SimpleNamespace(read_json=lambda p: json.loads(p.read_text()),
                                      digest=digest,
                                      require=lambda condition, code: condition or
                                      (_ for _ in ()).throw(ValueError(code)))
        self.run = SimpleNamespace(root=self.root.parent, verify_collection=lambda: None)
        write_json(self.root / "exit.json", {"ExitCode": 0, "OOMKilled": False})
        write_json(self.root / "environment.json", {"checked": True})
        write_json(self.root / "generated-permissions.json", {"checked": True})
        self.result = {"trial": 0, "subject_identity": {"source": self.config["source"]},
                       "result_sha256": worker.result_digest("select-all")}
        self.raw = [{"sample": 0}]
        write_json(self.cell / "raw.ndjson", self.raw[0])
        self.result["raw_sha256"] = digest(self.cell / "raw.ndjson")
        write_json(self.cell / "resources.json", {"schema": "resource"})
        write_json(self.cell / "measurement.json", self.result)
        write_json(self.cell / "status.json", {"status": "passed", "worker_error": None,
            "stop_error": None, "server_stopped": True, "server_identity": {"pid": 12},
            "measurement": {"phase": "preflight",
                            "result_sha256": digest(self.cell / "measurement.json"),
                            "raw_sha256": self.result["raw_sha256"]}})
        write_json(self.root / "result.json", {"schema": "alopex.native-sql-measurement/v1",
            "status": "passed", "assets_digest": "b" * 64, "source": self.config["source"],
            "case": "select-all", "engine": "alopex-server", "trial": 0, "phase": "preflight",
            "cell_status_sha256": digest(self.cell / "status.json")})

    def verify(self, kind):
        with patch.object(worker, "verify_cell") as verify_cell:
            value = container.verify_collected(self.run, self.config, kind, "b" * 64,
                                               common=self.common)
            verify_cell.assert_called_once()
            return value

    def test_preflight_requires_saved_exact_rows(self):
        with self.assertRaisesRegex(ValueError, "COLLECT"):
            self.verify("preflight")
        write_json(self.cell / "exact-result.json", {"rows": 34250,
            "result": worker.expected("select-all")})
        status = json.loads((self.cell / "status.json").read_text())
        status["measurement"]["exact_result_sha256"] = digest(self.cell / "exact-result.json")
        write_json(self.cell / "status.json", status)
        report = json.loads((self.root / "result.json").read_text())
        report["cell_status_sha256"] = digest(self.cell / "status.json")
        write_json(self.root / "result.json", report)
        self.assertEqual(self.verify("preflight")["rows"], 34250)

    def test_tampered_exact_result_is_rejected(self):
        write_json(self.cell / "exact-result.json", {"rows": 34250,
            "result": list(reversed(worker.expected("select-all")))})
        status = json.loads((self.cell / "status.json").read_text())
        status["measurement"]["exact_result_sha256"] = digest(self.cell / "exact-result.json")
        write_json(self.cell / "status.json", status)
        report = json.loads((self.root / "result.json").read_text())
        report["cell_status_sha256"] = digest(self.cell / "status.json")
        write_json(self.root / "result.json", report)
        with self.assertRaisesRegex(ValueError, "SQL result"):
            self.verify("preflight")

    def test_cell_returns_resource_bound_raw(self):
        self.config["phase"] = "cell"
        self.config["subject"] = "candidate"
        resource = {"schema": "alopex.sql-operation-resources/v1", "status": "observed",
                    "coverage": worker.PUBLIC_COVERAGE,
                    "client": {name: 0 for name in worker.COUNTERS},
                    "server_processes": [{name: 0 for name in worker.COUNTERS}]}
        self.raw = [{"sample": i, "seconds": .001, "resources": resource}
                    for i in range(21)]
        (self.cell / "raw.ndjson").write_text("".join(json.dumps(row) + "\n"
                                                  for row in self.raw))
        self.result["raw_sha256"] = digest(self.cell / "raw.ndjson")
        write_json(self.cell / "measurement.json", self.result)
        public = [worker.public_record(self.config, i, .001, resource)
                  for i in range(21)]
        (self.cell / "public.ndjson").write_text("".join(
            json.dumps(row, separators=(",", ":")) + "\n" for row in public))
        report = json.loads((self.root / "result.json").read_text())
        report["phase"] = "cell"
        write_json(self.root / "result.json", report)
        status = json.loads((self.cell / "status.json").read_text())
        status["measurement"]["phase"] = "cell"
        status["measurement"]["raw_sha256"] = self.result["raw_sha256"]
        status["measurement"]["result_sha256"] = digest(self.cell / "measurement.json")
        status["measurement"]["public_sha256"] = digest(self.cell / "public.ndjson")
        write_json(self.cell / "status.json", status)
        report["cell_status_sha256"] = digest(self.cell / "status.json")
        write_json(self.root / "result.json", report)
        value = self.verify("cell")
        self.assertEqual(value["resource_digest"], digest(self.cell / "resources.json"))
        self.assertEqual(value["raw"], self.raw)
        self.assertEqual(value["public"], public)

    def test_report_and_exit_must_be_complete(self):
        self.config["phase"] = "cell"
        report = json.loads((self.root / "result.json").read_text())
        report["phase"] = "cell"
        write_json(self.root / "result.json", report)
        write_json(self.root / "exit.json", {"ExitCode": 1, "OOMKilled": False})
        with self.assertRaisesRegex(ValueError, "CASE"):
            self.verify("cell")


class CallbackTests(unittest.TestCase):
    def test_deadline_budget_clamps_commands_and_rejects_expiry(self):
        calls = []
        base = SimpleNamespace(before=lambda name: calls.append(("before", name)),
                               check=lambda: calls.append(("check",)),
                               run=lambda command, **kwargs: calls.append(
                                   ("run", kwargs["timeout"])))
        now = [10]
        budget = container.DeadlineBudget(base, 15, lambda: now[0])
        budget.before("accept")
        budget.check()
        budget.run(["command"], timeout=420, log="log")
        self.assertEqual(calls[-1], ("run", 5))
        now[0] = 15
        with self.assertRaisesRegex(TimeoutError, "RUN_DEADLINE"):
            budget.before("collect")

    def test_preflight_and_matrix_run_ids_are_distinct(self):
        config = {"id": "sql-select-all-0-published"}
        self.assertEqual(container.owned_run_id("preflight", config),
                         "preflight-sql-select-all-0-published")
        self.assertEqual(container.owned_run_id("cell", config),
                         "cell-sql-select-all-0-published")
        with self.assertRaises(ValueError):
            container.owned_run_id("other", config)

    def test_cleanup_collects_before_owned_deletion(self):
        events = []
        class Run:
            def __init__(self, output, run_id):
                self.path = Path(output) / "run.json"
                self.path.touch()
                self.record = {"phase": "stopped"}
            def load(self):
                events.append("load")
                return self
            def clean(self, inspect, remove):
                events.append("clean")
        def require(value, code):
            if not value:
                raise ValueError(code)
        with tempfile.TemporaryDirectory() as directory:
            staged = Path(directory) / "assets.json"
            staged.touch()
            profile = {"assets": str(staged), "private_output": directory, "run_id": "run"}
            runtime = (SimpleNamespace(require=require), None, None,
                       SimpleNamespace(collect=lambda *_: events.append("collect")),
                       SimpleNamespace(Run=Run))
            callbacks = container.ContainerCallbacks(None, runtime=runtime)
            row = {"id": "sql-select-all-0-published"}
            callbacks.active = (profile, SimpleNamespace(inspect=None, remove=None),
                                object(), dict(row), "preflight")
            callbacks.cleanup(staged, "preflight", row)
            self.assertEqual(events, ["load", "collect", "load", "clean"])
            self.assertIsNone(callbacks.active)

    def test_execute_narrows_reviewed_profile_and_uses_owned_entrypoint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            staged, profile_path = root / "assets.json", root / "profile.json"
            write_json(staged, {"schema": "assets"})
            config = {"id": "sql-select-all-0-published", "phase": "preflight"}
            write_json(root / "measurement.json", config)
            write_json(profile_path, {"schema": "profile"})
            profile = {"assets": str(staged), "private_output": str(root),
                       "run_id": "preflight-sql-select-all-0-published",
                       "platform": "linux/amd64", "limits": {"case_seconds": 300,
                       "run_seconds": 420}, "storage": {}}
            calls = []
            def require(value, code):
                if not value:
                    raise ValueError(code)
            common = SimpleNamespace(require=require, read_json=lambda p: json.loads(p.read_text()),
                load_profile=lambda _: profile,
                verify_assets=lambda *_: None)
            class Docker:
                def __init__(self, selected):
                    self.profile = selected
                def arguments(self, *args):
                    return ["create", "image"]
                def validate_container(self, name):
                    return {"Config": {"Entrypoint": ["python3"],
                                       "Cmd": ["-B", "/input/controller.py"]}}
            runtime = (common, SimpleNamespace(Budget=lambda _: object()),
                SimpleNamespace(Docker=Docker),
                SimpleNamespace(execute=lambda selected, docker, budget, cases:
                    calls.append((selected, docker.arguments(None, "image", None, None, None), cases))),
                SimpleNamespace())
            callbacks = container.ContainerCallbacks(lambda kind, config: profile_path,
                                                      runtime=runtime)
            result = callbacks.execute(staged, config,
                kind="preflight", worker_timeout_seconds=100.5, run_timeout_seconds=120.5)
            self.assertEqual(result, "preflight-sql-select-all-0-published")
            self.assertEqual(calls[0][0]["limits"]["case_seconds"], 101)
            self.assertEqual(calls[0][0]["limits"]["run_seconds"], 121)
            self.assertEqual(calls[0][1][-4:], ["python3", "image", "-B", "/input/controller.py"])
            self.assertEqual(profile["limits"], {"case_seconds": 300, "run_seconds": 420})


if __name__ == "__main__":
    unittest.main()
