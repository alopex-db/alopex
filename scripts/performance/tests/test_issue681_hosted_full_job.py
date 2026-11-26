"""One selected job must qualify before it acquires public inputs."""

from pathlib import Path
import hashlib
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import issue681_hosted_full_job as job


def directory(path):
    path.mkdir(mode=0o700, parents=True)
    path.chmod(0o700)
    return path


class HostedFullJobTests(unittest.TestCase):
    def setup_roots(self, root):
        workspace = directory(root / "checkout")
        task_parent = directory(root / "private-tasks")
        home_base = directory(root / "home")
        home_parent = directory(home_base / "private-home")
        return workspace, task_parent, home_base, home_parent

    def test_qualified_job_connects_one_guard_to_one_finish(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, task_parent, home_base, home_parent = self.setup_roots(
                Path(tmp).resolve())
            calls = []

            class Guard:
                def __call__(self):
                    calls.append("guard")

            guard = Guard()
            pins = {"before": {"source_commit": "a"},
                    "candidate": {"source_commit": "b"}}

            def acquire(work, task, home, *, manifest):
                self.assertEqual(work, workspace)
                self.assertEqual(task.parent.parent, task_parent)
                self.assertEqual(home.parent, home_parent)
                calls.append("acquire")
                return {"guard": guard}

            def build(acquired, work, parent):
                self.assertIs(acquired["guard"], guard)
                self.assertEqual(work, workspace)
                self.assertEqual(parent.name, "build")
                calls.append("build")
                return {"guard": guard, "pair": {"pins": pins}}

            def measure(acquired, built, work, parent, profile, finish):
                self.assertIs(acquired["guard"], built["guard"])
                self.assertEqual(work, workspace)
                self.assertEqual(finish, "rollback")
                self.assertEqual(profile, {"selected": True})
                output = directory(parent / "result") / "public.json"
                output.write_text("{}")
                output.chmod(0o600)
                calls.append("measure")
                return {"public": output, "guard": guard,
                        "finish": finish}

            with mock.patch.object(job.Path, "home", return_value=home_base), \
                 mock.patch.object(job.scoped_storage, "make_manifest",
                                   return_value=object()), \
                 mock.patch.object(job.qualify, "REPO", workspace), \
                 mock.patch.object(job.qualify, "run",
                                   side_effect=lambda _: (calls.append("qualify") or
                                       ({"case": job.qualify.CASE,
                                         "outcome": "passed"}, 0))), \
                 mock.patch.object(job.rust, "run",
                                   side_effect=lambda: (calls.append("rust") or
                                       ({"case": job.rust.CASE,
                                         "outcome": "passed"}, 0))), \
                 mock.patch.object(job.acquire, "acquire_all",
                                   side_effect=acquire), \
                 mock.patch.object(job.build_join, "build", side_effect=build), \
                 mock.patch.object(job.measure_join, "measure",
                                   side_effect=measure), \
                 mock.patch.object(job.transport, "validate",
                                   side_effect=lambda raw, **_: raw) as validate:
                result = job.execute({}, workspace, task_parent, home_parent,
                                     {"selected": True}, "rollback")
            self.assertEqual(calls, ["qualify", "rust", "acquire", "build",
                                     "measure", "guard"])
            self.assertEqual(result["case"], job.CASE)
            self.assertEqual(result["outcome"], "passed")
            self.assertTrue(result["public"].is_file())
            self.assertEqual(result["public_sha256"], hashlib.sha256(b"{}").hexdigest())
            self.assertEqual(result["pins"], pins)
            self.assertIsNot(result["pins"], pins)
            self.assertEqual(validate.call_args.kwargs["expected_pins"], pins)
            self.assertEqual(validate.call_args.kwargs["finish"], "rollback")
            self.assertTrue(result["task_root"].is_relative_to(task_parent))
            self.assertTrue(result["home_root"].is_relative_to(home_parent))

    def test_failed_preflight_never_acquires_or_creates_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, task_parent, home_base, home_parent = self.setup_roots(
                Path(tmp).resolve())
            for failed in ("qualify", "rust"):
                with self.subTest(failed=failed), \
                     mock.patch.object(job.Path, "home", return_value=home_base), \
                     mock.patch.object(job.qualify, "REPO", workspace), \
                     mock.patch.object(job.qualify, "run", return_value=(
                         {"case": job.qualify.CASE,
                          "outcome": "failed" if failed == "qualify" else "passed"},
                         int(failed == "qualify"))), \
                     mock.patch.object(job.rust, "run", return_value=(
                         {"case": job.rust.CASE,
                          "outcome": "failed" if failed == "rust" else "passed"},
                         int(failed == "rust"))) as rust_check, \
                     mock.patch.object(job.acquire, "acquire_all") as acquire:
                    with self.assertRaisesRegex(ValueError, "^PUBLIC_JOB$"):
                        job.execute({}, workspace, task_parent, home_parent,
                                    {}, "commit")
                    acquire.assert_not_called()
                    if failed == "qualify":
                        rust_check.assert_not_called()
                self.assertEqual(list(task_parent.iterdir()), [])
                self.assertEqual(list(home_parent.iterdir()), [])

    def test_later_stage_failure_cleans_only_owned_children(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, task_parent, home_base, home_parent = self.setup_roots(
                Path(tmp).resolve())

            def fail_measure(_acquired, _built, _workspace, parent, *_):
                output = directory(parent / "partial") / "public.json"
                output.write_text("partial")
                raise ValueError("/private/path")

            with mock.patch.object(job.Path, "home", return_value=home_base), \
                 mock.patch.object(job.qualify, "REPO", workspace), \
                 mock.patch.object(job.qualify, "run", return_value=(
                     {"case": job.qualify.CASE, "outcome": "passed"}, 0)), \
                 mock.patch.object(job.rust, "run", return_value=(
                     {"case": job.rust.CASE, "outcome": "passed"}, 0)), \
                 mock.patch.object(job.acquire, "acquire_all",
                                   return_value={"guard": lambda: None}), \
                 mock.patch.object(job.build_join, "build",
                                   return_value={"guard": lambda: None}), \
                 mock.patch.object(job.measure_join, "measure",
                                   side_effect=fail_measure):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_JOB$"):
                    job.execute({}, workspace, task_parent, home_parent,
                                {}, "commit")
            self.assertEqual(list(task_parent.iterdir()), [])
            self.assertEqual(list(home_parent.iterdir()), [])

    def test_mismatched_checkout_fails_before_qualification(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, task_parent, home_base, home_parent = self.setup_roots(
                Path(tmp).resolve())
            with mock.patch.object(job.Path, "home", return_value=home_base), \
                 mock.patch.object(job.qualify, "REPO", workspace / "other"), \
                 mock.patch.object(job.qualify, "run") as qualify:
                with self.assertRaisesRegex(ValueError, "^PUBLIC_JOB$"):
                    job.execute({}, workspace, task_parent, home_parent,
                                {}, "commit")
            qualify.assert_not_called()

    def test_noncanonical_snapshot_fails_and_removes_public_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, task_parent, home_base, home_parent = self.setup_roots(
                Path(tmp).resolve())
            guard = lambda: None

            def measure(_acquired, _built, _workspace, parent, *_):
                output = directory(parent / "result") / "public.json"
                output.write_bytes(b'{"local_path":"/private/host"}\n')
                output.chmod(0o600)
                return {"public": output, "guard": guard,
                        "finish": "commit"}

            with mock.patch.object(job.Path, "home", return_value=home_base), \
                 mock.patch.object(job.qualify, "REPO", workspace), \
                 mock.patch.object(job.qualify, "run", return_value=(
                     {"case": job.qualify.CASE, "outcome": "passed"}, 0)), \
                 mock.patch.object(job.rust, "run", return_value=(
                     {"case": job.rust.CASE, "outcome": "passed"}, 0)), \
                 mock.patch.object(job.acquire, "acquire_all",
                                   return_value={"guard": guard}), \
                 mock.patch.object(job.build_join, "build",
                                   return_value={"pair": {"pins": {}},
                                                 "guard": guard}), \
                 mock.patch.object(job.measure_join, "measure",
                                   side_effect=measure):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_JOB$"):
                    job.execute({}, workspace, task_parent, home_parent,
                                {}, "commit")
            self.assertEqual(list(task_parent.iterdir()), [])
            self.assertEqual(list(home_parent.iterdir()), [])

    def test_hosted_measure_wires_install_locked_vendor_build_measure_and_audit(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, task_parent, home_base, home_parent = self.setup_roots(
                Path(tmp).resolve())
            events = []

            class Guard:
                def __call__(self):
                    events.append("guard")

            guard = Guard()
            pins = {"before": {"source_commit": "a"},
                    "candidate": {"source_commit": "b"}}

            def fake_install(install_guard, log_path):
                self.assertTrue(callable(install_guard))
                self.assertEqual(log_path.name, "rustup-install.private.log")
                events.append("install")

            def fake_acquire(work, task, home, *, manifest, vendor_mode):
                self.assertEqual(work, workspace)
                self.assertEqual(vendor_mode, "cargo_vendor_locked")
                events.append("acquire")
                return {"guard": guard}

            def fake_build(acquired, work, parent):
                self.assertIs(acquired["guard"], guard)
                events.append("build")
                return {"guard": guard, "pair": {"pins": pins}}

            def fake_measure(acquired, built, work, parent, profile, finish,
                             *, budget_check=None):
                self.assertEqual(finish, "commit")
                self.assertTrue(callable(budget_check))
                output = directory(parent / "result") / "public.json"
                output.write_bytes(b'{"ok":true}\n')
                output.chmod(0o600)
                events.append("measure")
                return {"public": output, "guard": guard, "finish": finish}

            stages = []
            with mock.patch.object(job.Path, "home", return_value=home_base), \
                 mock.patch.object(job.scoped_storage, "make_manifest",
                                   return_value=object()), \
                 mock.patch.object(job.qualify, "REPO", workspace), \
                 mock.patch.object(job.capacity, "HostStorageGuard",
                                   return_value=guard), \
                 mock.patch.object(job.rust, "install_pinned_minimal",
                                   side_effect=fake_install), \
                 mock.patch.object(job.acquire, "acquire_all",
                                   side_effect=fake_acquire), \
                 mock.patch.object(job.build_join, "build",
                                   side_effect=fake_build), \
                 mock.patch.object(job.measure_join, "measure",
                                   side_effect=fake_measure), \
                 mock.patch.object(job.transport, "validate",
                                   side_effect=lambda raw, **_: raw):
                result = job.execute_hosted_measure(
                    {"RUNNER_TEMP": str(task_parent)}, workspace, task_parent,
                    home_parent, {"selected": True}, "commit",
                    on_stage=stages.append)
            self.assertEqual(events, ["install", "acquire", "build", "measure", "guard"])
            self.assertEqual(stages, ["toolchain_install", "acquire", "build",
                                      "measure", "audit"])
            self.assertEqual(result["outcome"], "passed")
            self.assertEqual(result["finish"], "commit")
            self.assertEqual(result["public_bytes"], b'{"ok":true}\n')
            self.assertEqual(result["public_sha256"],
                             hashlib.sha256(b'{"ok":true}\n').hexdigest())

    def test_hosted_measure_reports_failing_stage_and_cleans_owned_trees(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, task_parent, home_base, home_parent = self.setup_roots(
                Path(tmp).resolve())
            with mock.patch.object(job.Path, "home", return_value=home_base), \
                 mock.patch.object(job.scoped_storage, "make_manifest",
                                   return_value=object()), \
                 mock.patch.object(job.qualify, "REPO", workspace), \
                 mock.patch.object(job.capacity, "HostStorageGuard",
                                   return_value=lambda: None), \
                 mock.patch.object(job.rust, "install_pinned_minimal",
                                   side_effect=ValueError("/private/leak")):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_JOB:toolchain_install$"):
                    job.execute_hosted_measure(
                        {"RUNNER_TEMP": str(task_parent)}, workspace,
                        task_parent, home_parent, {}, "commit")
            self.assertEqual(list(task_parent.iterdir()), [])
            self.assertEqual(list(home_parent.iterdir()), [])

    def test_cli_emits_allowlisted_acquire_substage_detail_on_failure(self):
        import contextlib
        import io
        import json

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            workspace = directory(root / "checkout")
            runner_temp = directory(root / "runner-temp")
            home_base = directory(root / "home")
            stdout = io.StringIO()

            def fake_exec(env, ws, tp, hp, profile, finish, *,
                          expected_job, on_stage, on_substage, on_case=None):
                on_stage("toolchain_install")
                on_substage("sources", "passed", None, 2)
                on_substage("cargo_vendor", "failed", "FETCH_NORMALIZE_TIMEOUT", 11)
                raise ValueError("PUBLIC_JOB:acquire")

            env = {
                "HNSW_FINISH": "commit",
                "RUNNER_TEMP": str(runner_temp),
                "GITHUB_WORKSPACE": str(workspace),
            }
            with mock.patch.object(job.Path, "home", return_value=home_base), \
                 mock.patch.object(job.identity_gate, "run",
                                   return_value=({"case": "HNSW_HOSTED_ADMISSION",
                                                  "outcome": "passed"}, 0)), \
                 mock.patch.object(job, "execute_hosted_measure", side_effect=fake_exec), \
                 contextlib.redirect_stdout(stdout):
                rc = job.run_hosted_measure_cli(env)
            self.assertEqual(rc, 1)
            lines = [json.loads(line) for line in stdout.getvalue().splitlines()]
            final = lines[-1]
            self.assertEqual(final["outcome"], "failed")
            self.assertEqual(final["failed_stage"], "acquire")
            self.assertEqual(final["failed_substage"], "cargo_vendor")
            self.assertEqual(final["detail_code"], "FETCH_NORMALIZE_TIMEOUT")
            self.assertEqual(final["acquire_substages"],
                             {"sources": "passed", "cargo_vendor": "failed"})
            self.assertNotIn(str(root), stdout.getvalue())

    def test_cli_emits_allowlisted_build_substage_detail_on_failure(self):
        import contextlib
        import io
        import json

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            workspace = directory(root / "checkout")
            runner_temp = directory(root / "runner-temp")
            home_base = directory(root / "home")
            stdout = io.StringIO()

            def fake_exec(env, ws, tp, hp, profile, finish, *,
                          expected_job, on_stage, on_substage, on_case=None):
                on_stage("toolchain_install")
                on_substage("sources", "passed", None, 2)
                on_stage("acquire")
                on_substage("before", "failed", "BUILD_ASSETS_PREFIX_BINARY", 120)
                raise ValueError("PUBLIC_JOB:build")

            env = {
                "HNSW_FINISH": "commit",
                "RUNNER_TEMP": str(runner_temp),
                "GITHUB_WORKSPACE": str(workspace),
            }
            with mock.patch.object(job.Path, "home", return_value=home_base), \
                 mock.patch.object(job.identity_gate, "run",
                                   return_value=({"case": "HNSW_HOSTED_ADMISSION",
                                                  "outcome": "passed"}, 0)), \
                 mock.patch.object(job, "execute_hosted_measure", side_effect=fake_exec), \
                 contextlib.redirect_stdout(stdout):
                rc = job.run_hosted_measure_cli(env)
            self.assertEqual(rc, 1)
            lines = [json.loads(line) for line in stdout.getvalue().splitlines()]
            final = lines[-1]
            self.assertEqual(final["outcome"], "failed")
            self.assertEqual(final["failed_stage"], "build")
            self.assertEqual(final["failed_substage"], "before")
            self.assertEqual(final["detail_code"], "BUILD_ASSETS_PREFIX_BINARY")
            self.assertEqual(final["build_substages"], {"before": "failed"})
            self.assertNotIn(str(root), stdout.getvalue())

    def test_hosted_measure_100k_projection_stops_early_and_cleans_trees(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, task_parent, home_base, home_parent = self.setup_roots(
                Path(tmp).resolve())
            guard = lambda: None
            pins = {"before": {"source_commit": "a"},
                    "candidate": {"source_commit": "b"}}

            def fake_measure(acquired, built, work, parent, profile, finish,
                             *, budget_check=None, on_substage=None, on_case=None):
                # Pair 0 at 100k takes 2,500s each -> 8 remaining * 2,500s = 20,000s > 19,800s soft budget
                self.assertTrue(budget_check(20, 100_000, 0, "before", 2500))
                with self.assertRaisesRegex(ValueError, "^MEASURE_TIME_BUDGET_PROJECTED$"):
                    budget_check(21, 100_000, 0, "candidate", 2500)
                raise ValueError("PUBLIC_STAGE")

            with mock.patch.object(job.Path, "home", return_value=home_base), \
                 mock.patch.object(job.scoped_storage, "make_manifest",
                                   return_value=object()), \
                 mock.patch.object(job.qualify, "REPO", workspace), \
                 mock.patch.object(job.capacity, "HostStorageGuard",
                                   return_value=guard), \
                 mock.patch.object(job.rust, "install_pinned_minimal"), \
                 mock.patch.object(job.acquire, "acquire_all",
                                   return_value={"guard": guard}), \
                 mock.patch.object(job.build_join, "build",
                                   return_value={"guard": guard, "pair": {"pins": pins}}), \
                 mock.patch.object(job.measure_join, "measure",
                                   side_effect=fake_measure):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_JOB:measure$"):
                    job.execute_hosted_measure(
                        {"RUNNER_TEMP": str(task_parent)}, workspace,
                        task_parent, home_parent, {}, "commit")
            self.assertEqual(job.SOFT_BUDGET_SECONDS, 330 * 60)
            self.assertEqual(list(task_parent.iterdir()), [])
            self.assertEqual(list(home_parent.iterdir()), [])

    def test_cli_emits_allowlisted_per_case_progress_and_completed_cases_on_budget_stop(self):
        import contextlib
        import io
        import json

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            workspace = directory(root / "checkout")
            runner_temp = directory(root / "runner-temp")
            home_base = directory(root / "home")
            stdout = io.StringIO()

            def fake_exec(env, ws, tp, hp, profile, finish, *,
                          expected_job, on_stage, on_substage, on_case):
                on_stage("toolchain_install")
                on_stage("acquire")
                on_stage("build")
                on_substage("admission_gate", "passed", None, 6)
                on_case(0, 1_000, 0, "before", 1, 900)
                on_case(1, 1_000, 0, "candidate", 1, 905)
                on_substage("cases", "failed", "MEASURE_TIME_BUDGET_PROJECTED", 3000)
                raise ValueError("PUBLIC_JOB:measure")

            env = {
                "HNSW_FINISH": "commit",
                "RUNNER_TEMP": str(runner_temp),
                "GITHUB_WORKSPACE": str(workspace),
            }
            with mock.patch.object(job.Path, "home", return_value=home_base), \
                 mock.patch.object(job.identity_gate, "run",
                                   return_value=({"case": "HNSW_HOSTED_ADMISSION",
                                                  "outcome": "passed"}, 0)), \
                 mock.patch.object(job, "execute_hosted_measure", side_effect=fake_exec), \
                 contextlib.redirect_stdout(stdout):
                rc = job.run_hosted_measure_cli(env)
            self.assertEqual(rc, 1)
            lines = [json.loads(line) for line in stdout.getvalue().splitlines()]
            case_lines = [row for row in lines if row.get("substage") == "case"]
            self.assertEqual(len(case_lines), 2)
            self.assertEqual(case_lines[0]["position"], 0)
            self.assertEqual(case_lines[0]["total_cases"], 30)
            self.assertEqual(case_lines[0]["rows"], 1_000)
            self.assertEqual(case_lines[1]["arm"], "candidate")
            final = lines[-1]
            self.assertEqual(final["failed_stage"], "measure")
            self.assertEqual(final["detail_code"], "MEASURE_TIME_BUDGET_PROJECTED")
            self.assertEqual(final["completed_cases"], 2)
            self.assertNotIn(str(root), stdout.getvalue())

    def test_downstream_30_case_ledger_pair_transport_and_export_contract(self):
        import contextlib
        import copy
        import io
        import json
        import os

        import issue681_hosted_acquire_source as acquire_source
        import issue681_public_admission as admission
        import issue681_public_pair as pair
        from test_issue681_public_pair import PINS, fixture

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            workspace = directory(root / "checkout")
            runner_temp = directory(root / "runner-temp")
            home_base = directory(root / "home")
            stdout = io.StringIO()

            pins = copy.deepcopy(PINS)
            for arm in pair.ARMS:
                src = acquire_source.PINS[arm]
                pins[arm].update(
                    source_commit=src["commit"],
                    source_tree=src["tree"],
                    example_sha256=src["example"],
                    cargo_lock_sha256=src["lock"],
                    rust_version="1.90.0",
                )
            reports = fixture("commit")
            for rep, (_, _, arm) in zip(reports, pair._order()):
                rep["source_commit"] = pins[arm]["source_commit"]
            report_iter = iter(reports)

            class RealLedgerAdmission:
                def __init__(self, profile, *, roots, log_dir):
                    self.log_dir = log_dir
                    self.log = None

                def _write(self, kind, rows=None, trial=None, arm=None):
                    value = {
                        "schema": admission.LEDGER_SCHEMA,
                        "kind": kind,
                        "rows": rows,
                        "trial": trial,
                        "arm": arm,
                        "allocated_bytes": 15_000_000_000,
                        "idle_window_seconds": 0 if kind == "case_complete" else admission.WINDOW_SECONDS,
                        "min_idle_basis_points": None if kind == "case_complete" else 9_950,
                        "status": "passed",
                    }
                    raw = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
                    os.write(self.log, raw)
                    os.fsync(self.log)

                def __enter__(self):
                    self.log = os.open(self.log_dir / "admission.jsonl",
                                       os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                    os.fchmod(self.log, 0o600)
                    self._write("preflight")
                    return self

                def __exit__(self, *_):
                    if self.log is not None:
                        os.close(self.log)
                        self.log = None
                    return False

                def admit_stage(self, rows):
                    self._write("stage", rows=rows)
                    return True

                def admit_case(self, rows, trial, arm):
                    self._write("case", rows=rows, trial=trial, arm=arm)
                    return True

                def complete_case(self, rows, trial, arm):
                    self._write("case_complete", rows=rows, trial=trial, arm=arm)
                    return True

            guard = object.__new__(job.capacity.HostStorageGuard)

            def fake_acquire(ws, task_dir, home_dir, *, manifest, vendor_mode,
                             on_substage=None, stage_budget_seconds=None):
                for arm in pair.ARMS:
                    directory(task_dir / "sources" / arm)
                cargo_home = directory(home_dir / "cargo")
                acquired = dict.fromkeys(job.build_join.ACQUIRED_KEYS)
                acquired.update(
                    guard=guard, task=task_dir, cargo_home=cargo_home,
                    sources={arm: task_dir / "sources" / arm for arm in pair.ARMS})
                return acquired

            def fake_build(acquired, ws, build_dir, *, on_substage=None):
                staging = directory(build_dir / "bundles" / "pair")
                for arm in pair.ARMS:
                    directory(staging / arm)
                return {
                    "pair": {"pins": pins, "staging": staging},
                    "build_task": build_dir,
                    "guard": guard,
                    "cargo_home": acquired["cargo_home"],
                }

            def fake_case_runner(**kwargs):
                rep = next(report_iter)
                out = kwargs["directory"] / "public.json"
                out.write_bytes(job.measure_join.run.case._canonical(rep))
                out.chmod(0o600)
                return rep

            def fake_audit_case(public_dir, private_dir, binary, pin, rows, trial, finish):
                return json.loads((public_dir / "public.json").read_bytes())

            orig_run_mode = job.measure_join.run._run_mode

            def run_mode_with_fake_case(**kwargs):
                kwargs["case_runner"] = fake_case_runner
                return orig_run_mode(**kwargs)

            env = {
                "HNSW_FINISH": "commit",
                "RUNNER_TEMP": str(runner_temp),
                "GITHUB_WORKSPACE": str(workspace),
            }
            with mock.patch.object(job.Path, "home", return_value=home_base), \
                 mock.patch.object(job.qualify, "REPO", workspace), \
                 mock.patch.object(job.scoped_storage, "make_manifest", return_value=object()), \
                 mock.patch.object(job.identity_gate, "run",
                                   return_value=({"case": "HNSW_HOSTED_ADMISSION",
                                                  "outcome": "passed"}, 0)), \
                 mock.patch.object(job.capacity.HostStorageGuard, "__init__", return_value=None), \
                 mock.patch.object(job.capacity.HostStorageGuard, "__call__", return_value=None), \
                 mock.patch.object(job.rust, "install_pinned_minimal"), \
                 mock.patch.object(job.acquire, "acquire_all", side_effect=fake_acquire), \
                 mock.patch.object(job.build_join, "build", side_effect=fake_build), \
                 mock.patch.object(job.measure_join.admission, "Admission", RealLedgerAdmission), \
                 mock.patch.object(job.measure_join.run.assets, "verify",
                                   return_value={"binary": root / "bin"}), \
                 mock.patch.object(job.measure_join.stage_audit.assets, "verify",
                                   return_value={"binary": root / "bin"}), \
                 mock.patch.object(job.measure_join.run, "_run_mode",
                                   side_effect=run_mode_with_fake_case), \
                 mock.patch.object(job.measure_join.run.audit, "audit_case",
                                   side_effect=fake_audit_case), \
                 mock.patch.object(job.measure_join.stage_audit.audit, "audit_case",
                                   side_effect=fake_audit_case), \
                 contextlib.redirect_stdout(stdout):
                rc = job.run_hosted_measure_cli(env)
            self.assertEqual(rc, 0, stdout.getvalue())
            export_file = runner_temp / "issue681-public-export" / "issue681-public-commit.json"
            self.assertTrue(export_file.is_file())
            raw = export_file.read_bytes()
            self.assertLessEqual(len(raw), job.transport.MAX_BYTES)
            digest = hashlib.sha256(raw).hexdigest()
            self.assertEqual(
                job.transport.validate(raw, expected_sha256=digest,
                                       expected_pins=pins, finish="commit"),
                raw)
            lines = [json.loads(line) for line in stdout.getvalue().splitlines()]
            case_lines = [row for row in lines if row.get("substage") == "case"]
            self.assertEqual(len(case_lines), 30)
            self.assertEqual(lines[-1]["outcome"], "passed")
            self.assertEqual(lines[-1]["public_sha256"], digest)
            self.assertNotIn(str(root), stdout.getvalue())
