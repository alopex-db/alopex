"""One selected job must qualify before it acquires public inputs."""

from pathlib import Path
import hashlib
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
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

            def fake_measure(acquired, built, work, parent, profile, finish):
                self.assertEqual(finish, "commit")
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
                          expected_job, on_stage, on_substage):
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
