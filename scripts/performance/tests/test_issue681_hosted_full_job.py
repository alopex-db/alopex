"""One selected job must qualify before it acquires public inputs."""

from pathlib import Path
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

            def acquire(work, task, home):
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
                return {"guard": guard}

            def measure(acquired, built, work, parent, profile, finish):
                self.assertIs(acquired["guard"], built["guard"])
                self.assertEqual(work, workspace)
                self.assertEqual(finish, "rollback")
                self.assertEqual(profile, {"selected": True})
                output = directory(parent / "result") / "public.json"
                output.write_text("{}")
                calls.append("measure")
                return {"public": output, "guard": guard,
                        "finish": finish}

            with mock.patch.object(job.Path, "home", return_value=home_base), \
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
                                   side_effect=measure):
                result = job.execute({}, workspace, task_parent, home_parent,
                                     {"selected": True}, "rollback")
            self.assertEqual(calls, ["qualify", "rust", "acquire", "build",
                                     "measure", "guard"])
            self.assertEqual(result["case"], job.CASE)
            self.assertEqual(result["outcome"], "passed")
            self.assertTrue(result["public"].is_file())
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
