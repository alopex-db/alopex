"""Selected private-job entry and allowlisted handoff contracts."""

import hashlib
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_private_release as release
from test_issue681_public_transport import public_fixture

RUN = "a" * 64
SOURCE = "b" * 40
PROFILE = {"schema": release.admission.SCHEMA,
           "runner_kind": "private_linux", "cpu_count": 4,
           "additional_bytes": 8_000_000_000}


def private(path):
    path.mkdir(mode=0o700, parents=True)
    path.chmod(0o700)
    return path


class PrivateReleaseTests(unittest.TestCase):
    def setUp(self):
        selected = mock.patch.dict(release.os.environ,
                                   {"ALOPEX_SELECTED_RUN_ID": RUN,
                                    "RUNNER_KIND": PROFILE["runner_kind"]})
        selected.start()
        self.addCleanup(selected.stop)

    def roots(self, root):
        workspace = private(root / "checkout")
        task = private(root / "tasks")
        home_base = private(root / "home")
        home = private(home_base / "private")
        return workspace, task, home_base, home

    def test_source_requires_exact_clean_head_with_private_stderr(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, _, _, _ = self.roots(Path(tmp).resolve())
            good = [subprocess.CompletedProcess([], 0, SOURCE.encode() + b"\n",
                                                b"private notice"),
                    subprocess.CompletedProcess([], 0, b"", b"private notice")]
            with mock.patch.object(release.qualify, "REPO", workspace), \
                 mock.patch.object(release.subprocess, "run", side_effect=good):
                release._source(workspace, SOURCE)
            dirty = [good[0], subprocess.CompletedProcess([], 0, b" M file\n", b"")]
            with mock.patch.object(release.qualify, "REPO", workspace), \
                 mock.patch.object(release.subprocess, "run", side_effect=dirty):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_JOB$"):
                    release._source(workspace, SOURCE)

    def test_bad_binding_never_acquires(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, task, home_base, home = self.roots(Path(tmp).resolve())
            with mock.patch.object(release.job.Path, "home", return_value=home_base), \
                 mock.patch.object(release.qualify, "REPO", workspace), \
                 mock.patch.object(release, "_source", side_effect=ValueError(
                     "/private/source")) as source, \
                 mock.patch.object(release.job.acquire, "acquire_all") as acquire:
                for changes in ({"run_id": "bad"}, {"finish": "partial"},
                                {"profile": {"selected": True}}):
                    with self.subTest(changes=changes), self.assertRaisesRegex(
                            ValueError, "^PUBLIC_JOB$"):
                        release.execute(**{**{
                            "workspace": workspace, "task_parent": task,
                            "home_parent": home, "profile": PROFILE,
                            "finish": "commit", "source_sha": SOURCE,
                            "run_id": RUN}, **changes})
                source.assert_not_called()
                with mock.patch.dict(release.os.environ,
                                     {"ALOPEX_SELECTED_RUN_ID": "0" * 64}):
                    with self.assertRaisesRegex(ValueError, "^PUBLIC_JOB$"):
                        release.execute(workspace=workspace, task_parent=task,
                                        home_parent=home, profile=PROFILE,
                                        finish="commit", source_sha=SOURCE,
                                        run_id=RUN)
                source.assert_not_called()
                with mock.patch.dict(release.os.environ,
                                     {"RUNNER_KIND": "wrong_kind"}):
                    with self.assertRaisesRegex(ValueError, "^PUBLIC_JOB$"):
                        release.execute(workspace=workspace, task_parent=task,
                                        home_parent=home, profile=PROFILE,
                                        finish="commit", source_sha=SOURCE,
                                        run_id=RUN)
                source.assert_not_called()
                with self.assertRaisesRegex(ValueError, "^PUBLIC_JOB$"):
                    release.execute(workspace=workspace, task_parent=task,
                                    home_parent=home, profile=PROFILE,
                                    finish="commit", source_sha=SOURCE,
                                    run_id=RUN)
                acquire.assert_not_called()

    def test_selected_preflight_orders_one_finish_and_binds_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, task, home_base, home = self.roots(Path(tmp).resolve())
            calls = []
            raw, pins = public_fixture("commit")
            guard = lambda: calls.append("guard")

            def build(acquired, _workspace, _parent):
                self.assertIs(acquired["guard"], guard)
                calls.append("build")
                return {"pair": {"pins": pins}, "guard": guard}

            def measure(acquired, built, _workspace, parent, profile, finish):
                self.assertIs(acquired["guard"], built["guard"])
                self.assertEqual((profile, finish), (PROFILE, "commit"))
                output = private(parent / "result") / "public.json"
                output.write_bytes(raw)
                output.chmod(0o600)
                calls.append("measure")
                return {"public": output, "guard": guard, "finish": finish}

            with mock.patch.object(release.job.Path, "home", return_value=home_base), \
                 mock.patch.object(release.qualify.scoped_storage,
                                   "make_manifest", return_value=object()), \
                 mock.patch.object(release.qualify, "REPO", workspace), \
                 mock.patch.object(release, "_source",
                                   side_effect=lambda *_: calls.append("source")), \
                 mock.patch.object(release.qualify, "platform_security",
                                   side_effect=lambda: calls.append("platform")), \
                 mock.patch.object(release.qualify, "mounted_allocation",
                                   return_value=(1, {workspace.stat().st_dev:
                                                    40_000_000_000})), \
                 mock.patch.object(release.job.rust, "run",
                                   side_effect=lambda: (calls.append("rust") or
                                       ({"case": release.job.rust.CASE,
                                         "outcome": "passed"}, 0))), \
                 mock.patch.object(release.job.acquire, "acquire_all",
                                   side_effect=lambda *_, **__: (calls.append("acquire") or
                                                          {"guard": guard})), \
                 mock.patch.object(release.job.build_join, "build",
                                   side_effect=build), \
                 mock.patch.object(release.job.measure_join, "measure",
                                   side_effect=measure):
                result = release.execute(workspace=workspace,
                                         task_parent=task, home_parent=home,
                                         profile=PROFILE, finish="commit",
                                         source_sha=SOURCE, run_id=RUN)
            self.assertEqual(calls[:5], ["source", "platform", "rust",
                                          "acquire", "build"])
            self.assertIn("measure", calls)
            self.assertEqual(result["run_id"], RUN)
            self.assertEqual(result["source_sha"], SOURCE)
            self.assertEqual(result["finish"], "commit")
            self.assertEqual(result["profile_sha256"],
                             release._profile_digest(PROFILE))
            self.assertEqual(result["public_sha256"],
                             hashlib.sha256(raw).hexdigest())

            kwargs = {"run_id": RUN, "source_sha": SOURCE,
                      "finish": "commit",
                      "profile_sha256": result["profile_sha256"],
                      "expected_digest": result["public_sha256"],
                      "expected_pins": pins}
            self.assertEqual(release.handoff(result, **kwargs),
                             {"public_bytes": raw,
                              "sha256": result["public_sha256"]})
            for key, wrong in (("run_id", "0" * 64),
                               ("source_sha", "0" * 40),
                               ("finish", "rollback"),
                               ("profile_sha256", "0" * 64),
                               ("expected_digest", "0" * 64)):
                with self.subTest(key=key), self.assertRaisesRegex(
                        ValueError, "^PUBLIC_TRANSPORT$"):
                    release.handoff(result, **{**kwargs, key: wrong})
            result["public"].write_bytes(b'{"local_path":"/private/host"}')
            with self.assertRaisesRegex(ValueError, "^PUBLIC_TRANSPORT$"):
                release.handoff(result, **kwargs)

    def test_platform_failure_has_fixed_code_and_no_acquisition(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, task, home_base, home = self.roots(Path(tmp).resolve())
            with mock.patch.object(release.job.Path, "home", return_value=home_base), \
                 mock.patch.object(release.qualify, "REPO", workspace), \
                 mock.patch.object(release, "_source"), \
                 mock.patch.object(release.qualify, "platform_security",
                                   side_effect=ValueError("/private/status")), \
                 mock.patch.object(release.job.acquire, "acquire_all") as acquire:
                with self.assertRaisesRegex(ValueError, "^PUBLIC_JOB$"):
                    release.execute(workspace=workspace, task_parent=task,
                                    home_parent=home, profile=PROFILE,
                                    finish="rollback", source_sha=SOURCE,
                                    run_id=RUN)
                acquire.assert_not_called()
            self.assertEqual(list(task.iterdir()), [])
