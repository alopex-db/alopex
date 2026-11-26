"""Host qualification must bind the PR head and keep failures publishable."""

import contextlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_qualify as qualify


HEAD = "a" * 40


class HostedQualificationTest(unittest.TestCase):
    def synthetic_mount(self, root):
        device = root.stat().st_dev
        value = (f"1 1 {os.major(device)}:{os.minor(device)} / / rw - "
                 "ext4 synthetic rw\n")
        patcher = mock.patch.object(qualify.scoped_storage, "_mount_text",
                                    return_value=value)
        patcher.start()
        self.addCleanup(patcher.stop)

    def event(self, directory, *, base="integration/v0.8.17-candidate"):
        path = Path(directory) / "event.json"
        path.write_text(json.dumps({
            "repository": {"full_name": "alopex-db/alopex"},
            "pull_request": {
                "number": 681, "draft": True,
                "head": {"sha": HEAD,
                         "repo": {"full_name": "alopex-db/alopex"}},
                "base": {"ref": base}}}))
        return path

    def environment(self, event):
        return {"GITHUB_EVENT_NAME": "pull_request",
                "GITHUB_REPOSITORY": "alopex-db/alopex",
                "GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "1",
                "EXPECTED_HEAD_SHA": HEAD,
                "GITHUB_REF": "refs/pull/681/merge",
                "GITHUB_EVENT_PATH": str(event)}

    def test_exact_head_context_and_wrong_base(self):
        with tempfile.TemporaryDirectory() as directory:
            checkout = Path(directory)
            event = self.event(directory)
            env = self.environment(event)

            def git(args, timeout=30):
                return HEAD if args[-2:] == ["rev-parse", "HEAD"] else ""

            with mock.patch.object(qualify, "command", side_effect=git):
                qualify.context(env, checkout=checkout)
                self.event(directory, base="main")
                with self.assertRaises(ValueError):
                    qualify.context(env, checkout=checkout)

    def test_private_exception_never_reaches_public_result(self):
        with mock.patch.object(qualify, "context", return_value=None), \
             mock.patch.object(qualify, "platform_security",
                               side_effect=OSError("/home/private/secret")):
            result, status = qualify.run({})
        self.assertEqual(status, 1)
        self.assertEqual(result, {"case": qualify.CASE, "outcome": "failed",
                                  "error_code": "PLATFORM"})
        self.assertNotIn("/home", json.dumps(result))

    def test_main_prints_only_fixed_result(self):
        output = io.StringIO()
        with mock.patch.object(qualify, "run", return_value=(
                {"case": qualify.CASE, "outcome": "failed",
                 "error_code": "STORAGE"}, 1)), \
             contextlib.redirect_stdout(output):
            self.assertEqual(qualify.main(), 1)
        self.assertEqual(json.loads(output.getvalue()),
                         {"case": qualify.CASE, "outcome": "failed",
                          "error_code": "STORAGE"})

    def test_scoped_inventory_counts_hardlink_once_and_rejects_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            self.synthetic_mount(root)
            home, workspace, temporary = (root / name for name in
                                          ("home", "workspace", "temporary"))
            for path in (home, workspace, temporary):
                path.mkdir()
            checkout = workspace / "checkout"
            checkout.mkdir()
            source = checkout / "source"
            source.write_bytes(b"x" * 4096)
            alias = checkout / "alias"
            alias.hardlink_to(source)
            manifest = qualify.scoped_storage.make_manifest(
                checkout, temporary, home=home, scratch=root / "scratch")
            used, free = qualify.mounted_allocation(manifest)
            self.assertGreater(used, 0)
            self.assertIn(root.stat().st_dev, free)
            alias.unlink()
            alias.symlink_to(source)
            with self.assertRaises(ValueError):
                qualify.mounted_allocation(manifest)

    def test_storage_budget_accepts_complete_upper_bound_and_rejects_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            self.synthetic_mount(root)
            home, workspace, temporary = (root / name for name in
                                          ("home", "workspace", "temporary"))
            for path in (home, workspace, temporary):
                path.mkdir()
            (workspace / "checkout").mkdir()
            env = {"GITHUB_WORKSPACE": str(workspace / "checkout"),
                   "RUNNER_TEMP": str(temporary)}
            manifest = qualify.scoped_storage.make_manifest(
                workspace / "checkout", temporary, home=home,
                scratch=root / "scratch")
            device = workspace.stat().st_dev
            with mock.patch.object(qualify.Path, "home", return_value=home), \
                 mock.patch.object(qualify, "mounted_allocation",
                                   return_value=(20_000_000_000,
                                                 {device: 20_000_000_000})):
                qualify.storage(env, manifest)
            with mock.patch.object(qualify.Path, "home", return_value=home), \
                 mock.patch.object(qualify, "mounted_allocation",
                                   return_value=(31_000_000_000,
                                                 {device: 20_000_000_000})):
                with self.assertRaises(ValueError):
                    qualify.storage(env, manifest)

    def test_platform_checks_selected_cgroup_and_child_counter(self):
        status = ("CapEff:\t0000000000000000\nSeccomp:\t2\n"
                  "NoNewPrivs:\t1\n")
        mounts = "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n"

        def read(path, *args, **kwargs):
            return status if str(path) == "/proc/self/status" else mounts

        with mock.patch.object(qualify.sys, "platform", "linux"), \
             mock.patch.object(qualify.platform, "machine",
                               return_value="x86_64"), \
             mock.patch.object(qualify.os, "geteuid", return_value=501), \
             mock.patch.object(qualify.os, "sched_getaffinity",
                               return_value={0, 1, 2, 3}, create=True), \
             mock.patch.object(qualify.Path, "read_text", read), \
             mock.patch.object(qualify.Path, "iterdir", return_value=iter(())), \
             mock.patch.object(qualify.admission, "current_allocation",
                               return_value=("/", 0)) as allocation, \
             mock.patch.object(qualify.admission,
                               "child_counter_probe") as counter:
            qualify.platform_security()
            allocation.assert_called_once_with((0, 1, 2, 3))
            counter.assert_called_once_with()

    def test_platform_rejects_disabled_seccomp_before_admission(self):
        proc_status = ("CapEff:\t0000000000000000\nSeccomp:\t0\n"
                       "NoNewPrivs:\t1\n")

        with mock.patch.object(qualify.sys, "platform", "linux"), \
             mock.patch.object(qualify.platform, "machine",
                               return_value="x86_64"), \
             mock.patch.object(qualify.os, "geteuid", return_value=501), \
             mock.patch.object(qualify.Path, "read_text",
                               return_value=proc_status), \
             mock.patch.object(qualify.admission,
                               "current_allocation") as allocation, \
             mock.patch.object(qualify.admission,
                               "child_counter_probe") as counter, \
             mock.patch.object(qualify, "context"), \
             mock.patch.object(qualify, "storage") as storage:
            result, status = qualify.run({})

        self.assertEqual(status, 1)
        self.assertEqual(result, {"case": qualify.CASE, "outcome": "failed",
                                  "error_code": "PLATFORM"})
        allocation.assert_not_called()
        counter.assert_not_called()
        storage.assert_not_called()

    def test_platform_vm_flag_admits_seccomp_0_and_2_but_rejects_1(self):
        mounts = "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n"
        for mode, should_pass in (("0", True), ("2", True), ("1", False)):
            proc_status = f"CapEff:\t0000000000000000\nSeccomp:\t{mode}\nNoNewPrivs:\t0\n"
            def read(path, *args, status_text=proc_status, **kwargs):
                return status_text if str(path) == "/proc/self/status" else mounts
            with self.subTest(mode=mode), \
                 mock.patch.object(qualify.sys, "platform", "linux"), \
                 mock.patch.object(qualify.platform, "machine", return_value="x86_64"), \
                 mock.patch.object(qualify.os, "geteuid", return_value=501), \
                 mock.patch.object(qualify.os, "sched_getaffinity",
                                   return_value={0, 1, 2, 3}, create=True), \
                 mock.patch.object(qualify.Path, "read_text", read), \
                 mock.patch.object(qualify.Path, "iterdir", return_value=iter(())), \
                 mock.patch.object(qualify.admission, "current_allocation", return_value=("/", 0)), \
                 mock.patch.object(qualify.admission, "child_counter_probe"):
                if should_pass:
                    qualify.platform_security(allow_vm_unfiltered_seccomp=True)
                else:
                    with self.assertRaises(ValueError):
                        qualify.platform_security(allow_vm_unfiltered_seccomp=True)

    def test_full_orchestration_passes_and_rejects_counter_or_storage_gap(self):
        with mock.patch.object(qualify, "context"), \
             mock.patch.object(qualify, "platform_security"), \
             mock.patch.object(qualify, "storage"):
            self.assertEqual(qualify.run({}),
                             ({"case": qualify.CASE, "outcome": "passed"}, 0))
        with mock.patch.object(qualify, "context"), \
             mock.patch.object(qualify, "platform_security",
                               side_effect=ValueError("counter missing")):
            self.assertEqual(qualify.run({})[0]["error_code"], "PLATFORM")
        with mock.patch.object(qualify, "context"), \
             mock.patch.object(qualify, "platform_security"), \
             mock.patch.object(qualify, "storage",
                               side_effect=ValueError("allocation unknown")):
            self.assertEqual(qualify.run({})[0]["error_code"], "STORAGE")


if __name__ == "__main__":
    unittest.main()
