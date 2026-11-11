"""Small deterministic directory-change regressions for capacity monitoring."""
import errno
import json
from pathlib import Path
import sys
import tempfile
import unittest
import subprocess
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from capacity import Budget, DU_BINARY, SUDO_BINARY, allocated_bytes, readonly_sudo_du_bytes
from common import Failure, load_profile


class CompleteTraversal(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.original_lstat = Path.lstat

    def expected(self, *paths):
        return sum(self.original_lstat(path).st_blocks * 512 for path in paths)

    def test_deleted_entry_restarts_all_counting_and_includes_replacement(self):
        stable = self.root / 'stable'
        stable.write_bytes(b'x' * 4096)
        transient = self.root / 'checkpoint.meta'
        transient.write_bytes(b'x')
        replacement = self.root / 'replacement'
        stable_reads = []
        original_iterdir = Path.iterdir

        def ordered(path):
            if path == self.root and transient.exists():
                return iter((transient, stable))  # Count stable before the failure.
            return original_iterdir(path)

        def observed(path, *args, **kwargs):
            if path == stable:
                stable_reads.append(path)
            if path == transient:
                transient.unlink()
                replacement.write_bytes(b'y' * 8192)
                raise FileNotFoundError(errno.ENOENT, 'entry replaced')
            return self.original_lstat(path, *args, **kwargs)

        with patch.object(Path, 'iterdir', ordered), patch.object(Path, 'lstat', observed):
            measured = allocated_bytes([self.root])
        self.assertEqual(len(stable_reads), 2)
        self.assertEqual(measured, self.expected(self.root, stable, replacement))

    def test_directory_removed_during_listing_restarts_from_inventory_roots(self):
        stable = self.root / 'stable'
        stable.write_bytes(b'x' * 4096)
        transient = self.root / 'temporary'
        transient.mkdir()
        original_iterdir = Path.iterdir
        root_reads = []

        def observed(path):
            if path == self.root:
                root_reads.append(path)
            if path == transient:
                transient.rmdir()
                raise FileNotFoundError(errno.ENOENT, 'directory removed')
            return original_iterdir(path)

        with patch.object(Path, 'iterdir', observed):
            measured = allocated_bytes([self.root])
        self.assertEqual(len(root_reads), 2)
        self.assertEqual(measured, self.expected(self.root, stable))

    def test_repeated_disappearance_fails_after_three_complete_attempts(self):
        transient = self.root / 'checkpoint.meta'
        transient.write_bytes(b'x')
        reads = []

        def observed(path, *args, **kwargs):
            if path == transient:
                reads.append(path)
                raise FileNotFoundError(errno.ENOENT, 'persistent churn')
            return self.original_lstat(path, *args, **kwargs)

        with patch.object(Path, 'lstat', observed), self.assertRaises(Failure) as caught:
            Budget({'roots': [self.root]}).used()
        self.assertEqual(caught.exception.code, 'MEASUREMENT')
        self.assertEqual(len(reads), 3)

    def test_inventory_root_disappearance_is_not_retried(self):
        root = self.root / 'inventory'
        root.mkdir()
        reads = []

        def observed(path, *args, **kwargs):
            if path == root:
                reads.append(path)
                root.rmdir()
                raise FileNotFoundError(errno.ENOENT, 'root removed')
            return self.original_lstat(path, *args, **kwargs)

        with patch.object(Path, 'lstat', observed), self.assertRaises(Failure) as caught:
            allocated_bytes([root])
        self.assertEqual(caught.exception.code, 'MEASUREMENT')
        self.assertEqual(len(reads), 1)

    def test_permission_and_io_errors_fail_without_retry(self):
        target = self.root / 'file'
        target.write_bytes(b'x')
        for error in (PermissionError(errno.EACCES, 'denied'), OSError(errno.EIO, 'I/O error')):
            reads = []

            def observed(path, *args, **kwargs):
                if path == target:
                    reads.append(path)
                    raise error
                return self.original_lstat(path, *args, **kwargs)

            with self.subTest(error=error.errno), patch.object(Path, 'lstat', observed):
                with self.assertRaises(OSError) as caught:
                    allocated_bytes([self.root])
                self.assertEqual(caught.exception.errno, error.errno)
                self.assertEqual(len(reads), 1)

    def test_generator_inventory_is_preserved_across_restart(self):
        first, second = (self.root / name for name in ('first', 'second'))
        first.mkdir()
        second.mkdir()
        target = first / 'checkpoint.meta'
        target.write_bytes(b'x')
        payload = second / 'payload'
        payload.write_bytes(b'y' * 4096)
        disappeared = False

        def observed(path, *args, **kwargs):
            nonlocal disappeared
            if path == target and not disappeared:
                disappeared = True
                raise FileNotFoundError(errno.ENOENT, 'transient entry lookup')
            return self.original_lstat(path, *args, **kwargs)

        with patch.object(Path, 'lstat', observed):
            measured = allocated_bytes(root for root in (first, second))
        self.assertEqual(measured, self.expected(first, second, target, payload))


class ReadonlyDockerRoot(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.parent = Path(self.temporary.name).resolve()
        self.docker = self.parent / "docker"
        self.other = self.parent / "other"
        self.docker.mkdir()
        self.other.mkdir()
        self.config = {"roots": [str(self.docker), str(self.other)],
                       "readonly_sudo_du_root": str(self.docker),
                       "stop_bytes": 40_000_000_000, "reserve_bytes": 1_000_000_000,
                       "poll_seconds": 2, "planned_bytes": {"build": 3_000_000_000}}

    def result(self, stdout, *, returncode=0, stderr=""):
        return subprocess.CompletedProcess([], returncode, stdout, stderr)

    def test_one_root_is_counted_with_exact_readonly_command_and_budget(self):
        expected = self.other.stat().st_blocks * 512 + 2048
        with patch("capacity.subprocess.run", return_value=self.result(f"2\t{self.docker}\n")) as run:
            budget = Budget(self.config)
            self.assertEqual(budget.used(), expected)
            self.assertEqual(budget.before("build"), expected)
            self.assertEqual(budget.check(), expected)
        self.assertEqual(run.call_args.args[0],
                         [str(SUDO_BINARY), "-n", str(DU_BINARY), "-sk", "--", str(self.docker)])
        self.assertEqual(run.call_args.kwargs["stdin"], subprocess.DEVNULL)
        self.assertEqual(run.call_args.kwargs["timeout"], 2)

    def test_opted_in_sudo_scan_uses_sixty_seconds_without_changing_poll_or_budget(self):
        self.config["readonly_sudo_du_timeout_seconds"] = 60
        with patch("capacity.subprocess.run", return_value=self.result(f"2\t{self.docker}\n")) as run:
            budget = Budget(self.config)
            budget.before("build")
            budget.check()
        self.assertEqual(run.call_args.kwargs["timeout"], 60)
        self.assertEqual(budget.config["poll_seconds"], 2)
        self.assertEqual(budget.config["stop_bytes"], 40_000_000_000)
        self.assertEqual(budget.config["reserve_bytes"], 1_000_000_000)

    def test_opted_in_sudo_scan_timeout_is_bounded_and_fails_closed(self):
        for value in (0, 61, True, "60"):
            with self.subTest(value=value):
                self.config["readonly_sudo_du_timeout_seconds"] = value
                with self.assertRaises(Failure) as caught:
                    Budget(self.config)
                self.assertEqual(caught.exception.code, "MEASUREMENT")
        self.config["readonly_sudo_du_timeout_seconds"] = 60
        with patch("capacity.subprocess.run",
                   side_effect=subprocess.TimeoutExpired("du", 60)) as run:
            with self.assertRaises(Failure) as caught:
                Budget(self.config).used()
        self.assertEqual(caught.exception.code, "MEASUREMENT")
        self.assertEqual(run.call_args.kwargs["timeout"], 60)

    def test_bad_du_output_exit_and_timeout_fail_closed(self):
        results = [self.result(""), self.result(f"2\t{self.docker}\nextra\n"),
                   self.result(f"2\t{self.other}\n"), self.result(f"-1\t{self.docker}\n"),
                   self.result(f"2\t{self.docker}\n", stderr="warning"),
                   self.result(f"2\t{self.docker}\n", returncode=1)]
        for result in results:
            with self.subTest(result=result), patch("capacity.subprocess.run", return_value=result):
                with self.assertRaises(Failure) as caught:
                    readonly_sudo_du_bytes(self.docker, timeout=2)
                self.assertEqual(caught.exception.code, "MEASUREMENT")
        with patch("capacity.subprocess.run", side_effect=subprocess.TimeoutExpired("du", 2)):
            with self.assertRaises(Failure) as caught:
                Budget(self.config).used()
            self.assertEqual(caught.exception.code, "MEASUREMENT")

    def test_zero_is_a_complete_observation_and_malformed_values_fail(self):
        with patch("capacity.subprocess.run", return_value=self.result(f"0\t{self.docker}\n")):
            self.assertEqual(readonly_sudo_du_bytes(self.docker, timeout=2), 0)
        for value in ("", " 0", "+0", "0.0", "zero", "0\twrong"):
            with self.subTest(value=value), patch("capacity.subprocess.run",
                                                 return_value=self.result(f"{value}\t{self.docker}\n")):
                with self.assertRaises(Failure):
                    readonly_sudo_du_bytes(self.docker, timeout=2)

    def test_untrusted_or_missing_absolute_executable_fails_before_sudo(self):
        untrusted = self.parent / "sudo"
        untrusted.write_bytes(b"fixture")
        untrusted.chmod(0o755)
        for executable in (untrusted, self.parent / "missing"):
            with self.subTest(executable=executable), patch("capacity.SUDO_BINARY", executable), \
                 patch("capacity.subprocess.run") as run:
                with self.assertRaises(Failure) as caught:
                    readonly_sudo_du_bytes(self.docker, timeout=2)
                self.assertEqual(caught.exception.code, "MEASUREMENT")
                run.assert_not_called()

        original = Path.lstat
        for binary in (SUDO_BINARY, DU_BINARY):
            def unsafe(path, *args, **kwargs):
                info = original(path, *args, **kwargs)
                if path == binary:
                    return SimpleNamespace(st_mode=info.st_mode | 0o022,
                                           st_uid=info.st_uid, st_dev=info.st_dev,
                                           st_ino=info.st_ino)
                return info

            with self.subTest(binary=binary), patch.object(Path, "lstat", unsafe), \
                 patch("capacity.subprocess.run") as run:
                with self.assertRaises(Failure) as caught:
                    readonly_sudo_du_bytes(self.docker, timeout=2)
                self.assertEqual(caught.exception.code, "MEASUREMENT")
                run.assert_not_called()

    def test_root_replacement_during_du_fails(self):
        def replaced(*args, **kwargs):
            old = self.parent / "old-docker"
            self.docker.rename(old)
            self.docker.mkdir()
            return self.result(f"2\t{self.docker}\n")

        with patch("capacity.subprocess.run", side_effect=replaced), self.assertRaises(Failure) as caught:
            readonly_sudo_du_bytes(self.docker, timeout=2)
        self.assertEqual(caught.exception.code, "MEASUREMENT")

    def test_overlap_and_replaced_root_are_rejected(self):
        self.config["roots"].append(str(self.parent))
        with self.assertRaises(Failure) as caught:
            Budget(self.config)
        self.assertEqual(caught.exception.code, "MEASUREMENT")
        self.config["roots"].pop()
        budget = Budget(self.config)
        self.docker.rmdir()
        self.docker.mkdir()
        with self.assertRaises(Failure) as caught:
            budget.used()
        self.assertEqual(caught.exception.code, "MEASUREMENT")

    def test_symlink_from_other_root_into_docker_root_is_rejected(self):
        (self.other / "alias").symlink_to(self.docker)
        with self.assertRaises(Failure) as caught:
            Budget(self.config).used()
        self.assertEqual(caught.exception.code, "MEASUREMENT")

    def test_stop_and_reserve_still_gate_sudo_measurement(self):
        self.config["stop_bytes"] = 4_000_000_000
        with patch("capacity.subprocess.run", return_value=self.result(f"1\t{self.docker}\n")):
            with self.assertRaises(Failure) as caught:
                Budget(self.config).before("build")
        self.assertEqual(caught.exception.code, "CAPACITY")

    def test_private_profile_requires_exact_nonoverlapping_runtime_root(self):
        profile = {"schema": "alopex.test-env-profile/v1", "run_id": "test",
                   "docker_context": "local", "platform": "linux/amd64",
                   "execution_mode": "native", "image_lock": "lock", "assets": "assets",
                   "private_output": "output", "cache": "cache",
                   "cpu": {"server": [0], "client": [1]},
                   "limits": {"memory_bytes": 1, "swap_bytes": 0, "pids": 1,
                              "case_seconds": 1, "run_seconds": 1, "stop_seconds": 1},
                   "storage": {**self.config,
                               "runtime_storage_roots": [str(self.docker)],
                               "planned_bytes": {key: 1 for key in
                                                 ("fetch", "build", "runtime", "accept", "collect")}},
                   "security": {"lsm_kind": "apparmor", "lsm_profile": "test",
                                "seccomp_profile": "test", "seccomp_sha256": "a" * 64}}
        path = self.parent / "profile.json"
        path.write_text(json.dumps(profile))
        path.chmod(0o600)
        self.assertEqual(load_profile(path)["storage"]["readonly_sudo_du_root"], str(self.docker))
        profile["storage"]["readonly_sudo_du_timeout_seconds"] = 60
        path.write_text(json.dumps(profile))
        self.assertEqual(load_profile(path)["storage"]["readonly_sudo_du_timeout_seconds"], 60)
        for invalid in (0, 61, True, "60"):
            profile["storage"]["readonly_sudo_du_timeout_seconds"] = invalid
            path.write_text(json.dumps(profile))
            with self.assertRaises(Failure) as caught:
                load_profile(path)
            self.assertEqual(caught.exception.code, "MEASUREMENT")
        profile["storage"]["readonly_sudo_du_timeout_seconds"] = 60
        profile["storage"]["runtime_storage_roots"] = [str(self.other)]
        path.write_text(json.dumps(profile))
        with self.assertRaises(Failure) as caught:
            load_profile(path)
        self.assertEqual(caught.exception.code, "MEASUREMENT")


if __name__ == '__main__':
    unittest.main()
