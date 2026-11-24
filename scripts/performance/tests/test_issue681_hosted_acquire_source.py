"""Exact public source acquisition is bound before any network command."""

import hashlib
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_acquire_source as acquire


class HostedSourceAcquisitionTest(unittest.TestCase):
    def test_fixed_source_pins_match_local_git_objects(self):
        repository = Path(__file__).resolve().parents[3]
        for pin in acquire.PINS.values():
            with self.subTest(commit=pin["commit"][:8]):
                tree = subprocess.check_output(
                    ["git", "-C", str(repository), "rev-parse",
                     pin["commit"] + "^{tree}"],
                    stderr=subprocess.DEVNULL).decode().strip()
                self.assertEqual(tree, pin["tree"])
                for name, expected in ((acquire.producer.EXAMPLE,
                                        pin["example"]),
                                       ("Cargo.lock", pin["lock"])):
                    content = subprocess.check_output(
                        ["git", "-C", str(repository), "show",
                         pin["commit"] + ":" + name],
                        stderr=subprocess.DEVNULL)
                    self.assertEqual(hashlib.sha256(content).hexdigest(),
                                     expected)

    def test_guard_precedes_both_exact_fetches_and_source_checks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            workspace = root / "workspace"
            workspace.mkdir(mode=0o700)
            parent = root / "sources"
            parent.mkdir(mode=0o700)
            events = []
            guard = lambda: events.append("guard")

            def fetch(args, cwd, active_guard, output, env):
                self.assertIs(active_guard, guard)
                self.assertNotEqual(env["HOME"], str(Path.home()))
                self.assertEqual(env["HOME"], env["XDG_CONFIG_HOME"])
                self.assertEqual(env["GIT_CONFIG_GLOBAL"], "/dev/null")
                self.assertEqual(env["GIT_ASKPASS"], "/bin/false")
                events.append((cwd.name, tuple(args)))

            with mock.patch.object(acquire, "_trusted_git"), \
                 mock.patch.object(acquire.capacity, "HostStorageGuard",
                                   side_effect=lambda workspace: (
                                       events.append("created") or guard)), \
                 mock.patch.object(acquire, "_git", side_effect=fetch), \
                 mock.patch.object(acquire.producer, "verify_source") as verify:
                result = acquire.acquire_sources(workspace, parent)
            self.assertIs(result["guard"], guard)
            self.assertEqual(result["git_binary"], acquire.GIT)
            self.assertEqual(result["git_env"]["HOME"],
                             str(parent / "git-home"))
            self.assertEqual(events[0], "created")
            fetches = [item for item in events if isinstance(item, tuple) and
                       item[1][0] == "fetch"]
            self.assertEqual(fetches, [
                (name, ("fetch", "-q", "--no-tags", "--depth=1", "origin",
                        acquire.PINS[name]["commit"]))
                for name in ("before", "candidate")])
            self.assertEqual(verify.call_count, 2)

    def test_second_source_failure_removes_both_private_checkouts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            workspace = root / "workspace"
            workspace.mkdir(mode=0o700)
            parent = root / "sources"
            parent.mkdir(mode=0o700)

            def fetch(args, cwd, active_guard, output, env):
                if cwd.name == "candidate" and args[0] == "fetch":
                    raise OSError("private fetch failure")

            with mock.patch.object(acquire, "_trusted_git"), \
                 mock.patch.object(acquire.capacity, "HostStorageGuard",
                                   return_value=lambda: None), \
                 mock.patch.object(acquire, "_git", side_effect=fetch), \
                 mock.patch.object(acquire.producer, "verify_source"):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_ACQUIRE$"):
                    acquire.acquire_sources(workspace, parent)
            self.assertFalse((parent / "before").exists())
            self.assertFalse((parent / "candidate").exists())
            self.assertEqual([entry.name for entry in parent.iterdir()],
                             ["git-acquire.private.log"])

    def test_active_git_guard_failure_reaps_owned_process_group(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            log = root / "private.log"
            home = root / "git-home"
            home.mkdir(mode=0o700)
            process = mock.Mock(pid=1234, returncode=None)
            process.poll.return_value = None
            process.wait.side_effect = [subprocess.TimeoutExpired("git", 1), None]
            calls = 0

            def guard():
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise ValueError("private capacity detail")

            with log.open("wb") as output, \
                 mock.patch.object(acquire.subprocess, "Popen",
                                   return_value=process), \
                 mock.patch.object(acquire.os, "killpg") as killpg:
                with self.assertRaisesRegex(ValueError,
                                            "private capacity detail"):
                    acquire._git(["fetch", "origin", "0" * 40], root,
                                 guard, output, acquire._git_env(home))
            killpg.assert_called_once_with(1234, acquire.signal.SIGTERM)
            self.assertEqual(process.wait.call_args_list[-1],
                             mock.call(timeout=20))

    def test_process_exit_race_still_reaps_git(self):
        process = mock.Mock(pid=1234)
        process.poll.return_value = None
        with mock.patch.object(acquire.os, "killpg",
                               side_effect=ProcessLookupError):
            acquire._stop_git(process)
        process.wait.assert_called_once_with(timeout=20)


if __name__ == "__main__":
    unittest.main()
