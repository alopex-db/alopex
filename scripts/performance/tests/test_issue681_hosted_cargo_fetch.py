"""Unit tests for UC-681-WATCHED-CARGO-FETCH-LOCKED."""

import hashlib
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_acquire_source as acquire
import issue681_hosted_cargo_fetch as cargo_fetch


class FakeGuard(cargo_fetch.capacity.HostStorageGuard):
    def __init__(self, fail_on=None):
        self.calls = 0
        self.fail_on = fail_on

    def __call__(self):
        self.calls += 1
        if self.fail_on is not None and self.calls >= self.fail_on:
            raise ValueError("STORAGE")


class WatchedCargoFetchTests(unittest.TestCase):
    def _make_sources(self, root: Path):
        sources = {}
        for name in ("before", "candidate"):
            src = root / name
            src.mkdir(mode=0o700)
            (src / "Cargo.toml").write_text("[workspace]\n")
            lock_bytes = f"lock-{name}\n".encode()
            (src / "Cargo.lock").write_bytes(lock_bytes)
            sources[name] = src
        return sources

    def test_fetch_and_stage_vendor_writes_offline_config_and_cleans_fetch_home(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            home = root / "home"
            home.mkdir(mode=0o700)
            cargo_home = home / "cargo-stage"
            cargo_home.mkdir(mode=0o700)
            scratch = root / "scratch"
            scratch.mkdir(mode=0o700)
            sources = self._make_sources(root)
            fake_pins = {
                name: {"lock": hashlib.sha256((sources[name] / "Cargo.lock").read_bytes()).hexdigest()}
                for name in ("before", "candidate")
            }
            guard = FakeGuard()
            captured = {}

            def fake_popen(argv, *, cwd, env, stdin, stdout, stderr, start_new_session):
                captured["argv"] = argv
                captured["env"] = env
                target_dir = Path(argv[-1])
                crate = target_dir / "demo-0.1.0"
                crate.mkdir(mode=0o755)
                (crate / "lib.rs").write_bytes(b"pub fn ok() {}\n")
                (crate / "lib.rs").chmod(0o644)
                proc = mock.Mock(pid=1111, returncode=0)
                proc.poll.side_effect = [None, 0]
                proc.wait.return_value = 0
                return proc

            with mock.patch.object(cargo_fetch.Path, "home", return_value=home), \
                 mock.patch.object(acquire, "PINS", fake_pins), \
                 mock.patch.object(cargo_fetch.rust_preflight, "_binary",
                                   return_value=Path("/usr/bin/cargo")), \
                 mock.patch.object(cargo_fetch.subprocess, "Popen", side_effect=fake_popen):
                result = cargo_fetch.fetch_and_stage_vendor(
                    sources, cargo_home, scratch, guard)

            self.assertEqual(result["acquisition_mode"], "cargo_vendor_locked")
            self.assertEqual(result["vendor_root"], cargo_home / "vendor")
            self.assertEqual(len(result["vendor_tree_sha256"]), 64)
            self.assertEqual(len(result["vendor_mode_sha256"]), 64)
            self.assertEqual(stat.S_IMODE((cargo_home / "vendor" / "demo-0.1.0" / "lib.rs").stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE((cargo_home / "config.toml").stat().st_mode), 0o600)
            self.assertIn("vendored-sources", (cargo_home / "config.toml").read_text())
            self.assertIn("--locked", captured["argv"])
            self.assertEqual(captured["env"]["RUSTUP_TOOLCHAIN"], "1.90.0")
            self.assertEqual(captured["env"]["RUSTUP_AUTO_INSTALL"], "0")
            # Ephemeral download CARGO_HOME must be removed after vendoring.
            self.assertFalse(Path(captured["env"]["CARGO_HOME"]).exists())

    def test_symlink_in_vendored_tree_and_guard_abort_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            home = root / "home"
            home.mkdir(mode=0o700)
            cargo_home = home / "cargo-stage"
            cargo_home.mkdir(mode=0o700)
            scratch = root / "scratch"
            scratch.mkdir(mode=0o700)
            sources = self._make_sources(root)
            fake_pins = {
                name: {"lock": hashlib.sha256((sources[name] / "Cargo.lock").read_bytes()).hexdigest()}
                for name in ("before", "candidate")
            }

            def symlink_popen(argv, **kwargs):
                target_dir = Path(argv[-1])
                (target_dir / "badlink").symlink_to(root)
                proc = mock.Mock(pid=2222, returncode=0)
                proc.poll.return_value = 0
                return proc

            with mock.patch.object(cargo_fetch.Path, "home", return_value=home), \
                 mock.patch.object(acquire, "PINS", fake_pins), \
                 mock.patch.object(cargo_fetch.rust_preflight, "_binary",
                                   return_value=Path("/usr/bin/cargo")), \
                 mock.patch.object(cargo_fetch.subprocess, "Popen", side_effect=symlink_popen):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                    cargo_fetch.fetch_and_stage_vendor(
                        sources, cargo_home, scratch, FakeGuard())
            self.assertEqual(list(cargo_home.iterdir()), [])

            # Storage guard abort must kill the cargo process group and clean up.
            abort_scratch = root / "scratch2"
            abort_scratch.mkdir(mode=0o700)
            abort_proc = mock.Mock(pid=3333, returncode=None)
            abort_proc.poll.return_value = None
            abort_proc.wait.side_effect = [subprocess.TimeoutExpired("cargo", 1), None]
            with mock.patch.object(cargo_fetch.Path, "home", return_value=home), \
                 mock.patch.object(acquire, "PINS", fake_pins), \
                 mock.patch.object(cargo_fetch.rust_preflight, "_binary",
                                   return_value=Path("/usr/bin/cargo")), \
                 mock.patch.object(cargo_fetch.subprocess, "Popen", return_value=abort_proc), \
                 mock.patch.object(cargo_fetch.os, "killpg") as killpg:
                with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                    cargo_fetch.fetch_and_stage_vendor(
                        sources, cargo_home, abort_scratch, FakeGuard(fail_on=3))
            killpg.assert_called_once_with(3333, cargo_fetch.signal.SIGTERM)
            self.assertEqual(list(cargo_home.iterdir()), [])

    def test_normalize_throttles_guard_and_enforces_deadline(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            vendor = root / "vendor"
            vendor.mkdir(mode=0o700)
            for idx in range(600):
                crate = vendor / f"c{idx // 50}"
                crate.mkdir(mode=0o755, exist_ok=True)
                (crate / f"f{idx}.rs").write_bytes(b"fn x(){}\n")
            guard = FakeGuard()
            diag = {}
            tree, modes = cargo_fetch._normalize_and_digest(
                vendor, guard, deadline=cargo_fetch.time.monotonic() + 30, diag=diag)
            self.assertEqual(len(tree), 64)
            self.assertEqual(len(modes), 64)
            # Bounded by entry stride (256) + start/end, never O(N) per file.
            self.assertLessEqual(guard.calls, 10)

            # Expired deadline fails closed with fixed FETCH_NORMALIZE_TIMEOUT.
            timeout_diag = {}
            with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                cargo_fetch._normalize_and_digest(
                    vendor, FakeGuard(), deadline=cargo_fetch.time.monotonic() - 1,
                    diag=timeout_diag)
            self.assertEqual(timeout_diag.get("code"), "FETCH_NORMALIZE_TIMEOUT")

    def test_fetch_removes_ephemeral_cache_before_normalize_and_reports_fixed_diag(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            home = root / "home"
            home.mkdir(mode=0o700)
            cargo_home = home / "cargo-stage"
            cargo_home.mkdir(mode=0o700)
            scratch = root / "scratch"
            scratch.mkdir(mode=0o700)
            sources = self._make_sources(root)
            fake_pins = {
                name: {"lock": hashlib.sha256((sources[name] / "Cargo.lock").read_bytes()).hexdigest()}
                for name in ("before", "candidate")
            }
            seen = {}

            def fake_popen(argv, *, cwd, env, stdin, stdout, stderr, start_new_session):
                seen["fetch_home"] = Path(env["CARGO_HOME"])
                (seen["fetch_home"] / "cache.bin").write_bytes(b"cache")
                target_dir = Path(argv[-1])
                crate = target_dir / "demo-0.1.0"
                crate.mkdir(mode=0o755)
                (crate / "lib.rs").write_bytes(b"pub fn ok() {}\n")
                proc = mock.Mock(pid=4444, returncode=0)
                proc.poll.return_value = 0
                return proc

            real_norm = cargo_fetch._normalize_and_digest

            def check_norm(vendor_root, guard, **kwargs):
                seen["fetch_home_exists_at_norm"] = seen["fetch_home"].exists()
                return real_norm(vendor_root, guard, **kwargs)

            diag = {}
            with mock.patch.object(cargo_fetch.Path, "home", return_value=home), \
                 mock.patch.object(acquire, "PINS", fake_pins), \
                 mock.patch.object(cargo_fetch.rust_preflight, "_binary",
                                   return_value=Path("/usr/bin/cargo")), \
                 mock.patch.object(cargo_fetch.subprocess, "Popen", side_effect=fake_popen), \
                 mock.patch.object(cargo_fetch, "_normalize_and_digest", side_effect=check_norm):
                cargo_fetch.fetch_and_stage_vendor(
                    sources, cargo_home, scratch, FakeGuard(), diag=diag)
            self.assertFalse(seen["fetch_home_exists_at_norm"])
            self.assertEqual(diag, {})

            # Non-zero exit populates fixed FETCH_VENDOR_EXIT diagnostic.
            scratch2 = root / "scratch2"
            scratch2.mkdir(mode=0o700)
            cargo_home2 = home / "cargo-stage-2"
            cargo_home2.mkdir(mode=0o700)
            fail_proc = mock.Mock(pid=5555, returncode=101)
            fail_proc.poll.return_value = 101
            fail_diag = {}
            with mock.patch.object(cargo_fetch.Path, "home", return_value=home), \
                 mock.patch.object(acquire, "PINS", fake_pins), \
                 mock.patch.object(cargo_fetch.rust_preflight, "_binary",
                                   return_value=Path("/usr/bin/cargo")), \
                 mock.patch.object(cargo_fetch.subprocess, "Popen", return_value=fail_proc):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                    cargo_fetch.fetch_and_stage_vendor(
                        sources, cargo_home2, scratch2, FakeGuard(), diag=fail_diag)
            self.assertEqual(fail_diag.get("code"), "FETCH_VENDOR_EXIT")

    def test_normalize_prunes_empty_dirs_and_matches_verify_tree(self):
        import issue681_hosted_vendor_tree as vendor_tree

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            vendor = root / "vendor"
            vendor.mkdir(mode=0o700)
            crate = vendor / "demo-0.1.0"
            (crate / "src").mkdir(parents=True, mode=0o755)
            (crate / "src" / "lib.rs").write_bytes(b"pub fn ok() {}\n")
            (crate / "empty-dir" / "nested-empty").mkdir(parents=True, mode=0o755)
            guard = FakeGuard()
            tree_sha, mode_sha = cargo_fetch._normalize_and_digest(
                vendor, guard, deadline=cargo_fetch.time.monotonic() + 30)
            self.assertFalse((crate / "empty-dir").exists())
            verified = vendor_tree.verify_tree(
                vendor, guard, mode_sha, expected_tree_sha256=tree_sha)
            self.assertEqual(verified, tree_sha)


if __name__ == "__main__":
    unittest.main()
