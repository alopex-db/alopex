"""Scoped storage and precheckout workflow contract, without host mutation."""

import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_storage as storage


class HostedStorageTests(unittest.TestCase):
    def layout(self, root):
        device = root.stat().st_dev
        self.mount_text = (f"1 1 {os.major(device)}:{os.minor(device)} / / rw - "
                           "ext4 synthetic rw\n")
        patcher = mock.patch.object(storage, "_mount_text", return_value=self.mount_text)
        patcher.start()
        self.addCleanup(patcher.stop)
        home, work, temp = (root / name for name in ("home", "work", "temp"))
        for path in (home, work, temp):
            path.mkdir(mode=0o700)
        checkout = work / "checkout"
        checkout.mkdir(mode=0o700)
        return home, checkout, temp

    def test_counts_scoped_roots_and_exact_scratch_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            home, checkout, temp = self.layout(root)
            cargo = home / ".cargo"
            cargo.mkdir(mode=0o700)
            (cargo / "crate").write_bytes(b"x" * 8192)
            (checkout / "copy").hardlink_to(cargo / "crate")
            scratch = root / "scratch"
            with mock.patch.object(storage.Path, "home", return_value=home):
                manifest = storage.make_manifest(checkout, temp, home=home,
                                                 scratch=scratch)
                first, pools = storage.snapshot(manifest)
                scratch.mkdir(mode=0o700)
                (scratch / "output").write_bytes(b"x" * 4096)
                second, _ = storage.snapshot(manifest)
            self.assertGreater(second, first)
            self.assertEqual(set(pools), {root.stat().st_dev})
            self.assertNotIn(home, manifest.roots)

    def test_symlink_unknown_device_and_unreadable_root_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            home, checkout, temp = self.layout(root)
            manifest = storage.make_manifest(checkout, temp, home=home,
                                             scratch=root / "scratch")
            (checkout / "link").symlink_to(temp)
            with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                storage.snapshot(manifest)
            (checkout / "link").unlink()
            with mock.patch.object(storage.os, "scandir", side_effect=PermissionError):
                with self.assertRaises(PermissionError):
                    storage.snapshot(manifest)

    def test_shared_scratch_parent_and_exact_roots(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            home, checkout, temp = self.layout(root)
            pool = root / "scratchpool"
            pool.mkdir(mode=0o700)
            scratch = pool / "scratch"
            manifest = storage.make_manifest(checkout, temp, home=home, scratch=scratch)
            self.assertEqual(storage.snapshot(manifest)[0], storage.snapshot(manifest)[0])
            self.assertNotIn(pool, manifest.roots)
            original = Path.lstat
            def root_owned(path):
                info = original(path)
                if path == pool:
                    fields = list(info)
                    fields[0] = stat.S_IFDIR | 0o1777
                    fields[4] = 0
                    return os.stat_result(fields)
                return info
            with mock.patch.object(Path, "lstat", root_owned):
                sticky = storage.make_manifest(checkout, temp, home=home,
                                               scratch=scratch)
                storage.snapshot(sticky)
            pool.chmod(0o777)
            with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                storage.make_manifest(checkout, temp, home=home, scratch=scratch)
            pool.chmod(0o700)
            link = root / "pool-link"
            link.symlink_to(pool, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                storage.make_manifest(checkout, temp, home=home,
                                      scratch=link / "scratch")
            scratch.mkdir(mode=0o700)
            with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                storage.make_manifest(checkout, temp, home=home, scratch=scratch)
            for target in (scratch, Path(str(scratch) + ".lock")):
                if target != scratch:
                    scratch.rmdir()
                    target.mkdir(mode=0o700)
                def foreign_owned(path):
                    info = original(path)
                    if path == target:
                        fields = list(info)
                        fields[4] = os.geteuid() + 1
                        return os.stat_result(fields)
                    return info
                with mock.patch.object(Path, "lstat", foreign_owned):
                    with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                        storage.snapshot(manifest)
            Path(str(scratch) + ".lock").rmdir()
            previous = root / "previous-pool"
            pool.rename(previous)
            pool.mkdir(mode=0o700)
            with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                storage.snapshot(manifest)

    def test_pinned_root_replacement_mode_owner_and_device_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            home, checkout, temp = self.layout(root)
            manifest = storage.make_manifest(checkout, temp, home=home,
                                             scratch=root / "scratch")
            temp.chmod(0o755)
            with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                storage.snapshot(manifest)
            temp.chmod(0o700)
            original = storage._root_identity
            for field in (0, 2):
                def changed(path, *, scratch_parent=False, field=field):
                    identity = list(original(path, scratch_parent=scratch_parent))
                    if path == temp:
                        identity[field] += 1
                    return tuple(identity)
                with mock.patch.object(storage, "_root_identity", side_effect=changed):
                    with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                        storage.snapshot(manifest)
            previous = root / "previous-temp"
            temp.rename(previous)
            temp.mkdir(mode=0o700)
            with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                storage.snapshot(manifest)

    def test_mount_topology_admitted_root_nested_bind_and_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            home, checkout, temp = self.layout(root)
            device = root.stat().st_dev
            prefix = f"{os.major(device)}:{os.minor(device)}"
            admitted = (self.mount_text +
                        f"2 1 {prefix} / {checkout} rw - ext4 synthetic rw\n")
            with mock.patch.object(storage, "_mount_text", return_value=admitted):
                manifest = storage.make_manifest(checkout, temp, home=home,
                                                 scratch=root / "scratch")
                storage.snapshot(manifest)
            nested = (admitted + f"3 2 {prefix} /outside {checkout / 'nested'} "
                      "rw - ext4 synthetic rw\n")
            with mock.patch.object(storage, "_mount_text", return_value=nested):
                with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                    storage.snapshot(manifest)
            with mock.patch.object(storage, "_mount_text",
                                   side_effect=(admitted, nested)):
                with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                    storage.snapshot(manifest)
            for changed in (admitted.replace("2 1 ", "4 1 "),
                            admitted.replace(f"{prefix} / {checkout}",
                                             f"{prefix} /different {checkout}")):
                with mock.patch.object(storage, "_mount_text", return_value=changed):
                    with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                        storage.snapshot(manifest)
            for malformed in ("bad\n", self.mount_text + self.mount_text,
                              self.mount_text + "x" * storage.MAX_MOUNT_BYTES):
                with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                    storage.parse_mountinfo(malformed)

    def test_stacked_mounts_and_escaped_backslash(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            home, checkout, temp = self.layout(root)
            device = root.stat().st_dev
            prefix = f"{os.major(device)}:{os.minor(device)}"
            unrelated = (self.mount_text +
                         f"7 1 {prefix} / /unrelated\\134name rw - ext4 synthetic rw\n"
                         f"8 7 {prefix} /second /unrelated\\134name rw - ext4 synthetic rw\n")
            with mock.patch.object(storage, "_mount_text", return_value=unrelated):
                plain = storage.make_manifest(checkout, temp, home=home,
                                              scratch=root / "scratch")
                storage.snapshot(plain)
            self.assertEqual(plain.mounts, storage.mount_topology(
                plain.anchors, plain.roots, self.mount_text))
            ordered = (self.mount_text +
                       f"2 1 {prefix} /first {checkout} rw - ext4 synthetic rw\n"
                       f"3 2 {prefix} /second {checkout} rw - ext4 synthetic rw\n")
            with mock.patch.object(storage, "_mount_text", return_value=ordered):
                manifest = storage.make_manifest(checkout, temp, home=home,
                                                 scratch=root / "scratch")
                storage.snapshot(manifest)
            self.assertEqual({row[0] for row in manifest.mounts}, {1, 2, 3})
            ambiguous = ordered.replace("3 2 ", "3 1 ")
            with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                storage.mount_topology(manifest.anchors, manifest.roots, ambiguous)
            for changed in (ordered.replace("3 2 ", "4 2 "),
                            ordered.replace("/second", "/changed")):
                with mock.patch.object(storage, "_mount_text", return_value=changed):
                    with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                        storage.snapshot(manifest)
            escaped_work = root / "work\\name"
            escaped_work.mkdir(mode=0o700)
            escaped_checkout = escaped_work / "checkout"
            escaped_checkout.mkdir(mode=0o700)
            encoded = str(escaped_work).replace("\\", r"\134")
            relevant = (unrelated +
                        f"9 1 {prefix} / {encoded} rw - ext4 synthetic rw\n")
            with mock.patch.object(storage, "_mount_text", return_value=relevant):
                escaped_manifest = storage.make_manifest(
                    escaped_checkout, temp, home=home, scratch=root / "scratch")
                storage.snapshot(escaped_manifest)
            self.assertIn(9, {row[0] for row in escaped_manifest.mounts})
            for malformed in (r"/bad\q", r"/bad\000", "/bad/../path"):
                with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                    storage._mount_path(malformed)

    def test_cargo_bin_regular_toolchain_symlinks_admitted_and_unsafe_links_denied(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            home, checkout, temp = self.layout(root)
            cargo = home / ".cargo"
            cargo_bin = cargo / "bin"
            rustup = home / ".rustup" / "toolchains" / "1.90.0" / "bin"
            sys_rust = root / "sys-rust" / "bin"
            for directory_path in (cargo, cargo_bin, rustup, sys_rust):
                directory_path.mkdir(parents=True, mode=0o700)
            rustup_bin = cargo_bin / "rustup"
            rustup_bin.write_bytes(b"r" * 4096)
            rustup_bin.chmod(0o755)
            rustc_target = rustup / "rustc"
            rustc_target.write_bytes(b"c" * 8192)
            rustc_target.chmod(0o755)
            sys_cargo = sys_rust / "cargo-audit"
            sys_cargo.write_bytes(b"s" * 4096)
            sys_cargo.chmod(0o755)
            (cargo_bin / "cargo").symlink_to(rustup_bin)
            (cargo_bin / "rustc").symlink_to(rustc_target)
            (cargo_bin / "cargo-audit").symlink_to(sys_cargo)
            scratch = root / "scratch"
            with mock.patch.object(storage, "TOOLCHAIN_SYSTEM_PREFIXES",
                                   (root / "sys-rust",)):
                manifest = storage.make_manifest(checkout, temp, home=home,
                                                 scratch=scratch)
                total_with_links, _ = storage.snapshot(manifest)
                # Inode deduplication: adding a second symlink to rustup_bin
                # only adds symlink inode blocks, never double-counting target.
                (cargo_bin / "rustdoc").symlink_to(rustup_bin)
                total_second_link, _ = storage.snapshot(manifest)
                self.assertGreaterEqual(total_second_link, total_with_links)
                self.assertLess(total_second_link - total_with_links, 4096)
                (cargo_bin / "rustdoc").unlink()
                # Writable regular target must fail.
                sys_cargo.chmod(0o775)
                with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                    storage.snapshot(manifest)
                sys_cargo.chmod(0o755)
                # Outside-prefix regular target must fail.
                outside = temp / "outside-tool"
                outside.write_bytes(b"o" * 4096)
                outside.chmod(0o755)
                (cargo_bin / "outside").symlink_to(outside)
                with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                    storage.snapshot(manifest)
                (cargo_bin / "outside").unlink()
                # Directory target inside ~/.cargo/bin must fail.
                (cargo_bin / "dirlink").symlink_to(rustup, target_is_directory=True)
                with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                    storage.snapshot(manifest)
                (cargo_bin / "dirlink").unlink()
                # Dangling symlink inside ~/.cargo/bin must fail.
                (cargo_bin / "dangling").symlink_to(cargo_bin / "missing")
                with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                    storage.snapshot(manifest)
                (cargo_bin / "dangling").unlink()
                # Nested symlink deeper than ~/.cargo/bin/<name> must fail.
                sub = cargo_bin / "sub"
                sub.mkdir(mode=0o700)
                (sub / "deep").symlink_to(rustup_bin)
                with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                    storage.snapshot(manifest)
                (sub / "deep").unlink()
                sub.rmdir()
                # Symlink in ~/.cache/bin pointing to ~/.cargo/bin/rustup must fail.
                cache_bin = home / ".cache" / "bin"
                cache_bin.mkdir(parents=True, mode=0o700)
                (cache_bin / "cargo").symlink_to(rustup_bin)
                manifest_with_cache = storage.make_manifest(
                    checkout, temp, home=home, scratch=scratch)
                with self.assertRaisesRegex(ValueError, "^STORAGE$"):
                    storage.snapshot(manifest_with_cache)

    def test_workflow_keeps_precheckout_before_fetch_and_fixed_output(self):
        workflow = Path(__file__).resolve().parents[3] / ".github/workflows/alopex-performance.yml"
        parsed = subprocess.check_output(
            ["ruby", "-rjson", "-ryaml", "-e",
             "puts JSON.generate(YAML.load_file(ARGV.fetch(0)))", str(workflow)],
            text=True)
        job = json.loads(parsed)["jobs"]["hnsw_release_admission"]
        steps = job["steps"]
        self.assertEqual(len(steps), 3)
        self.assertEqual(job["timeout-minutes"], 10)
        self.assertIn("STORAGE_ROOTS", steps[0]["run"])
        self.assertIn("baseline + 10_000_000_000 < 40_000_000_000",
                      steps[0]["run"])
        self.assertIn("info.st_blocks * 512", steps[0]["run"])
        self.assertEqual(steps[0]["run"].count("        check_anchors()"), 2)
        self.assertIn("HNSW_ADMISSION_ROOT_SEAL=", steps[0]["run"])
        self.assertNotIn("f_blocks - fs.f_bfree", steps[0]["run"])
        self.assertIn("Fetch exact selected source", steps[1]["name"])
        self.assertNotIn("upload-artifact", json.dumps(steps))
        build_script = (workflow.parents[2] / "scripts/build-nim-parser.sh").read_text()
        self.assertIn('mkdir -m 700 "${build_lock}"', build_script)
        self.assertIn('mkdir -m 700 "${build_root}"', build_script)

    def test_precheckout_and_checked_out_scanner_agree_on_fixture(self):
        workflow = Path(__file__).resolve().parents[3] / ".github/workflows/alopex-performance.yml"
        parsed = subprocess.check_output(
            ["ruby", "-rjson", "-ryaml", "-e",
             "puts JSON.generate(YAML.load_file(ARGV.fetch(0)))", str(workflow)],
            text=True)
        run = json.loads(parsed)["jobs"]["hnsw_release_admission"]["steps"][0]["run"]
        source = run.split("python3 - <<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            home, checkout, temp = self.layout(root)
            scratch = root / "scratch"
            source = source.replace("home = pathlib.Path.home()",
                                    f"home = pathlib.Path({str(home)!r})")
            source = source.replace(
                "scratch = pathlib.Path('/tmp/alopex-nim-parser-v0.8.17-contract-0.28.0-x86_64-unknown-linux-gnu')",
                f"scratch = pathlib.Path({str(scratch)!r})")
            source = source.replace("platform.system() == 'Linux' and platform.machine() == 'x86_64'",
                                    "True")
            mount_line = "            mounts = mount_topology(anchors, roots, mount_text())"
            mount_stub = f"            mount_text = lambda: {self.mount_text!r}"
            source = source.replace(mount_line, f"{mount_stub}\n{mount_line}")
            manifest = storage.make_manifest(checkout, temp, home=home,
                                             scratch=scratch)
            expected, _ = storage.snapshot(manifest)
            # Simulate hosted GITHUB_WORKSPACE starting at 0o755: Step 1 must tighten
            # to 0o700 before sealing so Step 2 chmod(0o700) never invalidates root_seal.
            checkout.chmod(0o755)
            output = temp / "admission.env"
            env = dict(os.environ, GITHUB_EVENT_NAME="workflow_dispatch",
                       GITHUB_JOB="hnsw_release_admission", ISSUE_NUMBER="681",
                       EXPECTED_CONTROLLER_SHA="a" * 40,
                       EXPECTED_HNSW_SOURCE_TREE="b" * 40,
                       GITHUB_SHA="a" * 40, GITHUB_REF="refs/heads/test",
                       RUNNER_ENVIRONMENT="github-hosted",
                       GITHUB_WORKSPACE=str(checkout), RUNNER_TEMP=str(temp),
                       GITHUB_ENV=str(output))
            result = subprocess.run([sys.executable, "-B", "-c", source],
                                    env=env, text=True, capture_output=True,
                                    timeout=10)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(stat.S_IMODE(checkout.stat().st_mode), 0o700)
            passed_payload = json.loads(result.stdout)
            self.assertEqual(passed_payload["case"], "HNSW_HOSTED_PRECHECK")
            self.assertEqual(passed_payload["outcome"], "passed")
            self.assertEqual(passed_payload["checks"]["env_write"], "passed")
            self.assertEqual(passed_payload["checks"]["tree_workspace"], "passed")
            self.assertEqual(passed_payload["checks"]["tree_temp"], "passed")
            self.assertEqual(passed_payload["checks"]["tree_cargo"], "skipped_missing")
            self.assertEqual(passed_payload["checks"]["budget"], "passed")
            self.assertEqual(passed_payload["checks"]["free_space"], "passed")
            self.assertEqual(output.read_text(),
                             f"HNSW_ADMISSION_BASELINE={expected}\n"
                             f"HNSW_ADMISSION_ROOT_SEAL={storage.root_seal(manifest)}\n")
            device = root.stat().st_dev
            prefix = f"{os.major(device)}:{os.minor(device)}"
            admitted_map = (self.mount_text +
                            f"2 1 {prefix} / {checkout} rw - ext4 synthetic rw\n")
            admitted_source = source.replace(
                mount_stub, f"            mount_text = lambda: {admitted_map!r}")
            with mock.patch.object(storage, "_mount_text", return_value=admitted_map):
                admitted_manifest = storage.make_manifest(checkout, temp, home=home,
                                                          scratch=scratch)
                storage.snapshot(admitted_manifest)
                admitted_seal = storage.root_seal(admitted_manifest)
            admitted_inline = subprocess.run([sys.executable, "-B", "-c", admitted_source],
                                             env=env, text=True, capture_output=True,
                                             timeout=10)
            self.assertEqual(json.loads(admitted_inline.stdout)["outcome"], "passed")
            self.assertIn("HNSW_ADMISSION_ROOT_SEAL=" + admitted_seal,
                          output.read_text())
            stacked_map = (self.mount_text +
                           f"2 1 {prefix} /first {checkout} rw - ext4 synthetic rw\n"
                           f"3 2 {prefix} /second {checkout} rw - ext4 synthetic rw\n")
            stacked_source = source.replace(
                mount_stub, f"            mount_text = lambda: {stacked_map!r}")
            with mock.patch.object(storage, "_mount_text", return_value=stacked_map):
                stacked_manifest = storage.make_manifest(checkout, temp, home=home,
                                                         scratch=scratch)
                stacked_seal = storage.root_seal(stacked_manifest)
            stacked_inline = subprocess.run([sys.executable, "-B", "-c", stacked_source],
                                            env=env, text=True, capture_output=True,
                                            timeout=10)
            self.assertEqual(json.loads(stacked_inline.stdout)["outcome"], "passed")
            self.assertIn("HNSW_ADMISSION_ROOT_SEAL=" + stacked_seal,
                          output.read_text())
            ambiguous_source = source.replace(
                mount_stub,
                f"            mount_text = lambda: {stacked_map.replace('3 2 ', '3 1 ')!r}")
            ambiguous_inline = subprocess.run([sys.executable, "-B", "-c",
                                               ambiguous_source], env=env,
                                              text=True, capture_output=True, timeout=10)
            ambiguous_data = json.loads(ambiguous_inline.stdout)
            self.assertEqual(ambiguous_data["outcome"], "failed")
            self.assertEqual(ambiguous_data["error_code"], "STORAGE_MOUNT")
            self.assertEqual(ambiguous_data["checks"]["mount_pre"], "failed")
            unrelated_map = (self.mount_text +
                             f"7 1 {prefix} / /unrelated\\134name rw - ext4 synthetic rw\n"
                             f"8 7 {prefix} /next /unrelated\\134name rw - ext4 synthetic rw\n")
            unrelated_source = source.replace(
                mount_stub, f"            mount_text = lambda: {unrelated_map!r}")
            unrelated_inline = subprocess.run([sys.executable, "-B", "-c",
                                               unrelated_source], env=env,
                                              text=True, capture_output=True, timeout=10)
            self.assertEqual(json.loads(unrelated_inline.stdout)["outcome"], "passed")
            escaped_work = root / "work\\name"
            escaped_work.mkdir(mode=0o700)
            escaped_checkout = escaped_work / "checkout"
            escaped_checkout.mkdir(mode=0o700)
            encoded = str(escaped_work).replace("\\", r"\134")
            escaped_map = (unrelated_map +
                           f"9 1 {prefix} / {encoded} rw - ext4 synthetic rw\n")
            escaped_source = source.replace(
                mount_stub, f"            mount_text = lambda: {escaped_map!r}")
            escaped_env = dict(env, GITHUB_WORKSPACE=str(escaped_checkout))
            with mock.patch.object(storage, "_mount_text", return_value=escaped_map):
                escaped_manifest = storage.make_manifest(
                    escaped_checkout, temp, home=home, scratch=scratch)
                escaped_seal = storage.root_seal(escaped_manifest)
            escaped_inline = subprocess.run([sys.executable, "-B", "-c", escaped_source],
                                            env=escaped_env, text=True,
                                            capture_output=True, timeout=10)
            self.assertEqual(json.loads(escaped_inline.stdout)["outcome"], "passed")
            self.assertIn("HNSW_ADMISSION_ROOT_SEAL=" + escaped_seal,
                          output.read_text())
            nested_map = (admitted_map +
                          f"3 2 {prefix} /outside {checkout / 'nested'} "
                          "rw - ext4 synthetic rw\n")
            nested_source = source.replace(
                mount_stub, f"            mount_text = lambda: {nested_map!r}")
            nested_inline = subprocess.run([sys.executable, "-B", "-c", nested_source],
                                           env=env, text=True, capture_output=True,
                                           timeout=10)
            nested_data = json.loads(nested_inline.stdout)
            self.assertEqual(nested_data["outcome"], "failed")
            self.assertEqual(nested_data["error_code"], "STORAGE_MOUNT")
            self.assertNotIn(str(checkout), nested_inline.stdout)
            drift_source = source.replace(
                mount_stub,
                f"            mount_values = ({admitted_map!r}, {admitted_map!r}, {nested_map!r})\n"
                "            mount_calls = iter(mount_values)\n"
                "            mount_text = lambda: next(mount_calls)")
            drift_inline = subprocess.run([sys.executable, "-B", "-c", drift_source],
                                          env=env, text=True, capture_output=True,
                                          timeout=10)
            drift_data = json.loads(drift_inline.stdout)
            self.assertEqual(drift_data["outcome"], "failed")
            self.assertEqual(drift_data["error_code"], "STORAGE_MOUNT")
            self.assertEqual(drift_data["checks"]["mount_pre"], "passed")
            self.assertEqual(drift_data["checks"]["mount_post"], "failed")
            sticky_source = source.replace(
                "    def root_identity(path, scratch_parent=False):",
                "    original_lstat = pathlib.Path.lstat\n"
                "    def sticky_lstat(path):\n"
                "        info = original_lstat(path)\n"
                "        if path == scratch.parent:\n"
                "            fields = list(info)\n"
                "            fields[0] = stat.S_IFDIR | 0o1777\n"
                "            fields[4] = 0\n"
                "            return os.stat_result(fields)\n"
                "        return info\n"
                "    pathlib.Path.lstat = sticky_lstat\n"
                "    def root_identity(path, scratch_parent=False):")
            sticky = subprocess.run([sys.executable, "-B", "-c", sticky_source],
                                    env=env, text=True, capture_output=True,
                                    timeout=10)
            self.assertEqual(json.loads(sticky.stdout)["outcome"], "passed")
            original_lstat = Path.lstat
            def root_owned(path):
                info = original_lstat(path)
                if path == scratch.parent:
                    fields = list(info)
                    fields[0] = stat.S_IFDIR | 0o1777
                    fields[4] = 0
                    return os.stat_result(fields)
                return info
            with mock.patch.object(Path, "lstat", root_owned):
                sticky_manifest = storage.make_manifest(checkout, temp, home=home,
                                                        scratch=scratch)
                self.assertIn("HNSW_ADMISSION_ROOT_SEAL=" +
                              storage.root_seal(sticky_manifest), output.read_text())
            low_free_source = source.replace(
                "    free_by_device = {}",
                "    original_statvfs = os.statvfs\n"
                "    def low_statvfs(path):\n"
                "        fs = original_statvfs(path)\n"
                "        if path == scratch.parent:\n"
                "            values = list(fs)\n"
                "            values[4] = 0\n"
                "            return os.statvfs_result(values)\n"
                "        return fs\n"
                "    os.statvfs = low_statvfs\n"
                "    free_by_device = {}")
            low = subprocess.run([sys.executable, "-B", "-c", low_free_source],
                                 env=env, text=True, capture_output=True,
                                 timeout=10)
            low_data = json.loads(low.stdout)
            self.assertEqual(low_data["outcome"], "failed")
            self.assertEqual(low_data["error_code"], "STORAGE_FREE")
            self.assertEqual(low_data["checks"]["free_space"], "failed")
            scratch.mkdir(mode=0o700)
            occupied = subprocess.run([sys.executable, "-B", "-c", source],
                                      env=env, text=True, capture_output=True,
                                      timeout=10)
            self.assertEqual(json.loads(occupied.stdout)["error_code"],
                             "STORAGE_WORKSPACE")
            scratch.rmdir()
            root.chmod(0o777)
            nonsticky = subprocess.run([sys.executable, "-B", "-c", source],
                                       env=env, text=True, capture_output=True,
                                       timeout=10)
            self.assertEqual(json.loads(nonsticky.stdout)["error_code"],
                             "STORAGE_WORKSPACE")
            root.chmod(0o700)
            (temp / "leak").symlink_to(home)
            failed = subprocess.run([sys.executable, "-B", "-c", source],
                                    env=env, text=True, capture_output=True,
                                    timeout=10)
            failed_data = json.loads(failed.stdout)
            self.assertEqual(failed_data["outcome"], "failed")
            self.assertEqual(failed_data["error_code"], "STORAGE_LINK_TREE_TEMP")
            self.assertEqual(failed_data["checks"]["tree_temp"], "STORAGE_LINK_TREE_TEMP")
            self.assertEqual(failed_data["tree_findings"]["temp"], ["LINK_DIR_TARGET_OTHER"])
            self.assertNotIn(str(home), failed.stdout)
            (temp / "leak").unlink()
            prior_env = output.read_text()
            (home / ".cargo").symlink_to(temp, target_is_directory=True)
            cache_link = subprocess.run([sys.executable, "-B", "-c", source],
                                        env=env, text=True, capture_output=True,
                                        timeout=10)
            cache_link_data = json.loads(cache_link.stdout)
            self.assertEqual(cache_link_data["outcome"], "failed")
            self.assertEqual(cache_link_data["error_code"], "STORAGE_LINK_ROOT_CARGO")
            self.assertEqual(cache_link_data["root_link_failures"], ["STORAGE_LINK_ROOT_CARGO"])
            self.assertNotIn(str(temp), cache_link.stdout)
            self.assertEqual(output.read_text(), prior_env)
            (home / ".cargo").unlink()
            (home / ".cargo").symlink_to(home / "missing", target_is_directory=True)
            dangling = subprocess.run([sys.executable, "-B", "-c", source],
                                      env=env, text=True, capture_output=True,
                                      timeout=10)
            self.assertEqual(json.loads(dangling.stdout)["error_code"],
                             "STORAGE_LINK_ROOT_CARGO")
            self.assertEqual(output.read_text(), prior_env)
            (home / ".cargo").unlink()
            (home / ".cargo").mkdir(mode=0o700)
            (home / ".cargo" / "proxy").symlink_to(temp)
            nested_link = subprocess.run([sys.executable, "-B", "-c", source],
                                         env=env, text=True, capture_output=True,
                                         timeout=10)
            self.assertEqual(json.loads(nested_link.stdout)["error_code"],
                             "STORAGE_LINK_TREE_CARGO")
            self.assertNotIn(str(temp), nested_link.stdout)
            self.assertEqual(output.read_text(), prior_env)
            (home / ".cargo" / "proxy").unlink()
            (home / ".cargo" / "proxy").symlink_to(root / "outside")
            outside_link = subprocess.run([sys.executable, "-B", "-c", source],
                                          env=env, text=True, capture_output=True,
                                          timeout=10)
            outside_data = json.loads(outside_link.stdout)
            self.assertEqual(outside_data["error_code"], "STORAGE_LINK_TREE_CARGO")
            self.assertEqual(outside_data["tree_findings"]["cargo"], ["LINK_DANGLING_OR_LOOP"])
            self.assertNotIn(str(root / "outside"), outside_link.stdout)
            self.assertEqual(output.read_text(), prior_env)
            (home / ".cargo" / "proxy").unlink()
            # Standard ~/.cargo/bin regular-file toolchain symlink passes and agrees with storage.py.
            cargo_bin = home / ".cargo" / "bin"
            cargo_bin.mkdir(mode=0o700)
            rustup_proxy = cargo_bin / "rustup"
            rustup_proxy.write_bytes(b"p" * 4096)
            rustup_proxy.chmod(0o755)
            (cargo_bin / "cargo").symlink_to(rustup_proxy)
            output.write_text("")
            cargo_manifest = storage.make_manifest(checkout, temp, home=home,
                                                   scratch=scratch)
            expected_cargo, _ = storage.snapshot(cargo_manifest)
            cargo_pass = subprocess.run([sys.executable, "-B", "-c", source],
                                        env=env, text=True, capture_output=True,
                                        timeout=10)
            self.assertEqual(cargo_pass.returncode, 0, cargo_pass.stdout + cargo_pass.stderr)
            cargo_pass_data = json.loads(cargo_pass.stdout)
            self.assertEqual(cargo_pass_data["outcome"], "passed")
            self.assertEqual(cargo_pass_data["checks"]["tree_cargo"], "passed")
            self.assertEqual(output.read_text(),
                             f"HNSW_ADMISSION_BASELINE={expected_cargo}\n"
                             f"HNSW_ADMISSION_ROOT_SEAL={storage.root_seal(cargo_manifest)}\n")
            prior_env = output.read_text()
            # Single-run multi-failure summary across cargo, cache, rustup, and free_space.
            (cargo_bin / "cargo").unlink()
            rustup_proxy.unlink()
            cargo_bin.rmdir()
            (home / ".cargo" / "bin").symlink_to(temp, target_is_directory=True)
            (home / ".cache").mkdir(mode=0o700)
            (home / ".cache" / "c").symlink_to(root / "missing-cache")
            (home / ".rustup").mkdir(mode=0o700)
            (home / ".rustup" / "r").symlink_to(home / ".rustup")
            multi = subprocess.run([sys.executable, "-B", "-c", low_free_source],
                                   env=env, text=True, capture_output=True,
                                   timeout=10)
            self.assertEqual(multi.returncode, 1)
            multi_data = json.loads(multi.stdout)
            self.assertEqual(multi_data["outcome"], "failed")
            self.assertEqual(multi_data["error_code"], "STORAGE_LINK_TREE_CARGO")
            self.assertEqual(multi_data["checks"]["tree_workspace"], "passed")
            self.assertEqual(multi_data["checks"]["tree_temp"], "passed")
            self.assertEqual(multi_data["checks"]["tree_cargo"], "STORAGE_LINK_TREE_CARGO")
            self.assertEqual(multi_data["checks"]["tree_cache"], "STORAGE_LINK_TREE_CACHE")
            self.assertEqual(multi_data["checks"]["tree_rustup"], "STORAGE_LINK_TREE_RUSTUP")
            self.assertEqual(multi_data["checks"]["free_space"], "failed")
            self.assertEqual(multi_data["checks"]["env_write"], "skipped_failed")
            self.assertEqual(multi_data["tree_findings"]["cargo"], ["CARGO_BIN_DIR_LINK_OTHER"])
            self.assertEqual(multi_data["tree_findings"]["cache"], ["LINK_DANGLING_OR_LOOP"])
            self.assertEqual(multi_data["tree_findings"]["rustup"], ["LINK_DIR_TARGET_TOOLCHAIN_PREFIX"])
            self.assertNotIn(str(root), multi.stdout)
            self.assertNotIn("/", multi.stdout)
            self.assertEqual(output.read_text(), prior_env)
            (home / ".cargo" / "bin").unlink()
            (home / ".cargo").rmdir()
            (home / ".cache" / "c").unlink()
            (home / ".cache").rmdir()
            (home / ".rustup" / "r").unlink()
            (home / ".rustup").rmdir()
            changed_source = source.replace(
                "    code = 'STORAGE_ROOT_IDENTITY'\n"
                "    if checks['root_identity_pre'] == 'passed':",
                "    temporary.chmod(0o755)\n"
                "    code = 'STORAGE_ROOT_IDENTITY'\n"
                "    if checks['root_identity_pre'] == 'passed':")
            drift = subprocess.run([sys.executable, "-B", "-c", changed_source],
                                   env=env, text=True, capture_output=True,
                                   timeout=10)
            drift_identity = json.loads(drift.stdout)
            self.assertEqual(drift_identity["outcome"], "failed")
            self.assertEqual(drift_identity["error_code"], "STORAGE_ROOT_IDENTITY")
            self.assertEqual(drift_identity["checks"]["root_identity_post"], "failed")
            self.assertNotIn(str(temp), drift.stdout)
            bounded_source = source.replace("    deadline = time.monotonic() + 90",
                                            "    deadline = 0")
            bounded = subprocess.run([sys.executable, "-B", "-c", bounded_source],
                                     env=env, text=True, capture_output=True,
                                     timeout=10)
            bounded_data = json.loads(bounded.stdout)
            self.assertEqual(bounded_data["error_code"], "STORAGE_BOUNDS")
            self.assertEqual(bounded_data["checks"]["bounds"], "failed")
            tree_source = source.replace(
                "    seen = set()\n    baseline = entries = 0",
                "    def unreadable(_path):\n"
                "        raise PermissionError('private path')\n"
                "    os.scandir = unreadable\n"
                "    seen = set()\n    baseline = entries = 0")
            unreadable = subprocess.run([sys.executable, "-B", "-c", tree_source],
                                        env=env, text=True, capture_output=True,
                                        timeout=10)
            unreadable_data = json.loads(unreadable.stdout)
            self.assertEqual(unreadable_data["error_code"], "STORAGE_TREE")
            self.assertIn("TREE_SCANDIR_ERROR", unreadable_data["tree_findings"]["workspace"])
            self.assertNotIn("private path", unreadable.stdout)
            scratch_race = source.replace(
                "    seen = set()\n    baseline = entries = 0",
                "    scratch.symlink_to(temporary)\n"
                "    seen = set()\n    baseline = entries = 0")
            late_scratch = subprocess.run([sys.executable, "-B", "-c", scratch_race],
                                          env=env, text=True, capture_output=True,
                                          timeout=10)
            self.assertTrue(late_scratch.stdout, late_scratch.stderr)
            self.assertEqual(json.loads(late_scratch.stdout)["error_code"],
                             "STORAGE_LINK_TREE_SCRATCH")
            self.assertEqual(output.read_text(), prior_env)
            scratch.unlink()
            home_race = source.replace(
                "    for path in fixed:\n",
                f"    home.rename(pathlib.Path({str(root / 'oldhome')!r}))\n"
                f"    home.symlink_to(pathlib.Path({str(root / 'oldhome')!r}))\n"
                "    for path in fixed:\n")
            late_home = subprocess.run([sys.executable, "-B", "-c", home_race],
                                       env=env, text=True, capture_output=True,
                                       timeout=10)
            self.assertEqual(json.loads(late_home.stdout)["error_code"],
                             "STORAGE_LINK_ROOT_HOME")
            self.assertEqual(output.read_text(), prior_env)


if __name__ == "__main__":
    unittest.main()
