"""Exact-source and parser admission before a hosted public asset build."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_public_assets_produce as produce


def sha(data):
    return hashlib.sha256(data).hexdigest()


class SourceBuildAdmissionTests(unittest.TestCase):
    def test_host_guard_runs_before_during_and_after_cargo(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            work = root / "work"
            work.mkdir()
            process = mock.Mock(pid=1234, returncode=0)
            process.poll.side_effect = [None, 0]
            calls = []
            with mock.patch.object(produce.subprocess, "Popen",
                                   return_value=process) as popen, \
                 mock.patch.object(produce.case.storage, "inventory",
                                   return_value={"allocated_bytes": 1}):
                produce._build(root, root / "target", work,
                               root / "parser/lib.so", [root], 10,
                               "1.90.0", {}, lambda: calls.append("guard"),
                               root / "cargo-home")
            self.assertEqual(calls, ["guard", "guard", "guard"])
            process.wait.assert_called_once_with(timeout=15)
            self.assertEqual(popen.call_args.kwargs["env"]["CARGO_HOME"],
                             str(root / "cargo-home"))
            self.assertEqual(popen.call_args.kwargs["env"]["CARGO_NET_OFFLINE"],
                             "true")

    def test_only_exact_private_vendor_config_is_admitted(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp).resolve() / "home"
            home.mkdir(mode=0o700)
            cargo_home = home / "cargo-home"
            cargo_home.mkdir(mode=0o700)
            vendor = cargo_home / "vendor"
            vendor.mkdir(mode=0o700)
            config = cargo_home / "config.toml"
            config.write_text("[source.crates-io]\n"
                              "replace-with = \"vendored-sources\"\n\n"
                              "[source.vendored-sources]\n"
                              "directory = " + json.dumps(str(vendor)) + "\n")
            config.chmod(0o600)
            with mock.patch.object(produce.Path, "home", return_value=home):
                produce.verify_cargo_home(cargo_home)
                config.write_text("[source.crates-io]\n")
                with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                    produce.verify_cargo_home(cargo_home)
                config.unlink()
                with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                    produce.verify_cargo_home(cargo_home)

    def test_invalid_cargo_config_stops_before_cargo_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            for name in ("source", "target", "work", "parser", "output",
                         "cargo-home", "cargo-home/vendor"):
                (root / name).mkdir(mode=0o700, parents=True)
            cargo_home = root / "cargo-home"
            config = cargo_home / "config.toml"
            config.write_text("[source.crates-io]\n")
            config.chmod(0o600)
            inputs = dict(
                source=root / "source", target=root / "target",
                work=root / "work", output=root / "output/bundle",
                parser_record=root / "record", parser_root=root / "parser",
                parser_library=root / "parser/lib.so",
                source_commit="1" * 40, source_tree="2" * 40,
                example_sha256="3" * 64, cargo_lock_sha256="4" * 64,
                parser_record_sha256="5" * 64, parser_sha256="6" * 64,
                rust_version="1.90.0", additional_storage_bytes=8_000_000_000,
                private_roots=(str(root),), build_lock=root / "build.lock",
                host_guard=lambda: None, git_binary=Path("/usr/bin/git"),
                git_env={}, cargo_home=cargo_home)
            with mock.patch.object(produce.sys, "platform", "linux"), \
                 mock.patch.object(produce.platform, "machine",
                                   return_value="x86_64"), \
                 mock.patch.object(produce.Path, "home", return_value=root), \
                 mock.patch.object(produce.subprocess, "Popen") as popen:
                for missing in (False, True):
                    if missing:
                        config.unlink()
                    with self.subTest(missing=missing):
                        with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                            produce.produce(**inputs)
                popen.assert_not_called()

    def test_active_host_guard_failure_reaps_cargo_group(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            work = root / "work"
            work.mkdir()
            process = mock.Mock(pid=1234, returncode=None)
            process.poll.return_value = None
            process.wait.side_effect = [subprocess.TimeoutExpired("cargo", 15),
                                        None]
            calls = []

            def guard():
                calls.append("guard")
                if len(calls) == 2:
                    raise ValueError("private host scan failed")

            with mock.patch.object(produce.subprocess, "Popen", return_value=process), \
                 mock.patch.object(produce.case.storage, "inventory",
                                   return_value={"allocated_bytes": 1}), \
                 mock.patch.object(produce.os, "killpg") as killpg:
                with self.assertRaisesRegex(ValueError, "private host scan failed"):
                    produce._build(root, root / "target", work,
                                   root / "parser/lib.so", [root], 10,
                                   "1.90.0", {}, guard, root / "cargo-home")
            self.assertEqual(calls, ["guard", "guard"])
            killpg.assert_called_once_with(1234, produce.signal.SIGTERM)
            self.assertEqual(process.wait.call_args_list[-1],
                             mock.call(timeout=20))

    def test_exact_clean_source_and_changed_example(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp).resolve() / "source"
            source.mkdir(mode=0o700)
            for name, data in (
                (produce.EXAMPLE, b"fn main() {}\n"),
                (produce.CONTRACT, b"0.28.0\n"),
                ("Cargo.lock", b"locked\n"),
                ("Cargo.toml", b"[workspace.package]\nversion = '0.8.17'\n"),
            ):
                path = source / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
            def git(*args):
                return subprocess.check_output(
                    ["git", *args], cwd=source, stderr=subprocess.DEVNULL).decode().strip()
            git("init", "-q")
            git("add", ".")
            git("-c", "user.name=test", "-c", "user.email=test@example.invalid",
                "commit", "-qm", "source")
            commit, tree = git("rev-parse", "HEAD"), git("rev-parse", "HEAD^{tree}")
            git_home = Path(tmp).resolve() / "git-home"
            git_home.mkdir(mode=0o700)
            git_env = {"HOME": str(git_home), "XDG_CONFIG_HOME": str(git_home),
                       "PATH": "/usr/bin:/bin", "LANG": "C.UTF-8",
                       "GIT_TERMINAL_PROMPT": "0", "GIT_CONFIG_GLOBAL": "/dev/null",
                       "GIT_CONFIG_NOSYSTEM": "1", "GIT_ASKPASS": "/bin/false"}
            produce.verify_source(source, commit, tree,
                                  sha(b"fn main() {}\n"), sha(b"locked\n"),
                                  git_binary=Path("/usr/bin/git"), git_env=git_env)
            (source / produce.EXAMPLE).write_text("fn main() { panic!() }\n")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                produce.verify_source(source, commit, tree,
                                      sha(b"fn main() {}\n"), sha(b"locked\n"),
                                      git_binary=Path("/usr/bin/git"),
                                      git_env=git_env)

    def test_parser_sidecars_must_match_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            parser = root / "parser"
            parser.mkdir(mode=0o700)
            members = {"library": ("lib/parser.so", b"library"),
                       "static_library": ("lib/parser.a", b"static"),
                       "build_identity": ("meta/identity.json", b"identity")}
            record = {"contract_version": "0.28.0",
                      "target": produce.HOST}
            for role, (name, data) in members.items():
                path = parser / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
                record[role] = {"path": name, "size": len(data), "sha256": sha(data)}
            (parser / "lib/CONTRACT_VERSION").write_bytes(b"0.28.0\n")
            (parser / "lib/SHA256SUMS").write_text("".join(
                record[role]["sha256"] + "  " + Path(record[role]["path"]).name + "\n"
                for role in ("library", "static_library")))
            record_path = root / "record.json"
            raw = json.dumps(record).encode()
            record_path.write_bytes(raw)
            library = parser / "lib/parser.so"
            self.assertEqual(produce.verify_parser(record_path, parser, library),
                             sha(raw))
            (parser / "lib/parser.a").write_bytes(b"wrong")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                produce.verify_parser(record_path, parser, library)


if __name__ == "__main__":
    unittest.main()
