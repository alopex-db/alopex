"""Parser tool preflight rejects untrusted identities and keeps output private."""

from pathlib import Path
import shlex
import stat
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_parser_tools as tools


class HostedParserToolsTest(unittest.TestCase):
    def fixture(self, root, *, nim_version="2.2.10", nimble_sha=None,
                nim_script=None, nimble_script=None):
        output = root / tools.archive.EXTRACTED_NAME
        bin_dir = output / "nim-2.2.10/bin"
        bin_dir.mkdir(parents=True, mode=0o700)
        output.chmod(0o700)
        bin_dir.parent.chmod(0o700)
        bin_dir.chmod(0o700)
        nimble_sha = nimble_sha or tools.NIMBLE_SHA
        nim_text = (nim_script or
                    "#!/bin/sh\nprintf 'Nim Compiler Version " +
                    nim_version + " [Linux: amd64]\\n'\n")
        nimble_text = (nimble_script or
                       "#!/bin/sh\nprintf 'nimble v0.22.3 compiled\\ngit hash: " +
                       nimble_sha + "\\n'\n")
        for name, script in (("nim", nim_text), ("nimble", nimble_text)):
            path = bin_dir / name
            path.write_text(script)
            path.chmod(0o700)
        return output

    def test_exact_versions_and_revision_pass_without_public_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            output = self.fixture(root)
            guards = []
            paths = tools.verify_tools(output, lambda: guards.append(True))
            self.assertEqual(set(paths), {"nim", "nimble"})
            self.assertFalse((root / "tools-probe.partial").exists())
            self.assertGreaterEqual(len(guards), 4)
            self.assertFalse((root / "download.private.log").exists())

    def test_wrong_nim_or_nimble_revision_is_rejected(self):
        for kind in ("nim", "nimble"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp).resolve()
                output = self.fixture(root,
                                      nim_version="2.2.12" if kind == "nim" else "2.2.10",
                                      nimble_sha="0" * 40 if kind == "nimble" else None)
                with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                    tools.verify_tools(output, lambda: None)
                self.assertFalse((root / "tools-probe.partial").exists())
                private_log = root / "download.private.log"
                self.assertEqual(stat.S_IMODE(private_log.stat().st_mode), 0o600)
                self.assertNotIn(str(root), private_log.read_text())

    def test_symlink_tool_is_rejected_before_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            output = self.fixture(root)
            nimble = output / "nim-2.2.10/bin/nimble"
            nimble.unlink()
            nimble.symlink_to(output / "nim-2.2.10/bin/nim")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                tools.verify_tools(output, lambda: None)
            self.assertFalse((root / "tools-probe.partial").exists())

    def test_active_guard_failure_stops_probe(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            output = self.fixture(root, nim_script="#!/bin/sh\nsleep 5\n")
            count = 0

            def guard():
                nonlocal count
                count += 1
                if count == 2:
                    raise RuntimeError("private diagnostic")

            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                tools.verify_tools(output, guard)
            self.assertFalse((root / "tools-probe.partial").exists())

    def test_output_over_bound_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            output = self.fixture(root,
                                  nim_script="#!/bin/sh\n"
                                  "printf 'Nim Compiler Version 2.2.10\\n'\n"
                                  "head -c 5000 /dev/zero\n")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                tools.verify_tools(output, lambda: None)
            self.assertFalse((root / "tools-probe.partial").exists())
            self.assertLess((root / "download.private.log").stat().st_size, 100)

    def test_background_child_is_stopped_after_leader_exits(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            marker = root / "survived"
            script = ("#!/bin/sh\n(sleep 0.5; echo survived > " +
                      shlex.quote(str(marker)) + ") &\n"
                      "printf 'Nim Compiler Version 2.2.10 [Linux: amd64]\\n'\n")
            output = self.fixture(root, nim_script=script)
            tools.verify_tools(output, lambda: None)
            time.sleep(0.8)
            self.assertFalse(marker.exists())

    def test_symlinked_bin_directory_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            output = self.fixture(root)
            package = output / "nim-2.2.10"
            (package / "bin").rename(package / "real-bin")
            (package / "bin").symlink_to(package / "real-bin")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                tools.verify_tools(output, lambda: None)
            self.assertFalse((root / "tools-probe.partial").exists())

    def test_mixed_nim_version_banner_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            output = self.fixture(
                root, nim_script="#!/bin/sh\n"
                "printf 'Nim Compiler Version 2.2.12\\n"
                "Nim Compiler Version 2.2.10\\n'\n")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                tools.verify_tools(output, lambda: None)

    def test_timed_out_probe_is_stopped_and_cleaned(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            output = self.fixture(root, nim_script="#!/bin/sh\nsleep 5\n")
            with mock.patch.object(tools, "TIME_LIMIT", 0.2):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                    tools.verify_tools(output, lambda: None)
            self.assertFalse((root / "tools-probe.partial").exists())


if __name__ == "__main__":
    unittest.main()
