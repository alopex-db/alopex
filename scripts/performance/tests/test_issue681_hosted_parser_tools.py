"""Parser tool preflight rejects untrusted identities and keeps output private."""

from pathlib import Path
import hashlib
import io
import shlex
import stat
import sys
import tarfile
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

    def _make_nimble_archive(self, payload: bytes, *, extra_member=None,
                             member_name="nimble", member_mode=0o755,
                             symlink_member=False):
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            info = tarfile.TarInfo(member_name)
            if symlink_member:
                info.type = tarfile.SYMTYPE
                info.linkname = "/bin/sh"
                info.mode = 0o777
                tf.addfile(info)
            else:
                info.size = len(payload)
                info.mode = member_mode
                tf.addfile(info, io.BytesIO(payload))
            if extra_member is not None:
                extra = tarfile.TarInfo(extra_member)
                extra.size = 1
                extra.mode = 0o644
                tf.addfile(extra, io.BytesIO(b"x"))
        return buf.getvalue()

    def _fake_requests(self, archive_bytes, *,
                       redirect_location="https://release-assets.githubusercontent.com/github-production-release-asset/1?sig=ok"):
        class FakeConn:
            def close(self):
                pass

        class FakeResp:
            def __init__(self, status, headers, body=b""):
                self.status = status
                self._headers = headers
                self._stream = io.BytesIO(body)

            def getheader(self, name):
                return self._headers.get(name)

            def read(self, size=-1):
                return self._stream.read(size)

            def close(self):
                pass

        calls = []

        def fake_request(host, target):
            calls.append((host, target))
            if len(calls) == 1:
                return FakeConn(), FakeResp(302, {"Location": redirect_location})
            return FakeConn(), FakeResp(
                200, {"Content-Length": str(len(archive_bytes))}, archive_bytes)

        return fake_request, calls

    def test_install_pinned_nimble_replaces_bundled_tool_and_verifies(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            output = self.fixture(root, nimble_sha="0" * 40)
            payload = (
                "#!/bin/sh\nprintf 'nimble v0.22.3 compiled\\ngit hash: " +
                tools.NIMBLE_SHA + "\\n'\n"
            ).encode()
            archive_bytes = self._make_nimble_archive(payload)
            fake_request, calls = self._fake_requests(archive_bytes)
            diag = {}
            with mock.patch.object(tools, "NIMBLE_ARCHIVE_BYTES", len(archive_bytes)), \
                 mock.patch.object(tools, "NIMBLE_ARCHIVE_SHA256",
                                   hashlib.sha256(archive_bytes).hexdigest()), \
                 mock.patch.object(tools, "NIMBLE_BIN_BYTES", len(payload)), \
                 mock.patch.object(tools, "NIMBLE_BIN_SHA256",
                                   hashlib.sha256(payload).hexdigest()), \
                 mock.patch.object(tools.archive, "_request", side_effect=fake_request):
                nimble = tools.install_pinned_nimble(output, lambda: None, diag=diag)
            self.assertEqual(nimble, output / "nim-2.2.10/bin/nimble")
            self.assertEqual(stat.S_IMODE(nimble.stat().st_mode), 0o700)
            self.assertEqual(diag, {})
            self.assertEqual(len(calls), 2)
            self.assertEqual(calls[0], (tools.archive.FIRST_HOST, tools.NIMBLE_DOWNLOAD_PATH))
            self.assertFalse((nimble.parent / "nimble.partial").exists())
            verified = tools.verify_tools(output, lambda: None)
            self.assertEqual(verified["nimble"], nimble)

    def test_install_pinned_nimble_rejects_bad_archive_and_records_diag(self):
        cases = (
            ("redirect", "NIMBLE_REDIRECT"),
            ("archive_sha", "NIMBLE_ARCHIVE_DIGEST"),
            ("extra_member", "NIMBLE_EXTRACT"),
            ("symlink_member", "NIMBLE_EXTRACT"),
            ("bin_sha", "NIMBLE_BIN_DIGEST"),
            ("capacity", "NIMBLE_CAPACITY"),
        )
        for case, expected_code in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp).resolve()
                output = self.fixture(root, nimble_sha="0" * 40)
                payload = b"#!/bin/sh\necho ok\n"
                archive_bytes = self._make_nimble_archive(
                    payload,
                    extra_member="extra.txt" if case == "extra_member" else None,
                    symlink_member=(case == "symlink_member"),
                )
                location = ("https://evil.example.com/x?a=1" if case == "redirect"
                            else "https://release-assets.githubusercontent.com/github-production-release-asset/1?sig=ok")
                fake_request, _ = self._fake_requests(archive_bytes,
                                                      redirect_location=location)
                expected_archive_sha = ("0" * 64 if case == "archive_sha"
                                        else hashlib.sha256(archive_bytes).hexdigest())
                expected_bin_sha = ("0" * 64 if case == "bin_sha"
                                    else hashlib.sha256(payload).hexdigest())
                guard = (mock.Mock(side_effect=ValueError("PUBLIC_CAPACITY"))
                         if case == "capacity" else (lambda: None))
                diag = {}
                with mock.patch.object(tools, "NIMBLE_ARCHIVE_BYTES", len(archive_bytes)), \
                     mock.patch.object(tools, "NIMBLE_ARCHIVE_SHA256", expected_archive_sha), \
                     mock.patch.object(tools, "NIMBLE_BIN_BYTES", len(payload)), \
                     mock.patch.object(tools, "NIMBLE_BIN_SHA256", expected_bin_sha), \
                     mock.patch.object(tools.archive, "_request", side_effect=fake_request):
                    with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                        tools.install_pinned_nimble(output, guard, diag=diag)
                self.assertEqual(diag.get("code"), expected_code)
                self.assertFalse((output / "nim-2.2.10/bin/nimble.partial").exists())


if __name__ == "__main__":
    unittest.main()
