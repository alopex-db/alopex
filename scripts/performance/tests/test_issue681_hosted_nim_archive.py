"""The hosted Nim fetch accepts only exact bytes from the fixed release route."""

import hashlib
import io
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_nim_archive as nim


class Response:
    def __init__(self, status, body=b"", *, location=None, length=None):
        self.status = status
        self.stream = io.BytesIO(body)
        self.location = location
        self.length = length

    def getheader(self, name):
        return {"Location": self.location,
                "Content-Length": self.length}.get(name)

    def read(self, count):
        return self.stream.read(count)

    def close(self):
        self.stream.close()


class Connection:
    def __init__(self, host, response, calls):
        self.host = host
        self.response = response
        self.calls = calls

    def request(self, method, target, *, headers):
        self.calls.append((self.host, method, target, headers))

    def getresponse(self):
        return self.response

    def close(self):
        pass


class HostedNimArchiveTest(unittest.TestCase):
    def test_official_release_pins_are_literal(self):
        self.assertEqual(nim.DOWNLOAD_PATH,
                         "/nim-lang/nightlies/releases/download/"
                         "2026-04-24-version-2-2-"
                         "bfeb3146d1638b39f69007a4ae5a23e23ae4e5ef/"
                         "nim-2.2.10-linux_x64.tar.xz")
        self.assertEqual(nim.EXPECTED_BYTES, 17_574_524)
        self.assertEqual(nim.EXPECTED_SHA256,
                         "0a3a38752e97e9d44aa479b3a7b37336dfe0176daf22ee5b5218ad0991ecd211")

    def setup_transport(self, body, *, host=nim.ASSET_HOST,
                        length=None, expected=None):
        redirect = ("https://" + host +
                    "/github-production-release-asset/fixed?sig=private")
        responses = [Response(302, location=redirect),
                     Response(200, body, length=length)]
        calls = []

        def connection_factory(selected_host, *, timeout):
            self.assertEqual(timeout, 30)
            return Connection(selected_host, responses.pop(0), calls)

        patches = [mock.patch.object(nim.http.client, "HTTPSConnection",
                                     side_effect=connection_factory),
                   mock.patch.object(nim, "EXPECTED_BYTES",
                                     len(body) if expected is None else expected),
                   mock.patch.object(nim, "EXPECTED_SHA256",
                                     hashlib.sha256(body).hexdigest())]
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        return calls

    def test_exact_release_redirect_and_bytes_are_promoted(self):
        with tempfile.TemporaryDirectory() as tmp:
            body = b"nim release fixture"
            calls = self.setup_transport(body, length=str(len(body)))
            guards = []
            output = nim.fetch_archive(Path(tmp).resolve(),
                                       lambda: guards.append(True))
            self.assertEqual(output.read_bytes(), body)
            self.assertGreaterEqual(len(guards), 3)
            self.assertEqual([row[0] for row in calls],
                             [nim.FIRST_HOST, nim.ASSET_HOST])
            self.assertEqual(calls[0][2], nim.DOWNLOAD_PATH)
            self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o600)
            self.assertFalse(output.with_name(output.name + ".partial").exists())

    def test_unexpected_redirect_host_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.setup_transport(b"bytes", host="unapproved.example")
            root = Path(tmp).resolve()
            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                nim.fetch_archive(root, lambda: None)
            self.assertFalse((root / nim.NAME).exists())
            self.assertFalse((root / (nim.NAME + ".partial")).exists())
            self.assertEqual((root / "download.private.log").read_text(),
                             "redirect:ValueError\n")
            self.assertEqual(stat.S_IMODE((root / "download.private.log").stat().st_mode),
                             0o600)

    def test_size_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.setup_transport(b"bytes", expected=6)
            root = Path(tmp).resolve()
            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                nim.fetch_archive(root, lambda: None)
            self.assertFalse((root / nim.NAME).exists())
            self.assertFalse((root / (nim.NAME + ".partial")).exists())

    def test_hash_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.setup_transport(b"bytes")
            with mock.patch.object(nim, "EXPECTED_SHA256", "0" * 64):
                root = Path(tmp).resolve()
                with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                    nim.fetch_archive(root, lambda: None)
                self.assertFalse((root / nim.NAME).exists())
                self.assertFalse((root / (nim.NAME + ".partial")).exists())

    def test_guard_failure_leaves_no_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.setup_transport(b"bytes")
            root = Path(tmp).resolve()
            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                nim.fetch_archive(root, lambda: (_ for _ in ()).throw(
                    RuntimeError("private diagnostic")))
            self.assertFalse((root / nim.NAME).exists())
            self.assertFalse((root / (nim.NAME + ".partial")).exists())

    def test_existing_partial_is_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            partial = root / (nim.NAME + ".partial")
            partial.write_bytes(b"owned by earlier invocation")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                nim.fetch_archive(root, lambda: None)
            self.assertEqual(partial.read_bytes(), b"owned by earlier invocation")
            self.assertFalse((root / nim.NAME).exists())

    def test_dangling_output_symlink_is_preserved_before_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            output = root / nim.NAME
            output.symlink_to(root / "missing")
            with mock.patch.object(nim.http.client, "HTTPSConnection") as network:
                with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                    nim.fetch_archive(root, lambda: None)
            self.assertTrue(output.is_symlink())
            network.assert_not_called()

    def test_guard_failure_during_transfer_removes_owned_partial(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.setup_transport(b"bytes")
            root = Path(tmp).resolve()
            calls = 0

            def guard():
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise RuntimeError("private diagnostic")

            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                nim.fetch_archive(root, guard)
            self.assertFalse((root / nim.NAME).exists())
            self.assertFalse((root / (nim.NAME + ".partial")).exists())
            self.assertEqual((root / "download.private.log").read_text(),
                             "transfer:RuntimeError\n")


if __name__ == "__main__":
    unittest.main()
