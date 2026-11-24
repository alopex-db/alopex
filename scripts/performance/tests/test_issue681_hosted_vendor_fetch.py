"""The selected job accepts only the fixed public vendor input."""

import hashlib
import io
from pathlib import Path
import stat
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_vendor_fetch as vendor


class Response:
    def __init__(self, status, body=b"", *, location=None, length=None):
        self.status = status
        self.stream = io.BytesIO(body)
        self.location = location
        self.length = length
        self.closed = False

    def getheader(self, name):
        return {"Location": self.location, "Content-Length": self.length}.get(name)

    def read(self, count):
        return self.stream.read(count)

    def close(self):
        self.closed = True
        self.stream.close()


class Connection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class HostedVendorFetchTest(unittest.TestCase):
    def setup_transport(self, body=b"vendor fixture", *, host=vendor.ASSET_HOST,
                        first_status=302, second_status=200, length=None,
                        expected=None, digest=None, location=None):
        location = ("https://" + host +
                    "/github-production-release-asset/fixed?sig=private") if location is None else location
        responses = [Response(first_status, location=location),
                     Response(second_status, body, length=length)]
        calls = []

        def request(hostname, target):
            calls.append((hostname, target))
            return Connection(), responses[len(calls) - 1]

        patches = [mock.patch.object(vendor, "_request", side_effect=request),
                   mock.patch.object(vendor, "EXPECTED_BYTES",
                                     len(body) if expected is None else expected),
                   mock.patch.object(vendor, "EXPECTED_SHA256",
                                     hashlib.sha256(body).hexdigest()
                                     if digest is None else digest)]
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        return calls, responses

    def test_exact_route_bytes_and_private_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            body = b"vendor fixture"
            calls, responses = self.setup_transport(body, length=str(len(body)))
            guards = []
            output = vendor.fetch_vendor(Path(tmp).resolve(),
                                         lambda: guards.append(True))
            self.assertEqual(output.read_bytes(), body)
            self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o600)
            self.assertEqual(calls, [
                ("github.com", vendor.DOWNLOAD_PATH),
                (vendor.ASSET_HOST,
                 "/github-production-release-asset/fixed?sig=private")])
            self.assertTrue(all(response.closed for response in responses))
            self.assertGreaterEqual(len(guards), 3)
            self.assertFalse((Path(tmp) / (vendor.NAME + ".partial")).exists())

    def test_literal_public_asset_pin(self):
        self.assertEqual(vendor.EXPECTED_BYTES, 301_598_193)
        self.assertEqual(vendor.EXPECTED_SHA256,
                         "dd6b618ad9d45a5703f32bafb30fb936e119c827fed2ac6d649cda22b5489a3c")
        self.assertEqual(vendor.DOWNLOAD_PATH,
                         "/alopex-db/alopex/releases/download/"
                         "issue681-v0817-cargo-input-dd6b618a/public-cargo-vendor.zip")

    def test_unapproved_redirect_or_response_rejected(self):
        for options in ({"host": "other.example"}, {"first_status": 200},
                        {"second_status": 404}, {"second_status": 302},
                        {"location": "https://release-assets.githubusercontent.com/wrong?sig=x"},
                        {"location": "https://release-assets.githubusercontent.com/"
                                     "github-production-release-asset/fixed?sig=x\nInjected: yes"}):
            with self.subTest(options=options), tempfile.TemporaryDirectory() as tmp:
                self.setup_transport(**options)
                with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                    vendor.fetch_vendor(Path(tmp).resolve(), lambda: None)
                self.assertFalse((Path(tmp) / vendor.NAME).exists())
                self.assertFalse((Path(tmp) / (vendor.NAME + ".partial")).exists())

    def test_wrong_count_length_or_digest_rejected(self):
        for options in ({"expected": 15}, {"length": "15"},
                        {"digest": "0" * 64}):
            with self.subTest(options=options), tempfile.TemporaryDirectory() as tmp:
                self.setup_transport(**options)
                with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                    vendor.fetch_vendor(Path(tmp).resolve(), lambda: None)
                self.assertFalse((Path(tmp) / vendor.NAME).exists())
                self.assertFalse((Path(tmp) / (vendor.NAME + ".partial")).exists())

    def test_guard_interrupt_removes_only_its_partial(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.setup_transport()
            count = 0

            def guard():
                nonlocal count
                count += 1
                if count == 2:
                    raise RuntimeError("private details")

            root = Path(tmp).resolve()
            with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                vendor.fetch_vendor(root, guard)
            self.assertFalse((root / vendor.NAME).exists())
            self.assertFalse((root / (vendor.NAME + ".partial")).exists())
            logs = list(root.glob("vendor-fetch.*.private.log"))
            self.assertEqual(len(logs), 1)
            self.assertEqual(logs[0].read_text(),
                             "transfer:RuntimeError\n")

    def test_slow_read_expires_whole_step_deadline(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, responses = self.setup_transport()
            def slow_read(_count):
                time.sleep(0.2)
                return b"vendor fixture"
            responses[1].read = slow_read
            root = Path(tmp).resolve()
            with mock.patch.object(vendor, "TIME_LIMIT", 0.02):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                    vendor.fetch_vendor(root, lambda: None)
            self.assertFalse((root / vendor.NAME).exists())
            self.assertFalse((root / (vendor.NAME + ".partial")).exists())
            self.assertEqual(next(root.glob("vendor-fetch.*.private.log")).read_text(),
                             "transfer:TimeoutError\n")

    def test_read_failure_cleanup_and_repeated_failure_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            for _ in range(2):
                _, responses = self.setup_transport()
                responses[1].read = mock.Mock(side_effect=OSError("private path"))
                with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                    vendor.fetch_vendor(root, lambda: None)
            logs = list(root.glob("vendor-fetch.*.private.log"))
            self.assertEqual(len(logs), 2)
            self.assertEqual({path.read_text() for path in logs},
                             {"transfer:OSError\n"})
            self.assertFalse((root / (vendor.NAME + ".partial")).exists())

    def test_cleanup_failure_stays_fixed_public_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.setup_transport(digest="0" * 64)
            root = Path(tmp).resolve()
            with mock.patch.object(vendor, "_remove_same_inode",
                                   side_effect=OSError("private cleanup detail")):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                    vendor.fetch_vendor(root, lambda: None)
            self.assertEqual(next(root.glob("vendor-fetch.*.private.log")).read_text(),
                             "cleanup:OSError\n")

    def test_existing_output_and_partial_preserved_without_network(self):
        for name in (vendor.NAME, vendor.NAME + ".partial"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp).resolve()
                existing = root / name
                existing.write_bytes(b"earlier invocation")
                with mock.patch.object(vendor, "_request") as request:
                    with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                        vendor.fetch_vendor(root, lambda: None)
                request.assert_not_called()
                self.assertEqual(existing.read_bytes(), b"earlier invocation")

    def test_competing_output_at_promotion_is_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.setup_transport()
            root = Path(tmp).resolve()

            def compete(source, output):
                output.write_bytes(b"competing data")
                raise FileExistsError("competition")

            with mock.patch.object(vendor, "_promote_noreplace",
                                   side_effect=compete):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_VENDOR$"):
                    vendor.fetch_vendor(root, lambda: None)
            self.assertEqual((root / vendor.NAME).read_bytes(), b"competing data")
            self.assertFalse((root / (vendor.NAME + ".partial")).exists())


if __name__ == "__main__":
    unittest.main()
