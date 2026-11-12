"""Offline bounds and cleanup checks. Never calls a Docker daemon."""
import importlib.util
import json
import os
from pathlib import Path
import socket
import stat
import tempfile
import threading
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "s04_wire_preflight.py"
SPEC = importlib.util.spec_from_file_location("s04_wire_preflight", SCRIPT)
preflight = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(preflight)


class Parser(unittest.TestCase):
    def parse(self, wire):
        reader, writer = socket.socketpair()
        try:
            writer.sendall(wire)
            writer.shutdown(socket.SHUT_WR)
            return preflight.read_request(reader)
        finally:
            reader.close()
            writer.close()

    def test_capture_preserves_exact_body(self):
        line, headers, body = self.parse(
            b"POST /v1.51/containers/create?name=x HTTP/1.1\r\n"
            b"Host: api.moby.localhost\r\nContent-Length: 2\r\n\r\n{}")
        self.assertIn(b"/containers/create", line)
        self.assertEqual(headers, [("host", "api.moby.localhost"), ("content-length", "2")])
        self.assertEqual(body, b"{}")

    def test_negative_wire_cases(self):
        cases = {
            "duplicate": b"POST /x HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx",
            "chunked": b"POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
            "oversize": b"POST /x HTTP/1.1\r\nContent-Length: 65537\r\n\r\n",
            "truncated": b"POST /x HTTP/1.1\r\nContent-Length: 2\r\n\r\nx",
            "extra": b"POST /x HTTP/1.1\r\nContent-Length: 1\r\n\r\nxy",
            "header": b"GET /_ping HTTP/1.1\r\nBroken\r\n\r\n",
            "folded": b"GET /_ping HTTP/1.1\r\n folded: x\r\n\r\n",
            "line": b"PUT /x HTTP/1.1\r\n\r\n",
        }
        for name, wire in cases.items():
            with self.subTest(name=name), self.assertRaises(preflight.PreflightError):
                self.parse(wire)


class PrivacyAndCleanup(unittest.TestCase):
    def test_controller_sources_match_pinned_head(self):
        hashes = preflight.source_contract()
        self.assertEqual(set(hashes), {"scripts/test-env/common.py",
                                       "scripts/test-env/docker_backend.py",
                                       "scripts/ops/hosted_test_env_qualification.py"})

    def test_private_output_requires_owner_only_directory_and_no_overwrite(self):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            parent = Path(directory)
            output = parent / "evidence.json"
            os.chmod(parent, 0o700)
            preflight.private_write(output, {"private": True})
            self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o600)
            self.assertEqual(json.loads(output.read_text()), {"private": True})
            with self.assertRaises(FileExistsError):
                preflight.private_write(output, {})
            os.chmod(parent, 0o755)
            with self.assertRaises(preflight.PreflightError):
                preflight.private_write(parent / "other.json", {})

    def test_fake_socket_is_removed_with_task_directory(self):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            parent = Path(directory)
            sock = parent / "fake.sock"
            records, errors = [], []
            ready, stop = threading.Event(), threading.Event()
            thread = threading.Thread(target=preflight.serve,
                                      args=(sock, records, errors, ready, stop))
            thread.start()
            self.assertTrue(ready.wait(2))
            self.assertEqual(stat.S_IMODE(sock.stat().st_mode), 0o600)
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.connect(str(sock))
                client.sendall(b"HEAD /_ping HTTP/1.1\r\nHost: offline\r\n\r\n")
                self.assertIn(b"200", client.recv(512))
            stop.set()
            thread.join(3)
            self.assertFalse(thread.is_alive())
            self.assertEqual(len(records), 1)
        self.assertFalse(sock.exists())


if __name__ == "__main__":
    unittest.main()
