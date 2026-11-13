"""Offline bounds and cleanup checks. Never calls a Docker daemon."""
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import socket
import stat
import struct
import tempfile
import threading
import unittest
from unittest import mock

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
        self.assertEqual(hashes, preflight.SOURCE_HASHES)

    def test_controller_source_mismatch_fails_closed(self):
        path = "scripts/test-env/common.py"
        original = preflight.SOURCE_HASHES[path]
        try:
            preflight.SOURCE_HASHES[path] = "0" * 64
            with self.assertRaisesRegex(preflight.PreflightError, "SOURCE_MISMATCH"):
                preflight.source_contract()
        finally:
            preflight.SOURCE_HASHES[path] = original

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


class DaemonPeer(unittest.TestCase):
    def probe(self, pid, proc_root):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            path = Path(directory) / "daemon.sock"
            path.touch()
            with mock.patch.object(preflight.socket, "SO_PEERCRED", 17, create=True), \
                 mock.patch.object(preflight.stat, "S_ISSOCK", return_value=True), \
                 mock.patch.object(preflight.socket, "socket") as factory:
                client = factory.return_value.__enter__.return_value
                client.getsockopt.return_value = struct.pack("3i", pid, 1, 1)
                return preflight.inspect_daemon_peer(path, proc_root)

    def test_connected_peer_with_inaccessible_proc_is_unobservable(self):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            peer = self.probe(12345, Path(directory) / "empty-proc")
            self.assertEqual(peer["status"], "peer_executable_unobservable")
            self.assertGreater(peer["pid"], 0)
            self.assertNotIn("executable_sha256", peer)

    def test_unmapped_peer_pid_does_not_claim_binary_provenance(self):
        peer = self.probe(0, Path("/proc"))
        self.assertEqual(peer["status"], "pid_unmapped")
        self.assertNotIn("executable_sha256", peer)

    def test_visible_peer_hashes_exact_executable(self):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            root = Path(directory)
            executable = root / "daemon-binary"
            executable.write_bytes(b"review-me")
            proc = root / "proc" / "12345"
            proc.mkdir(parents=True)
            (proc / "exe").symlink_to(executable)
            with mock.patch.object(preflight.shutil, "which", return_value=None):
                peer = self.probe(12345, root / "proc")
            self.assertEqual(peer["status"], "peer_executable_observed")
            self.assertEqual(peer["executable_sha256"],
                             hashlib.sha256(b"review-me").hexdigest())
            self.assertNotIn("package_lookup", peer)

    def test_oversized_peer_executable_is_not_hashed(self):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            root = Path(directory)
            executable = root / "daemon-binary"
            executable.write_bytes(b"too-large")
            proc = root / "proc" / "12345"
            proc.mkdir(parents=True)
            (proc / "exe").symlink_to(executable)
            with mock.patch.object(preflight, "MAX_PEER_EXECUTABLE", 4):
                peer = self.probe(12345, root / "proc")
            self.assertEqual(peer["status"], "peer_executable_too_large")
            self.assertNotIn("executable_sha256", peer)

    def test_unreadable_peer_executable_is_not_attributed(self):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            root = Path(directory)
            proc = root / "proc" / "12345"
            proc.mkdir(parents=True)
            (proc / "exe").symlink_to(root / "missing")
            peer = self.probe(12345, root / "proc")
            self.assertEqual(peer["status"], "peer_executable_unobservable")
            self.assertNotIn("executable_sha256", peer)

    def test_connection_failure_records_no_provenance(self):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            path = Path(directory) / "daemon.sock"
            path.touch()
            with mock.patch.object(preflight.stat, "S_ISSOCK", return_value=True), \
                 mock.patch.object(preflight.socket, "socket") as factory:
                factory.return_value.__enter__.return_value.connect.side_effect = OSError("denied")
                peer = preflight.inspect_daemon_peer(path)
            self.assertEqual(peer, {"status": "connection_unavailable",
                                    "parser_provenance": "unverified"})


if __name__ == "__main__":
    unittest.main()
