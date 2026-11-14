"""Offline bounds and cleanup checks. Never calls a Docker daemon."""
import argparse
import errno
from contextlib import ExitStack
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import stat
import subprocess
import struct
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[1] / "s04_wire_preflight.py"
SPEC = importlib.util.spec_from_file_location("s04_wire_preflight", SCRIPT)
preflight = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(preflight)


def reap_child_bounded(pid, timeout=2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            reaped, _ = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return
        if reaped == pid:
            return
        time.sleep(0.01)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            reaped, _ = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return
        if reaped == pid:
            return
        time.sleep(0.01)
    raise AssertionError("child_reap_timeout")


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
    def test_capture_requires_actual_checkout_head(self):
        args = argparse.Namespace(source_head="a" * 40)
        with mock.patch.object(preflight.subprocess, "run", return_value=subprocess.CompletedProcess(
                [], 0, stdout=("b" * 40 + "\n").encode(), stderr=b"")), \
             self.assertRaisesRegex(preflight.PreflightError, "SOURCE_MISMATCH"):
            preflight.capture(args)
        with mock.patch.object(preflight.subprocess, "run", return_value=subprocess.CompletedProcess(
                [], 1, stdout=b"", stderr=b"missing checkout")), \
             self.assertRaisesRegex(preflight.PreflightError, "SOURCE_MISMATCH"):
            preflight.capture(args)
        with mock.patch.object(preflight.subprocess, "run", return_value=subprocess.CompletedProcess(
                [], 0, stdout=("a" * 40 + "\n").encode(), stderr=b"")), \
             self.assertRaisesRegex(preflight.PreflightError, "SOURCE_MISMATCH"):
            preflight.capture(argparse.Namespace(source_head="A" * 40))
        with mock.patch.object(preflight.subprocess, "run", return_value=subprocess.CompletedProcess(
                [], 0, stdout=("a" * 40 + "\n").encode(), stderr=b"")), \
             mock.patch.object(preflight, "source_contract", side_effect=RuntimeError("matched")), \
             self.assertRaisesRegex(RuntimeError, "matched"):
            preflight.capture(args)

    def test_controller_sources_match_pinned_hashes(self):
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
    def test_bounded_child_reap_kills_after_deadline(self):
        with mock.patch.object(time, "monotonic", side_effect=[0, 3, 3, 3.1]), \
             mock.patch.object(os, "kill") as kill, \
             mock.patch.object(os, "waitpid", return_value=(12345, 0)) as wait:
            reap_child_bounded(12345)
        kill.assert_called_once_with(12345, signal.SIGKILL)
        wait.assert_called_once_with(12345, os.WNOHANG)

    @staticmethod
    def write_stat(proc, pid=12345, starttime=42):
        (proc / "stat").write_text(f"{pid} (daemon) S " + "0 " * 18 + f"{starttime}\n")

    def probe(self, pid, proc_root, alive=(True, True), pidfd_available=True):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            path = Path(directory) / "daemon.sock"
            path.touch()
            pidfd = os.open(os.devnull, os.O_RDONLY)

            def socket_option(_level, option, _size):
                if option == preflight.socket.SO_PEERCRED:
                    return struct.pack("3i", pid, 1, 1)
                if option == preflight.SO_PEERPIDFD and pidfd_available:
                    return struct.pack("i", pidfd)
                raise OSError(errno.ENOPROTOOPT, "unsupported")

            with mock.patch.object(preflight.socket, "SO_PEERCRED", 17, create=True), \
                 mock.patch.object(preflight.stat, "S_ISSOCK", return_value=True), \
                 mock.patch.object(preflight, "pidfd_alive", side_effect=alive), \
                 mock.patch.object(preflight.socket, "socket") as factory:
                factory.return_value.getsockopt.side_effect = socket_option
                try:
                    return preflight.inspect_daemon_peer(path, proc_root)
                finally:
                    if not pidfd_available:
                        os.close(pidfd)

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
            self.write_stat(proc)
            (proc / "exe").symlink_to(executable)
            with mock.patch.object(preflight.shutil, "which", return_value=None):
                peer = self.probe(12345, root / "proc")
            self.assertEqual(peer["status"], "peer_executable_observed")
            self.assertEqual(peer["executable_sha256"],
                             hashlib.sha256(b"review-me").hexdigest())
            self.assertEqual(peer["pid_lifecycle"], "stable")
            self.assertEqual(peer["pid_before"]["starttime"], 42)
            self.assertEqual(peer["pid_after"]["starttime"], 42)
            self.assertNotIn("package_lookup", peer)

    def test_oversized_peer_executable_is_not_hashed(self):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            root = Path(directory)
            executable = root / "daemon-binary"
            executable.write_bytes(b"too-large")
            proc = root / "proc" / "12345"
            proc.mkdir(parents=True)
            self.write_stat(proc)
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
            self.assertEqual(peer["executable_errno_class"], "not_found")
            self.assertNotIn("executable_sha256", peer)

    def test_permission_denied_executable_is_distinct_from_exited_peer(self):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            proc = Path(directory) / "proc" / "12345"
            proc.mkdir(parents=True)
            self.write_stat(proc)
            real_open = preflight.os.open

            def deny_executable(path, flags, *args, **kwargs):
                if str(path).endswith("/exe"):
                    raise PermissionError(errno.EACCES, "denied")
                return real_open(path, flags, *args, **kwargs)

            with mock.patch.object(preflight.os, "open", side_effect=deny_executable):
                peer = self.probe(12345, Path(directory) / "proc")
            self.assertEqual(peer["executable_errno_class"], "permission_denied")
            self.assertEqual(peer["executable_error_stage"], "open")
            self.assertEqual(peer["pid_lifecycle"], "stable")
            self.assertNotIn("executable_sha256", peer)

    def test_stat_and_readlink_denials_have_fixed_stage_and_class(self):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            root = Path(directory)
            executable = root / "daemon-binary"
            executable.write_bytes(b"review-me")
            proc = root / "proc" / "12345"
            proc.mkdir(parents=True)
            self.write_stat(proc)
            (proc / "exe").symlink_to(executable)
            real_stat = preflight.Path.stat
            real_readlink = preflight.os.readlink

            def deny_stat(path, *args, **kwargs):
                if str(path).endswith("/exe"):
                    raise PermissionError(errno.EACCES, "denied")
                return real_stat(path, *args, **kwargs)

            def deny_readlink(path, *args, **kwargs):
                if str(path).endswith("/exe"):
                    raise PermissionError(errno.EACCES, "denied")
                return real_readlink(path, *args, **kwargs)

            for stage in ("stat", "readlink"):
                patcher = (mock.patch.object(preflight.Path, "stat", autospec=True,
                                             side_effect=deny_stat) if stage == "stat" else
                           mock.patch.object(preflight.os, "readlink", side_effect=deny_readlink))
                with self.subTest(stage=stage), patcher:
                    peer = self.probe(12345, root / "proc")
                self.assertEqual(peer["executable_error_stage"], stage)
                self.assertEqual(peer["executable_errno_class"], "permission_denied")
                self.assertEqual(peer["pid_lifecycle"], "stable")
                self.assertNotIn("executable_sha256", peer)

    def test_vanished_or_reused_pid_never_attributes_executable(self):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            root = Path(directory)
            executable = root / "daemon-binary"
            executable.write_bytes(b"review-me")
            proc = root / "proc" / "12345"
            proc.mkdir(parents=True)
            (proc / "exe").symlink_to(executable)
            for after, lifecycle in (({"state": "not_found"}, "exited"),
                                     ({"state": "present", "starttime": 43}, "reused")):
                with self.subTest(lifecycle=lifecycle), \
                     mock.patch.object(preflight, "pid_starttime", side_effect=[
                         {"state": "present", "starttime": 42}, after]), \
                     mock.patch.object(preflight.shutil, "which", return_value=None):
                    peer = self.probe(12345, root / "proc")
                self.assertEqual(peer["pid_lifecycle"], lifecycle)
                self.assertEqual(peer["status"], "peer_executable_unobservable")
                self.assertNotIn("executable_sha256", peer)

    def test_peer_exit_before_first_stat_cannot_attribute_reused_pid(self):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            root = Path(directory)
            executable = root / "replacement-binary"
            executable.write_bytes(b"replacement")
            proc = root / "proc" / "12345"
            proc.mkdir(parents=True)
            self.write_stat(proc, starttime=99)
            (proc / "exe").symlink_to(executable)
            with mock.patch.object(preflight.shutil, "which", return_value=None):
                peer = self.probe(12345, root / "proc", alive=(False, False))
            self.assertEqual(peer["pid_before"]["starttime"], 99)
            self.assertEqual(peer["pid_after"]["starttime"], 99)
            self.assertEqual(peer["pid_lifecycle"], "exited")
            self.assertNotIn("executable_sha256", peer)

    def test_missing_kernel_peer_binding_fails_closed(self):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            root = Path(directory)
            executable = root / "daemon-binary"
            executable.write_bytes(b"review-me")
            proc = root / "proc" / "12345"
            proc.mkdir(parents=True)
            self.write_stat(proc)
            (proc / "exe").symlink_to(executable)
            peer = self.probe(12345, root / "proc", pidfd_available=False)
            self.assertEqual(peer["peer_pidfd"], "unavailable")
            self.assertEqual(peer["pid_lifecycle"], "unbound")
            self.assertNotIn("executable_sha256", peer)

    def test_exec_swap_after_readlink_does_not_attribute_old_fd(self):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            root = Path(directory)
            old = root / "old-binary"
            new = root / "new-binary"
            old.write_bytes(b"old")
            new.write_bytes(b"new")
            proc = root / "proc" / "12345"
            proc.mkdir(parents=True)
            self.write_stat(proc)
            exe = proc / "exe"
            exe.symlink_to(old)
            real_readlink = preflight.os.readlink

            def swap_executable(path, *args, **kwargs):
                if str(path) == str(exe):
                    exe.unlink()
                    exe.symlink_to(new)
                    return str(new)
                return real_readlink(path, *args, **kwargs)

            with mock.patch.object(preflight.os, "readlink", side_effect=swap_executable):
                peer = self.probe(12345, root / "proc")
            self.assertEqual(peer["status"], "peer_executable_changed")
            self.assertEqual(peer["pid_lifecycle"], "stable")
            self.assertNotIn("executable_sha256", peer)

    def test_connection_failure_records_no_provenance(self):
        with tempfile.TemporaryDirectory(prefix="s04-", dir="/private/tmp") as directory:
            path = Path(directory) / "daemon.sock"
            path.touch()
            with mock.patch.object(preflight.stat, "S_ISSOCK", return_value=True), \
                 mock.patch.object(preflight.socket, "socket") as factory:
                factory.return_value.connect.side_effect = OSError("denied")
                peer = preflight.inspect_daemon_peer(path)
            self.assertEqual(peer, {"status": "connection_unavailable",
                                    "parser_provenance": "unverified"})

    @unittest.skipUnless(sys.platform == "linux", "Linux SO_PEERPIDFD only")
    def test_real_unix_peer_pidfd_tracks_exit_without_docker(self):
        with tempfile.TemporaryDirectory(prefix="s04-peer-") as directory:
            path = str(Path(directory) / "peer.sock")
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            listener.settimeout(2)
            listener.bind(path)
            listener.listen(1)
            read_end, write_end = os.pipe()
            child = os.fork()
            if child == 0:
                try:
                    os.close(write_end)
                    listener.close()
                    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                        client.connect(path)
                        os.read(read_end, 1)
                finally:
                    os._exit(0)
            os.close(read_end)
            peer_fd = None
            accepted = None
            try:
                accepted, _ = listener.accept()
                raw = accepted.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
                self.assertEqual(struct.unpack("3i", raw)[0], child)
                try:
                    raw_fd = accepted.getsockopt(socket.SOL_SOCKET, preflight.SO_PEERPIDFD, 4)
                except OSError as error:
                    self.skipTest("SO_PEERPIDFD unavailable: " + preflight.errno_class(error))
                peer_fd = struct.unpack("i", raw_fd)[0]
                self.assertTrue(preflight.pidfd_alive(peer_fd))
                os.write(write_end, b"x")
                reaped_pid = child
                reap_child_bounded(child)
                child = None
                self.assertFalse(preflight.pidfd_alive(peer_fd))
                with self.assertRaises(ChildProcessError):
                    os.waitpid(reaped_pid, os.WNOHANG)
            finally:
                with ExitStack() as cleanup:
                    cleanup.callback(listener.close)
                    cleanup.callback(os.close, write_end)
                    if accepted is not None:
                        cleanup.callback(accepted.close)
                    if peer_fd is not None:
                        cleanup.callback(os.close, peer_fd)
                    if child is not None:
                        try:
                            os.write(write_end, b"x")
                        except OSError:
                            pass
                        reap_child_bounded(child)


if __name__ == "__main__":
    unittest.main()
