import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import Failure
from permissions import generated_work, private_tree
from verify_capacity import active_faults, FIXTURE_BYTES


class PrivateTrees(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="alopex-test-env-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_extracted_modes_are_configured_before_execution(self):
        (self.root / "bin").mkdir(mode=0o755)
        script = self.root / "bin/run"
        script.write_text("#!/bin/sh\nexit 0\n")
        script.chmod(0o755)
        data = self.root / "data"
        data.write_bytes(b"x")
        data.chmod(0o644)
        with self.assertRaises(Failure):
            private_tree(self.root)
        private_tree(self.root, configure=True)
        rows = {row["path"]: row for row in private_tree(self.root)}
        self.assertEqual(rows["bin"]["mode"], "0o700")
        self.assertEqual(rows["bin/run"]["mode"], "0o700")
        self.assertEqual(rows["data"]["mode"], "0o600")

    def test_output_audit_rejects_broad_permissions_without_rewriting_them(self):
        path = self.root / "output"
        path.write_bytes(b"x")
        path.chmod(0o644)
        with self.assertRaises(Failure):
            private_tree(self.root)
        self.assertEqual(path.stat().st_mode & 0o777, 0o644)

    def test_venv_links_require_internal_target_or_declared_interpreter(self):
        (self.root / "bin").mkdir(mode=0o700)
        link = self.root / "bin/python"
        link.symlink_to(sys.executable)
        with self.assertRaises(Failure):
            private_tree(self.root)
        private_tree(self.root, external_targets=[sys.executable])
        link.unlink()
        (self.root / "outside").symlink_to(sys.executable)
        with self.assertRaises(Failure):
            private_tree(self.root, external_targets=[sys.executable])

    def test_socket_has_private_mode_and_is_never_normalized(self):
        with socket.socket(socket.AF_UNIX) as sock:
            path = self.root / "socket"
            sock.bind(str(path))
            path.chmod(0o700)
            self.assertEqual(private_tree(self.root)[-1]["kind"], "socket")
            with self.assertRaises(Failure):
                private_tree(self.root, configure=True)

    def test_complete_audit_includes_temporary_files_and_rejects_undeclared_outputs(self):
        for name in ('home', 'tmp', 'cache', 'results'):
            (self.root / name).mkdir(mode=0o700)
        generated_work(self.root, sys.executable)
        temporary = self.root / 'tmp/leftover'
        temporary.write_bytes(b'x')
        temporary.chmod(0o644)
        with self.assertRaises(Failure):
            generated_work(self.root, sys.executable)
        temporary.chmod(0o600)
        generated_work(self.root, sys.executable)
        (self.root / 'undeclared').mkdir(mode=0o700)
        with self.assertRaises(Failure):
            generated_work(self.root, sys.executable)


@unittest.skipUnless(os.name == "posix", "requires Unix process groups and allocated-byte metadata")
class ActiveWatchdog(unittest.TestCase):
    def test_known_tiny_fixture_and_missing_root_stop_and_reap_active_children(self):
        with tempfile.TemporaryDirectory(prefix="alopex-test-env-") as directory:
            root = Path(directory)
            result = active_faults(root)
            self.assertEqual(FIXTURE_BYTES, 16384)
            self.assertEqual(result["cases"]["capacity"]["error_code"], "CAPACITY")
            self.assertEqual(result["cases"]["measurement"]["error_code"], "MEASUREMENT")
            self.assertTrue(all(row["reaped"] and row["original_data_retained"]
                                for row in result["cases"].values()))
            self.assertEqual(list(root.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
