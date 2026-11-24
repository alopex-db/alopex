"""Offline storage admission checks with injected host measurements."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.ops.final_sql_runtime import admission


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.backing = self.root / "docker-backing"
        self.backing.mkdir()
        self.profile = self.root / "profile.json"
        self.storage = {
            "roots": [str(self.root)],
            "runtime_storage_roots": [str(self.backing)],
            "stop_bytes": 40_000_000_000,
            "reserve_bytes": 1_000_000_000,
        }
        self.write_profile()

    def write_profile(self, **changes):
        profile = dict(storage=self.storage, docker_context="qualified-local",
                       platform="linux/amd64", execution_mode="native",
                       image_lock=str(self.root / "image.lock"),
                       private_output=str(self.root / "runs"),
                       cache=str(self.root / "cache"),
                       cpu={"server": [1], "client": [2]},
                       security={"lsm_kind": "apparmor", "lsm_profile": "docker-default",
                                 "seccomp_profile": str(self.root / "seccomp.json"),
                                 "seccomp_sha256": "a" * 64},
                       limits={"memory_bytes": 1_000_000_000, "swap_bytes": 0,
                               "pids": 256, "stop_seconds": 10,
                               "case_seconds": 300, "run_seconds": 420})
        profile.update(changes)
        self.profile.write_text(json.dumps(profile))
        self.sha = hashlib.sha256(self.profile.read_bytes()).hexdigest()

    def adapter(self, values, **changes):
        measurements = iter(values)

        class Budget:
            def __init__(self, storage):
                self.storage = storage

            def used(self):
                return next(measurements)

        kwargs = dict(expected_profile_sha256=self.sha,
                      expected_backing_root=self.backing,
                      cell_profile_for=lambda kind, row: self.profile,
                      loader=lambda path: json.loads(path.read_text()),
                      budget_factory=Budget)
        kwargs.update(changes)
        return admission.StorageAdmission(self.profile, **kwargs)

    def test_admits_then_remeasures_before_each_cell(self):
        guard = self.adapter([34_000_000_000, 36_000_000_000, 38_000_000_000])
        self.assertIs(guard.admit_storage(5200), True)
        self.assertEqual(guard.capacity("preflight", {"id": "sql-a"}, 5100),
                         36_000_000_000)
        self.assertEqual(guard.capacity("cell", {"id": "sql-b"}, 5000),
                         38_000_000_000)

    def test_admission_reserves_four_gb_plus_one_gb(self):
        guard = self.adapter([35_000_000_000])
        with self.assertRaisesRegex(ValueError, "STORAGE_CAPACITY"):
            guard.admit_storage(5200)

    def test_cell_rejects_growth_and_stop_threshold(self):
        for used in (38_000_000_001, 39_000_000_000, 50_000_000_000):
            with self.subTest(used=used):
                guard = self.adapter([34_000_000_000, used])
                guard.admit_storage(5200)
                with self.assertRaisesRegex(ValueError, "STORAGE_CAPACITY"):
                    guard.capacity("cell", {"id": "sql-a"}, 100)

    def test_measurement_error_and_invalid_value_fail_closed(self):
        for value in (PermissionError("unreadable"), None, -1, True):
            with self.subTest(value=value):
                def budget_factory(_storage):
                    class Budget:
                        def used(self):
                            if isinstance(value, Exception):
                                raise value
                            return value
                    return Budget()
                guard = self.adapter([], budget_factory=budget_factory)
                with self.assertRaisesRegex(ValueError, "STORAGE_MEASUREMENT"):
                    guard.admit_storage(5200)

    def test_profile_and_backing_mismatch_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "STORAGE_PROFILE"):
            self.adapter([], expected_backing_root=self.root)
        with self.assertRaisesRegex(ValueError, "STORAGE_PROFILE"):
            self.adapter([], expected_profile_sha256="0" * 64)
        guard = self.adapter([34_000_000_000])
        guard.admit_storage(5200)
        self.write_profile(docker_context="other-host")
        with self.assertRaisesRegex(ValueError, "STORAGE_PROFILE"):
            guard.capacity("cell", {"id": "sql-a"}, 100)

    def test_security_and_memory_changes_fail_closed(self):
        for changed in ({"security": {"lsm_kind": "apparmor", "lsm_profile": "unconfined",
                                      "seccomp_profile": str(self.root / "seccomp.json"),
                                      "seccomp_sha256": "a" * 64}},
                        {"limits": {"memory_bytes": 2_000_000_000,
                                    "swap_bytes": 0, "pids": 256,
                                    "stop_seconds": 10, "case_seconds": 300,
                                    "run_seconds": 420}}):
            cell = self.root / "cell-profile.json"
            guard = self.adapter([34_000_000_000],
                                 cell_profile_for=lambda kind, row: cell)
            guard.admit_storage(5200)
            value = json.loads(self.profile.read_text())
            value.update(changed)
            cell.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError, "STORAGE_PROFILE"):
                guard.capacity("cell", {"id": "sql-a"}, 100)

    def test_cell_storage_change_and_missing_measurement_fail_closed(self):
        guard = self.adapter([34_000_000_000])
        guard.admit_storage(5200)
        self.storage = dict(self.storage, reserve_bytes=2_000_000_000)
        self.write_profile()
        with self.assertRaisesRegex(ValueError, "STORAGE_PROFILE"):
            guard.capacity("cell", {"id": "sql-a"}, 100)
        self.storage = dict(self.storage, reserve_bytes=1_000_000_000)
        self.write_profile()
        with self.assertRaisesRegex(ValueError, "STORAGE_MEASUREMENT"):
            guard.capacity("cell", {"id": "sql-a"}, 100)


if __name__ == "__main__":
    unittest.main()
