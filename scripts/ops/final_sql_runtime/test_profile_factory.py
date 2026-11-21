"""Offline checks for the sealed 52-run profile inventory."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.ops.final_sql_runtime import matrix, profile_factory


class ProfileFactoryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.profile_dir = self.root / "profiles"
        self.staging = self.root / "staging"
        self.backing = self.root / "backing"
        for path in (self.profile_dir, self.staging, self.backing):
            path.mkdir(mode=0o700)
        self.qualified = self.root / "qualified.json"
        self.files = [{"role": role, "path": role, "size": 1, "sha256": "d" * 64}
                      for role in ("server", "wheel", "library", "receipt")]
        self.pins = {subject: {"source": {"commit": "a" * 40, "tree": "b" * 40,
                                         "profile": "release"}, "platform": "linux/amd64",
                               "assets_digest": "e" * 64, "files": self.files}
                     for subject in matrix.SUBJECTS}
        base = {
            "schema": "alopex.test-env-profile/v1", "run_id": "hosted-804-123",
            "docker_context": "qualified-context", "platform": "linux/amd64",
            "execution_mode": "native", "image_lock": str(self.root / "image.lock"),
            "assets": str(self.root / "qualified-assets.json"),
            "private_output": str(self.root / "runs"), "cache": str(self.root / "cache"),
            "cpu": {"server": [0], "client": [1]},
            "limits": {"memory_bytes": 1_610_612_736, "swap_bytes": 0, "pids": 256,
                       "case_seconds": 60, "run_seconds": 900, "stop_seconds": 10},
            "storage": {"roots": [str(self.root)],
                        "runtime_storage_roots": [str(self.backing)],
                        "stop_bytes": 40_000_000_000, "reserve_bytes": 1_000_000_000,
                        "poll_seconds": 1,
                        "planned_bytes": {key: 1 for key in
                                          ("fetch", "build", "runtime", "accept", "collect")}},
            "security": {"lsm_kind": "apparmor", "lsm_profile": "docker-default",
                         "seccomp_profile": str(self.root / "seccomp.json"),
                         "seccomp_sha256": "c" * 64},
        }
        self.qualified.write_text(json.dumps(base))
        self.qualified.chmod(0o600)
        self.sha = hashlib.sha256(self.qualified.read_bytes()).hexdigest()

    def factory(self, **changes):
        values = dict(expected_profile_sha256=self.sha, profile_dir=self.profile_dir,
                      staging_root=self.staging, source_pins=self.pins)
        values.update(changes)
        return profile_factory.ProfileFactory(self.qualified, **values)

    def test_seals_52_unique_phase_profiles_and_preserves_host_contract(self):
        factory = self.factory()
        self.assertEqual(len(factory.profile_hashes), 52)
        row = next(row for row in matrix.plan() if row["case"] == "select-all"
                   and row["trial"] == 0)
        preflight = json.loads(factory.profile_for("preflight", row).read_text())
        cell = json.loads(factory.profile_for("cell", row).read_text())
        base = json.loads(self.qualified.read_text())
        self.assertNotEqual(preflight["run_id"], cell["run_id"])
        self.assertNotEqual(preflight["assets"], cell["assets"])
        for key in ("docker_context", "platform", "execution_mode", "image_lock",
                    "private_output", "cache", "cpu", "security", "storage"):
            self.assertEqual(preflight[key], base[key])
        for key in ("memory_bytes", "swap_bytes", "pids", "stop_seconds"):
            self.assertEqual(preflight["limits"][key], base["limits"][key])
        self.assertEqual((preflight["limits"]["case_seconds"],
                          preflight["limits"]["run_seconds"]), (300, 420))
        self.assertEqual(factory.destination_for("preflight", row) / "assets.json",
                         Path(preflight["assets"]))

    def test_tamper_duplicate_and_stale_coordinate_fail_closed(self):
        factory = self.factory()
        row = matrix.plan()[0]
        path = factory.profile_for("cell", row)
        path.write_text(path.read_text() + " ")
        with self.assertRaisesRegex(ValueError, "PROFILE_PIN"):
            factory.profile_for("cell", row)
        with self.assertRaisesRegex(ValueError, "PROFILE_EXISTS"):
            self.factory()
        with self.assertRaisesRegex(ValueError, "PROFILE_COORDINATE"):
            factory.profile_for("cell", {**row, "trial": 99})
        with self.assertRaisesRegex(ValueError, "PROFILE_COORDINATE"):
            factory.profile_for("preflight", {**row, "trial": 1})

    def test_staged_manifest_requires_pinned_source_and_config(self):
        factory = self.factory()
        row = matrix.plan()[0]
        destination = factory.destination_for("cell", row)
        destination.mkdir(mode=0o700)
        (destination / "assets.json").write_text("{}")
        (destination / "measurement.json").write_text(json.dumps(
            {**row, "phase": "cell", "source": self.pins[row["subject"]]["source"]}))
        (destination / "assets.json").chmod(0o600)
        (destination / "measurement.json").chmod(0o600)
        with patch.object(factory.common, "verify_assets", return_value={
                "source_commit": "a" * 40, "source_tree": "b" * 40,
                "platform": "linux/amd64", "files": self.files}):
            self.assertEqual(factory.verify_staged("cell", row),
                             destination / "assets.json")
        with patch.object(factory.common, "verify_assets", return_value={
                "source_commit": "d" * 40, "platform": "linux/amd64",
                "files": self.files}):
            with self.assertRaisesRegex(ValueError, "STAGED_SOURCE"):
                factory.profile_for("cell", row)
        with patch.object(factory.common, "verify_assets", return_value={
                "source_commit": "a" * 40, "source_tree": "b" * 40,
                "platform": "linux/amd64", "files": [
                    dict(self.files[0], sha256="0" * 64), *self.files[1:]]}):
            with self.assertRaisesRegex(ValueError, "STAGED_PRODUCT"):
                factory.profile_for("cell", row)


if __name__ == "__main__":
    unittest.main()
