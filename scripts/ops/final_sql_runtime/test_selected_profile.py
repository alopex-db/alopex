"""Selected profile comes from the live current-job #804 proof."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.ops.final_sql_runtime import selected_profile as selected


class SelectedProfileTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.private = self.root / "alopex-804-qualification"
        self.private.mkdir(mode=0o700)
        self.backing = self.root / "backing"
        self.backing.mkdir()
        self.storage = {"runtime_storage_roots": [str(self.backing)]}
        self.profile = self.private / "profile.json"
        self.profile.write_text(json.dumps({"storage": self.storage,
                                            "private_output": str(self.private / "runs")}))
        self.profile.chmod(0o600)
        self.digest = hashlib.sha256(self.profile.read_bytes()).hexdigest()
        self.args = {"lock_path": self.root / "lock.json", "expected": {},
                     "repo": self.root, "runner_temp": self.root,
                     "environment": {}}

    def select(self, **proof_changes):
        proof = {"profile_sha256": self.digest, "storage": self.storage}
        proof.update(proof_changes)
        class Common:
            @staticmethod
            def load_profile(path):
                return json.loads(path.read_bytes())
        with (patch.object(selected.same_job, "attest_live", return_value=proof) as live,
              patch.object(selected.container, "_runtime",
                           return_value=(Common, None, None, None, None))):
            result = selected.from_live(self.args)
            live.assert_called_once_with(**self.args)
            return result

    def test_one_private_profile_and_backing_root(self):
        self.assertEqual(self.select(), (self.profile, self.digest, self.backing))

    def test_wrong_hash_ambiguous_root_and_symlink_fail(self):
        with self.assertRaisesRegex(ValueError, "SELECTED_PROFILE"):
            self.select(profile_sha256="0" * 64)
        with self.assertRaisesRegex(ValueError, "SELECTED_PROFILE"):
            self.select(storage={"runtime_storage_roots": [str(self.backing)] * 2})
        self.profile.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "SELECTED_PROFILE"):
            self.select()
        self.profile.chmod(0o600)
        self.backing.rename(self.root / "real-backing")
        self.backing.symlink_to(self.root / "real-backing", target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "SELECTED_PROFILE"):
            self.select()


if __name__ == "__main__":
    unittest.main()
