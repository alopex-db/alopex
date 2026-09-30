from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "scripts/release/candidate_manifest.py"


def load_module():
    spec = importlib.util.spec_from_file_location("candidate_manifest", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class CandidateManifestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = load_module()
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.artifacts = self.root / "artifacts"
        self.artifacts.mkdir()
        (self.artifacts / "alopex-linux-x86_64").write_bytes(b"binary")
        (self.artifacts / "alopex-0.8.12.tar.gz").write_bytes(b"crate")
        self.verification = self.root / "verification.json"
        self.verification.write_text(
            json.dumps(
                {
                    "commit_sha": "a" * 40,
                    "runs": [
                        {"name": "CI Success", "run_id": 42, "conclusion": "success"},
                        {"name": "Extended Verification", "run_id": 43, "conclusion": "success"},
                    ],
                }
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_build_and_verify_bind_exact_sha_runs_and_artifact_bytes(self) -> None:
        manifest_path = self.root / "candidate-manifest.json"

        manifest = self.module.build_manifest(
            version="0.8.12",
            rc_tag="v0.8.12-rc.1",
            commit_sha="a" * 40,
            artifact_dir=self.artifacts,
            verification_path=self.verification,
        )
        self.module.write_manifest(manifest_path, manifest)

        self.assertEqual(manifest["schema"], "alopex-release-candidate-v1")
        self.assertEqual(
            [item["name"] for item in manifest["artifacts"]],
            ["alopex-0.8.12.tar.gz", "alopex-linux-x86_64"],
        )
        self.assertEqual([run["run_id"] for run in manifest["verification"]], [42, 43])
        self.module.verify_manifest(
            manifest_path=manifest_path,
            artifact_dir=self.artifacts,
            stable_tag="v0.8.12",
            stable_sha="a" * 40,
        )

    def test_build_rejects_missing_or_unsuccessful_verification(self) -> None:
        payload = json.loads(self.verification.read_text(encoding="utf-8"))
        payload["runs"][1]["conclusion"] = "failure"
        self.verification.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "successful"):
            self.module.build_manifest(
                version="0.8.12",
                rc_tag="v0.8.12-rc.1",
                commit_sha="a" * 40,
                artifact_dir=self.artifacts,
                verification_path=self.verification,
            )

    def test_verify_rejects_changed_bytes_or_identity(self) -> None:
        manifest_path = self.root / "candidate-manifest.json"
        manifest = self.module.build_manifest(
            version="0.8.12",
            rc_tag="v0.8.12-rc.1",
            commit_sha="a" * 40,
            artifact_dir=self.artifacts,
            verification_path=self.verification,
        )
        self.module.write_manifest(manifest_path, manifest)

        (self.artifacts / "alopex-linux-x86_64").write_bytes(b"change")
        with self.assertRaisesRegex(ValueError, "digest"):
            self.module.verify_manifest(
                manifest_path=manifest_path,
                artifact_dir=self.artifacts,
                stable_tag="v0.8.12",
                stable_sha="a" * 40,
            )
        with self.assertRaisesRegex(ValueError, "stable tag"):
            self.module.verify_manifest(
                manifest_path=manifest_path,
                artifact_dir=self.artifacts,
                stable_tag="v0.8.13",
                stable_sha="a" * 40,
            )

    def test_build_rejects_wrong_rc_version_or_commit(self) -> None:
        for rc_tag, commit_sha in (
            ("v0.8.13-rc.1", "a" * 40),
            ("v0.8.12", "a" * 40),
            ("v0.8.12-rc.1", "short"),
        ):
            with self.subTest(rc_tag=rc_tag, commit_sha=commit_sha):
                with self.assertRaises(ValueError):
                    self.module.build_manifest(
                        version="0.8.12",
                        rc_tag=rc_tag,
                        commit_sha=commit_sha,
                        artifact_dir=self.artifacts,
                        verification_path=self.verification,
                    )

    def test_build_rejects_nested_or_non_file_artifacts(self) -> None:
        (self.artifacts / "nested").mkdir()

        with self.assertRaisesRegex(ValueError, "regular files"):
            self.module.build_manifest(
                version="0.8.12",
                rc_tag="v0.8.12-rc.1",
                commit_sha="a" * 40,
                artifact_dir=self.artifacts,
                verification_path=self.verification,
            )


if __name__ == "__main__":
    unittest.main()
