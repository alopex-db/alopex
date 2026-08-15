from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from .verify import extract_compat_data


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "scripts/parity/fixtures/compat/v0.8.4"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class CompatibilityFixtureContractTests(unittest.TestCase):
    def test_v084_fixture_is_complete_and_content_addressed(self) -> None:
        provenance = json.loads(
            (FIXTURE / "provenance.json").read_text(encoding="utf-8")
        )
        self.assertEqual(provenance["schema"], "alopex-compat-fixture/v1")
        self.assertEqual(
            provenance["source"],
            {
                "version": "0.8.4",
                "tag": "v0.8.4",
                "peeled_sha": "9a0cea1d24e7672f59cae72d9218b9cc698d9162",
                "binary_sha256": provenance["source"]["binary_sha256"],
            },
        )
        self.assertRegex(provenance["source"]["binary_sha256"], r"^[0-9a-f]{64}$")

        expected = FIXTURE / "expected.json"
        self.assertEqual(provenance["expected_sha256"], sha256(expected))
        archive = FIXTURE / provenance["archive"]["path"]
        self.assertEqual(provenance["archive"]["sha256"], sha256(archive))

        recorded_corpus = {
            item["path"]: item["sha256"] for item in provenance["corpus"]
        }
        actual_corpus = {
            path.relative_to(ROOT).as_posix(): sha256(path)
            for path in sorted((ROOT / "scripts/parity/corpus").glob("0[1-7]_*.sql"))
        }
        self.assertEqual(recorded_corpus, actual_corpus)

        recorded_data = {
            item["path"]: item["sha256"] for item in provenance["data"]
        }
        with tempfile.TemporaryDirectory() as scratch:
            data_dir = extract_compat_data(FIXTURE, Path(scratch), recorded_data)
            actual_data = {
                path.relative_to(data_dir).as_posix(): sha256(path)
                for path in sorted(data_dir.rglob("*"))
                if path.is_file()
            }
        self.assertTrue(actual_data)
        self.assertEqual(recorded_data, actual_data)


if __name__ == "__main__":
    unittest.main()
