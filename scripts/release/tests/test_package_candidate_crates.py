from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "scripts/release/package_candidate_crates.py"


def load_module():
    spec = importlib.util.spec_from_file_location("package_candidate_crates", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class CandidatePackageIndexTests(unittest.TestCase):
    def test_index_record_uses_the_candidate_archive_and_dependency_alias(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "alopex-core-0.8.14.crate"
            archive.write_bytes(b"candidate archive")
            metadata = {
                "name": "alopex-core",
                "vers": "0.8.14",
                "deps": [
                    {
                        "name": "serde",
                        "version_req": "1",
                        "features": ["derive"],
                        "optional": False,
                        "default_features": True,
                        "target": None,
                        "kind": "normal",
                        "registry": None,
                        "explicit_name_in_toml": "serde_alias",
                    }
                ],
                "features": {},
                "links": None,
                "rust_version": None,
            }

            record = module.index_record(metadata, archive)

        self.assertEqual(record["name"], "alopex-core")
        self.assertEqual(record["deps"][0]["name"], "serde_alias")
        self.assertEqual(record["deps"][0]["package"], "serde")
        self.assertEqual(record["cksum"], "2bec12b4b590104ae734d2d91e6d9f1bdbb938b65d031606bdb15ee7b1e9bd41")

    def test_index_path_follows_crates_io_layout(self) -> None:
        module = load_module()
        self.assertEqual(module.index_path("a"), Path("1/a"))
        self.assertEqual(module.index_path("ab"), Path("2/ab"))
        self.assertEqual(module.index_path("abc"), Path("3/a/abc"))
        self.assertEqual(module.index_path("alopex-core"), Path("al/op/alopex-core"))

    def test_candidate_entry_keeps_existing_public_versions(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            index = module.SparseIndex(root)
            public_path = root / ".upstream-cache" / "al/op/alopex-core"
            public_path.parent.mkdir(parents=True)
            public_path.write_text('{"name":"alopex-core","vers":"0.3.4"}\n')
            archive = root / "alopex-core-0.8.14.crate"
            archive.write_bytes(b"candidate archive")
            index.add(
                {
                    "name": "alopex-core",
                    "vers": "0.8.14",
                    "deps": [],
                    "features": {},
                    "links": None,
                    "rust_version": None,
                },
                archive,
            )

            versions = [
                json.loads(line)["vers"]
                for line in (root / "al/op/alopex-core").read_text().splitlines()
            ]
        self.assertEqual(versions, ["0.3.4", "0.8.14"])


if __name__ == "__main__":
    unittest.main()
