"""Public-only HNSW asset ingress; synthetic files, no product build."""

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_public_assets as assets


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def make_bundle(root, *, example=b"fn main() {}\n", qualification="asset_only"):
    root.mkdir(mode=0o700)
    elf = b"\x7fELF\x02\x01" + b"\0" * 12 + b"\x3e\x00"
    members = {"binary": elf + b"binary", "library": elf + b"parser",
               "example": example}
    pin = {"source_commit": "a" * 40, "source_tree": "1" * 40,
           "binary_sha256": digest(members["binary"]),
           "parser_sha256": digest(members["library"]),
           "example_sha256": digest(example), "assets_digest": "0" * 64,
           "parser_record_sha256": "5" * 64,
           "cargo_lock_sha256": "6" * 64, "rust_version": "1.96.0",
           "version": "0.8.17", "profile": "release",
           "platform": "linux/amd64", "parser_contract": "0.28.0"}
    source = {"commit": pin["source_commit"], "tree": pin["source_tree"],
              "profile": "release"}
    smoke = {"status": "passed", "rows": 2, "dimensions": 4,
             "phases": 9, "events": 19}
    receipt = {"schema": assets.RECEIPT_SCHEMA, "source": source,
               "version": pin["version"], "binary_sha256": pin["binary_sha256"],
               "parser_sha256": pin["parser_sha256"],
               "example_source_sha256": pin["example_sha256"],
               "parser_record_sha256": pin["parser_record_sha256"],
               "cargo_lock_sha256": pin["cargo_lock_sha256"],
               "rust_version": pin["rust_version"],
               "parser_contract": pin["parser_contract"],
               "build": {"command": assets.COMMAND, "incremental": False,
                         "release_debug": False, "path_remap_applied": True},
               "smoke": smoke,
               "completion_smoke": {mode: dict(smoke, mode=mode,
                                               result_validated=True,
                                               elapsed_ns=100)
                                    for mode in ("commit", "rollback")},
               "qualification": qualification}
    members["receipt"] = canonical(receipt)
    rows = []
    for role, name in assets.FILES.items():
        path = root / "assets" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(members[role])
        rows.append({"role": role, "path": name, "size": len(members[role]),
                     "sha256": digest(members[role])})
    manifest = {"schema": assets.SCHEMA, "source": source,
                "platform": "linux/amd64", "files": rows,
                "expanded_limit_bytes": 268_435_456}
    raw = canonical(manifest)
    (root / "assets/assets.json").write_bytes(raw)
    pin["assets_digest"] = digest(raw)
    (root / "result.json").write_bytes(canonical({
        "schema": assets.RESULT_SCHEMA, "status": "passed",
        "source": source, "assets_digest": pin["assets_digest"],
        "performance_qualification": False}))
    return pin


class PublicAssetTests(unittest.TestCase):
    def test_exact_public_bundle_is_admitted(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "bundle"
            pin = make_bundle(root)
            result = assets.verify(root, pin, private_roots=(str(root),))
            self.assertEqual(set(result), set(assets.FILES))

    def test_extra_encrypted_member_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "bundle"
            pin = make_bundle(root)
            (root / "private-evidence.cms").write_bytes(b"encrypted")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_ASSET$"):
                assets.verify(root, pin, private_roots=(str(root),))

    def test_local_path_and_free_text_receipt_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "bundle"
            pin = make_bundle(root, example=b"/private/host/secret\n")
            diag = {}
            with self.assertRaisesRegex(ValueError, "^PUBLIC_ASSET$"):
                assets.verify(root, pin, private_roots=(str(root), "/private/host"),
                              diag=diag)
            self.assertEqual(diag.get("code"), "BUILD_ASSETS_PREFIX_EXAMPLE")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "bundle"
            pin = make_bundle(root, qualification="/private/host/diagnostic")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_ASSET$"):
                assets.verify(root, pin, private_roots=(str(root),))

    def test_symlinked_member_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "bundle"
            pin = make_bundle(root)
            file = root / "assets/example.rs"
            file.unlink()
            file.symlink_to(root / "assets/assets.json")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_ASSET$"):
                assets.verify(root, pin, private_roots=(str(root),))


if __name__ == "__main__":
    unittest.main()
