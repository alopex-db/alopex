#!/usr/bin/env python3
"""Behavioral tests for parser release-asset staging."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts/release/stage_parser_assets.py"
TARGETS = {
    "aarch64-apple-darwin": ("libalopex_sql_parser.dylib", "libalopex_sql_parser.a"),
    "x86_64-apple-darwin": ("libalopex_sql_parser.dylib", "libalopex_sql_parser.a"),
    "x86_64-pc-windows-msvc": ("alopex_sql_parser.dll", "alopex_sql_parser.lib"),
    "x86_64-unknown-linux-gnu": ("libalopex_sql_parser.so", "libalopex_sql_parser.a"),
}


def load_module():
    spec = importlib.util.spec_from_file_location("stage_parser_assets", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class StageParserAssetsTests(unittest.TestCase):
    def test_stages_verified_dynamic_and_static_libraries(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            asset_dir = root / "assets"
            asset_dir.mkdir()
            manifest = {"contract_version": "0.25.0", "assets": []}
            expected: dict[str, tuple[bytes, bytes]] = {}
            for target, (dynamic_name, static_name) in TARGETS.items():
                dynamic = f"dynamic:{target}".encode()
                static = f"static:{target}".encode()
                archive_name = f"{target}.tar.gz"
                archive = asset_dir / archive_name
                with tarfile.open(archive, "w:gz") as handle:
                    for name, content in ((dynamic_name, dynamic), (static_name, static)):
                        member = tarfile.TarInfo(f"alopex-sql-parser/{target}/{name}")
                        member.size = len(content)
                        handle.addfile(member, io.BytesIO(content))
                archive_bytes = archive.read_bytes()
                manifest["assets"].append(
                    {
                        "target": target,
                        "archive": {
                            "filename": archive_name,
                            "sha256": hashlib.sha256(archive_bytes).hexdigest(),
                            "size": len(archive_bytes),
                        },
                        "library": {
                            "path": f"alopex-sql-parser/{target}/{dynamic_name}",
                            "sha256": hashlib.sha256(dynamic).hexdigest(),
                            "size": len(dynamic),
                        },
                        "static_library": {
                            "path": f"alopex-sql-parser/{target}/{static_name}",
                            "sha256": hashlib.sha256(static).hexdigest(),
                            "size": len(static),
                        },
                    }
                )
                expected[target] = (dynamic, static)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            vendor_dir = root / "vendor"

            module.stage(manifest_path, asset_dir, vendor_dir)

            for target, (dynamic_name, static_name) in TARGETS.items():
                dynamic, static = expected[target]
                target_dir = vendor_dir / target
                self.assertEqual((target_dir / dynamic_name).read_bytes(), dynamic)
                self.assertEqual((target_dir / static_name).read_bytes(), static)
                self.assertEqual((target_dir / "CONTRACT_VERSION").read_text(), "0.25.0\n")
                self.assertEqual(
                    (target_dir / "SHA256SUMS").read_text(),
                    f"{hashlib.sha256(dynamic).hexdigest()}  {dynamic_name}\n"
                    f"{hashlib.sha256(static).hexdigest()}  {static_name}\n",
                )
            self.assertEqual(
                (vendor_dir / "parser-vendor-manifest.json").read_bytes(),
                manifest_path.read_bytes(),
            )

    def test_rejects_archive_member_digest_mismatch_without_creating_vendor(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(
                json.dumps({"contract_version": "0.25.0", "assets": []}), encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "assets"):
                module.stage(manifest_path, root, root / "vendor")


if __name__ == "__main__":
    unittest.main()
