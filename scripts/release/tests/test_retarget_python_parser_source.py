#!/usr/bin/env python3
"""Unit tests for staging a released parser manifest into Python wheel source."""

import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts/release/retarget_python_parser_source.py"


def load_module():
    spec = importlib.util.spec_from_file_location("retarget_python_parser_source", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class RetargetPythonParserSourceTests(unittest.TestCase):
    def manifest(self) -> dict:
        targets = (
            "aarch64-apple-darwin",
            "x86_64-apple-darwin",
            "x86_64-pc-windows-msvc",
            "x86_64-unknown-linux-gnu",
        )
        return {
            "schema": "alopex-parser-vendor-manifest-v1",
            "alopex_version": "0.8.5",
            "contract_version": "0.9.0",
            "assets": [
                {
                    "target": target,
                    "library": {
                        "path": f"alopex-sql-parser/{target}/library",
                        "sha256": f"{index + 1:064x}",
                        "size": 1,
                    },
                }
                for index, target in enumerate(targets)
            ],
        }

    def test_retargets_manifest_version_and_digest_idempotently(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "release.json"
            destination = root / "vendor.json"
            build_support = root / "build_support.rs"
            source.write_text(json.dumps(self.manifest(), sort_keys=True))
            build_support.write_text(
                'pub(crate) const REQUIRED_ALOPEX_VERSION: &str = "0.8.4";\n'
                'pub(crate) const VENDOR_MANIFEST_SHA256: &str =\n'
                '    "db70742bea017a4d2683ad0d17f602b25dbcdfa7f512e3c283fbb9f7fcce298d";\n'
            )

            module.retarget(source, destination, build_support)
            first = build_support.read_bytes()
            module.retarget(source, destination, build_support)

            digest = hashlib.sha256(source.read_bytes()).hexdigest()
            self.assertEqual(destination.read_bytes(), source.read_bytes())
            self.assertEqual(build_support.read_bytes(), first)
            self.assertIn(b'REQUIRED_ALOPEX_VERSION: &str = "0.8.5"', first)
            self.assertIn(digest.encode(), first)

    def test_invalid_target_matrix_fails_before_writing(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "release.json"
            destination = root / "vendor.json"
            build_support = root / "build_support.rs"
            manifest = self.manifest()
            manifest["assets"].pop()
            source.write_text(json.dumps(manifest))
            build_support.write_text("unchanged")

            with self.assertRaises(ValueError):
                module.retarget(source, destination, build_support)

            self.assertFalse(destination.exists())
            self.assertEqual(build_support.read_text(), "unchanged")

    def test_contract_v040_manifest_is_rejected_before_retargeting(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "release.json"
            destination = root / "vendor.json"
            build_support = root / "build_support.rs"
            manifest = self.manifest()
            manifest["contract_version"] = "0.4.0"
            source.write_text(json.dumps(manifest))
            build_support.write_text("unchanged")

            with self.assertRaisesRegex(ValueError, "contract mismatch"):
                module.retarget(source, destination, build_support)

            self.assertFalse(destination.exists())
            self.assertEqual(build_support.read_text(), "unchanged")

    def test_selects_fixed_or_single_versioned_vendor_manifest(self) -> None:
        module = load_module()
        versioned_filename = (
            f"parser-vendor-manifest-v{self.manifest()['alopex_version']}.json"
        )
        for filename in (
            "parser-vendor-manifest.json",
            versioned_filename,
        ):
            with (
                self.subTest(filename=filename),
                tempfile.TemporaryDirectory() as directory,
            ):
                vendor_dir = Path(directory)
                expected = vendor_dir / filename
                expected.write_text("manifest")
                (vendor_dir / "unrelated.json").write_text("ignored")

                self.assertEqual(module.select_vendor_manifest(vendor_dir), expected)

    def test_vendor_manifest_selection_fails_closed_on_missing_or_ambiguous_layout(
        self,
    ) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            vendor_dir = Path(directory)
            with self.assertRaisesRegex(ValueError, "expected exactly one"):
                module.select_vendor_manifest(vendor_dir)

            (vendor_dir / "parser-vendor-manifest.json").write_text("fixed")
            versioned = (
                vendor_dir
                / f"parser-vendor-manifest-v{self.manifest()['alopex_version']}.json"
            )
            versioned.write_text("versioned")
            with self.assertRaisesRegex(ValueError, "expected exactly one"):
                module.select_vendor_manifest(vendor_dir)

    def test_cli_retargets_the_single_historical_vendor_manifest_in_place(self) -> None:
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "release.json"
            vendor_dir = root / "vendor"
            vendor_dir.mkdir()
            historical = (
                vendor_dir
                / f"parser-vendor-manifest-v{self.manifest()['alopex_version']}.json"
            )
            build_support = root / "build_support.rs"
            source.write_text(json.dumps(self.manifest(), sort_keys=True))
            historical.write_text("old manifest")
            build_support.write_text(
                'pub(crate) const REQUIRED_ALOPEX_VERSION: &str = "0.8.4";\n'
                'pub(crate) const VENDOR_MANIFEST_SHA256: &str =\n'
                '    "db70742bea017a4d2683ad0d17f602b25dbcdfa7f512e3c283fbb9f7fcce298d";\n'
            )

            argv = [
                str(SCRIPT),
                "--manifest",
                str(source),
                "--vendor-dir",
                str(vendor_dir),
                "--build-support",
                str(build_support),
            ]
            with patch("sys.argv", argv):
                module.main()

            self.assertEqual(historical.read_bytes(), source.read_bytes())
            self.assertFalse((vendor_dir / "parser-vendor-manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
