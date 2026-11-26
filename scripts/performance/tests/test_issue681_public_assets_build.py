"""Producer boundary checks with synthetic assets; no product build."""

from pathlib import Path
import os
import stat
import tempfile
import unittest

from test_issue681_public_assets import digest

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_public as case
import issue681_public_assets as assets
import issue681_public_assets_build as build


def fixture_smoke(binary, mode):
    return {"status": "passed", "rows": 2, "dimensions": 4,
            "phases": 9, "events": 19, "mode": mode,
            "result_validated": True, "elapsed_ns": 100}


class PublicAssetBuildTests(unittest.TestCase):
    def test_fresh_six_member_bundle_passes_independent_consumer(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            elf = b"\x7fELF\x02\x01" + b"\0" * 12 + b"\x3e\x00"
            binary, library, example = (root / name for name in
                                        ("binary", "library", "example"))
            binary.write_bytes(elf + b"binary")
            library.write_bytes(elf + b"parser")
            example.write_bytes(b"fn main() {}\n")
            output = root / "bundle"
            pin = build._package(
                output, binary, library, example, source_commit="a" * 40,
                source_tree="1" * 40, parser_record_sha256="5" * 64,
                cargo_lock_sha256="6" * 64, rust_version="1.96.0",
                private_roots=(str(root),), smoke_runner=fixture_smoke)
            self.assertEqual(set(path.relative_to(output).as_posix()
                                 for path in output.rglob("*") if path.is_file()),
                             assets.MEMBERS)
            self.assertEqual(pin["binary_sha256"], digest(binary.read_bytes()))
            self.assertEqual(set(assets.verify(output, pin,
                                               private_roots=(str(root),))),
                             set(assets.FILES))

    def test_failed_smoke_creates_no_bundle(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary, library, example = (root / name for name in
                                        ("binary", "library", "example"))
            for path in (binary, library, example):
                path.write_bytes(b"asset")
            output = root / "bundle"
            def failed_smoke(binary, mode):
                raise RuntimeError("private diagnostic")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                build._package(output, binary, library, example,
                               source_commit="a" * 40, source_tree="1" * 40,
                               parser_record_sha256="5" * 64,
                               cargo_lock_sha256="6" * 64,
                               rust_version="1.96.0", private_roots=(str(root),),
                               smoke_runner=failed_smoke)
            self.assertFalse(output.exists())

    def test_protocol_smoke_checks_both_completion_modes(self):
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp) / "fixture"
            script = """#!/usr/bin/env python3
import json, sys
phases = %r + (sys.argv[3],)
for phase in phases:
    for kind in ('start', 'end'):
        data = {'event': kind}
        if kind == 'end': data['elapsed_ns'] = 100
        print(json.dumps({'phase': phase, 'data': data}), flush=True)
        assert sys.stdin.readline().strip() == 'continue'
print(json.dumps({'phase': 'complete', 'data':
                  {'finish': sys.argv[3], 'result_validated': True}}), flush=True)
assert sys.stdin.readline().strip() == 'continue'
""" % (case.PHASES,)
            binary.write_text(script)
            os.chmod(binary, 0o700)
            for mode in ("commit", "rollback"):
                self.assertEqual(build.smoke(binary, mode)["status"], "passed")

    def test_hardlinked_cargo_binary_packages_with_exec_mode_and_single_link(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            elf = b"\x7fELF\x02\x01" + b"\0" * 12 + b"\x3e\x00"
            hashed_binary = root / "issue681_hnsw_first_write-abcdef1234"
            binary = root / "issue681_hnsw_first_write"
            library = root / "libalopex_sql_parser.so"
            example = root / "example.rs"
            hashed_binary.write_bytes(elf + b"binary")
            os.link(hashed_binary, binary)
            self.assertEqual(binary.stat().st_nlink, 2)
            library.write_bytes(elf + b"parser")
            example.write_bytes(b"fn main() {}\n")
            output = root / "bundle"
            diag = {}
            pin = build._package(
                output, binary, library, example, source_commit="a" * 40,
                source_tree="1" * 40, parser_record_sha256="5" * 64,
                cargo_lock_sha256="6" * 64, rust_version="1.90.0",
                private_roots=(str(root),), smoke_runner=fixture_smoke,
                diag=diag)
            packaged_bin = output / "assets" / assets.FILES["binary"]
            packaged_lib = output / "assets" / assets.FILES["library"]
            self.assertEqual(packaged_bin.stat().st_nlink, 1)
            self.assertEqual(stat.S_IMODE(packaged_bin.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE(packaged_lib.stat().st_mode), 0o600)
            self.assertEqual(pin["binary_sha256"], digest(binary.read_bytes()))
            # Packaged bundle members still strictly reject hardlinks.
            extra_link = root / "extra-link"
            os.link(packaged_bin, extra_link)
            with self.assertRaisesRegex(ValueError, "^PUBLIC_ASSET$"):
                assets.verify(output, pin, private_roots=(str(root),))

    def test_packaging_error_reports_package_stage_not_stale_smoke(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            elf = b"\x7fELF\x02\x01" + b"\0" * 12 + b"\x3e\x00"
            binary = root / "binary"
            library = root / "library"
            library_extra = root / "library-link"
            example = root / "example"
            binary.write_bytes(elf + b"binary")
            library.write_bytes(elf + b"parser")
            os.link(library, library_extra)
            example.write_bytes(b"fn main() {}\n")
            diag = {}
            with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                build._package(
                    root / "bundle", binary, library, example,
                    source_commit="a" * 40, source_tree="1" * 40,
                    parser_record_sha256="5" * 64, cargo_lock_sha256="6" * 64,
                    rust_version="1.90.0", private_roots=(str(root),),
                    smoke_runner=fixture_smoke, diag=diag)
            self.assertEqual(diag.get("code"), "BUILD_PACKAGE_STAGE")


if __name__ == "__main__":
    unittest.main()
