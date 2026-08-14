#!/usr/bin/env python3
"""Behavior tests for deterministic Nim parser target archives."""

from __future__ import annotations

import gzip
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
MANIFEST_TOOL = REPOSITORY_ROOT / "scripts/release/parser_asset_manifest.py"
BUILD_SCRIPT = REPOSITORY_ROOT / "scripts/build-nim-parser.sh"
PARSER_DIR = REPOSITORY_ROOT / "crates/alopex-sql/nim-sql-parser"
RELEASE_WORKFLOW = REPOSITORY_ROOT / ".github/workflows/release.yml"
PY_RELEASE_WORKFLOW = REPOSITORY_ROOT / ".github/workflows/alopex-py-release.yml"
NIMBLE_SHA = "42ef70c2102a942c46f13eb76872326edd525cec"
BUILD_PROFILE = "nim-release-library-v1"
TARGET_LIBRARIES = {
    "x86_64-unknown-linux-gnu": "libalopex_sql_parser.so",
    "x86_64-apple-darwin": "libalopex_sql_parser.dylib",
    "aarch64-apple-darwin": "libalopex_sql_parser.dylib",
    "x86_64-pc-windows-msvc": "alopex_sql_parser.dll",
}

MODULE_SPEC = importlib.util.spec_from_file_location(
    "alopex_parser_asset_manifest", MANIFEST_TOOL
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"could not load {MANIFEST_TOOL}")
MANIFEST = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = MANIFEST
MODULE_SPEC.loader.exec_module(MANIFEST)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def normalized_archive(path: Path, members: list[tuple[str, bytes]]) -> None:
    with path.open("wb") as raw:
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=0
        ) as compressed:
            with tarfile.open(
                fileobj=compressed, mode="w", format=tarfile.GNU_FORMAT
            ) as archive:
                for name, content in members:
                    info = tarfile.TarInfo(name=name)
                    info.size = len(content)
                    info.mode = 0o644
                    info.mtime = 0
                    info.uid = 0
                    info.gid = 0
                    info.uname = ""
                    info.gname = ""
                    archive.addfile(info, io.BytesIO(content))


class ParserAssetManifestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="alopex-parser-manifest-")
        self.root = Path(self.temp.name)
        self.source = self.root / "parser-source"
        (self.source / "src").mkdir(parents=True)
        (self.source / "PARSER_CONTRACT_VERSION").write_text(
            "0.4.0\n", encoding="utf-8"
        )
        (self.source / "nim_sql_parser.nimble").write_text(
            'version = "0.4.0"\n', encoding="utf-8"
        )
        (self.source / "src/alopex_sql_parser.nim").write_text(
            "proc parserVersion(): string = \"0.4.0\"\n", encoding="utf-8"
        )

        self.nim = self.root / "nim"
        self.nimble = self.root / "nimble"
        self.nim.write_bytes(b"exact-nim-2.2.10")
        self.nimble.write_bytes(b"exact-nimble-0.22.3")

        self.npeg = self.root / "npeg-1.3.0"
        self.msgpack = self.root / "msgpack4nim-0.4.4"
        self.npeg.mkdir()
        self.msgpack.mkdir()
        (self.npeg / "npeg.nimble").write_text(
            'version = "1.3.0"\n', encoding="utf-8"
        )
        (self.npeg / "npeg.nim").write_bytes(b"npeg-content")
        canonical_json(
            self.npeg / "nimblemeta.json",
            {
                "metaData": {
                    "binaries": [],
                    "downloadMethod": "git",
                    "files": ["/npeg.nimble", "/npeg.nim"],
                    "specialVersions": ["1.3.0"],
                    "url": "",
                    "vcsRevision": MANIFEST.REQUIRED_PACKAGES["npeg"][
                        "vcs_revision"
                    ],
                },
                "version": 1,
            },
        )
        (self.msgpack / "msgpack4nim.nimble").write_text(
            'version = "0.4.4"\n', encoding="utf-8"
        )
        (self.msgpack / "msgpack4nim.nim").write_bytes(b"msgpack-content")
        canonical_json(
            self.msgpack / "nimblemeta.json",
            {
                "metaData": {
                    "binaries": [],
                    "downloadMethod": "git",
                    "files": ["/msgpack4nim.nimble", "/msgpack4nim.nim"],
                    "specialVersions": ["0.4.4"],
                    "url": "",
                    "vcsRevision": MANIFEST.REQUIRED_PACKAGES["msgpack4nim"][
                        "vcs_revision"
                    ],
                },
                "version": 1,
            },
        )

        self.metadata_dir = self.root / "nimble-metadata"
        self.metadata_dir.mkdir()
        self.metadata: dict[str, Path] = {}
        for name in (
            "packages_official.json",
            "packages_temp.json",
        ):
            path = self.metadata_dir / name
            canonical_json(path, {"fixture": name})
            self.metadata[name] = path

        self.output = self.root / "output"
        self.output.mkdir()

        self.original_required_packages = MANIFEST.REQUIRED_PACKAGES
        self.original_required_metadata = MANIFEST.REQUIRED_REGISTRY_METADATA
        fixture_packages: dict[str, dict[str, object]] = {}
        for name, root in (("msgpack4nim", self.msgpack), ("npeg", self.npeg)):
            requirement = dict(self.original_required_packages[name])
            requirement.update(
                MANIFEST.tree_identity(
                    root, f"{name} fixture", MANIFEST.package_path_ignored
                )
            )
            fixture_packages[name] = requirement
        fixture_metadata: dict[str, dict[str, object]] = {}
        for name, path in self.metadata.items():
            content = path.read_bytes()
            fixture_metadata[name] = {
                "sha256": hashlib.sha256(content).hexdigest(),
                "size": len(content),
                "source_revision": MANIFEST.REQUIRED_PACKAGE_LIST_REVISION,
            }
        MANIFEST.REQUIRED_PACKAGES = fixture_packages
        MANIFEST.REQUIRED_REGISTRY_METADATA = fixture_metadata

    def tearDown(self) -> None:
        MANIFEST.REQUIRED_PACKAGES = self.original_required_packages
        MANIFEST.REQUIRED_REGISTRY_METADATA = self.original_required_metadata
        self.temp.cleanup()


    def run_cli(
        self, *arguments: str, expected: int = 0
    ) -> subprocess.CompletedProcess[str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            returncode = MANIFEST.main(list(arguments))
        result = subprocess.CompletedProcess(
            [str(MANIFEST_TOOL), *arguments],
            returncode,
            stdout.getvalue(),
            stderr.getvalue(),
        )
        self.assertEqual(
            result.returncode,
            expected,
            msg=f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
        )
        return result

    def pack(
        self,
        target: str = "x86_64-unknown-linux-gnu",
        *,
        output: Path | None = None,
        nim_version: str = "2.2.10",
        nimble_version: str = "0.22.3",
        nimble_sha: str = NIMBLE_SHA,
        build_profile: str = BUILD_PROFILE,
        source: Path | None = None,
        npeg: Path | None = None,
        msgpack: Path | None = None,
        npeg_version: str = "1.3.0",
        library_name: str | None = None,
        expected: int = 0,
    ) -> tuple[Path, Path]:
        output = output or self.output
        output.mkdir(parents=True, exist_ok=True)
        library = self.root / (library_name or TARGET_LIBRARIES[target])
        library.write_bytes(f"native-library:{target}".encode())
        arguments = [
            "pack-target",
            "--alopex-version",
            "0.8.4",
            "--contract-version",
            "0.4.0",
            "--target",
            target,
            "--library",
            str(library),
            "--source-root",
            str(source or self.source),
            "--nim-version",
            nim_version,
            "--nim-binary",
            str(self.nim),
            "--nimble-version",
            nimble_version,
            "--nimble-sha",
            nimble_sha,
            "--nimble-binary",
            str(self.nimble),
            "--build-profile",
            build_profile,
            "--package",
            f"npeg={npeg_version}={npeg or self.npeg}",
            "--package",
            f"msgpack4nim=0.4.4={msgpack or self.msgpack}",
        ]
        for name, path in self.metadata.items():
            arguments.extend(("--registry-metadata", f"{name}={path}"))
        arguments.extend(("--output-dir", str(output)))
        self.run_cli(*arguments, expected=expected)
        stem = f"alopex-parser-v0.8.4-contract-0.4.0-{target}"
        return output / f"{stem}.json", output / f"{stem}.tar.gz"

    def verify_target(
        self, record: Path, output: Path | None = None, expected: int = 0
    ) -> subprocess.CompletedProcess[str]:
        return self.run_cli(
            "verify-target",
            "--record",
            str(record),
            "--asset-dir",
            str(output or record.parent),
            expected=expected,
        )

    def build_script_fixture(
        self, *, nimble_version: str = "0.22.3"
    ) -> dict[str, str]:
        seed = self.root / "seed"
        packages = seed / "pkgs2"
        packages.mkdir(parents=True)
        shutil.copytree(self.npeg, packages / "npeg-1.3.0-fixture")
        shutil.copytree(
            self.msgpack, packages / "msgpack4nim-0.4.4-fixture"
        )
        for name, path in self.metadata.items():
            shutil.copy2(path, seed / name)

        fake_nim = self.root / "fake-nim"
        fake_nim.write_text(
            """#!/usr/bin/env python3
import pathlib
import sys

if "--version" in sys.argv[1:]:
    print("Nim Compiler Version 2.2.10 [Linux: amd64]")
    raise SystemExit(0)

output = next(arg.split(":", 1)[1] for arg in sys.argv[1:] if arg.startswith("-o:"))
dependency_paths = sorted(
    arg.split(":", 1)[1]
    for arg in sys.argv[1:]
    if arg.startswith("--path:")
)
pathlib.Path(output).write_text("\\n".join(dependency_paths), encoding="utf-8")
""",
            encoding="utf-8",
        )
        fake_nim.chmod(0o755)

        fake_nimble = self.root / "fake-nimble"
        fake_nimble.write_text(
            f"""#!/usr/bin/env python3
import pathlib
import sys

arguments = sys.argv[1:]
if "--version" in arguments:
    print("nimble v{nimble_version}")
    print("git hash: {NIMBLE_SHA}")
    raise SystemExit(0)

nimble_dir = pathlib.Path(
    next(arg.split(":", 1)[1] for arg in arguments if arg.startswith("--nimbleDir:"))
)
package = arguments[-1]
prefix = {{"npeg": "npeg-1.3.0-", "msgpack4nim": "msgpack4nim-0.4.4-"}}[package]
matches = sorted(
    path
    for path in (nimble_dir / "pkgs2").iterdir()
    if path.name.startswith(prefix)
)
if len(matches) != 1:
    raise SystemExit(2)
print(matches[0])
""",
            encoding="utf-8",
        )
        fake_nimble.chmod(0o755)

        environment = os.environ.copy()
        environment.update(
            {
                "ALOPEX_NIM_BIN": str(fake_nim),
                "ALOPEX_NIMBLE_BIN": str(fake_nimble),
                "ALOPEX_NIMBLE_SEED_DIR": str(seed),
            }
        )
        return environment

    def run_build_script(
        self, environment: dict[str, str], destination: Path
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "bash",
                str(BUILD_SCRIPT),
                "--backend",
                "host",
                "--target",
                "x86_64-unknown-linux-gnu",
                "--archive-dir",
                str(destination),
            ],
            cwd=REPOSITORY_ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_repeated_pack_is_byte_identical_and_two_level_verified(self) -> None:
        first_record, first_archive = self.pack()
        first_record_bytes = first_record.read_bytes()
        first_archive_bytes = first_archive.read_bytes()

        second_record, second_archive = self.pack()
        self.assertEqual(second_record.read_bytes(), first_record_bytes)
        self.assertEqual(second_archive.read_bytes(), first_archive_bytes)
        self.verify_target(second_record)

        record = json.loads(second_record.read_text(encoding="utf-8"))
        self.assertEqual(record["schema"], "alopex-parser-target-record-v1")
        self.assertEqual(record["alopex_version"], "0.8.4")
        self.assertEqual(record["contract_version"], "0.4.0")
        self.assertEqual(record["target"], "x86_64-unknown-linux-gnu")
        self.assertEqual(
            record["builder"]["compile"]["profile"], BUILD_PROFILE
        )
        self.assertEqual(
            record["builder"]["compile"]["dependency_path_policy"],
            "fixed-/tmp-target-qualified-v1",
        )
        packer = record["builder"]["packer"]
        self.assertEqual(
            packer["build_script"]["binary_sha256"], sha256(BUILD_SCRIPT)
        )
        self.assertEqual(
            packer["manifest_tool"]["binary_sha256"], sha256(MANIFEST_TOOL)
        )
        self.assertTrue(packer["python"]["version"])
        self.assertTrue(packer["python"]["binary_sha256"])
        self.assertTrue(packer["zlib"]["compile_version"])
        self.assertTrue(packer["zlib"]["runtime_version"])
        self.assertEqual(
            record["packing"]["archive_size_limit"],
            MANIFEST.MAX_ARCHIVE_BYTES,
        )
        self.assertEqual(
            record["packing"]["decompressed_size_limit"],
            MANIFEST.MAX_DECOMPRESSED_ARCHIVE_BYTES,
        )
        self.assertEqual(
            record["packing"]["member_count_limit"],
            MANIFEST.MAX_ARCHIVE_MEMBERS,
        )
        self.assertEqual(
            record["packing"]["member_size_limit"],
            MANIFEST.MAX_ARCHIVE_MEMBER_BYTES,
        )
        self.assertEqual(record["archive"]["sha256"], sha256(second_archive))
        self.assertNotIn("commit", json.dumps(record).lower())

    def test_pack_rejects_wrong_toolchain_and_missing_identity(self) -> None:
        self.pack(nim_version="2.2.9", expected=2)
        self.pack(nimble_version="0.22.2", expected=2)
        self.pack(nimble_sha="0" * 40, expected=2)
        self.pack(build_profile="unrecorded-profile", expected=2)
        self.pack(npeg_version="1.2.9", expected=2)
        self.metadata["packages_temp.json"].unlink()
        self.pack(expected=2)

    def test_pack_rejects_wrong_library_name_and_target(self) -> None:
        record, archive = self.pack(library_name="wrong-parser.so", expected=2)
        self.assertFalse(record.exists())
        self.assertFalse(archive.exists())

        result = self.run_cli(
            "pack-target",
            "--alopex-version",
            "0.8.4",
            "--contract-version",
            "0.4.0",
            "--target",
            "x86_64-unknown-freebsd",
            "--library",
            str(self.nim),
            "--source-root",
            str(self.source),
            "--nim-version",
            "2.2.10",
            "--nim-binary",
            str(self.nim),
            "--nimble-version",
            "0.22.3",
            "--nimble-sha",
            NIMBLE_SHA,
            "--nimble-binary",
            str(self.nimble),
            "--build-profile",
            BUILD_PROFILE,
            "--package",
            f"npeg=1.3.0={self.npeg}",
            "--package",
            f"msgpack4nim=0.4.4={self.msgpack}",
            "--registry-metadata",
            f"packages_official.json={self.metadata['packages_official.json']}",
            "--registry-metadata",
            f"packages_temp.json={self.metadata['packages_temp.json']}",
            "--output-dir",
            str(self.output),
            expected=2,
        )
        self.assertIn("target", result.stderr.lower())

    def test_source_and_package_content_are_identity_inputs(self) -> None:
        first_record, _ = self.pack()
        first = json.loads(first_record.read_text(encoding="utf-8"))

        (self.source / "src/alopex_sql_parser.nim").write_bytes(b"source-drift")
        second_dir = self.root / "source-drift"
        second_record, _ = self.pack(output=second_dir)
        second = json.loads(second_record.read_text(encoding="utf-8"))
        self.assertNotEqual(
            first["parser_source"]["tree_sha256"],
            second["parser_source"]["tree_sha256"],
        )

        (self.npeg / "npeg.nim").write_bytes(b"same-version-content-drift")
        third_dir = self.root / "package-drift"
        third_record, third_archive = self.pack(
            output=third_dir, expected=2
        )
        self.assertFalse(third_record.exists())
        self.assertFalse(third_archive.exists())

        (self.npeg / "npeg.nim").write_bytes(b"npeg-content")

        canonical_json(
            self.metadata["packages_official.json"],
            {"fixture": "packages_official.json", "revision": 2},
        )
        fourth_dir = self.root / "metadata-drift"
        fourth_record, fourth_archive = self.pack(
            output=fourth_dir, expected=2
        )
        self.assertFalse(fourth_record.exists())
        self.assertFalse(fourth_archive.exists())

    def test_development_identity_sidecars_are_not_source_inputs(self) -> None:
        first_record, _ = self.pack()
        first = json.loads(first_record.read_text(encoding="utf-8"))

        (self.source / "CONTRACT_VERSION").write_text(
            "0.4.0\n", encoding="utf-8"
        )
        (self.source / "SHA256SUMS").write_text(
            f"{'0' * 64}  libalopex_sql_parser.so\n", encoding="utf-8"
        )
        second_record, _ = self.pack(output=self.root / "development-sidecars")
        second = json.loads(second_record.read_text(encoding="utf-8"))

        self.assertEqual(first["parser_source"], second["parser_source"])

    def test_nested_identity_named_file_remains_a_source_input(self) -> None:
        first_record, _ = self.pack()
        first = json.loads(first_record.read_text(encoding="utf-8"))

        nested = self.source / "docs"
        nested.mkdir()
        (nested / "CONTRACT_VERSION").write_text("source material\n", encoding="utf-8")
        second_record, _ = self.pack(output=self.root / "nested-identity-name")
        second = json.loads(second_record.read_text(encoding="utf-8"))

        self.assertNotEqual(first["parser_source"], second["parser_source"])

    def test_generated_package_metadata_file_order_is_not_an_identity_input(
        self,
    ) -> None:
        first_record, _ = self.pack()
        first = json.loads(first_record.read_text(encoding="utf-8"))

        metadata_path = self.npeg / "nimblemeta.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata["metaData"]["files"].reverse()
        canonical_json(metadata_path, metadata)

        second_record, _ = self.pack(output=self.root / "metadata-order")
        second = json.loads(second_record.read_text(encoding="utf-8"))
        self.assertEqual(first["packages"], second["packages"])

    def test_generated_package_metadata_path_separator_is_not_an_identity_input(
        self,
    ) -> None:
        first_record, _ = self.pack()
        first = json.loads(first_record.read_text(encoding="utf-8"))

        metadata_path = self.npeg / "nimblemeta.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata["metaData"]["files"] = [
            path.replace("/", "\\") for path in metadata["metaData"]["files"]
        ]
        canonical_json(metadata_path, metadata)

        second_record, _ = self.pack(output=self.root / "metadata-separator")
        second = json.loads(second_record.read_text(encoding="utf-8"))
        self.assertEqual(first["packages"], second["packages"])

    def test_generated_package_metadata_semantics_are_validated(self) -> None:
        metadata_path = self.npeg / "nimblemeta.json"
        original = json.loads(metadata_path.read_text(encoding="utf-8"))

        missing_file = json.loads(json.dumps(original))
        missing_file["metaData"]["files"].pop()
        canonical_json(metadata_path, missing_file)
        record, archive = self.pack(
            output=self.root / "missing-metadata-file", expected=2
        )
        self.assertFalse(record.exists())
        self.assertFalse(archive.exists())

        wrong_revision = json.loads(json.dumps(original))
        wrong_revision["metaData"]["vcsRevision"] = "0" * 40
        canonical_json(metadata_path, wrong_revision)
        record, archive = self.pack(
            output=self.root / "wrong-metadata-revision", expected=2
        )
        self.assertFalse(record.exists())
        self.assertFalse(archive.exists())

        unsafe_path = json.loads(json.dumps(original))
        unsafe_path["metaData"]["files"][0] = "/../outside.nim"
        canonical_json(metadata_path, unsafe_path)
        record, archive = self.pack(
            output=self.root / "unsafe-metadata-path", expected=2
        )
        self.assertFalse(record.exists())
        self.assertFalse(archive.exists())

    def test_pack_rejects_contract_drift_and_symlinked_source(self) -> None:
        (self.source / "PARSER_CONTRACT_VERSION").write_text(
            "0.3.0\n", encoding="utf-8"
        )
        self.pack(expected=2)
        (self.source / "PARSER_CONTRACT_VERSION").write_text(
            "0.4.0\n", encoding="utf-8"
        )
        os.symlink(
            self.source / "src/alopex_sql_parser.nim",
            self.source / "src/linked.nim",
        )
        self.pack(expected=2)

    def test_verify_rejects_outer_internal_and_unsafe_archive_changes(self) -> None:
        record_path, archive_path = self.pack()
        original_archive = archive_path.read_bytes()
        archive_path.write_bytes(original_archive + b"tamper")
        self.verify_target(record_path, expected=2)

        record = json.loads(record_path.read_text(encoding="utf-8"))
        library_path = record["library"]["path"]
        identity_path = record["build_identity"]["path"]
        with tarfile.open(fileobj=io.BytesIO(original_archive), mode="r:gz") as archive:
            identity = archive.extractfile(identity_path).read()
            library = archive.extractfile(library_path).read()
        normalized_archive(
            archive_path,
            [(identity_path, identity), (library_path, b"different-library")],
        )
        record["archive"]["size"] = archive_path.stat().st_size
        record["archive"]["sha256"] = sha256(archive_path)
        canonical_json(record_path, record)
        self.verify_target(record_path, expected=2)

        normalized_archive(
            archive_path,
            [("../outside", b"bad"), (identity_path, identity)],
        )
        record["archive"]["size"] = archive_path.stat().st_size
        record["archive"]["sha256"] = sha256(archive_path)
        canonical_json(record_path, record)
        result = self.verify_target(record_path, expected=2)
        self.assertIn("unsafe", result.stderr.lower())

        normalized_archive(
            archive_path,
            [(identity_path, identity), (identity_path, identity)],
        )
        record["archive"]["size"] = archive_path.stat().st_size
        record["archive"]["sha256"] = sha256(archive_path)
        canonical_json(record_path, record)
        duplicate = self.verify_target(record_path, expected=2)
        self.assertIn("duplicate", duplicate.stderr.lower())

        normalized_archive(
            archive_path,
            [(library_path, library), (identity_path, identity)],
        )
        record["archive"]["size"] = archive_path.stat().st_size
        record["archive"]["sha256"] = sha256(archive_path)
        canonical_json(record_path, record)
        noncanonical = self.verify_target(record_path, expected=2)
        self.assertIn("canonical", noncanonical.stderr.lower())

    def test_verify_bounds_archive_decompression(self) -> None:
        record_path, archive_path = self.pack()
        with mock.patch.object(
            MANIFEST.gzip,
            "decompress",
            side_effect=AssertionError("unbounded gzip API used"),
        ):
            self.verify_target(record_path)

        oversized_tar = b"\0" * (16 * 1024 * 1024 + 1)
        with archive_path.open("wb") as raw:
            with gzip.GzipFile(
                filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=0
            ) as compressed:
                compressed.write(oversized_tar)
        record = json.loads(record_path.read_text(encoding="utf-8"))
        record["archive"]["size"] = archive_path.stat().st_size
        record["archive"]["sha256"] = sha256(archive_path)
        canonical_json(record_path, record)
        result = self.verify_target(record_path, expected=2)
        self.assertIn("decompressed size limit", result.stderr.lower())

    def test_assemble_rejects_duplicate_and_inconsistent_records(self) -> None:
        linux_record, _ = self.pack(target="x86_64-unknown-linux-gnu")
        duplicate_result = self.run_cli(
            "assemble-manifest",
            "--record",
            str(linux_record),
            "--record",
            str(linux_record),
            "--asset-dir",
            str(self.output),
            "--output",
            str(self.root / "manifest.json"),
            expected=2,
        )
        self.assertIn("duplicate", duplicate_result.stderr.lower())

        (self.source / "src/alopex_sql_parser.nim").write_bytes(
            b"different-reviewed-source"
        )
        mac_record, _ = self.pack(
            target="x86_64-apple-darwin", output=self.output
        )
        inconsistent = self.run_cli(
            "assemble-manifest",
            "--record",
            str(linux_record),
            "--record",
            str(mac_record),
            "--asset-dir",
            str(self.output),
            "--output",
            str(self.root / "manifest.json"),
            expected=2,
        )
        self.assertIn("identity", inconsistent.stderr.lower())

    def test_manifest_regeneration_and_verification_are_canonical(self) -> None:
        records = [
            self.pack(target=target, output=self.output)[0]
            for target in TARGET_LIBRARIES
        ]
        manifest = self.root / "vendor.json"
        arguments: list[str] = [
            "assemble-manifest",
        ]
        for record in reversed(records):
            arguments.extend(("--record", str(record)))
        arguments.extend(
            (
                "--asset-dir",
                str(self.output),
                "--output",
                str(manifest),
            )
        )
        self.run_cli(*arguments)
        first = manifest.read_bytes()
        self.run_cli(*arguments)
        self.assertEqual(manifest.read_bytes(), first)
        self.run_cli(
            "verify-manifest",
            "--manifest",
            str(manifest),
            "--asset-dir",
            str(self.output),
        )
        parsed = json.loads(manifest.read_text(encoding="utf-8"))
        self.assertNotIn("commit", json.dumps(parsed).lower())
        self.assertEqual(
            [asset["target"] for asset in parsed["assets"]],
            sorted(TARGET_LIBRARIES),
        )

    def test_manifest_rejects_an_incomplete_target_matrix(self) -> None:
        linux_record, _ = self.pack(target="x86_64-unknown-linux-gnu")
        manifest = self.root / "incomplete-vendor.json"
        result = self.run_cli(
            "assemble-manifest",
            "--record",
            str(linux_record),
            "--asset-dir",
            str(self.output),
            "--output",
            str(manifest),
            expected=2,
        )
        self.assertIn("target matrix", result.stderr.lower())
        self.assertFalse(manifest.exists())

    def test_release_envelope_is_canonical_and_binds_tag(self) -> None:
        records = [
            self.pack(target=target, output=self.output)[0]
            for target in TARGET_LIBRARIES
        ]
        manifest = self.root / "vendor.json"
        arguments = ["assemble-manifest"]
        for record in records:
            arguments.extend(("--record", str(record)))
        arguments.extend(("--asset-dir", str(self.output), "--output", str(manifest)))
        self.run_cli(*arguments)
        envelope = self.root / "parser-assets.json"
        self.run_cli(
            "release-envelope",
            "--manifest",
            str(manifest),
            "--asset-dir",
            str(self.output),
            "--tag",
            "v0.8.4",
            "--tag-sha",
            "0123456789abcdef0123456789abcdef01234567",
            "--output",
            str(envelope),
        )
        parsed = json.loads(envelope.read_text(encoding="utf-8"))
        self.assertEqual(parsed["schema"], MANIFEST.RELEASE_ENVELOPE_SCHEMA)
        self.assertEqual(parsed["source"]["tag"], "v0.8.4")
        self.assertEqual(len(parsed["assets"]), 4)
        self.assertEqual(envelope.read_bytes(), MANIFEST.canonical_json_bytes(parsed))

    def test_build_script_names_exact_builder_and_archive_contract(self) -> None:
        text = BUILD_SCRIPT.read_text(encoding="utf-8")
        self.assertIn('REQUIRED_NIM_VERSION="2.2.10"', text)
        self.assertIn('REQUIRED_NIMBLE_VERSION="0.22.3"', text)
        self.assertIn(f'REQUIRED_BUILD_PROFILE="{BUILD_PROFILE}"', text)
        self.assertIn(NIMBLE_SHA, text)
        self.assertIn("--archive-dir", text)
        self.assertIn("pack-target", text)
        self.assertNotIn("sort -V", text)

    def test_build_script_uses_canonical_paths_and_preflights_inputs(self) -> None:
        text = BUILD_SCRIPT.read_text(encoding="utf-8")
        self.assertIn(
            'build_root="/tmp/alopex-nim-parser-v${REQUIRED_ALOPEX_VERSION}-'
            'contract-${REQUIRED_CONTRACT_VERSION}-${HOST_TARGET}"',
            text,
        )
        self.assertNotIn("mktemp -d", text)
        self.assertIn(
            'npeg_source_arg="$(to_native_path "${npeg_resolved}")"', text
        )
        self.assertIn(
            'msgpack_source_arg="$(to_native_path "${msgpack_resolved}")"',
            text,
        )
        self.assertNotIn('${npeg_resolved}/src', text)
        self.assertLess(text.index("verify-inputs"), text.index('"${NIM_BIN}" c'))

    def test_build_script_rejects_nimble_0_22_2_before_target_bytes(self) -> None:
        library = PARSER_DIR / "libalopex_sql_parser.so"
        self.addCleanup(library.unlink, missing_ok=True)
        destination = self.root / "wrong-nimble"
        result = self.run_build_script(
            self.build_script_fixture(nimble_version="0.22.2"), destination
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("Nimble 0.22.3 is required", result.stderr)
        self.assertFalse(destination.exists())
        self.assertFalse(library.exists())

    def test_build_script_rejects_archive_on_non_host_backend(self) -> None:
        result = subprocess.run(
            [
                "bash",
                str(BUILD_SCRIPT),
                "--backend",
                "docker",
                "--target",
                "x86_64-unknown-linux-gnu",
                "--archive-dir",
                str(self.output),
            ],
            cwd=REPOSITORY_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("host", result.stderr.lower())


class ReleaseWorkflowContractTests(unittest.TestCase):
    """Keep publication evidence tied to the peeled tag and staged sources."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")

    def test_publish_does_not_create_a_synthetic_git_commit(self) -> None:
        self.assertNotIn("git add \"${NIM_SQL_PARSER_DIR}/vendor\"", self.workflow)
        self.assertNotIn("Commit vendored libraries locally", self.workflow)
        self.assertNotIn("--allow-dirty", self.workflow)
        self.assertIn("git archive", self.workflow)

    def test_release_build_verifies_archive_and_native_smoke_before_upload(self) -> None:
        verify = self.workflow.index("verify-target")
        upload = self.workflow.index("uses: actions/upload-artifact@v4")
        self.assertLess(verify, upload)
        self.assertIn("native smoke", self.workflow.lower())
        self.assertIn("parser-assets-v0.8.5.json", self.workflow)

    def test_release_envelope_binds_peeled_tag_sha_and_manifest(self) -> None:
        self.assertIn("git rev-parse", self.workflow)
        self.assertIn("git describe --tags --exact-match", self.workflow)
        self.assertIn("release-envelope", self.workflow)

    def test_python_release_consumes_public_core_assets_without_nim_rebuild(self) -> None:
        workflow = PY_RELEASE_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn('CORE_TAG: v0.8.5', workflow)
        self.assertIn("seq 1 30", workflow)
        self.assertIn("parser-assets-v0.8.5.json", workflow)
        self.assertIn("parser_asset_manifest.py verify-manifest", workflow)
        self.assertNotIn("setup-nim-action", workflow)
        self.assertNotIn("nimble lib", workflow)
        self.assertNotIn("nim-${NIM_VERSION}", workflow)


if __name__ == "__main__":
    unittest.main()
