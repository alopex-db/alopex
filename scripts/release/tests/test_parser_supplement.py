import argparse
import copy
import hashlib
from pathlib import Path
import unittest
from unittest.mock import patch

from scripts.release import parser_supplement as supplement
from scripts.release.tests import test_parser_asset_manifest as fixtures


class ParserSupplementTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ParserAssetManifestTests(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.module_patch = patch.object(supplement, "assets", fixtures.MANIFEST)
        self.module_patch.start()
        self.addCleanup(self.module_patch.stop)
        self.root = self.fixture.root
        self.primary = self.root / "primary.json"
        args = ["assemble-manifest"]
        for target in sorted(fixtures.PRIMARY_TARGETS):
            args.extend(("--record", str(self.fixture.pack(target=target)[0])))
        args.extend(("--asset-dir", str(self.fixture.output), "--output", str(self.primary)))
        self.fixture.run_cli(*args)
        self.envelope = self.root / "envelope.json"
        self.fixture.run_cli("release-envelope", "--manifest", str(self.primary),
                             "--asset-dir", str(self.fixture.output),
                             "--tag", f"v{fixtures.MANIFEST.REQUIRED_ALOPEX_VERSION}",
                             "--tag-sha", "a" * 40, "--output", str(self.envelope))
        self.record, self.archive = self.fixture.pack(target="x86_64-apple-darwin")
        self.output = self.root / supplement.FILENAME
        self.destination = self.root / "installed"
        self.create_args = argparse.Namespace(
            primary=self.primary, primary_sha256=self.digest(self.primary),
            envelope=self.envelope, envelope_sha256=self.digest(self.envelope),
            record=self.record, archive=self.archive, output=self.output,
        )
        supplement.create(self.create_args)
        self.install_args = argparse.Namespace(
            primary=self.primary, primary_sha256=self.digest(self.primary),
            supplement=self.output, supplement_sha256=self.digest(self.output),
            archive=self.archive, destination=self.destination,
        )

    @staticmethod
    def digest(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def rewrite(self, path, value):
        path.write_bytes(fixtures.MANIFEST.canonical_json_bytes(value))

    def payload(self, path):
        return fixtures.MANIFEST.load_canonical_json(path, "test")

    def test_install_preserves_primary_and_exact_asset_bytes(self):
        before = self.primary.read_bytes()
        supplement.install(self.install_args)
        record = self.payload(self.record)
        self.assertEqual(before, self.primary.read_bytes())
        self.assertEqual(self.output.read_bytes(), (self.destination / supplement.FILENAME).read_bytes())
        self.assertEqual({p.name for p in self.destination.iterdir()}, {
            "libalopex_sql_parser.dylib", "libalopex_sql_parser.a", "BUILD_IDENTITY.json",
            "CONTRACT_VERSION", "SHA256SUMS", "PARSER_SUPPLEMENT.json",
        })
        for field in ("library", "static_library", "build_identity"):
            path = self.destination / Path(record[field]["path"]).name
            self.assertEqual(record[field]["sha256"], self.digest(path))
            self.assertEqual(record[field]["size"], path.stat().st_size)
        self.assertEqual("0.28.0\n", (self.destination / "CONTRACT_VERSION").read_text())
        expected = "".join(f"{record[f]['sha256']}  {Path(record[f]['path']).name}\n"
                           for f in ("library", "static_library"))
        self.assertEqual(expected, (self.destination / "SHA256SUMS").read_text())

    def test_rejects_each_missing_or_wrong_pin_before_install(self):
        for field in ("primary_sha256", "supplement_sha256"):
            for pin in ("", "0" * 64, "not-sha256"):
                with self.subTest(field=field, pin=pin):
                    args = copy.copy(self.install_args)
                    setattr(args, field, pin)
                    with self.assertRaises(fixtures.MANIFEST.ParserAssetError):
                        supplement.install(args)
                    self.assertFalse(self.destination.exists())

    def test_rejects_supplement_identity_changes_even_when_repinned(self):
        original = self.payload(self.output)
        changes = [
            (("schema",), "unknown"),
            (("primary_manifest_sha256",), "0" * 64),
            (("source", "tag"), "v0.8.17-rc.1"),
            (("source", "tag_sha"), "invalid"),
            (("record", "target"), "x86_64-unknown-linux-gnu"),
            (("record", "alopex_version"), "0.0.0"),
            (("record", "contract_version"), "0.0.0"),
            (("record", "library", "path"), "../escape"),
            (("record", "parser_source", "tree_sha256"), "0" * 64),
        ]
        for keys, value in changes:
            with self.subTest(keys=keys):
                changed = copy.deepcopy(original)
                node = changed
                for key in keys[:-1]:
                    node = node[key]
                node[keys[-1]] = value
                self.rewrite(self.output, changed)
                self.install_args.supplement_sha256 = self.digest(self.output)
                with self.assertRaises(fixtures.MANIFEST.ParserAssetError):
                    supplement.install(self.install_args)
                self.assertFalse(self.destination.exists())

    def test_rejects_changed_archive_and_leaves_no_installation(self):
        self.archive.write_bytes(b"broken")
        with self.assertRaises(fixtures.MANIFEST.ParserAssetError):
            supplement.install(self.install_args)
        self.assertFalse(self.destination.exists())
        self.assertEqual([], list(self.root.glob("alopex-parser-install-*")))

    def test_rejects_valid_foreign_source_archive_only_on_primary_input_binding(self):
        # Keep the entire record/archive internally valid. Changing just the
        # declaration would be rejected by archive identity even if the primary
        # comparison were accidentally removed.
        source = self.fixture.source / "src/alopex_sql_parser.nim"
        original_source = source.read_bytes()
        original = self.payload(self.output)
        for index, suffix in enumerate((b"\n# a\n", b"\n# b\n", b"\n# c\n")):
            with self.subTest(suffix=suffix):
                source.write_bytes(original_source + suffix)
                record_path, archive = self.fixture.pack(
                    target="x86_64-apple-darwin", output=self.root / f"foreign-{index}"
                )
                record = self.payload(record_path)
                fixtures.MANIFEST.verify_record_archive(record, archive)
                self.assertNotEqual(original["record"]["parser_source"], record["parser_source"])
                changed = copy.deepcopy(original)
                changed["record"] = record
                self.rewrite(self.output, changed)
                self.install_args.supplement_sha256 = self.digest(self.output)
                self.install_args.archive = archive
                with self.assertRaisesRegex(fixtures.MANIFEST.ParserAssetError, "inputs differ"):
                    supplement.install(self.install_args)
                self.assertFalse(self.destination.exists())

    def test_rejects_symlink_and_oversized_supplement(self):
        alias = self.root / "alias.json"
        try:
            alias.symlink_to(self.output)
        except OSError as error:
            self.skipTest(f"host cannot create symlink: {error}")
        self.install_args.supplement = alias
        with self.assertRaises(fixtures.MANIFEST.ParserAssetError):
            supplement.install(self.install_args)
        alias.unlink()
        alias.write_bytes(b" " * (fixtures.MANIFEST.MAX_CANONICAL_JSON_BYTES + 1))
        self.install_args.supplement_sha256 = self.digest(alias)
        with self.assertRaises(fixtures.MANIFEST.ParserAssetError):
            supplement.install(self.install_args)
        self.assertFalse(self.destination.exists())

    def test_installs_the_verified_archive_snapshot_when_input_changes(self):
        verify = fixtures.MANIFEST.verify_record_archive

        def change_original_after_verification(record, snapshot):
            verify(record, snapshot)
            self.archive.write_bytes(b"changed concurrently")

        with patch.object(fixtures.MANIFEST, "verify_record_archive", change_original_after_verification):
            supplement.install(self.install_args)
        record = self.payload(self.record)
        for field in ("library", "static_library"):
            path = self.destination / Path(record[field]["path"]).name
            self.assertEqual(record[field]["sha256"], self.digest(path))

    def test_never_overwrites_existing_destination_or_supplement(self):
        supplement.install(self.install_args)
        before = {p.name: p.read_bytes() for p in self.destination.iterdir()}
        with self.assertRaisesRegex(fixtures.MANIFEST.ParserAssetError, "already exists"):
            supplement.install(self.install_args)
        with self.assertRaises(FileExistsError):
            supplement.create(self.create_args)
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.destination.iterdir()})

    def test_failed_copy_removes_only_owned_destination(self):
        with patch.object(supplement.shutil, "copyfile", side_effect=OSError("disk full")):
            with self.assertRaisesRegex(OSError, "disk full"):
                supplement.install(self.install_args)
        self.assertFalse(self.destination.exists())
        self.assertTrue(self.primary.exists())
        self.assertTrue(self.archive.exists())

    def test_rejects_envelope_pin_and_binding_changes(self):
        self.output.unlink()
        args = copy.copy(self.create_args)
        args.envelope_sha256 = "0" * 64
        with self.assertRaisesRegex(fixtures.MANIFEST.ParserAssetError, "SHA-256 mismatch"):
            supplement.create(args)
        value = self.payload(self.envelope)
        value["manifest"]["sha256"] = "0" * 64
        self.rewrite(self.envelope, value)
        args.envelope_sha256 = self.digest(self.envelope)
        with self.assertRaisesRegex(fixtures.MANIFEST.ParserAssetError, "does not bind"):
            supplement.create(args)
        self.assertFalse(self.output.exists())

    def test_rejects_a_supplement_for_an_existing_primary_asset(self):
        value = self.payload(self.primary)
        value["assets"].append(fixtures.MANIFEST.manifest_asset(self.payload(self.record)))
        value["assets"].sort(key=lambda a: a["target"])
        self.rewrite(self.primary, value)
        extra = self.payload(self.output)
        extra["primary_manifest_sha256"] = self.digest(self.primary)
        self.rewrite(self.output, extra)
        self.install_args.primary_sha256 = self.digest(self.primary)
        self.install_args.supplement_sha256 = self.digest(self.output)
        with self.assertRaisesRegex(fixtures.MANIFEST.ParserAssetError, "cannot replace"):
            supplement.install(self.install_args)


if __name__ == "__main__":
    unittest.main()
