"""One verified parser archive becomes the shared private build input."""

import hashlib
import json
from pathlib import Path
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_parser_stage as stage
from scripts.release.tests import test_parser_asset_manifest as fixture_tools


class HostedParserStageTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixture_tools.ParserAssetManifestTests(
            "test_pack_rejects_wrong_library_name_and_target")
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.record_path, self.archive_path = self.fixture.pack()
        self.record_path.parent.chmod(0o700)
        self.destination = self.fixture.root / "stage-output"
        self.destination.mkdir(mode=0o700)
        for name in ("REQUIRED_PACKAGES", "REQUIRED_REGISTRY_METADATA"):
            patcher = mock.patch.object(
                stage.manifest, name, getattr(fixture_tools.MANIFEST, name))
            patcher.start()
            self.addCleanup(patcher.stop)

    def stage(self, guard=lambda: None):
        return stage.stage_parser(self.record_path.parent, self.destination, guard)

    def assert_rejected_without_output(self):
        with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
            self.stage()
        self.assertFalse((self.destination / stage.ROOT_NAME).exists())
        self.assertEqual(list(self.destination.glob(".parser-input.*")), [])

    def rewrite_archive(self, members):
        content = stage.manifest.normalized_archive_bytes(members)
        self.archive_path.write_bytes(content)
        record = json.loads(self.record_path.read_bytes())
        record["archive"].update(
            size=len(content), sha256=hashlib.sha256(content).hexdigest())
        self.record_path.write_bytes(
            (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode())

    def test_valid_archive_passes_actual_build_consumer(self):
        calls = []
        result = self.stage(lambda: calls.append(1))
        self.assertEqual(result["parser_record"], self.record_path)
        self.assertEqual(result["parser_library"].read_bytes(),
                         b"native-library:x86_64-unknown-linux-gnu")
        self.assertEqual(stage.producer.verify_parser(
            result["parser_record"], result["parser_root"],
            result["parser_library"]), result["parser_record_sha256"])
        self.assertGreater(len(calls), 4)

    def test_changed_record_and_archive_are_rejected(self):
        original = self.record_path.read_bytes()
        self.record_path.write_bytes(original + b" ")
        self.assert_rejected_without_output()
        self.record_path.write_bytes(original)
        self.archive_path.write_bytes(self.archive_path.read_bytes() + b"x")
        self.assert_rejected_without_output()

    def test_changed_member_and_extra_member_are_rejected(self):
        original = self.archive_path.read_bytes()
        members = stage.manifest.read_normalized_archive(original)
        library = next(name for name in members if name.endswith(".so"))
        members[library] += b"changed"
        self.rewrite_archive(members)
        self.assert_rejected_without_output()
        members = stage.manifest.read_normalized_archive(original)
        members["extra.txt"] = b"extra"
        self.rewrite_archive(members)
        self.assert_rejected_without_output()

    def test_sidecar_mismatch_and_guard_interruption_clean_partial(self):
        verify = stage.producer.verify_parser

        def alter_sidecar(record_path, root, library):
            (library.parent / "CONTRACT_VERSION").write_bytes(b"changed\n")
            return verify(record_path, root, library)

        with mock.patch.object(stage.producer, "verify_parser",
                               side_effect=alter_sidecar):
            self.assert_rejected_without_output()
        calls = []

        def fail_after_write():
            calls.append(1)
            if len(calls) == 5:
                raise RuntimeError("private storage guard failed")

        with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
            self.stage(fail_after_write)
        self.assertFalse((self.destination / stage.ROOT_NAME).exists())
        self.assertEqual(list(self.destination.glob(".parser-input.*")), [])

    def test_record_swap_before_and_after_promotion_is_rejected(self):
        verify = stage.producer.verify_parser

        def replace_record(record_path):
            record = json.loads(record_path.read_bytes())
            record["unverified_provenance"] = "changed"
            record_path.write_bytes(
                (json.dumps(record, sort_keys=True, separators=(",", ":"))
                 + "\n").encode())

        def swap_before(record_path, root, library):
            replace_record(record_path)
            return verify(record_path, root, library)

        original = self.record_path.read_bytes()
        with mock.patch.object(stage.producer, "verify_parser",
                               side_effect=swap_before):
            self.assert_rejected_without_output()
        self.record_path.write_bytes(original)
        calls = []

        def swap_after(record_path, root, library):
            calls.append(1)
            if len(calls) == 2:
                replace_record(record_path)
            return verify(record_path, root, library)

        with mock.patch.object(stage.producer, "verify_parser",
                               side_effect=swap_after):
            self.assert_rejected_without_output()

    def test_post_promotion_guard_failure_removes_owned_output(self):
        promoted = []
        original = stage.nim_archive._promote_noreplace

        def promote(partial, output):
            original(partial, output)
            promoted.append(1)

        def guard():
            if promoted:
                raise RuntimeError("private host guard failed")

        with mock.patch.object(stage.nim_archive, "_promote_noreplace",
                               side_effect=promote):
            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                self.stage(guard)
        self.assertEqual(len(promoted), 1)
        self.assertFalse((self.destination / stage.ROOT_NAME).exists())
        self.assertEqual(list(self.destination.glob(".parser-input.*")), [])

    def test_manifest_reader_rejects_file_growth_after_stat(self):
        source = self.archive_path
        original_fstat = stage.manifest.os.fstat

        def grow_after_stat(fd):
            info = original_fstat(fd)
            with source.open("ab") as stream:
                stream.write(b"x" * 256)
            return info

        with mock.patch.object(stage.manifest.os, "fstat",
                               side_effect=grow_after_stat):
            with self.assertRaises(stage.manifest.ParserAssetError):
                stage.manifest.read_regular_file(
                    source, "archive", max_bytes=source.stat().st_size)

    def test_partial_replacement_is_not_promoted_or_removed(self):
        replaced = []

        def guard():
            if replaced:
                return
            partials = list(self.destination.glob(".parser-input.*"))
            if not partials:
                return
            partial = partials[0]
            original = self.fixture.root / "original-partial"
            partial.rename(original)
            partial.mkdir(mode=0o700)
            (partial / "competitor").write_bytes(b"keep")
            replaced.append(partial)

        with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
            self.stage(guard)
        self.assertEqual(len(replaced), 1)
        self.assertEqual((replaced[0] / "competitor").read_bytes(), b"keep")
        self.assertFalse((self.destination / stage.ROOT_NAME).exists())

    def test_existing_symlink_and_promotion_race_preserve_other_output(self):
        output = self.destination / stage.ROOT_NAME
        output.symlink_to(self.fixture.root / "missing")
        self.assert_rejected_without_output()
        self.assertTrue(output.is_symlink())
        output.unlink()
        promote = stage.nim_archive._promote_noreplace

        def compete(partial, target):
            target.mkdir(mode=0o700)
            (target / "competitor").write_bytes(b"keep")
            return promote(partial, target)

        with mock.patch.object(stage.nim_archive, "_promote_noreplace",
                               side_effect=compete):
            with self.assertRaisesRegex(ValueError, "^PUBLIC_PARSER_INPUT$"):
                self.stage()
        self.assertEqual((output / "competitor").read_bytes(), b"keep")
        self.assertEqual(list(self.destination.glob(".parser-input.*")), [])


if __name__ == "__main__":
    unittest.main()
