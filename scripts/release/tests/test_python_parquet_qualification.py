"""Qualification failure contracts with tiny files; no product or dependency build."""
from pathlib import Path
import json
import sys
import tempfile
from types import ModuleType
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET
import zipfile

from scripts.release import verify_python_parquet as qualify


class QualificationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="alopex-parquet-qualification-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()

    def wheel(self):
        prefix = self.root / "venv"
        package = prefix / "site-packages/alopex/__init__.py"
        extension = package.with_name("_alopex.so")
        package.parent.mkdir(parents=True)
        package.write_bytes(b"package-fixture")
        extension.write_bytes(b"extension-fixture")
        wheel = self.root / "fixture.whl"
        with zipfile.ZipFile(wheel, "w") as archive:
            for file in (package, extension):
                archive.writestr("alopex/" + file.name, file.read_bytes())
        return wheel, package, extension, prefix

    def junit(self):
        root = ET.Element("testsuites")
        suite = ET.SubElement(root, "testsuite")
        for name, count in qualify.EXPECTED_CASES.items():
            for index in range(count):
                ET.SubElement(suite, "testcase", name=f"{name}[{index}]")
        return root, suite

    def save(self, root):
        path = self.root / "cases.xml"
        ET.ElementTree(root).write(path)
        return path

    def test_both_loaded_modules_match_wheel_without_recording_local_paths(self):
        result = qualify.installed_identity(*self.wheel())
        self.assertEqual(set(result), {"package", "extension"})
        self.assertNotIn(str(self.root), str(result))

    def test_wrong_package_or_extension_bytes_are_rejected(self):
        args = self.wheel()
        for file in args[1:3]:
            original = file.read_bytes()
            file.write_bytes(b"other-installed-artifact")
            with self.assertRaisesRegex(RuntimeError, "differs"):
                qualify.installed_identity(*args)
            file.write_bytes(original)

    def test_matching_module_outside_owned_venv_is_rejected(self):
        wheel, package, extension, prefix = self.wheel()
        outside = self.root / "checkout/alopex/__init__.py"
        outside.parent.mkdir(parents=True)
        outside.write_bytes(package.read_bytes())
        with self.assertRaisesRegex(RuntimeError, "outside"):
            qualify.installed_identity(wheel, outside, extension, prefix)

    def test_missing_wheel_member_is_rejected(self):
        args = self.wheel()
        with zipfile.ZipFile(args[0], "w") as archive:
            archive.writestr("alopex/__init__.py", args[1].read_bytes())
        with self.assertRaisesRegex(RuntimeError, "missing"):
            qualify.installed_identity(*args)

    def test_all_34_public_cases_are_required(self):
        root, _ = self.junit()
        self.assertEqual(len(qualify.verify_cases(self.save(root))), 34)

    def test_skipped_failed_and_error_cases_never_qualify(self):
        for tag in ("skipped", "failure", "error"):
            root, suite = self.junit()
            ET.SubElement(suite[0], tag)
            with self.subTest(tag=tag), self.assertRaisesRegex(RuntimeError, "every case"):
                qualify.verify_cases(self.save(root))

    def test_missing_duplicate_or_replaced_case_never_qualifies(self):
        for change in ("missing", "duplicate", "replacement"):
            root, suite = self.junit()
            if change == "missing":
                suite.remove(suite[0])
            elif change == "duplicate":
                suite[1].set("name", suite[0].get("name"))
            else:
                suite[0].set("name", "test_unrelated[0]")
            with self.subTest(change=change), self.assertRaisesRegex(RuntimeError, "inventory"):
                qualify.verify_cases(self.save(root))

    def exercise_main(self, mutate_wheel):
        wheel, package, extension, prefix = self.wheel()
        workspace = self.root / "source"
        tests = workspace / "crates/alopex-py/tests"
        tests.mkdir(parents=True)
        for name in ("test_vector_parquet.py", "conftest.py"):
            (tests / name).write_text("# synthetic source fixture\n")
        product = ModuleType("alopex"); product.__file__ = str(package)
        native = ModuleType("alopex._alopex"); native.__file__ = str(extension)
        product._alopex = native
        pytest = ModuleType("pytest")
        def execute(args):
            junit = Path(args[args.index("--junitxml") + 1])
            root, _ = self.junit(); ET.ElementTree(root).write(junit)
            if mutate_wheel:
                with zipfile.ZipFile(wheel, "a") as archive:
                    archive.writestr("changed-metadata", b"changed after install")
            return 0
        pytest.main = execute
        output = self.root / "report.json"
        modules = {"alopex":product, "alopex._alopex":native, "pytest":pytest, "pyarrow":ModuleType("pyarrow")}
        with patch.dict(sys.modules, modules), patch.object(qualify.sys, "prefix", str(prefix)), \
             patch.object(qualify, "version", return_value="fixture"), \
             patch.object(qualify.subprocess, "check_output", return_value="a" * 40 + "\n"):
            code = qualify.main(["--wheel", str(wheel), "--workspace", str(workspace), "--report", str(output)])
        report = json.loads(output.read_text())
        self.assertNotIn(str(self.root), output.read_text())
        self.assertEqual(list(prefix.glob("vector-parquet-*")), [])
        return code, report

    def test_main_preserves_source_and_wheel_evidence_and_reclaims_scratch(self):
        code, report = self.exercise_main(False)
        self.assertEqual(code, 0)
        self.assertEqual(report["status"], "passed")
        self.assertEqual(len(report["cases"]), 34)

    def test_archive_change_outside_imported_modules_rejects_qualification(self):
        code, report = self.exercise_main(True)
        self.assertEqual(code, 1)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["error_code"], "VECTOR_PARQUET_QUALIFICATION")


if __name__ == "__main__":
    unittest.main()
