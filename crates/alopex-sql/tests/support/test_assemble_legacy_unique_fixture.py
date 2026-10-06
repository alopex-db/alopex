"""Assembler behavior only; synthetic JSON here is not engine fixture evidence."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("assemble_legacy_unique_fixture.py")
CASES = ("column", "named", "composite", "nullable", "duplicate",
         "named_duplicate", "composite_duplicate")
COMMIT = "d3917e1fa21d41097742a971e14094c0d6ae397e"


class AssembleBehavior(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="alopex-fixture-assembler-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.output = self.root / "fixture.json"
        self.manifest = self.root / "manifest.json"
        identity = dict(source_commit=COMMIT, parser_contract="0.25.0",
                        compiler="synthetic assembler test", commands=["<owned-temp>/producer"])
        for field in ("source_manifest_sha256", "producer_source_sha256", "producer_binary_sha256",
                      "dependencies_sha256", "parser_record_sha256"):
            identity[field] = "0" * 64
        (self.root / "identity.json").write_text(json.dumps(identity))
        for name in CASES:
            (self.root / f"{name}.json").write_text(json.dumps(dict(
                schema="alopex-sql-kv-fixture/v1", source_commit=COMMIT,
                parser_contract="0.25.0", case=name, sql="SELECT 1", entries=[["01", "abcd"]])))

    def run_assembler(self):
        return subprocess.run([sys.executable, str(SCRIPT), "--cases-dir", str(self.root),
                               "--identity", str(self.root / "identity.json"),
                               "--output", str(self.output), "--manifest", str(self.manifest)],
                              capture_output=True, text=True, timeout=5)

    def test_seven_cases_and_manifest_digest(self):
        result = self.run_assembler()
        self.assertEqual(result.returncode, 0, result.stderr)
        fixture = json.loads(self.output.read_bytes())
        manifest = json.loads(self.manifest.read_bytes())
        self.assertEqual([case["case"] for case in fixture["cases"]], list(CASES))
        self.assertEqual(fixture["cases"][0]["entries"], [["01", "abcd"]])
        self.assertEqual(manifest["fixture_sha256"], hashlib.sha256(self.output.read_bytes()).hexdigest())

    def test_missing_case_has_no_outputs(self):
        (self.root / "named.json").unlink()
        self.assertNotEqual(self.run_assembler().returncode, 0)
        self.assertFalse(self.output.exists())
        self.assertFalse(self.manifest.exists())

    def test_invalid_hex_has_no_outputs(self):
        path = self.root / "column.json"
        case = json.loads(path.read_text())
        case["entries"][0][1] = "not-hex"
        path.write_text(json.dumps(case))
        self.assertNotEqual(self.run_assembler().returncode, 0)
        self.assertFalse(self.output.exists())
        self.assertFalse(self.manifest.exists())

    def test_existing_output_is_unchanged(self):
        self.output.write_bytes(b"owned existing evidence")
        self.assertNotEqual(self.run_assembler().returncode, 0)
        self.assertEqual(self.output.read_bytes(), b"owned existing evidence")
        self.assertFalse(self.manifest.exists())

    def test_partial_pair_refuses_retry_and_fresh_pair_succeeds(self):
        self.manifest = self.root / "missing-parent" / "manifest.json"
        self.assertNotEqual(self.run_assembler().returncode, 0)
        partial = self.output.read_bytes()
        self.assertFalse(self.manifest.exists())
        self.manifest = self.root / "manifest.json"
        self.assertNotEqual(self.run_assembler().returncode, 0)
        self.assertEqual(self.output.read_bytes(), partial)
        self.output = self.root / "fresh-fixture.json"
        self.assertEqual(self.run_assembler().returncode, 0)


if __name__ == "__main__":
    unittest.main()
