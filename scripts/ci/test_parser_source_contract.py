"""Exercise the real guard against tiny Git histories, without building Nim."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

GUARD = Path(__file__).with_name("check_parser_wire_contract.py").resolve()
PARSER = "crates/alopex-sql/nim-sql-parser"


class ParserSourceContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="alopex-wire-guard-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.git("init", "-q")
        self.version("0.28.0")
        for name in ("parser.nim", "lexer.nim"):
            self.write(f"{PARSER}/src/{name}", "# original\n")
        self.base = self.commit()

    def git(self, *args):
        return subprocess.check_output(
            ["git", "-c", "user.name=Contract Test", "-c",
             "user.email=contract@example.invalid", *args],
            cwd=self.root, env=dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull),
            stderr=subprocess.PIPE, text=True,
        ).strip()

    def write(self, path, text):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    def version(self, value):
        self.write(f"{PARSER}/PARSER_CONTRACT_VERSION", value + "\n")
        self.write("crates/alopex-sql/build_support.rs",
                   f'pub(crate) const REQUIRED_CONTRACT_VERSION: &str = "{value}";\n')
        self.write("scripts/release/parser_asset_manifest.py",
                   f'REQUIRED_CONTRACT_VERSION = "{value}"\n')

    def commit(self):
        self.git("add", ".")
        self.git("commit", "-qm", "fixture")
        return self.git("rev-parse", "HEAD")

    def check(self):
        return subprocess.run([sys.executable, str(GUARD), self.base, self.commit()],
                              cwd=self.root, text=True, capture_output=True)

    def test_parser_change_without_bump_is_rejected(self):
        self.write(f"{PARSER}/src/parser.nim", "# changed parser\n")
        result = self.check()
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("parser.nim", result.stderr)

    def test_lexer_change_without_bump_is_rejected(self):
        self.write(f"{PARSER}/src/lexer.nim", "# changed lexer\n")
        result = self.check()
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("lexer.nim", result.stderr)

    def test_source_deletion_without_bump_is_rejected(self):
        (self.root / f"{PARSER}/src/lexer.nim").unlink()
        self.assertEqual(self.check().returncode, 1)

    def test_source_change_with_coherent_bump_passes(self):
        self.write(f"{PARSER}/src/parser.nim", "# changed parser\n")
        self.version("0.29.0")
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_inconsistent_bump_is_rejected(self):
        self.write(f"{PARSER}/PARSER_CONTRACT_VERSION", "0.29.0\n")
        result = self.check()
        self.assertEqual(result.returncode, 1)
        self.assertIn("declarations disagree", result.stderr)

    def test_unrelated_change_does_not_require_bump(self):
        self.write("README.md", "documentation only\n")
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
