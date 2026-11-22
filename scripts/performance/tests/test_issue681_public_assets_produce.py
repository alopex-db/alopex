"""Exact-source and parser admission before a hosted public asset build."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_public_assets_produce as produce


def sha(data):
    return hashlib.sha256(data).hexdigest()


class SourceBuildAdmissionTests(unittest.TestCase):
    def test_exact_clean_source_and_changed_example(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source"
            source.mkdir(mode=0o700)
            for name, data in (
                (produce.EXAMPLE, b"fn main() {}\n"),
                (produce.CONTRACT, b"0.28.0\n"),
                ("Cargo.lock", b"locked\n"),
                ("Cargo.toml", b"[workspace.package]\nversion = '0.8.17'\n"),
            ):
                path = source / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
            def git(*args):
                return subprocess.check_output(
                    ["git", *args], cwd=source, stderr=subprocess.DEVNULL).decode().strip()
            git("init", "-q")
            git("add", ".")
            git("-c", "user.name=test", "-c", "user.email=test@example.invalid",
                "commit", "-qm", "source")
            commit, tree = git("rev-parse", "HEAD"), git("rev-parse", "HEAD^{tree}")
            produce.verify_source(source, commit, tree,
                                  sha(b"fn main() {}\n"), sha(b"locked\n"))
            (source / produce.EXAMPLE).write_text("fn main() { panic!() }\n")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                produce.verify_source(source, commit, tree,
                                      sha(b"fn main() {}\n"), sha(b"locked\n"))

    def test_parser_sidecars_must_match_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            parser = root / "parser"
            parser.mkdir(mode=0o700)
            members = {"library": ("lib/parser.so", b"library"),
                       "static_library": ("lib/parser.a", b"static"),
                       "build_identity": ("meta/identity.json", b"identity")}
            record = {"contract_version": "0.28.0",
                      "target": produce.HOST}
            for role, (name, data) in members.items():
                path = parser / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
                record[role] = {"path": name, "size": len(data), "sha256": sha(data)}
            (parser / "lib/CONTRACT_VERSION").write_bytes(b"0.28.0\n")
            (parser / "lib/SHA256SUMS").write_text("".join(
                record[role]["sha256"] + "  " + Path(record[role]["path"]).name + "\n"
                for role in ("library", "static_library")))
            record_path = root / "record.json"
            raw = json.dumps(record).encode()
            record_path.write_bytes(raw)
            library = parser / "lib/parser.so"
            self.assertEqual(produce.verify_parser(record_path, parser, library),
                             sha(raw))
            (parser / "lib/parser.a").write_bytes(b"wrong")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_BUILD$"):
                produce.verify_parser(record_path, parser, library)


if __name__ == "__main__":
    unittest.main()
