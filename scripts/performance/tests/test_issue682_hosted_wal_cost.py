"""Unit, contract, negative, and child-protocol tests for #682 hosted WAL cost."""

from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest

PERF_DIR = Path(__file__).resolve().parents[1]
if str(PERF_DIR) not in sys.path:
    sys.path.insert(0, str(PERF_DIR))

import issue682_hosted_wal_cost as mod


def _sample_rows(*, after_commit_ns: int = 100, before_commit_ns: int = 100):
    rows = []
    for engine in mod.ARMS:
        for surface, size in mod.SHAPES:
            for round_id in range(mod.ROUNDS):
                commit_ns = (
                    9_999
                    if round_id == 0
                    else (after_commit_ns if engine == "candidate" else before_commit_ns)
                )
                row = {m: 100 for m in mod.METRICS}
                row.update(
                    engine=engine,
                    surface=surface,
                    value_bytes=size,
                    round=round_id,
                    commit_ns=commit_ns,
                    status="passed",
                    reopen_cycles=2,
                    reopen_ns=[20, 30],
                )
                rows.append(row)
    return rows


def _sample_report(*, after_commit_ns: int = 100, before_commit_ns: int = 100):
    summary = mod.summarize(
        _sample_rows(after_commit_ns=after_commit_ns, before_commit_ns=before_commit_ns)
    )
    pins = {
        arm: {
            "source_commit": mod.PINS[arm]["commit"],
            "source_tree": mod.PINS[arm]["tree"],
            "cargo_lock_sha256": mod.PINS[arm]["lock"],
            "parser_sha256": "1" * 64,
            "parser_record_sha256": "2" * 64,
            "native_sha256": ("a" if arm == "before" else "b") * 64,
            "python_tree_sha256": ("c" if arm == "before" else "d") * 64,
            "profile": "release",
            "ordinary_extension": True,
        }
        for arm in mod.ARMS
    }
    return {
        "schema": mod.SCHEMA,
        "status": "measured",
        "boundary": mod.BOUNDARY_TAG,
        "profile": "release",
        "platform": mod.TARGET,
        "rust_version": mod.RUST_VERSION,
        "parser_contract": mod.PARSER_CONTRACT_VERSION,
        "options": mod.OPTIONS,
        "pins": pins,
        **summary,
    }


class Issue682HostedWalCostTests(unittest.TestCase):
    def test_literal_pins_and_options_match_contract(self):
        self.assertEqual(
            mod.PINS["before"]["commit"], "d995c550152e734ce821eb3de93180b533aad2f7"
        )
        self.assertEqual(
            mod.PINS["before"]["tree"], "09a42bade3400f91836a59ba3104a3dd907704b3"
        )
        self.assertEqual(
            mod.PINS["before"]["lock"],
            "1679161e7f92fc40f8813cbbf1e9330dfa8796beefeb2317f815f31bf761b501",
        )
        self.assertEqual(
            mod.PINS["candidate"]["commit"], "09e95da3a2629693931eb3df70beccbb18d58368"
        )
        self.assertEqual(
            mod.PINS["candidate"]["tree"], "92de089b0330a034a404ed9af49e0c49bd76e240"
        )
        self.assertEqual(
            mod.PINS["candidate"]["lock"],
            "c1a1abdd274a633f7ff1f8e47eed70fdc05a032c745ba1645bf000df711b6103",
        )
        self.assertEqual(mod.OPTIONS["payload_bytes"], 65536)
        self.assertEqual(mod.OPTIONS["wal_segment_bytes"], 4124)
        self.assertEqual(mod.OPTIONS["inline_bytes"], 1024)
        self.assertEqual(mod.SHAPES, (("kv", 16), ("kv", 2048), ("sql", 16), ("sql", 2048)))
        self.assertEqual(mod.ROUNDS, 6)

    def test_summarize_excludes_warmup_and_flags_investigate_over_120(self):
        summary = mod.summarize(_sample_rows(after_commit_ns=150, before_commit_ns=100))
        self.assertEqual(summary["writer_processes"], 48)
        self.assertEqual(summary["reader_processes"], 96)
        self.assertTrue(summary["warmup_excluded_from_medians"])
        self.assertEqual(
            set(summary["commit_after_before_ratios"].values()), {1.5}
        )
        self.assertEqual(
            summary["investigate"], ["kv-16", "kv-2048", "sql-16", "sql-2048"]
        )
        for key, entry in summary["medians"].items():
            self.assertEqual(entry["commit_mad_ns"], 0)
            self.assertEqual(entry["reopen_ns"], 25.0)
            if key.startswith("candidate-"):
                self.assertEqual(entry["commit_ns"], 150)
            else:
                self.assertEqual(entry["commit_ns"], 100)

    def test_summarize_rejects_incomplete_duplicate_failed_and_unverified_rows(self):
        rows = _sample_rows()
        for damaged in (rows[:-1], rows[:-1] + [rows[0]]):
            with self.assertRaisesRegex(ValueError, "MEASURE_SUMMARY"):
                mod.summarize(damaged)
        for field, bad_value in (("status", "failed"), ("reopen_cycles", 1), ("reopen_ns", [20])):
            damaged = copy.deepcopy(rows)
            damaged[0][field] = bad_value
            with self.assertRaisesRegex(ValueError, "MEASURE_SUMMARY"):
                mod.summarize(damaged)

    def test_audit_accepts_canonical_report_and_rejects_tampering_or_leaks(self):
        report = _sample_report(after_commit_ns=125, before_commit_ns=100)
        raw = mod._canonical(report)
        parsed, sha = mod.audit_public_report(raw)
        self.assertEqual(sha, hashlib.sha256(raw).hexdigest())
        self.assertEqual(parsed["investigate"], ["kv-16", "kv-2048", "sql-16", "sql-2048"])

        # Non-canonical formatting rejected
        with self.assertRaisesRegex(ValueError, "AUDIT_PUBLIC"):
            mod.audit_public_report(json.dumps(report, indent=2).encode("utf-8"))

        # Tampered ratio rejected
        bad_ratio = copy.deepcopy(report)
        bad_ratio["commit_after_before_ratios"]["kv-16"] = 1.0
        with self.assertRaisesRegex(ValueError, "AUDIT_PUBLIC"):
            mod.audit_public_report(mod._canonical(bad_ratio))

        # Tampered investigate list rejected
        bad_inv = copy.deepcopy(report)
        bad_inv["investigate"] = []
        with self.assertRaisesRegex(ValueError, "AUDIT_PUBLIC"):
            mod.audit_public_report(mod._canonical(bad_inv))

        # Extra host/path key rejected
        extra = copy.deepcopy(report)
        extra["host_path"] = "/home/runner/work"
        with self.assertRaisesRegex(ValueError, "AUDIT_PUBLIC"):
            mod.audit_public_report(mod._canonical(extra))

        # Forbidden token in scope rejected
        leaked = copy.deepcopy(report)
        leaked["scope"] = "contains /tmp/secret path"
        with self.assertRaisesRegex(ValueError, "AUDIT_PUBLIC"):
            mod.audit_public_report(mod._canonical(leaked))

        # Mismatched parser digest between arms rejected
        diff_parser = copy.deepcopy(report)
        diff_parser["pins"]["candidate"]["parser_sha256"] = "9" * 64
        with self.assertRaisesRegex(ValueError, "AUDIT_PUBLIC"):
            mod.audit_public_report(mod._canonical(diff_parser))

    def test_checkpoint_budget_early_stop(self):
        mod.check_checkpoint_budget(100.0, 200.0, soft_budget_s=600.0, cleanup_margin_s=60.0)
        with self.assertRaisesRegex(ValueError, "TIME_BUDGET"):
            mod.check_checkpoint_budget(500.0, 100.0, soft_budget_s=600.0, cleanup_margin_s=60.0)

    def test_sizes_and_elf_checks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "overflow").mkdir()
            (root / "lsm.wal").write_bytes(b"wal")
            (root / "overflow" / "spool").write_bytes(b"spool")
            (root / "db.alopex").write_bytes(b"file")
            result = mod.sizes(root)
            self.assertEqual(
                (result["wal_bytes"], result["spool_bytes"], result["fixture_bytes"]),
                (3, 5, 12),
            )
            (root / "link").symlink_to(root / "lsm.wal")
            with self.assertRaisesRegex(ValueError, "MEASURE_VERIFY"):
                mod.sizes(root)

            elf = root / "valid.so"
            header = b"\x7fELF\x02\x01" + b"\x00" * 12 + b"\x3e\x00" + b"\x00" * 64
            elf.write_bytes(header)
            mod._check_elf_amd64(elf)

            bad_elf = root / "bad.so"
            bad_elf.write_bytes(b"not-elf" * 20)
            with self.assertRaisesRegex(ValueError, "BUILD_ELF"):
                mod._check_elf_amd64(bad_elf)

    def test_child_cli_write_verify_and_fault_hook_rejection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pkg = root / "pkg" / "alopex"
            pkg.mkdir(parents=True)
            ext = pkg / "_alopex.py"
            ext.write_text("# stub ordinary extension\n", encoding="utf-8")
            native_sha = hashlib.sha256(ext.read_bytes()).hexdigest()
            (pkg / "__init__.py").write_text(
                textwrap.dedent(
                    """\
                    import json
                    from pathlib import Path
                    from . import _alopex

                    class TxnMode:
                        READ_ONLY = "ro"
                        READ_WRITE = "rw"

                    class WalOverflowOptions:
                        def __init__(self, **kwargs):
                            self.kwargs = kwargs

                    class _Tx:
                        def __init__(self, db):
                            self.db = db
                            self.pending = {}
                        def put(self, k, v):
                            self.pending[k.decode()] = v.decode()
                        def get(self, k):
                            return self.db.data["kv"][k.decode()].encode()
                        def commit(self):
                            self.db.data["kv"].update(self.pending)
                            self.db.save()
                        def rollback(self):
                            pass

                    class Database:
                        def __init__(self, path):
                            self.path = Path(path)
                            self.dir = self.path.parent
                            if self.path.exists():
                                self.data = json.loads(self.path.read_text())
                            else:
                                self.data = {"kv": {}}
                        @classmethod
                        def open(cls, path, wal_overflow=None):
                            return cls(path)
                        def save(self):
                            self.path.write_text(json.dumps(self.data))
                            (self.dir / "lsm.wal").write_bytes(b"w" * 64)
                        def begin(self, mode):
                            return _Tx(self)
                        def wal_overflow_stats(self):
                            return {"gc_failures": 0}
                        def close(self):
                            pass
                    """
                ),
                encoding="utf-8",
            )
            fixture = root / "fixture"
            fixture.mkdir()
            env = {
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                "PYTHONPATH": str(root / "pkg"),
                "PYTHONDONTWRITEBYTECODE": "1",
            }
            script = str(PERF_DIR / "issue682_hosted_wal_cost.py")
            w = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    script,
                    "--child",
                    "write",
                    "--fixture",
                    str(fixture),
                    "--surface",
                    "kv",
                    "--size",
                    "16",
                    "--native-sha",
                    native_sha,
                ],
                env=env,
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            self.assertEqual(w.returncode, 0, w.stderr)
            wdata = json.loads(w.stdout)
            self.assertEqual(wdata["status"], "passed")
            self.assertEqual(wdata["wal_bytes"], 64)

            v = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    script,
                    "--child",
                    "verify",
                    "--fixture",
                    str(fixture),
                    "--surface",
                    "kv",
                    "--size",
                    "16",
                    "--native-sha",
                    native_sha,
                ],
                env=env,
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            self.assertEqual(v.returncode, 0, v.stderr)
            self.assertTrue(json.loads(v.stdout)["verified"])

            # Fault-hooked extension must be rejected.
            ext.write_text("_arm_wal_test_fault = lambda *a: None\n", encoding="utf-8")
            fault_sha = hashlib.sha256(ext.read_bytes()).hexdigest()
            bad = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    script,
                    "--child",
                    "verify",
                    "--fixture",
                    str(fixture),
                    "--surface",
                    "kv",
                    "--size",
                    "16",
                    "--native-sha",
                    fault_sha,
                ],
                env=env,
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            self.assertNotEqual(bad.returncode, 0)

    def test_all_imported_helper_attributes_exist(self):
        """Prevent missing-attribute bugs on reused #681 hosted helper modules."""
        import ast

        source_path = PERF_DIR / "issue682_hosted_wal_cost.py"
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        imported = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    name = alias.asname or alias.name
                    if name != "os":
                        imported[name] = getattr(mod, name)
        missing = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                alias = node.value.id
                if alias in imported and not hasattr(imported[alias], node.attr):
                    missing.append(f"{alias}.{node.attr}")
        self.assertEqual(missing, [])

    def test_parser_build_argv_matches_real_build_nim_parser_cli(self):
        """Verify parser build flags against the real build-nim-parser.sh script."""
        repo_root = PERF_DIR.parents[1]
        script = repo_root / "scripts/build-nim-parser.sh"
        help_run = subprocess.run(
            ["bash", str(script), "--help"],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        argv = mod._parser_build_argv(
            repo_root, Path("/tmp/archive"), Path("/tmp/lib/libalopex_sql_parser.so")
        )
        self.assertIn("--backend", argv)
        backend_val = argv[argv.index("--backend") + 1]
        self.assertEqual(backend_val, "host")
        self.assertIn("--backend auto|host|docker", help_run.stdout)
        for flag in ("--target", "--archive-dir", "--output"):
            self.assertIn(flag, argv)
            self.assertIn(flag, help_run.stdout)


if __name__ == "__main__":
    unittest.main()
