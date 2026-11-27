"""Tests for precondition-vs-SUT separated #681 hosted measurement."""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_acquire_source as acquire_source
import issue681_hosted_fixture_measure as mod
import issue681_public_admission as admission


def make_dir(path: Path) -> Path:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.chmod(0o700)
    return path


def make_pins(before_bin_sha: str, cand_bin_sha: str, bench_sha: str) -> dict:
    pins = {}
    for arm, bsha in (("before", before_bin_sha), ("candidate", cand_bin_sha)):
        src = acquire_source.PINS[arm]
        pins[arm] = {
            "source_commit": src["commit"],
            "source_tree": src["tree"],
            "binary_sha256": bsha,
            "bench_sha256": bench_sha,
            "parser_sha256": "1" * 64,
            "parser_record_sha256": "2" * 64,
            "cargo_lock_sha256": src["lock"],
            "profile": "release",
            "platform": "linux/amd64",
            "parser_contract": "0.28.0",
            "rust_version": "1.90.0",
            "version": "0.8.17",
        }
    return pins


def make_case_artifacts(public_dir: Path, private_dir: Path, *, pin: dict,
                        fixture_sha: str, rows: int, trial: int, finish: str,
                        arm: str) -> dict:
    phases_names = (*mod.SUT_PHASES, finish)
    phases = []
    audit_lines = []
    seq = 0
    for phase in phases_names:
        start_ev = {"phase": phase, "data": {"event": "start"}}
        audit_lines.append(mod._canonical({
            "sequence": seq, "event": start_ev, "observation": None,
        }))
        seq += 1
        base_ns = (50_000_000 if arm == "before" else 2_000_000) + trial * 100
        peak_rss = (1_600_000_000 if arm == "before" else 1_050_000_000) + rows
        end_data = {"event": "end"}
        if phase == "warm":
            end_data["graph_bytes"] = 64_000_000
            end_data["stats"] = {"hits": 1, "misses": 1}
            elapsed = None
        else:
            end_data["elapsed_ns"] = base_ns
            elapsed = base_ns
            if phase in ("cold_transaction_1", "cold_transaction_2",
                         "first_insert", "second_insert"):
                hits = 0 if arm == "before" else 1
                misses = 2 if arm == "before" else 1
                end_data["stats"] = {"hits": hits, "misses": misses}
        obs = {
            "phase": phase,
            "before_reset": {"rss_bytes": peak_rss - 1024, "high_water_bytes": peak_rss},
            "after_reset": {"rss_bytes": peak_rss - 1024, "high_water_bytes": peak_rss - 512},
            "after": {"rss_bytes": peak_rss - 1024, "high_water_bytes": peak_rss},
            "kernel_reported_peak_rss_bytes": peak_rss,
            "kernel_high_water_decreased": False,
            "aggregation": "maximum of reset and end kernel high-water readings",
            "scope": "child process between phase handshakes, including end-event serialization",
            "counter": "Linux VmHWM after clear_refs=5; kernel RSS accounting accuracy applies",
        }
        audit_lines.append(mod._canonical({
            "sequence": seq,
            "event": {"phase": phase, "data": end_data},
            "observation": obs,
        }))
        seq += 1
        stats = end_data.get("stats")
        phases.append({
            "phase": phase,
            "elapsed_ns": elapsed,
            "kernel_peak_rss_bytes": peak_rss,
            "cache_hits": None if stats is None else stats["hits"],
            "cache_misses": None if stats is None else stats["misses"],
            "graph_bytes": end_data.get("graph_bytes"),
        })
    comp_ev = {
        "phase": "complete",
        "data": {
            "finish": finish,
            "result_validated": True,
            "stats": {"hits": 1, "misses": 1},
        },
    }
    audit_lines.append(mod._canonical({
        "sequence": seq, "event": comp_ev, "observation": None,
    }))
    report = {
        "schema": mod.CASE_SCHEMA,
        "status": "measured",
        "source_commit": pin["source_commit"],
        "binary_sha256": pin["binary_sha256"],
        "bench_sha256": pin["bench_sha256"],
        "fixture_sha256": fixture_sha,
        "rows": rows,
        "dimensions": 384,
        "trial": trial,
        "finish": finish,
        "phases": phases,
        "result_validated": True,
        "rss_scope": mod.RSS_SCOPE,
    }
    report_raw = mod._canonical(report)
    audit_lines.append(mod._canonical({"receipt": {
        "schema": mod.AUDIT_SCHEMA,
        "source_commit": pin["source_commit"],
        "binary_sha256": pin["binary_sha256"],
        "bench_sha256": pin["bench_sha256"],
        "fixture_sha256": fixture_sha,
        "runner_sha256": hashlib.sha256(Path(mod.__file__).read_bytes()).hexdigest(),
        "rows": rows,
        "dimensions": 384,
        "trial": trial,
        "finish": finish,
        "child_exit": 0,
        "events": 15,
        "phases": 7,
        "public_sha256": hashlib.sha256(report_raw).hexdigest(),
    }}))
    mod._write_private(public_dir / "public.json", report_raw)
    mod._write_private(private_dir / "audit.jsonl", b"".join(audit_lines))
    return report


class RealLedgerAdmission:
    def __init__(self, profile, *, roots, log_dir):
        self.log_dir = log_dir
        self.log = None

    def _write(self, kind, rows=None, trial=None, arm=None):
        value = {
            "schema": admission.LEDGER_SCHEMA,
            "kind": kind,
            "rows": rows,
            "trial": trial,
            "arm": arm,
            "allocated_bytes": 12_000_000_000,
            "idle_window_seconds": 0 if kind == "case_complete" else admission.WINDOW_SECONDS,
            "min_idle_basis_points": None if kind == "case_complete" else 9_950,
            "status": "passed",
        }
        raw = mod._canonical(value)
        os.write(self.log, raw)
        os.fsync(self.log)

    def __enter__(self):
        self.log = os.open(self.log_dir / "admission.jsonl",
                           os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.fchmod(self.log, 0o600)
        self._write("preflight")
        return self

    def __exit__(self, *_):
        if self.log is not None:
            os.close(self.log)
            self.log = None
        return False

    def admit_stage(self, rows):
        self._write("stage", rows=rows)
        return True

    def admit_case(self, rows, trial, arm):
        self._write("case", rows=rows, trial=trial, arm=arm)
        return True

    def complete_case(self, rows, trial, arm):
        self._write("case_complete", rows=rows, trial=trial, arm=arm)
        return True


class HostedFixtureMeasureTests(unittest.TestCase):
    def test_both_finishes_prepare_once_per_scale_and_audit_all_60_sut_cases(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            workspace = make_dir(root / "checkout")
            runner_temp = make_dir(root / "runner-temp")
            home_base = make_dir(root / "home")
            before_bin = root / "before-bin"
            cand_bin = root / "cand-bin"
            before_bin.write_bytes(b"before-binary-bytes")
            cand_bin.write_bytes(b"candidate-binary-bytes")
            before_sha = hashlib.sha256(before_bin.read_bytes()).hexdigest()
            cand_sha = hashlib.sha256(cand_bin.read_bytes()).hexdigest()
            bench_sha = "3" * 64
            pins = make_pins(before_sha, cand_sha, bench_sha)
            guard = object.__new__(mod.capacity.HostStorageGuard)

            prepared_scales = []
            seen_db_copies = []

            def fake_acquire(ws, task_dir, home_dir, *, manifest, vendor_mode,
                             on_substage=None, stage_budget_seconds=None):
                self.assertEqual(vendor_mode, "cargo_vendor_locked")
                cargo = make_dir(home_dir / "cargo")
                return {
                    "guard": guard,
                    "task": task_dir,
                    "cargo_home": cargo,
                    "sources": {arm: make_dir(task_dir / "sources" / arm) for arm in mod.ARMS},
                    "parser": {"parser_library": root / "libalopex_sql_parser.so"},
                    "vendor": {},
                    "git_binary": Path("/usr/bin/git"),
                    "git_env": {},
                }

            def fake_build(acquired, ws, build_dir, *, on_substage=None):
                return {
                    "pins": pins,
                    "binaries": {"before": before_bin, "candidate": cand_bin},
                    "build_task": build_dir,
                    "parser_dir": root,
                    "guard": guard,
                }

            def fake_prepare(binary, fixture_dir, parser_dir, rows, *,
                             timeout, host_guard, diag=None):
                self.assertEqual(binary, before_bin)
                prepared_scales.append(rows)
                make_dir(fixture_dir)
                (fixture_dir / "sst-0001.dat").write_bytes(f"fixture-{rows}".encode())
                digest = mod.directory_digest(fixture_dir)
                return {
                    "rows": rows,
                    "dimensions": 384,
                    "builder_arm": "before",
                    "builder_commit": pins["before"]["source_commit"],
                    "seed_elapsed_ns": 1000 * rows,
                    "index_build_elapsed_ns": 5000 * rows,
                    "fixture_sha256": digest,
                }

            def fake_case_runner(**kwargs):
                arm = ("before"
                       if kwargs["binary"] == before_bin else "candidate")
                seen_db_copies.append(kwargs["db_dir"])
                # Mutate the trial copy to confirm fixture immutability is preserved.
                (kwargs["db_dir"] / "wal-trial.log").write_bytes(b"mutated")
                return make_case_artifacts(
                    kwargs["public_dir"], kwargs["private_dir"],
                    pin=pins[arm], fixture_sha=kwargs["fixture_sha256"],
                    rows=kwargs["rows"], trial=kwargs["trial"],
                    finish=kwargs["finish"], arm=arm)

            stdout = io.StringIO()
            env = {
                "HNSW_FINISH": "both",
                "RUNNER_TEMP": str(runner_temp),
                "GITHUB_WORKSPACE": str(workspace),
            }
            with mock.patch.object(mod.Path, "home", return_value=home_base), \
                 mock.patch.object(mod.qualify, "REPO", workspace), \
                 mock.patch.object(mod.scoped_storage, "make_manifest", return_value=object()), \
                 mock.patch.object(mod.identity_gate, "run",
                                   return_value=({"case": "HNSW_HOSTED_ADMISSION",
                                                  "outcome": "passed"}, 0)), \
                 mock.patch.object(mod.capacity.HostStorageGuard, "__init__", return_value=None), \
                 mock.patch.object(mod.capacity.HostStorageGuard, "__call__", return_value=None), \
                 mock.patch.object(mod.rust, "install_pinned_minimal"), \
                 mock.patch.object(mod.acquire, "acquire_all", side_effect=fake_acquire), \
                 mock.patch.object(mod, "build_fixture_pair", side_effect=fake_build), \
                 mock.patch.object(mod.public_admission, "Admission", RealLedgerAdmission), \
                 mock.patch.object(mod, "prepare_fixture", side_effect=fake_prepare), \
                 mock.patch.object(mod, "run_sut_case", side_effect=fake_case_runner), \
                 contextlib.redirect_stdout(stdout):
                rc = mod.run_hosted_fixture_cli(env)

            self.assertEqual(rc, 0, stdout.getvalue())
            # Precondition built ONCE per scale (3 total), NEVER 60 times!
            self.assertEqual(prepared_scales, [1_000, 10_000, 100_000])
            self.assertEqual(len(seen_db_copies), 60)
            for copy_dir in seen_db_copies:
                self.assertFalse(copy_dir.exists())

            lines = [json.loads(line) for line in stdout.getvalue().splitlines()]
            fixture_lines = [row for row in lines if row.get("substage") == "fixture_prepare"]
            case_lines = [row for row in lines if row.get("substage") == "case"]
            self.assertEqual([row["rows"] for row in fixture_lines], [1_000, 10_000, 100_000])
            self.assertEqual(len(case_lines), 60)
            final = lines[-1]
            self.assertEqual(final["outcome"], "passed")
            self.assertEqual(final["boundary"], mod.BOUNDARY_TAG)
            self.assertEqual(final["finishes"], ["commit", "rollback"])

            export_dir = runner_temp / "issue681-public-export"
            self.assertEqual(
                sorted(p.name for p in export_dir.iterdir()),
                ["issue681-public-commit.json", "issue681-public-rollback.json"],
            )
            for finish in ("commit", "rollback"):
                raw = (export_dir / f"issue681-public-{finish}.json").read_bytes()
                digest = hashlib.sha256(raw).hexdigest()
                self.assertEqual(final["public_sha256"][finish], digest)
                parsed = json.loads(raw)
                self.assertEqual(parsed["schema"], mod.SCHEMA)
                self.assertEqual(len(parsed["fixtures"]), 3)
                self.assertEqual(len(parsed["cases"]), 30)
                # 3 scales * (7 RSS + 6 timed phases) = 39 SUT summary entries
                self.assertEqual(len(parsed["summary"]), 39)
                self.assertTrue(all(
                    row["phase"] not in ("seed", "index_build")
                    for row in parsed["summary"]
                ))
                self.assertEqual(
                    mod.validate_public_report(
                        raw, expected_sha256=digest, expected_pins=pins,
                        expected_fixtures=parsed["fixtures"], finish=finish),
                    raw,
                )
            self.assertNotIn(str(root), stdout.getvalue())

    def test_fixture_mutation_fails_closed_with_immutability_code(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            workspace = make_dir(root / "checkout")
            measure_parent = make_dir(root / "measure")
            task = make_dir(root / "task")
            cargo = make_dir(root / "cargo")
            build_task = make_dir(root / "build")
            before_bin = root / "before-bin"
            cand_bin = root / "cand-bin"
            before_bin.write_bytes(b"before-bin")
            cand_bin.write_bytes(b"cand-bin")
            pins = make_pins(
                hashlib.sha256(before_bin.read_bytes()).hexdigest(),
                hashlib.sha256(cand_bin.read_bytes()).hexdigest(),
                "4" * 64,
            )
            guard = object.__new__(mod.capacity.HostStorageGuard)
            acquired = {"guard": guard, "task": task, "cargo_home": cargo}
            built = {
                "pins": pins,
                "binaries": {"before": before_bin, "candidate": cand_bin},
                "build_task": build_task,
                "parser_dir": root,
                "guard": guard,
            }
            profile = {
                "schema": admission.SCHEMA,
                "runner_kind": "github_hosted_ubuntu2404",
                "cpu_count": 4,
                "additional_bytes": 1_000_000_000,
            }
            fixture_ref = {}
            substages = []

            def fake_prepare(binary, fixture_dir, parser_dir, rows, *,
                             timeout, host_guard, diag=None):
                make_dir(fixture_dir)
                (fixture_dir / "data.sst").write_bytes(b"original")
                fixture_ref["dir"] = fixture_dir
                return {
                    "rows": rows,
                    "dimensions": 384,
                    "builder_arm": "before",
                    "builder_commit": pins["before"]["source_commit"],
                    "seed_elapsed_ns": 100,
                    "index_build_elapsed_ns": 200,
                    "fixture_sha256": mod.directory_digest(fixture_dir),
                }

            def fake_case_runner(**kwargs):
                arm = "before" if kwargs["binary"] == before_bin else "candidate"
                # Tamper with the shared fixture directory!
                (fixture_ref["dir"] / "data.sst").write_bytes(b"tampered")
                return make_case_artifacts(
                    kwargs["public_dir"], kwargs["private_dir"],
                    pin=pins[arm], fixture_sha=kwargs["fixture_sha256"],
                    rows=kwargs["rows"], trial=kwargs["trial"],
                    finish=kwargs["finish"], arm=arm)

            with mock.patch.object(mod.capacity.HostStorageGuard, "__call__", return_value=None), \
                 mock.patch.object(mod.public_admission, "Admission", RealLedgerAdmission):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_STAGE$"):
                    mod.measure_fixture_finishes(
                        acquired, built, workspace, measure_parent, profile,
                        ("commit",),
                        on_substage=lambda *args: substages.append(args),
                        case_runner=fake_case_runner,
                        fixture_preparer=fake_prepare)
            self.assertEqual(substages[-1][2], "MEASURE_FIXTURE_IMMUTABILITY")
            self.assertEqual(list(measure_parent.iterdir()), [])

    def test_case_failure_surfaces_allowlisted_error_code_and_cleans_up(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            workspace = make_dir(root / "checkout")
            measure_parent = make_dir(root / "measure")
            task = make_dir(root / "task")
            cargo = make_dir(root / "cargo")
            build_task = make_dir(root / "build")
            before_bin = root / "before-bin"
            cand_bin = root / "cand-bin"
            before_bin.write_bytes(b"before-bin")
            cand_bin.write_bytes(b"cand-bin")
            pins = make_pins(
                hashlib.sha256(before_bin.read_bytes()).hexdigest(),
                hashlib.sha256(cand_bin.read_bytes()).hexdigest(),
                "5" * 64,
            )
            guard = object.__new__(mod.capacity.HostStorageGuard)
            acquired = {"guard": guard, "task": task, "cargo_home": cargo}
            built = {
                "pins": pins,
                "binaries": {"before": before_bin, "candidate": cand_bin},
                "build_task": build_task,
                "parser_dir": root,
                "guard": guard,
            }
            profile = {
                "schema": admission.SCHEMA,
                "runner_kind": "github_hosted_ubuntu2404",
                "cpu_count": 4,
                "additional_bytes": 1_000_000_000,
            }
            substages = []

            def fake_prepare(binary, fixture_dir, parser_dir, rows, *,
                             timeout, host_guard, diag=None):
                make_dir(fixture_dir)
                (fixture_dir / "data.sst").write_bytes(b"ok")
                return {
                    "rows": rows,
                    "dimensions": 384,
                    "builder_arm": "before",
                    "builder_commit": pins["before"]["source_commit"],
                    "seed_elapsed_ns": 100,
                    "index_build_elapsed_ns": 200,
                    "fixture_sha256": mod.directory_digest(fixture_dir),
                }

            with mock.patch.object(mod.capacity.HostStorageGuard, "__call__", return_value=None), \
                 mock.patch.object(mod.public_admission, "Admission", RealLedgerAdmission):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_STAGE$"):
                    mod.measure_fixture_finishes(
                        acquired, built, workspace, measure_parent, profile,
                        ("commit",),
                        on_substage=lambda *args: substages.append(args),
                        case_runner=lambda **_: {
                            "status": "failed",
                            "error_code": "PUBLIC_COUNTER",
                            "completed_phases": 3,
                        },
                        fixture_preparer=fake_prepare)
            self.assertEqual(substages[-1][2], "MEASURE_CASE_PUBLIC_COUNTER")
            self.assertEqual(list(measure_parent.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
