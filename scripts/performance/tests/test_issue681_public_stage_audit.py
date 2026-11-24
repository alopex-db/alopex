"""Same-job stage audit joins all case outputs to their private observations."""

import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_public as public
import issue681_public_admission as admission
import issue681_public_pair as pair
import issue681_public_stage_audit as stage
from test_issue681_public import Counter, events
from test_issue681_public_pair import PINS


class PublicStageAuditTests(unittest.TestCase):
    def test_thirty_cases_are_rejoined_and_one_changed_audit_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pins = copy.deepcopy(PINS)
            binaries = {}
            for arm in pair.ARMS:
                binary = root / (arm + "-fixture")
                lines = [json.dumps(item, separators=(",", ":"))
                         for item in events()]
                binary.write_text(
                    f"#!{sys.executable}\nimport sys\n# {arm}\nlines={lines!r}\n"
                    "for line in lines:\n print(line,flush=True)\n"
                    " if sys.stdin.readline().strip() != 'continue': sys.exit(9)\n")
                binary.chmod(0o700)
                pins[arm]["binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
                binaries[arm] = binary
            reports = []
            locations = []
            with mock.patch.object(public.sys, "platform", "linux"), \
                    mock.patch.object(public, "_inventory"):
                for position, (rows, trial, arm) in enumerate(pair._order()):
                    public_dir = root / f"public-{position}"
                    private_dir = root / f"private-{position}"
                    public_dir.mkdir(mode=0o700)
                    private_dir.mkdir(mode=0o700)
                    report = public.run_case(
                        binary=binaries[arm], directory=public_dir,
                        private_dir=private_dir,
                        source_commit=pins[arm]["source_commit"],
                        binary_sha256=pins[arm]["binary_sha256"],
                        rows=rows, dimensions=384, trial=trial,
                        finish="rollback", timeout=10,
                        rss_limit=8_000_000_000, counter_factory=Counter)
                    self.assertEqual(report["status"], "measured")
                    reports.append(report)
                    locations.append({"arm": arm, "rows": rows, "trial": trial,
                                      "public": public_dir, "private": private_dir})
            assembled = pair.assemble(reports, pins, "rollback")
            admission_dir = root / "admission"
            admission_dir.mkdir(mode=0o700)
            schedule = [("preflight", None, None, None)]
            last_rows = None
            for rows, trial, arm in pair._order():
                if rows != last_rows:
                    schedule.append(("stage", rows, None, None))
                    last_rows = rows
                schedule.append(("case", rows, trial, arm))
                schedule.append(("case_complete", rows, trial, arm))
            ledger = admission_dir / "admission.jsonl"
            with ledger.open("wb") as stream:
                for kind, rows, trial, arm in schedule:
                    stream.write(public._canonical({
                        "schema": admission.LEDGER_SCHEMA,
                        "kind": kind, "rows": rows, "trial": trial,
                        "arm": arm, "allocated_bytes": 1_000,
                        "idle_window_seconds": 0 if kind == "case_complete" else 3,
                        "min_idle_basis_points": None if kind == "case_complete" else 10_000,
                        "status": "passed"}))
            ledger.chmod(0o600)
            def admit_asset(root, pin, *, private_roots):
                arm = next(arm for arm in pair.ARMS if pins[arm] == pin)
                return {"binary": binaries[arm]}
            asset_locations = {arm: root / arm for arm in pair.ARMS}
            with mock.patch.object(stage.assets, "verify", side_effect=admit_asset):
                self.assertTrue(stage.audit_stage(
                    assembled, case_locations=locations,
                    asset_locations=asset_locations, pins=pins,
                    finish="rollback", private_roots=(str(root),),
                    admission_dir=admission_dir,
                    additional_bytes=1_000_000_000))
                export = root / "export"
                export.mkdir(mode=0o700)
                def independent_audit(value):
                    return stage.audit_stage(
                        value, case_locations=locations,
                        asset_locations=asset_locations, pins=pins,
                        finish="rollback", private_roots=(str(root),),
                        admission_dir=admission_dir,
                        additional_bytes=1_000_000_000)
                pair.write(reports, pins, export, finish="rollback",
                           independent_audit=independent_audit)
                self.assertEqual(set(export.iterdir()), {export / "public.json"})
                ledger_raw = ledger.read_bytes()
                ledger.write_bytes(ledger_raw + b"private extra\n")
                with self.assertRaisesRegex(ValueError, "^STAGE_AUDIT$"):
                    independent_audit(assembled)
                ledger.write_bytes(ledger_raw)
                with (locations[5]["private"] / "audit.jsonl").open("ab") as stream:
                    stream.write(b"private tamper\n")
                with self.assertRaisesRegex(ValueError, "^STAGE_AUDIT$"):
                    stage.audit_stage(
                        assembled, case_locations=locations,
                        asset_locations=asset_locations, pins=pins,
                        finish="rollback", private_roots=(str(root),),
                        admission_dir=admission_dir,
                        additional_bytes=1_000_000_000)
                with self.assertRaisesRegex(ValueError, "^PUBLIC_PAIR$"):
                    pair.verify(export, pins=pins,
                                independent_audit=independent_audit)


if __name__ == "__main__":
    unittest.main()
